//! Single-consumer row transitions and read-only replay.
#![expect(clippy::unwrap_used, reason = "Fixture failures must abort the test.")]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::compiler::vm::refusal::ArithmeticOp;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ConsumerMatch, DefaultIds, DispatchError, EvaluationFailure,
    EvaluationRefusal, NoCheckpoint, SubscriptionEngine, SubscriptionRequest, SubscriptionScope,
    TableId, UnansweredCell,
};

const DDL: &str =
    "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT, b INT); CREATE TABLE s (id INT PRIMARY KEY);";
type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;
type Match = ConsumerMatch<DefaultIds, NoCheckpoint>;

fn build(specs: &[(u64, &str)]) -> (Engine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id::<Postgres, _>(&db, "t").unwrap();
    let mut engine = Engine::new(db, PostgreSqlDialect {});
    for &(consumer, sql) in specs {
        engine.register_select(consumer, sql).unwrap();
    }
    (engine, table)
}

fn row(a: Value<Postgres>, b: i64) -> Vec<Value<Postgres>> {
    vec![Value::Int(1), a, Value::Int(b)]
}

const fn flags(answer: &Match) -> (bool, bool, bool) {
    (answer.inserted(), answer.deleted(), answer.updated())
}

// Query first so live dispatch still observes any state a replay must leave untouched.
fn parity(engine: &mut Engine, event: &TestEvent<Postgres>, consumer: u64) -> Match {
    let selected = engine.matches_consumer(event, consumer).unwrap();
    let whole = engine.consumers(event).unwrap();
    assert_eq!(
        flags(&selected),
        (
            whole.inserted().contains(&consumer),
            whole.deleted().contains(&consumer),
            whole.updated().contains(&consumer)
        )
    );
    assert_eq!(
        selected.unanswered().iter().collect::<Vec<_>>(),
        whole
            .unanswered()
            .iter()
            .filter(|item| item.consumer_id == consumer)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        selected.evaluation_failures().iter().collect::<Vec<_>>(),
        whole
            .evaluation_failures()
            .iter()
            .filter(|item| item.consumer_id == consumer)
            .collect::<Vec<_>>()
    );
    assert_eq!(selected.checkpoint(), whole.checkpoint());
    selected
}

#[test]
fn diagnostics_preserve_other_matched_views_of_the_selected_consumer() {
    let (mut engine, table) = build(&[
        (1, "SELECT * FROM t"),
        (2, "SELECT * FROM t WHERE a + 1 > 0"),
    ]);
    let subscription_id = engine
        .register_select(1, "SELECT * FROM t WHERE a + 1 > 0")
        .unwrap()
        .subscription_id;
    for (a, refused) in [(Value::Int(i64::MAX), true), (Value::Missing, false)] {
        let selected = parity(&mut engine, &TestEvent::insert(table, row(a, 0)), 1);
        assert_eq!(flags(&selected), (true, false, false));
        if refused {
            assert_eq!(
                selected.evaluation_failures(),
                [EvaluationFailure {
                    subscription_id,
                    consumer_id: 1,
                    refusal: EvaluationRefusal::IntegerOverflow {
                        operation: ArithmeticOp::Add
                    },
                }]
            );
            assert_eq!(selected.unanswered(), []);
        } else {
            assert_eq!(
                selected.unanswered(),
                [UnansweredCell {
                    subscription_id,
                    consumer_id: 1,
                    column: 1,
                }]
            );
            assert_eq!(selected.evaluation_failures(), []);
        }
    }
}

#[test]
fn insert_and_delete_match_the_selected_consumers_filter() {
    let (mut engine, table) = build(&[
        (1, "SELECT * FROM t WHERE a > 0"),
        (2, "SELECT * FROM t WHERE a > 100"),
    ]);
    for (event, expected) in [
        (
            TestEvent::insert(table, row(Value::Int(5), 0)),
            (true, false, false),
        ),
        (
            TestEvent::delete(table, row(Value::Int(5), 0)),
            (false, true, false),
        ),
    ] {
        assert_eq!(flags(&parity(&mut engine, &event, 1)), expected);
        assert_eq!(
            flags(&parity(&mut engine, &event, 2)),
            (false, false, false)
        );
    }
}

#[test]
fn update_splits_all_four_view_transitions() {
    let (mut engine, table) = build(&[(1, "SELECT * FROM t WHERE a > 0")]);
    for (old, new, expected) in [
        (0, 5, (true, false, false)),
        (5, 0, (false, true, false)),
        (5, 6, (false, false, true)),
        (0, -1, (false, false, false)),
    ] {
        let event = TestEvent::update(table, row(Value::Int(old), 1), row(Value::Int(new), 1))
            .with_changed_columns([1u16]);
        assert_eq!(flags(&parity(&mut engine, &event, 1)), expected);
    }
}

#[test]
fn distinct_predicates_can_report_all_three_transitions_for_one_consumer() {
    let (mut engine, table) = build(&[
        (1, "SELECT * FROM t WHERE a > 0"),
        (1, "SELECT * FROM t WHERE b > 0"),
        (1, "SELECT * FROM t"),
    ]);
    let event = TestEvent::update(table, row(Value::Int(0), 5), row(Value::Int(5), 0))
        .with_changed_columns([1u16, 2u16]);
    assert_eq!(flags(&parity(&mut engine, &event, 1)), (true, true, true));
}

#[test]
fn subsets_suppress_unchanged_cells_and_require_old_bag_values() {
    for (sql, expected, missing) in [
        (
            "SELECT id, a FROM t WHERE b > 0",
            (false, false, true),
            false,
        ),
        ("SELECT a FROM t WHERE b > 0", (false, false, false), true),
    ] {
        let (mut engine, table) = build(&[]);
        let subscription_id = engine.register_select(1, sql).unwrap().subscription_id;
        let event = TestEvent::update(table, row(Value::Missing, 5), row(Value::Int(7), 5))
            .with_changed_columns([1u16]);
        let selected = parity(&mut engine, &event, 1);
        assert_eq!(flags(&selected), expected);
        let gap = UnansweredCell {
            subscription_id,
            consumer_id: 1,
            column: 1,
        };
        assert_eq!(
            selected.unanswered(),
            if missing {
                core::slice::from_ref(&gap)
            } else {
                &[]
            }
        );
        let event = TestEvent::delete(table, row(Value::Missing, 5));
        let selected = parity(&mut engine, &event, 1);
        assert_eq!(selected.deleted(), !missing);
        assert_eq!(
            selected.unanswered(),
            if missing {
                core::slice::from_ref(&gap)
            } else {
                &[]
            }
        );
    }
    let (mut engine, table) = build(&[(1, "SELECT id, a FROM t WHERE a > 0")]);
    let event = TestEvent::update(table, row(Value::Int(5), 1), row(Value::Int(5), 2))
        .with_changed_columns([2u16]);
    assert_eq!(
        flags(&parity(&mut engine, &event, 1)),
        (false, false, false)
    );
}

#[test]
fn missing_predicate_cells_are_admitted_and_null_is_answered() {
    let (mut engine, table) = build(&[]);
    let subscription_id = engine
        .register_select(1, "SELECT * FROM t WHERE a > 0")
        .unwrap()
        .subscription_id;
    for event in [
        TestEvent::insert(table, row(Value::Missing, 0)),
        TestEvent::delete(table, row(Value::Missing, 0)),
    ] {
        let selected = parity(&mut engine, &event, 1);
        assert_eq!(flags(&selected), (false, false, false));
        assert_eq!(
            selected.unanswered(),
            [UnansweredCell {
                subscription_id,
                consumer_id: 1,
                column: 1
            }]
        );
    }
    let selected = parity(
        &mut engine,
        &TestEvent::insert(table, row(Value::Null, 0)),
        1,
    );
    assert_eq!(flags(&selected), (false, false, false));
    assert_eq!(selected.unanswered(), []);
}

#[test]
fn update_preserves_missing_cell_and_refusal_precedence() {
    for (old, new, failures, unanswered) in [
        (Value::Int(1), Value::Missing, 0, true),
        (Value::Missing, Value::Int(1), 0, true),
        (Value::Missing, Value::Missing, 0, true),
        (Value::Int(i64::MAX), Value::Missing, 1, true),
        (Value::Missing, Value::Int(i64::MAX), 1, false),
        (Value::Int(i64::MAX), Value::Int(i64::MAX), 2, false),
    ] {
        let (mut engine, table) = build(&[]);
        let subscription_id = engine
            .register_select(1, "SELECT * FROM t WHERE a + 1 > 0")
            .unwrap()
            .subscription_id;
        let event = TestEvent::update(table, row(old, 0), row(new, 0)).with_changed_columns([1u16]);
        let selected = parity(&mut engine, &event, 1);
        assert_eq!(flags(&selected), (false, false, false));
        let refusal = EvaluationFailure {
            subscription_id,
            consumer_id: 1,
            refusal: EvaluationRefusal::IntegerOverflow {
                operation: ArithmeticOp::Add,
            },
        };
        assert_eq!(
            selected.evaluation_failures().iter().collect::<Vec<_>>(),
            vec![&refusal; failures]
        );
        let gap = UnansweredCell {
            subscription_id,
            consumer_id: 1,
            column: 1,
        };
        assert_eq!(
            selected.unanswered(),
            if unanswered {
                core::slice::from_ref(&gap)
            } else {
                &[]
            }
        );
    }
}

#[test]
fn diagnostics_name_every_selected_binding_and_no_neighbours() {
    let (mut engine, table) = build(&[(2, "SELECT * FROM t WHERE a > 0")]);
    let mut ids = Vec::new();
    for scope in [SubscriptionScope::Durable, SubscriptionScope::Session(9)] {
        ids.push(
            engine
                .register(SubscriptionRequest::new(1, "SELECT * FROM t WHERE a > 0").scope(scope))
                .unwrap()
                .subscription_id,
        );
    }
    let selected = parity(
        &mut engine,
        &TestEvent::insert(table, row(Value::Missing, 0)),
        1,
    );
    assert_eq!(
        selected.unanswered(),
        ids.iter()
            .map(|&subscription_id| UnansweredCell {
                subscription_id,
                consumer_id: 1,
                column: 1
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        flags(&parity(
            &mut engine,
            &TestEvent::insert(table, row(Value::Int(5), 0)),
            1
        )),
        (true, false, false)
    );
    engine.unregister_session(9);
    assert_eq!(
        flags(&parity(
            &mut engine,
            &TestEvent::insert(table, row(Value::Int(5), 0)),
            1
        )),
        (true, false, false)
    );
    assert!(engine.unregister_subscription(ids[0]));
    engine
        .register_select(3, "SELECT * FROM t WHERE b > 0")
        .unwrap();
    let event = TestEvent::insert(table, row(Value::Int(5), 0));
    assert_eq!(
        flags(&parity(&mut engine, &event, 3)),
        (false, false, false)
    );
    assert!(parity(&mut engine, &event, 2).inserted());
    let event = TestEvent::insert(table, row(Value::Int(5), 5));
    assert_eq!(
        flags(&parity(&mut engine, &event, 1)),
        (false, false, false)
    );
    assert_eq!(flags(&parity(&mut engine, &event, 3)), (true, false, false));
}

#[test]
fn truncate_targets_rows_and_known_tables_without_bindings_are_empty() {
    let (mut engine, table) = build(&[
        (1, "SELECT * FROM t WHERE a > 0"),
        (2, "SELECT COUNT(*) FROM t"),
    ]);
    let event = TestEvent::truncate(table);
    assert_eq!(flags(&parity(&mut engine, &event, 1)), (false, true, false));
    assert_eq!(
        flags(&parity(&mut engine, &event, 2)),
        (false, false, false)
    );
    let sibling = catalog_helpers::table_id::<Postgres, _>(engine.database(), "s").unwrap();
    assert_eq!(
        flags(&parity(
            &mut engine,
            &TestEvent::insert(sibling, vec![Value::Int(1)]),
            1
        )),
        (false, false, false)
    );
    let event = TestEvent::insert(99, vec![Value::Int(1)]);
    assert!(matches!(
        engine.matches_consumer(&event, 1),
        Err(DispatchError::UnknownTableId(99))
    ));
}

#[test]
fn positioned_events_keep_the_checkpoint_for_matched_and_absent_consumers() {
    use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};
    use subql::{PgChangeEvent, PgCommitPosition, PgLsn, PgXid};
    let db =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE items (id INT PRIMARY KEY);").unwrap();
    let mut engine: SubscriptionEngine<PgChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    engine
        .register_select(1, "SELECT * FROM items WHERE id > 0")
        .unwrap();
    let mut data = RowData::with_capacity(1);
    data.push("id".into(), ColumnValue::text("7"));
    let position = PgCommitPosition::new(PgLsn(0x100), PgXid(1), 1);
    let event = PgChangeEvent::new(
        ChangeEvent::insert("public", "items", 1, data, Lsn::new(0x100)),
        position,
    );
    for (consumer, inserted) in [(1, true), (99, false)] {
        let selected = engine.matches_consumer(&event, consumer).unwrap();
        assert_eq!(selected.inserted(), inserted);
        assert_eq!(selected.checkpoint(), Some(&position));
    }
}

#[cfg(feature = "membership-term")]
mod membership {
    use super::*;
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::ConfidenceLevel;
    const DDL: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY); CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id)); CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, qty BIGINT);";
    const TERM: &str = "SELECT * FROM docs WHERE project_id IN (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";

    fn members(sql: &str) -> (Engine, TableId, TableId, Vec<u64>) {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let docs = catalog_helpers::table_id::<Postgres, _>(&db, "docs").unwrap();
        let membership = catalog_helpers::table_id::<Postgres, _>(&db, "project_members").unwrap();
        let mut engine = Engine::new(db, PostgreSqlDialect {}).with_translator(
            TranslatorBuilder::new()
                .with_min_confidence(ConfidenceLevel::B)
                .build(),
        );
        let mut ids = Vec::new();
        for (consumer, user, project) in [(1, "alice", 7), (2, "bob", 9)] {
            ids.push(
                engine
                    .register(
                        SubscriptionRequest::new(consumer, sql)
                            .subscriber(Value::String(user.into()))
                            .term_values(
                                vec!["project_id"],
                                vec![(Value::String(user.into()), vec![Value::Int(project)])],
                            ),
                    )
                    .unwrap()
                    .subscription_id,
            );
        }
        (engine, docs, membership, ids)
    }

    #[test]
    fn replay_does_not_move_memberships_and_queries_use_live_admissions() {
        let (mut engine, docs, membership, _) = members(TERM);
        let admission = TestEvent::insert(
            membership,
            vec![Value::Int(9), Value::String("alice".into())],
        );
        let probe = TestEvent::insert(docs, row(Value::Int(9), 3));
        assert!(!engine.matches_consumer(&admission, 1).unwrap().inserted());
        assert!(!engine.matches_consumer(&probe, 1).unwrap().inserted());
        let _ = engine.consumers(&admission).unwrap();
        assert!(engine.matches_consumer(&probe, 1).unwrap().inserted());
        let departure = TestEvent::update(docs, row(Value::Int(9), 3), row(Value::Int(11), 3))
            .with_changed_columns([1u16]);
        assert_eq!(
            flags(&parity(&mut engine, &departure, 1)),
            (false, true, false)
        );
    }

    #[test]
    fn term_short_circuit_is_specific_to_the_selected_consumer() {
        let sql = format!("{TERM} OR qty + 9223372036854775807 > 0");
        let (mut engine, docs, _, ids) = members(&sql);
        let event = TestEvent::insert(docs, row(Value::Int(7), 1));
        let selected = parity(&mut engine, &event, 1);
        assert!(selected.inserted());
        assert_eq!(selected.evaluation_failures(), []);
        let selected = parity(&mut engine, &event, 2);
        assert!(!selected.inserted());
        assert_eq!(
            selected.evaluation_failures(),
            [EvaluationFailure {
                subscription_id: ids[1],
                consumer_id: 2,
                refusal: EvaluationRefusal::IntegerOverflow {
                    operation: ArithmeticOp::Add
                }
            }]
        );
    }

    #[test]
    fn missing_term_cells_are_reported_and_null_admits_nobody() {
        let (mut engine, docs, _, ids) = members(TERM);
        let selected = parity(
            &mut engine,
            &TestEvent::insert(docs, row(Value::Missing, 3)),
            1,
        );
        assert_eq!(
            selected.unanswered(),
            [UnansweredCell {
                subscription_id: ids[0],
                consumer_id: 1,
                column: 1
            }]
        );
        let selected = parity(
            &mut engine,
            &TestEvent::insert(docs, row(Value::Null, 3)),
            1,
        );
        assert!(!selected.inserted());
        assert_eq!(selected.unanswered(), []);
    }

    #[test]
    fn caller_terms_split_an_owner_change_between_consumers() {
        let (mut engine, _, membership, _) = members(TERM);
        let sql =
            "SELECT * FROM project_members WHERE user_id = current_setting('app.user_id', true)";
        for (consumer, user) in [(1, "alice"), (2, "bob")] {
            engine
                .register(
                    SubscriptionRequest::new(consumer, sql).subscriber(Value::String(user.into())),
                )
                .unwrap();
        }
        let event = TestEvent::update(
            membership,
            vec![Value::Int(7), Value::String("alice".into())],
            vec![Value::Int(7), Value::String("bob".into())],
        )
        .with_changed_columns([1u16]);
        assert_eq!(flags(&parity(&mut engine, &event, 1)), (false, true, false));
        assert_eq!(flags(&parity(&mut engine, &event, 2)), (true, false, false));
    }
}
