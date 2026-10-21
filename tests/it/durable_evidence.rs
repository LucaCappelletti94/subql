use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DurableAuthorizationError, DurableContext, DurableEngine,
    DurablePersistError, PgLsn, ReplayTable, SubscriptionEngine, SubscriptionRequest,
};

type Event = TestEvent<Postgres, PgLsn>;
type Engine = DurableEngine<Event, DefaultIds, ParserDB>;

fn compiler() -> (
    SubscriptionEngine<Event, DefaultIds, ParserDB>,
    subql::TableId,
) {
    let db =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE items (id INT PRIMARY KEY, cost INT);")
            .expect("catalog");
    let table = catalog_helpers::table_id::<Postgres, _>(&db, "items").expect("items");
    (SubscriptionEngine::new(db, PostgreSqlDialect {}), table)
}

fn context() -> DurableContext {
    DurableContext {
        source: b"source-generation-1".to_vec(),
        authority: b"policy-1".to_vec(),
    }
}

fn engine() -> (Engine, subql::TableId) {
    let (compiler, table) = compiler();
    (
        DurableEngine::new(compiler, context(), PgLsn(1)).expect("durable engine"),
        table,
    )
}

#[test]
fn separate_compensating_commits_survive_checkpoint_restore() {
    let (mut engine, table) = engine();
    engine
        .prepare_register(
            42,
            SubscriptionRequest::new(7, "SELECT COUNT(*) FROM items"),
            PgLsn(1),
            vec![ReplayTable {
                table_id: table,
                rows: vec![vec![Value::Int(1), Value::Int(5)]],
            }],
            1_000_000,
            |_| Ok(()),
        )
        .expect("admission")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist admission");
    let bytes = engine.checkpoint().expect("checkpoint");
    let revision = engine.revision();
    drop(engine);
    let (compiler, _) = compiler();
    let mut restored = Engine::restore(compiler, context(), revision, PgLsn(1), &bytes, |_| Ok(()))
        .expect("restore");
    for (after, at, event) in [
        (
            1,
            2,
            Event::insert(table, vec![Value::Int(2), Value::Int(4)]),
        ),
        (
            2,
            3,
            Event::delete(table, vec![Value::Int(2), Value::Int(4)]),
        ),
    ] {
        restored
            .prepare_transaction(
                b"source-generation-1",
                &PgLsn(after),
                PgLsn(at),
                &[event.with_checkpoint(PgLsn(at))],
            )
            .expect("prepare commit")
            .expect("new commit")
            .persist(|_| Ok::<_, ()>(()))
            .expect("persist commit");
    }
    assert_eq!(restored.unread(42), Some(&PgLsn(3)));
    restored
        .prepare_acknowledge(42, &PgLsn(2), |_| Ok(()))
        .expect("older catch-up")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist acknowledgement");
    assert_eq!(restored.unread(42), Some(&PgLsn(3)));
    restored
        .prepare_acknowledge(42, &PgLsn(3), |_| Ok(()))
        .expect("catch-up")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist acknowledgement");
    assert_eq!(restored.unread(42), None);
}

fn register(engine: &mut Engine, table: subql::TableId, quota: usize) {
    engine
        .prepare_register(
            42,
            SubscriptionRequest::new(7, "SELECT COUNT(*) FROM items"),
            PgLsn(1),
            vec![ReplayTable {
                table_id: table,
                rows: vec![vec![Value::Int(1), Value::Int(5)]],
            }],
            quota,
            |_| Ok(()),
        )
        .expect("admission")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist admission");
}

#[test]
fn failed_persistence_and_dropped_preparation_leave_checkpoint_retryable() {
    let (mut engine, table) = engine();
    register(&mut engine, table, 1_000_000);
    let revision = engine.revision();
    let events =
        [Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2))];
    let prepare = |engine: &mut Engine| {
        engine
            .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
            .expect("prepare")
            .expect("new commit")
            .persist(|receipt| {
                assert_eq!(receipt.changed[0].instance, 42);
                Err("storage unavailable")
            })
            .map_err(|DurablePersistError::Callback { error, .. }| error)
    };
    assert_eq!(prepare(&mut engine), Err("storage unavailable"));
    assert_eq!(engine.revision(), revision);
    assert_eq!(engine.checkpoint_position(), &PgLsn(1));
    assert_eq!(engine.unread(42), None);
    drop(
        engine
            .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
            .expect("prepare")
            .expect("new commit"),
    );
    assert_eq!(engine.revision(), revision);
    engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
        .expect("retry")
        .expect("new commit")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist retry");
    assert_eq!(engine.unread(42), Some(&PgLsn(2)));
    assert!(engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
        .expect("duplicate")
        .is_none());
    assert_eq!(engine.revision(), revision + 1);
}

#[test]
fn cancellation_inside_one_commit_and_unauthorized_acknowledgement_preserve_evidence() {
    let (mut engine, table) = engine();
    register(&mut engine, table, 1_000_000);
    let row = vec![Value::Int(2), Value::Int(4)];
    let events = [
        Event::insert(table, row.clone()).with_checkpoint(PgLsn(2)),
        Event::delete(table, row).with_checkpoint(PgLsn(2)),
    ];
    engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
        .expect("prepare")
        .expect("new commit")
        .persist(|receipt| {
            assert!(receipt.changed.is_empty());
            Ok::<_, ()>(())
        })
        .expect("persist");
    assert_eq!(engine.unread(42), None);
    let events =
        [Event::insert(table, vec![Value::Int(3), Value::Int(9)]).with_checkpoint(PgLsn(3))];
    engine
        .prepare_transaction(b"source-generation-1", &PgLsn(2), PgLsn(3), &events)
        .expect("prepare")
        .expect("new commit")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist");
    assert!(engine
        .prepare_acknowledge(42, &PgLsn(3), |_| Err(DurableAuthorizationError::Denied))
        .is_err());
    assert_eq!(engine.unread(42), Some(&PgLsn(3)));
    assert!(engine
        .prepare_acknowledge(42, &PgLsn(4), |_| Ok(()))
        .is_err());
    assert_eq!(engine.unread(42), Some(&PgLsn(3)));
}

#[test]
fn recovery_requires_the_committed_receipt_source_and_fresh_authority() {
    let (mut engine, table) = engine();
    register(&mut engine, table, 1_000_000);
    let bytes = engine.checkpoint().expect("checkpoint");
    let revision = engine.revision();
    let restore = |context, revision, at, bytes: &[u8], authorize: bool| {
        let (compiler, _) = compiler();
        Engine::restore(compiler, context, revision, at, bytes, |_| {
            if authorize {
                Ok(())
            } else {
                Err(DurableAuthorizationError::Denied)
            }
        })
    };
    assert!(restore(context(), revision, PgLsn(1), &[], true).is_err());
    assert!(restore(context(), revision + 1, PgLsn(1), &bytes, true).is_err());
    assert!(restore(context(), revision, PgLsn(2), &bytes, true).is_err());
    assert!(restore(context(), revision, PgLsn(1), &bytes, false).is_err());
    let mut wrong_source = context();
    wrong_source.source = b"other-generation".to_vec();
    assert!(restore(wrong_source, revision, PgLsn(1), &bytes, true).is_err());
    let mut wrong_authority = context();
    wrong_authority.authority = b"other-policy".to_vec();
    assert!(restore(wrong_authority, revision, PgLsn(1), &bytes, true).is_err());
    let mut trailing = bytes;
    trailing.push(0);
    assert!(restore(context(), revision, PgLsn(1), &trailing, true).is_err());
    assert_eq!(engine.unread(42), None);
    assert_eq!(engine.checkpoint_position(), &PgLsn(1));
}

#[test]
fn history_gaps_wrong_sources_and_mixed_commit_events_cannot_advance_progress() {
    let (mut engine, table) = engine();
    register(&mut engine, table, 1_000_000);
    let event = Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2));
    assert!(engine
        .prepare_transaction(b"source-generation-1", &PgLsn(0), PgLsn(2), &[event])
        .is_err());
    let event = Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2));
    assert!(engine
        .prepare_transaction(b"other-source", &PgLsn(1), PgLsn(2), &[event])
        .is_err());
    let event = Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(3));
    assert!(engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &[event])
        .is_err());
    assert_eq!(engine.checkpoint_position(), &PgLsn(1));
    assert_eq!(engine.unread(42), None);
}

#[tokio::test]
async fn cancelled_async_persistence_does_not_publish_staged_state() {
    let (mut engine, table) = engine();
    register(&mut engine, table, 1_000_000);
    let events =
        [Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2))];
    let prepared = engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
        .expect("prepare")
        .expect("new commit");
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(10),
        prepared.persist_async(async |_| { core::future::pending::<Result<(), ()>>().await }),
    )
    .await
    .is_err());
    assert_eq!(engine.checkpoint_position(), &PgLsn(1));
    assert_eq!(engine.unread(42), None);
}

#[test]
fn row_ordinals_share_a_complete_postgres_commit_boundary() {
    use subql::{Checkpoint, PgCommitPosition, PgXid};

    let commit = PgCommitPosition::at_commit(PgLsn(100), PgXid(7));
    for ordinal in [1, 2, 90] {
        let row = PgCommitPosition::new(PgLsn(100), PgXid(7), ordinal);
        assert_eq!(row.transaction_boundary(), Some(commit));
        assert!(row < commit);
    }
    let resume = PgCommitPosition::before_commit(PgLsn(100));
    assert_eq!(resume.transaction_boundary(), Some(resume));
    assert_eq!(subql::NoCheckpoint.transaction_boundary(), None);
}

#[test]
fn isolated_capacity_failure_preserves_unread_until_failure_persistence_succeeds() {
    let (mut measured, table) = engine();
    register(&mut measured, table, 1_000_000);
    let quota = measured
        .state_bytes(42)
        .expect("state size")
        .expect("instance")
        + 96;
    let (mut engine, table) = engine();
    register(&mut engine, table, quota);
    engine
        .prepare_register(
            43,
            SubscriptionRequest::new(8, "SELECT COUNT(*) FROM items"),
            PgLsn(1),
            vec![ReplayTable {
                table_id: table,
                rows: vec![vec![Value::Int(1), Value::Int(5)]],
            }],
            1_000_000,
            |_| Ok(()),
        )
        .expect("unrelated admission")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist admission");
    let events =
        [Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2))];
    engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
        .expect("prepare")
        .expect("new commit")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist");
    assert_eq!(engine.unread(42), Some(&PgLsn(2)));
    assert!(engine.failure(42).is_none());
    let events = (3..203)
        .map(|id| {
            Event::insert(table, vec![Value::Int(id), Value::Int(4)]).with_checkpoint(PgLsn(3))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        engine
            .prepare_transaction(b"source-generation-1", &PgLsn(2), PgLsn(3), &events)
            .expect("prepare")
            .expect("new commit")
            .persist(|receipt| {
                assert_eq!(receipt.failures[0].instance, 42);
                assert_eq!(receipt.failures[0].last_valid, PgLsn(2));
                assert_eq!(receipt.changed[0].instance, 43);
                Err("failure record rejected")
            })
            .map_err(|DurablePersistError::Callback { error, .. }| error),
        Err("failure record rejected")
    );
    assert!(engine.failure(42).is_none());
    assert_eq!(engine.checkpoint_position(), &PgLsn(2));
    assert_eq!(engine.unread(43), Some(&PgLsn(2)));
    engine
        .prepare_transaction(b"source-generation-1", &PgLsn(2), PgLsn(3), &events)
        .expect("retry")
        .expect("new commit")
        .persist(|_| Ok::<_, ()>(()))
        .expect("persist failure");
    assert_eq!(engine.unread(42), Some(&PgLsn(2)));
    assert_eq!(engine.last_valid(42), Some(&PgLsn(2)));
    assert_eq!(engine.failure(42).expect("isolated failure").at, PgLsn(3));
    assert_eq!(engine.last_valid(43), Some(&PgLsn(3)));
    assert_eq!(engine.unread(43), Some(&PgLsn(3)));
}

#[test]
fn unchanged_results_can_fill_the_exact_record_quota() {
    let admit = |engine: &mut Engine, table, quota| {
        engine
            .prepare_register(
                42,
                SubscriptionRequest::new(7, "SELECT COUNT(*) FROM items WHERE cost > 100"),
                PgLsn(1),
                vec![ReplayTable {
                    table_id: table,
                    rows: vec![vec![Value::Int(1), Value::Int(5)]],
                }],
                quota,
                |_| Ok(()),
            )
            .expect("the initial input fits")
            .persist(|_| Ok::<_, ()>(()))
            .expect("admission persists");
    };
    let advance = |engine: &mut Engine, table| {
        let event = Event::insert(table, vec![Value::Int(2), Value::Int(4)])
            .with_pk_columns([0])
            .with_checkpoint(PgLsn(2));
        engine
            .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &[event])
            .expect("the unmatched input applies")
            .expect("one new commit")
            .persist(|_| Ok::<_, ()>(()))
            .expect("the complete state persists");
    };
    let mut quota = 1_000_000;
    for _ in 0..2 {
        let (mut measured, table) = engine();
        admit(&mut measured, table, quota);
        advance(&mut measured, table);
        assert!(measured.failure(42).is_none());
        quota = measured
            .state_bytes(42)
            .expect("the record measures")
            .expect("the instance exists");
    }
    let (mut bounded, table) = engine();
    admit(&mut bounded, table, quota);
    advance(&mut bounded, table);
    assert!(
        bounded.failure(42).is_none(),
        "an unchanged result needs no unread boundary"
    );
    assert_eq!(bounded.unread(42), None);
    assert_eq!(bounded.last_valid(42), Some(&PgLsn(2)));
    assert_eq!(
        bounded.state_bytes(42).expect("the record measures"),
        Some(quota)
    );
}

#[tokio::test]
async fn persistence_errors_return_admission_and_transaction_for_retry() {
    let (mut engine, table) = engine();
    let prepared = engine
        .prepare_register(
            42,
            SubscriptionRequest::new(7, "SELECT COUNT(*) FROM items"),
            PgLsn(1),
            vec![ReplayTable {
                table_id: table,
                rows: vec![vec![Value::Int(1), Value::Int(5)]],
            }],
            1_000_000,
            |_| Ok(()),
        )
        .expect("admission");
    let revision = prepared.commit().revision;
    let DurablePersistError::Callback { error, preparation } = prepared
        .persist(|_| Err("admission storage unavailable"))
        .expect_err("storage refuses admission");
    assert_eq!(error, "admission storage unavailable");
    assert_eq!(preparation.commit().revision, revision);
    preparation
        .persist(|_| Ok::<_, ()>(()))
        .expect("the retained admission retries");
    assert_eq!(
        engine.answer(42),
        Some(&subql::ReplayAnswer::Scalar(subql::ReplayValue::Folded(
            subql::AggValue::CountStar(1)
        )))
    );

    let events =
        [Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2))];
    let prepared = engine
        .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &events)
        .expect("prepare")
        .expect("new commit");
    let DurablePersistError::Callback { error, preparation } = prepared
        .persist_async(async |_| Err("transaction storage unavailable"))
        .await
        .expect_err("storage refuses the transaction");
    assert_eq!(error, "transaction storage unavailable");
    assert_eq!(preparation.commit().revision, revision + 1);
    preparation
        .persist_async(async |_| Ok::<_, ()>(()))
        .await
        .expect("the retained transaction retries");
    assert_eq!(engine.checkpoint_position(), &PgLsn(2));
    assert_eq!(engine.unread(42), Some(&PgLsn(2)));
    assert_eq!(
        engine.answer(42),
        Some(&subql::ReplayAnswer::Scalar(subql::ReplayValue::Folded(
            subql::AggValue::CountStar(2)
        )))
    );
}
