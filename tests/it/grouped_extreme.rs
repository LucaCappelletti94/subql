//! Grouped extrema fold cheap changes and re-read only a displaced group.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::reexec::ReExecutionRead;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggregateResultValue, AggregateValueChange, DefaultIds, GroupedScalarInstall,
    GroupedScalarSeedInstall, Install, MaintenanceStopReason, PgLsn, SubscriptionEngine,
    SubscriptionRequest, TableId, Tier, TierKind,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        table,
    )
}

fn row(id: i64, region: Value<Postgres>, amount: Value<Postgres>) -> Vec<Value<Postgres>> {
    vec![Value::Int(id), region, amount, Value::String("paid".into())]
}

fn text_row(id: i64, region: &str, amount: i64) -> Vec<Value<Postgres>> {
    row(id, Value::String(region.into()), Value::Int(amount))
}

fn register(engine: &mut Engine, function: &str) -> (u64, subql::AggregateBootstrap) {
    let sql = format!(
        "SELECT region, {function}(amount) FROM orders WHERE status = 'paid' GROUP BY region"
    );
    let registered = engine
        .register(SubscriptionRequest::new(7u64, sql))
        .expect("grouped extreme registers");
    let Tier::GroupedScalar { bootstrap } = registered.tier else {
        panic!("expected grouped scalar tier")
    };
    (registered.subscription_id, bootstrap)
}

fn seed(
    engine: &mut Engine,
    subscription: u64,
    rows: Vec<Vec<Value<Postgres>>>,
) -> subql::AggregateMaintenanceOutput<DefaultIds, Postgres, PgLsn> {
    Install::install(
        engine,
        subscription,
        GroupedScalarSeedInstall {
            rows,
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("grouped extreme seed installs")
}

const fn scalar(value: Value<Postgres>) -> AggregateValueChange<Postgres> {
    AggregateValueChange::Set(AggregateResultValue::Scalar(value))
}

#[test]
fn registration_exposes_a_grouped_seed_with_extreme_and_row_count() {
    let (mut engine, _) = engine();
    let (_, bootstrap) = register(&mut engine, "MIN");

    assert_eq!(bootstrap.group_columns, 1);
    assert_eq!(
        bootstrap.kinds,
        vec![BuiltinKind::String, BuiltinKind::Int, BuiltinKind::Int]
    );
    assert!(bootstrap.sql.contains("MIN(\"amount\") AS c0"));
    assert!(bootstrap.sql.contains("COUNT(*) AS c1"));
}

#[test]
fn insert_folds_delete_requeries_only_the_displaced_group() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    let opening = seed(
        &mut engine,
        subscription,
        vec![
            vec![Value::String("north".into()), Value::Int(5), Value::Int(2)],
            vec![Value::String("south".into()), Value::Int(9), Value::Int(2)],
        ],
    );
    let north = opening
        .updates
        .iter()
        .find(|update| update.change == scalar(Value::Int(5)))
        .and_then(|update| update.group.clone())
        .expect("north group key");

    let inserted = Event::insert(orders, text_row(3, "north", 3))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&inserted).expect("insert dispatches");
    assert_eq!(output.aggregate_updates().len(), 1);
    assert_eq!(output.aggregate_updates()[0].change, scalar(Value::Int(3)));
    assert!(output.triggers().is_empty());

    let non_extreme = Event::delete(orders, text_row(4, "south", 12))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(20));
    let output = engine.dispatch(&non_extreme).expect("delete dispatches");
    assert!(output.aggregate_updates().is_empty());
    assert!(output.triggers().is_empty());

    let displaced = Event::delete(orders, text_row(3, "north", 3))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(30));
    let output = engine.dispatch(&displaced).expect("delete dispatches");
    assert!(output.aggregate_updates().is_empty());
    assert_eq!(output.triggers().len(), 1);
    let ReExecutionRead::GroupedScalar {
        group,
        query,
        column_kinds,
    } = &output.triggers()[0].read
    else {
        panic!("expected grouped scalar read")
    };
    assert_eq!(group, &north.key);
    assert_eq!(*column_kinds, [BuiltinKind::Int, BuiltinKind::Int]);
    assert!(
        query.sql().contains("\"region\" = $1"),
        "scoped SQL was {query:?}"
    );
    assert_eq!(query.binds(), &[Value::String("north".into())]);
    assert!(!query.sql().contains("south"));

    let installed = Install::install(
        &mut engine,
        subscription,
        GroupedScalarInstall {
            group: north.key.clone(),
            row: vec![Value::Int(5), Value::Int(1)],
            checkpoint: Some(PgLsn(30)),
        },
    )
    .expect("scoped result installs");
    assert_eq!(installed.updates[0].group.as_ref(), Some(&north));
    assert_eq!(installed.updates[0].change, scalar(Value::Int(5)));
}

#[test]
fn the_last_row_removes_the_group_without_a_read() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MAX");
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(5),
            Value::Int(1),
        ]],
    );
    let north = opening.updates[0].group.clone().expect("north key");

    let removed = Event::delete(orders, text_row(1, "north", 5))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&removed).expect("delete dispatches");
    assert_eq!(output.aggregate_updates().len(), 1);
    assert_eq!(output.aggregate_updates()[0].group.as_ref(), Some(&north));
    assert_eq!(
        output.aggregate_updates()[0].change,
        AggregateValueChange::Remove
    );
    assert!(output.triggers().is_empty());
}

#[test]
fn null_group_values_use_is_null_in_the_scoped_read() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![Value::Null, Value::Int(2), Value::Int(2)]],
    );
    let null_group = opening.updates[0].group.clone().expect("null group key");

    let removed = Event::delete(orders, row(1, Value::Null, Value::Int(2)))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&removed).expect("delete dispatches");
    let ReExecutionRead::GroupedScalar { group, query, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    assert_eq!(group, &null_group.key);
    assert!(
        query.sql().contains("\"region\" IS NULL"),
        "scoped query was {query:?}"
    );
    assert_eq!(query.binds(), []);
}

#[test]
fn an_extreme_removed_during_the_seed_read_requests_its_group_after_install() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    let event = Event::delete(orders, text_row(1, "north", 2))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let before_seed = engine.dispatch(&event).expect("delete queues");
    assert!(before_seed.aggregate_updates().is_empty());
    assert!(before_seed.triggers().is_empty());

    let installed = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(2),
        ]],
    );
    assert!(installed.updates.is_empty());
    assert_eq!(installed.triggers.len(), 1);
    assert!(matches!(
        installed.triggers[0].read,
        ReExecutionRead::GroupedScalar { .. }
    ));
}

#[test]
fn a_change_the_seed_read_already_saw_is_not_replayed() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    let event = Event::delete(orders, text_row(1, "north", 2))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(5));
    let queued = engine.dispatch(&event).expect("delete queues");
    assert!(queued.aggregate_updates().is_empty());
    assert!(queued.triggers().is_empty());

    let installed = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(2),
        ]],
    );
    assert!(
        installed.triggers.is_empty(),
        "a change at the read position was already in the seed rows"
    );
    assert_eq!(installed.updates.len(), 1);
    assert_eq!(installed.updates[0].change, scalar(Value::Int(2)));
}

#[test]
fn an_unreadable_old_group_transitions_under_the_same_identity() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(2),
        ]],
    );

    let event = Event::update(
        orders,
        row(1, Value::Missing, Value::Int(2)),
        text_row(1, "south", 4),
    )
    .with_pk_columns([0u16])
    .with_changed_columns([1u16, 2u16])
    .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("transition succeeds");

    assert_eq!(output.transitions().len(), 1);
    assert_eq!(output.transitions()[0].subscription_id, subscription);
    assert_eq!(output.transitions()[0].from, TierKind::GroupedScalar);
    assert_eq!(
        output.transitions()[0].reason,
        MaintenanceStopReason::MissingOldRow { table_id: orders }
    );
    assert!(matches!(output.transitions()[0].to, Tier::WholeRows { .. }));
}
#[test]
fn a_readable_group_recovers_a_missing_old_extreme_with_one_group_read() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(2),
        ]],
    );
    let mut old = text_row(1, "north", 2);
    old[2] = Value::Missing;
    let event = Event::update(orders, old, text_row(1, "north", 4))
        .with_pk_columns([0u16])
        .with_changed_columns([2u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("sparse update dispatches");
    assert_eq!(output.transitions(), []);
    assert_eq!(output.triggers().len(), 1);
    let ReExecutionRead::GroupedScalar { group, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    Install::install(
        &mut engine,
        subscription,
        GroupedScalarInstall {
            group: group.clone(),
            row: vec![Value::Int(4), Value::Int(2)],
            checkpoint: Some(PgLsn(10)),
        },
    )
    .expect("complete group state installs");
    let non_extreme = Event::delete(orders, text_row(2, "north", 5))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(20));
    let output = engine
        .dispatch(&non_extreme)
        .expect("later delete uses installed count");
    assert!(output.aggregate_updates().is_empty());
    assert!(output.triggers().is_empty());
    let current = Event::delete(orders, text_row(1, "north", 4))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(30));
    let output = engine
        .dispatch(&current)
        .expect("last row uses installed count");
    assert_eq!(output.aggregate_updates().len(), 1);
    assert_eq!(
        output.aggregate_updates()[0].change,
        AggregateValueChange::Remove
    );
    assert!(output.triggers().is_empty());
}

#[test]
fn an_installed_group_replaces_the_extreme() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(3),
        ]],
    );
    let displaced = Event::delete(orders, text_row(1, "north", 2))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&displaced).expect("delete dispatches");
    let ReExecutionRead::GroupedScalar { group, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    Install::install(
        &mut engine,
        subscription,
        GroupedScalarInstall {
            group: group.clone(),
            row: vec![Value::Int(7), Value::Int(2)],
            checkpoint: Some(PgLsn(10)),
        },
    )
    .expect("scoped result installs");

    let insert = Event::insert(orders, text_row(9, "north", 5))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(20));
    let output = engine.dispatch(&insert).expect("insert dispatches");
    assert_eq!(output.aggregate_updates().len(), 1);
    assert_eq!(
        output.aggregate_updates()[0].change,
        scalar(Value::Int(5)),
        "an insert between the stale and the installed extreme must fold"
    );
}

#[test]
fn an_installed_group_replaces_the_source_row_count() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(3),
        ]],
    );
    let displaced = Event::delete(orders, text_row(1, "north", 2))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&displaced).expect("delete dispatches");
    let ReExecutionRead::GroupedScalar { group, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    Install::install(
        &mut engine,
        subscription,
        GroupedScalarInstall {
            group: group.clone(),
            row: vec![Value::Int(7), Value::Int(1)],
            checkpoint: Some(PgLsn(10)),
        },
    )
    .expect("scoped result installs");

    let non_extreme = Event::delete(orders, text_row(2, "north", 9))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(20));
    let output = engine.dispatch(&non_extreme).expect("delete dispatches");
    assert_eq!(output.aggregate_updates().len(), 1);
    assert_eq!(
        output.aggregate_updates()[0].change,
        AggregateValueChange::Remove,
        "the installed count is the row count, so this delete empties the group"
    );
    assert!(output.triggers().is_empty());
}

#[test]
fn moving_an_extreme_requeries_the_old_group_and_folds_the_new_group() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    let opening = seed(
        &mut engine,
        subscription,
        vec![
            vec![Value::String("north".into()), Value::Int(2), Value::Int(2)],
            vec![Value::String("south".into()), Value::Int(9), Value::Int(1)],
        ],
    );
    let north = opening
        .updates
        .iter()
        .find(|update| update.change == scalar(Value::Int(2)))
        .and_then(|update| update.group.clone())
        .expect("north key");
    let south = opening
        .updates
        .iter()
        .find(|update| update.change == scalar(Value::Int(9)))
        .and_then(|update| update.group.clone())
        .expect("south key");
    let event = Event::update(orders, text_row(1, "north", 2), text_row(1, "south", 1))
        .with_pk_columns([0u16])
        .with_changed_columns([1u16, 2u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("move dispatches");
    assert_eq!(output.aggregate_updates().len(), 1);
    assert_eq!(output.aggregate_updates()[0].group.as_ref(), Some(&south));
    assert_eq!(output.aggregate_updates()[0].change, scalar(Value::Int(1)));
    assert_eq!(output.triggers().len(), 1);
    let ReExecutionRead::GroupedScalar { group, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    assert_eq!(group, &north.key);
}

#[test]
fn one_update_displacing_two_groups_keeps_both_reads() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![
            vec![Value::String("north".into()), Value::Int(2), Value::Int(2)],
            vec![Value::String("south".into()), Value::Int(9), Value::Int(2)],
        ],
    );
    let event = Event::update(
        orders,
        text_row(1, "north", 2),
        row(1, Value::String("south".into()), Value::Missing),
    )
    .with_pk_columns([0u16])
    .with_changed_columns([1u16, 2u16])
    .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("update dispatches");
    assert_eq!(
        output.triggers().len(),
        2,
        "the displaced old group and the unreadable new group each keep their read"
    );
    let mut groups: Vec<_> = output
        .triggers()
        .iter()
        .filter_map(|trigger| match &trigger.read {
            ReExecutionRead::GroupedScalar { group, .. } => Some(group.clone()),
            ReExecutionRead::Subscription => None,
        })
        .collect();
    groups.sort_unstable();
    groups.dedup();
    assert_eq!(groups.len(), 2, "the two reads name two distinct groups");
}

#[test]
fn removing_one_tied_extreme_conservatively_rereads_its_group() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(3),
        ]],
    );
    let event = Event::delete(orders, text_row(1, "north", 2))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("delete dispatches");
    assert!(output.aggregate_updates().is_empty());
    assert_eq!(output.triggers().len(), 1);
}

#[test]
fn a_new_group_past_the_limit_transitions_under_the_same_identity() {
    let (engine, orders) = engine();
    let mut engine = engine.with_max_groups_per_aggregate(1);
    let (subscription, _) = register(&mut engine, "MIN");
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(2),
            Value::Int(1),
        ]],
    );
    let event = Event::insert(orders, text_row(2, "south", 3))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("transition succeeds");
    assert_eq!(output.transitions().len(), 1);
    assert_eq!(output.transitions()[0].subscription_id, subscription);
    assert_eq!(output.transitions()[0].from, TierKind::GroupedScalar);
    assert_eq!(
        output.transitions()[0].reason,
        MaintenanceStopReason::GroupLimit { limit: 1 }
    );
    assert!(matches!(output.transitions()[0].to, Tier::WholeRows { .. }));
}

#[test]
fn grouped_max_preserves_a_text_extreme() {
    let (mut engine, orders) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            9u64,
            "SELECT region, MAX(status) FROM orders GROUP BY region",
        ))
        .expect("grouped text max registers");
    assert!(matches!(registered.tier, Tier::GroupedScalar { .. }));
    let opening = seed(
        &mut engine,
        registered.subscription_id,
        vec![vec![
            Value::String("north".into()),
            Value::String("paid".into()),
            Value::Int(1),
        ]],
    );
    assert_eq!(
        opening.updates[0].change,
        scalar(Value::String("paid".into()))
    );

    let mut changed = text_row(2, "north", 3);
    changed[3] = Value::String("void".into());
    let event = Event::insert(orders, changed)
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("text insert dispatches");
    assert_eq!(
        output.aggregate_updates()[0].change,
        scalar(Value::String("void".into()))
    );
}

#[test]
fn every_postgres_group_kind_renders_into_a_scoped_read() {
    let ddl = "CREATE TABLE samples (
        id INT PRIMARY KEY,
        enabled BOOL,
        payload BYTEA,
        created TIMESTAMP,
        zoned TIMESTAMPTZ,
        day DATE,
        clock TIME,
        label TEXT,
        token UUID,
        metric DOUBLE PRECISION,
        doc JSONB,
        amount INT
    );";
    let catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "samples").expect("samples resolves");
    let mut engine =
        SubscriptionEngine::<Event, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {});
    let sql = "SELECT enabled, payload, created, zoned, day, clock, label, token, metric, doc, MIN(amount) \
               FROM samples \
               GROUP BY enabled, payload, created, zoned, day, clock, label, token, metric, doc";
    let registered = engine
        .register(SubscriptionRequest::new(10u64, sql))
        .expect("all exact group kinds register");
    assert!(matches!(registered.tier, Tier::GroupedScalar { .. }));

    let day = chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");
    let clock = chrono::NaiveTime::from_hms_opt(3, 4, 5).expect("valid time");
    let created = day.and_time(clock);
    let token = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid UUID");
    let groups = vec![
        Value::Bool(true),
        Value::Bytes(vec![1, 2]),
        Value::Timestamp(created),
        Value::TimestampTz(created.and_utc()),
        Value::Date(day),
        Value::Time(clock),
        Value::String("north".into()),
        Value::Uuid(token),
        Value::Float(-0.0),
        Value::Jsonb(serde_json::from_str(r#"{"a":1.00,"b":true}"#).unwrap()),
    ];
    let mut seed_row = groups.clone();
    seed_row.extend([Value::Int(2), Value::Int(2)]);
    let seeded = seed(&mut engine, registered.subscription_id, vec![seed_row]);
    assert_eq!(seeded.updates.len(), 1);
    assert_eq!(
        seeded.updates[0]
            .group
            .as_ref()
            .expect("grouped update carries identity")
            .values,
        groups
    );

    let mut old = vec![Value::Int(1)];
    old.extend(groups.clone());
    old.push(Value::Int(2));
    let event = Event::delete(table, old)
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&event).expect("delete dispatches");
    let ReExecutionRead::GroupedScalar { query, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    assert_eq!(query.binds(), groups);
    assert!(!query.sql().contains("true"));
    assert!(!query.sql().contains("550e8400-e29b-41d4-a716-446655440000"));
}

mod having_on_grouped_extreme {
    use super::*;
    /// Register a grouped extreme with a `HAVING`, expecting the hybrid tier.
    fn register_having(engine: &mut Engine, sql: &str) -> (u64, subql::AggregateBootstrap) {
        let registered = engine
            .register(SubscriptionRequest::new(7u64, sql))
            .unwrap_or_else(|e| panic!("`{sql}` should register, got {e:?}"));
        let Tier::GroupedScalar { bootstrap } = registered.tier else {
            panic!(
                "`{sql}` should be a grouped extreme, got {:?}",
                registered.tier
            )
        };
        (registered.subscription_id, bootstrap)
    }

    #[test]
    fn a_having_extreme_registers_and_strips_the_clause() {
        let (mut engine, _) = engine();
        let (_, bootstrap) = register_having(
            &mut engine,
            "SELECT region, MIN(amount) FROM orders GROUP BY region HAVING MIN(amount) < 5",
        );
        assert!(
            !bootstrap.sql.to_uppercase().contains("HAVING"),
            "the seed must fetch every group, hidden ones included: {}",
            bootstrap.sql
        );
    }

    #[test]
    fn a_sibling_having_on_an_extreme_rides_the_capture() {
        let (mut engine, _) = engine();
        let registered = engine
            .register(SubscriptionRequest::new(
                7u64,
                "SELECT region, MIN(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
            ))
            .expect("the capture still answers");
        assert!(
            matches!(registered.tier, Tier::WholeRows { .. }),
            "extreme state holds no sum, so this rides the whole re-read, got {:?}",
            registered.tier
        );
    }

    #[test]
    fn an_extreme_crossing_on_a_fold_emits_entering() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register_having(
            &mut engine,
            "SELECT region, MIN(amount) FROM orders GROUP BY region HAVING MIN(amount) < 5",
        );
        let opening = seed(
            &mut engine,
            subscription,
            vec![vec![
                Value::String("north".into()),
                Value::Int(7),
                Value::Int(1),
            ]],
        );
        assert!(opening.updates.is_empty(), "minimum 7 does not pass");

        let entering = Event::insert(orders, text_row(2, "north", 3))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(10));
        let output = engine.dispatch(&entering).expect("insert dispatches");
        assert_eq!(output.aggregate_updates().len(), 1);
        assert_eq!(output.aggregate_updates()[0].change, scalar(Value::Int(3)));
    }

    #[test]
    fn an_extreme_crossing_out_on_a_scoped_read_install_emits_remove() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register_having(
            &mut engine,
            "SELECT region, MIN(amount) FROM orders GROUP BY region HAVING MIN(amount) < 5",
        );
        seed(
            &mut engine,
            subscription,
            vec![vec![
                Value::String("north".into()),
                Value::Int(7),
                Value::Int(1),
            ]],
        );
        let entering = Event::insert(orders, text_row(2, "north", 3))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(10));
        engine.dispatch(&entering).expect("insert dispatches");

        let displaced = Event::delete(orders, text_row(2, "north", 3))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(20));
        let output = engine.dispatch(&displaced).expect("delete dispatches");
        let ReExecutionRead::GroupedScalar { group, query, .. } = &output.triggers()[0].read else {
            panic!("expected grouped scalar read")
        };
        assert!(
            !query.sql().to_uppercase().contains("HAVING"),
            "the scoped read replaces the group's whole state, so the condition must not filter it: {query:?}"
        );
        let installed = Install::install(
            &mut engine,
            subscription,
            GroupedScalarInstall {
                group: group.clone(),
                row: vec![Value::Int(7), Value::Int(1)],
                checkpoint: Some(PgLsn(20)),
            },
        )
        .expect("scoped result installs");
        assert_eq!(installed.updates.len(), 1);
        assert_eq!(
            installed.updates[0].change,
            AggregateValueChange::Remove,
            "the replacement minimum 7 leaves the result"
        );
    }

    #[test]
    fn a_row_count_having_announces_without_an_extreme_change() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register_having(
            &mut engine,
            "SELECT region, MAX(amount) FROM orders GROUP BY region HAVING COUNT(*) > 1",
        );
        let opening = seed(
            &mut engine,
            subscription,
            vec![vec![
                Value::String("north".into()),
                Value::Int(9),
                Value::Int(1),
            ]],
        );
        assert!(opening.updates.is_empty(), "one row does not pass");

        let second = Event::insert(orders, text_row(2, "north", 4))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(10));
        let output = engine.dispatch(&second).expect("insert dispatches");
        assert_eq!(output.aggregate_updates().len(), 1);
        assert_eq!(
            output.aggregate_updates()[0].change,
            scalar(Value::Int(9)),
            "the second row crosses the row-count threshold without moving the maximum"
        );
    }

    #[test]
    fn a_hidden_extreme_group_that_empties_stays_silent() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register_having(
            &mut engine,
            "SELECT region, MIN(amount) FROM orders GROUP BY region HAVING MIN(amount) < 5",
        );
        seed(
            &mut engine,
            subscription,
            vec![vec![
                Value::String("north".into()),
                Value::Int(7),
                Value::Int(1),
            ]],
        );
        let removed = Event::delete(orders, text_row(1, "north", 7))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(10));
        let output = engine.dispatch(&removed).expect("delete dispatches");
        assert!(
            output.aggregate_updates().is_empty(),
            "a group never announced must not announce its removal"
        );
        assert!(output.triggers().is_empty());
    }

    /// A group born during the seed window is announced exactly once: the
    /// opening pass speaks for the final state, never on top of the replay.
    #[test]
    fn a_group_inserted_during_the_seed_window_is_announced_once() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register(&mut engine, "MIN");
        let event = Event::insert(orders, text_row(1, "south", 4))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(10));
        engine.dispatch(&event).expect("insert queues");

        let installed = seed(
            &mut engine,
            subscription,
            vec![vec![
                Value::String("north".into()),
                Value::Int(2),
                Value::Int(1),
            ]],
        );
        assert_eq!(
            installed.updates.len(),
            2,
            "north and south once each, got {:?}",
            installed.updates
        );
        let mut south_sets = installed
            .updates
            .iter()
            .filter(|update| update.change == scalar(Value::Int(4)));
        let south = south_sets.next().expect("replayed group opens");
        assert!(
            south_sets.next().is_none(),
            "the replayed insert must not double-announce"
        );
        let identity = south
            .group
            .as_ref()
            .expect("grouped update carries identity");
        assert_eq!(identity.values, vec![Value::String("south".into())]);
        assert_eq!(&identity.key[..5], b"SQGK\x01");
    }

    /// A scoped read that confirms the held extreme announces nothing: the
    /// consumer already holds this value, and the read only corrected the
    /// engine's private row count.
    #[test]
    fn a_confirming_reread_stays_silent() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register(&mut engine, "MIN");
        seed(
            &mut engine,
            subscription,
            vec![vec![
                Value::String("north".into()),
                Value::Int(2),
                Value::Int(3),
            ]],
        );
        let tied = Event::delete(orders, text_row(1, "north", 2))
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(10));
        let output = engine.dispatch(&tied).expect("delete dispatches");
        let ReExecutionRead::GroupedScalar { group, .. } = &output.triggers()[0].read else {
            panic!("expected grouped scalar read")
        };
        let installed = Install::install(
            &mut engine,
            subscription,
            GroupedScalarInstall {
                group: group.clone(),
                row: vec![Value::Int(2), Value::Int(2)],
                checkpoint: Some(PgLsn(10)),
            },
        )
        .expect("confirming result installs");
        assert!(
            installed.updates.is_empty(),
            "the extreme did not change, got {:?}",
            installed.updates
        );
    }

    /// `TRUNCATE` says goodbye only to announced groups: one hidden behind the
    /// `HAVING` was never delivered, so its disappearance announces nothing.
    #[test]
    fn truncate_removes_only_announced_groups() {
        let (mut engine, orders) = engine();
        let (subscription, _) = register_having(
            &mut engine,
            "SELECT region, MIN(amount) FROM orders GROUP BY region HAVING MIN(amount) < 5",
        );
        let opening = seed(
            &mut engine,
            subscription,
            vec![
                vec![Value::String("north".into()), Value::Int(7), Value::Int(1)],
                vec![Value::String("south".into()), Value::Int(2), Value::Int(1)],
            ],
        );
        assert_eq!(opening.updates.len(), 1, "north stays hidden");
        let south = opening.updates[0].group.clone().expect("south key");

        let truncated = Event::truncate(orders).with_checkpoint(PgLsn(10));
        let output = engine.dispatch(&truncated).expect("truncate dispatches");
        assert_eq!(
            output.aggregate_updates().len(),
            1,
            "north was never announced"
        );
        assert_eq!(output.aggregate_updates()[0].group.as_ref(), Some(&south));
        assert_eq!(
            output.aggregate_updates()[0].change,
            AggregateValueChange::Remove
        );
        assert!(output.triggers().is_empty());
    }
}
