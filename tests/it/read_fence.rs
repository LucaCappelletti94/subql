//! Every value the engine sets from a database read judges the stream's
//! changes against that read's snapshot, so a change the read holds is never
//! applied again and a change it missed is never lost.
//!
//! One Postgres read throughout, with snapshot `740:745:742` and WAL insert
//! position 2000 read after it. Against it, xid 741 committed at 1500 is seen
//! and arrives late. Xid 743 committed at 1800 is seen, though it committed
//! after any position read before the snapshot. Xid 742 committed at 1200 is
//! still running in the snapshot, as while it waits on a synchronous standby,
//! so the read missed it. Everything from 2000 on is beyond the read.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{ReExecutionRead, ScalarInstalled};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateResultValue, AggregateSeedInstall, AggregateValueChange,
    DefaultIds, GroupedScalarInstall, GroupedScalarSeedInstall, Install, PgCommitPosition, PgLsn,
    PgSnapshotFence, PgXid, ScalarInstall, SubscriptionEngine, SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgCommitPosition>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        orders,
    )
}

fn read_fence() -> PgSnapshotFence {
    PgSnapshotFence::parse("740:745:742", PgLsn(2000)).unwrap()
}

/// A read taken long before, which every change below passes.
fn early_fence() -> PgSnapshotFence {
    PgSnapshotFence::parse("700:701:", PgLsn(1000)).unwrap()
}

fn row(id: i64, region: &str, amount: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(region.into()),
        Value::Int(amount),
        Value::String("paid".into()),
    ]
}

const fn at(commit_lsn: u64, xid: u32) -> PgCommitPosition {
    PgCommitPosition::new(PgLsn(commit_lsn), PgXid(xid), 1)
}

fn insert(
    orders: TableId,
    id: i64,
    region: &str,
    amount: i64,
    position: PgCommitPosition,
) -> Event {
    Event::insert(orders, row(id, region, amount))
        .with_pk_columns([0u16])
        .with_checkpoint(position)
}

fn delete(
    orders: TableId,
    id: i64,
    region: &str,
    amount: i64,
    position: PgCommitPosition,
) -> Event {
    Event::delete(orders, row(id, region, amount))
        .with_pk_columns([0u16])
        .with_checkpoint(position)
}

fn register(engine: &mut Engine, sql: &str) -> u64 {
    engine
        .register(SubscriptionRequest::new(7u64, sql))
        .unwrap()
        .subscription_id
}

fn folded(updates: &[subql::AggregateValueUpdate<DefaultIds>]) -> Vec<AggValue> {
    updates
        .iter()
        .map(|update| update.folded_value().expect("a folded value"))
        .collect()
}

#[test]
fn a_seeded_count_applies_exactly_the_changes_its_read_missed() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
    );

    // Delivered while the read runs, invisible to its snapshot.
    engine
        .aggregate_updates(&insert(orders, 1, "north", 5, at(1200, 742)))
        .unwrap();

    // The read counted 741 and 743.
    let installed = Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(2)]],
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(folded(&installed), vec![AggValue::CountStar(3)]);

    for held in [
        insert(orders, 2, "north", 5, at(1500, 741)),
        insert(orders, 3, "north", 5, at(1800, 743)),
    ] {
        let late = engine.aggregate_updates(&held).unwrap();
        assert!(
            late.is_empty(),
            "the read already counted it, got {:?}",
            folded(&late)
        );
    }

    let beyond = engine
        .aggregate_updates(&insert(orders, 4, "north", 5, at(2100, 746)))
        .unwrap();
    assert_eq!(folded(&beyond), vec![AggValue::CountStar(4)]);
}

#[test]
fn a_truncate_the_read_already_saw_leaves_its_count_alone() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
    );
    // The read ran after the 741 truncate and counted 743's row.
    Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            fence: Some(read_fence()),
        },
    )
    .unwrap();

    let truncated = Event::truncate(orders).with_checkpoint(at(1500, 741));
    let late = engine.aggregate_updates(&truncated).unwrap();
    assert!(late.is_empty(), "got {:?}", folded(&late));
    assert_eq!(
        engine.current_aggregate_value(sub),
        Some(AggValue::CountStar(1))
    );
}

#[test]
fn a_seeded_grouped_count_drops_a_late_change_its_read_holds() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, COUNT(*) FROM orders WHERE status = 'paid' GROUP BY region",
    );

    engine
        .aggregate_updates(&insert(orders, 1, "north", 5, at(1200, 742)))
        .unwrap();
    let installed = Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(2),
                Value::Int(2),
            ]],
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(folded(&installed), vec![AggValue::CountStar(3)]);

    let late = engine
        .aggregate_updates(&insert(orders, 2, "north", 5, at(1800, 743)))
        .unwrap();
    assert!(
        late.is_empty(),
        "the read already counted it, got {:?}",
        folded(&late)
    );

    let beyond = engine
        .aggregate_updates(&insert(orders, 3, "north", 5, at(2100, 746)))
        .unwrap();
    assert_eq!(folded(&beyond), vec![AggValue::CountStar(4)]);
}

const fn scalar(value: i64) -> AggregateValueChange<Postgres> {
    AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(value)))
}

#[test]
fn a_scoped_group_read_keeps_what_it_missed_and_drops_what_it_holds() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
    );
    // North holds amounts 4 and 3.
    let opening = Install::install(
        &mut engine,
        sub,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(3),
                Value::Int(2),
            ]],
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let north = opening.updates[0].group.clone().unwrap();

    // Removing the extreme asks for a read of north alone.
    let displaced = engine
        .dispatch(&delete(orders, 10, "north", 3, at(1100, 720)))
        .unwrap();
    assert!(matches!(
        displaced.triggers()[0].read,
        ReExecutionRead::GroupedScalar { .. }
    ));
    // Delivered before the read lands, invisible to its snapshot.
    engine
        .dispatch(&insert(orders, 11, "north", 9, at(1200, 742)))
        .unwrap();

    // The read saw 4 and the late 741 row worth 7.
    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.key,
            row: vec![Value::Int(4), Value::Int(2)],
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(installed.updates[0].change, scalar(4));
    assert!(installed.triggers.is_empty());

    let late = engine
        .dispatch(&insert(orders, 12, "north", 7, at(1500, 741)))
        .unwrap();
    assert!(late.aggregate_updates().is_empty() && late.triggers().is_empty());

    // Three rows are left, 9, 7 and 4, so the third delete empties the group.
    for (id, amount, lsn) in [(11, 9, 2100), (12, 7, 2200)] {
        let output = engine
            .dispatch(&delete(orders, id, "north", amount, at(lsn, 750)))
            .unwrap();
        assert!(output.aggregate_updates().is_empty() && output.triggers().is_empty());
    }
    let emptied = engine
        .dispatch(&delete(orders, 13, "north", 4, at(2300, 751)))
        .unwrap();
    assert!(emptied.triggers().is_empty(), "no row is left to read");
    assert_eq!(
        emptied.aggregate_updates()[0].change,
        AggregateValueChange::Remove
    );
}

fn scalar_value(
    installed: ScalarInstalled<DefaultIds, Postgres, PgCommitPosition>,
) -> Value<Postgres> {
    let ScalarInstalled::Value(update) = installed else {
        panic!("expected a value, got {installed:?}")
    };
    update.value
}

#[test]
fn a_scalar_read_keeps_a_change_it_missed() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT MIN(amount) FROM orders WHERE status = 'paid'",
    );
    let first = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(first), Value::Int(4));

    let displaced = engine
        .dispatch(&delete(orders, 10, "north", 4, at(1100, 720)))
        .unwrap();
    assert_eq!(displaced.triggers().len(), 1);
    engine
        .dispatch(&insert(orders, 11, "north", 2, at(1200, 742)))
        .unwrap();

    // The read saw 5 as the smallest and not the 742 row worth 2.
    let installed = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(5),
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(installed), Value::Int(2));
}

#[test]
fn a_scalar_read_whose_answer_a_missed_change_removed_asks_again() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT MIN(amount) FROM orders WHERE status = 'paid'",
    );
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    engine
        .dispatch(&delete(orders, 10, "north", 4, at(1100, 720)))
        .unwrap();
    engine
        .dispatch(&delete(orders, 11, "north", 5, at(1200, 742)))
        .unwrap();

    // The read still saw the 742 row worth 5 as the smallest.
    let installed = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(5),
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(
        matches!(installed, ScalarInstalled::ReadAgain(_)),
        "got {installed:?}"
    );

    // The next read holds that delete, so its answer stands.
    let reread = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(6),
            checkpoint: None,
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(reread), Value::Int(6));
}
