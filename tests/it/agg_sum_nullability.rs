//! `SUM` over no rows is NULL, on every engine.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.4.11 and SQLite 3.51.1,
//! which agree completely:
//!
//! ```text
//! rows                       sum(a)   count(a)
//! none                       NULL     0
//! one row, a IS NULL         NULL     0
//! one row, a = 0             0        1
//! that row deleted again     NULL     0
//! ```
//!
//! So an empty sum is not zero, and the difference is not cosmetic: a
//! consumer reading `0` cannot tell "nothing to add up" from "the values
//! cancelled". `COUNT(a)` is exactly the guard, since it counts the rows
//! that contribute a value.
//!
//! subql reported `Sum(0.0)` for all four rows of that table, because
//! `AggValue::Sum` had no null to report and the running value kept no
//! count of what contributed to it.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateInstallError, AggregateValueUpdate, DefaultIds, PgLsn,
    SubscriptionEngine, SubscriptionRequest, SumValue, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";
const SUM: &str = "SELECT SUM(amount) FROM orders WHERE status = 'paid'";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

/// An engine holding one seeded `SUM` subscription, seeded from the
/// component row `[sum, count]` the bootstrap projects.
fn seeded(sum: Value<Postgres>, count: i64) -> (Engine, TableId, u64, AggValue) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id(&database, "orders").unwrap();
    let mut engine: Engine = SubscriptionEngine::new(database, PostgreSqlDialect {});
    let subscription = engine
        .register(SubscriptionRequest::new(7, SUM))
        .unwrap()
        .subscription_id;
    let value = install(&mut engine, subscription, vec![sum, Value::Int(count)]).unwrap();
    (engine, orders, subscription, value)
}

fn install(
    engine: &mut Engine,
    subscription: u64,
    row: Vec<Value<Postgres>>,
) -> Result<AggValue, AggregateInstallError> {
    subql::Install::install(
        engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![row],
            read_at: Some(PgLsn(10)),
        },
    )
    .map(|updates| folded(&updates))
}

fn folded(updates: &[AggregateValueUpdate<DefaultIds>]) -> AggValue {
    assert_eq!(updates.len(), 1, "an ungrouped total reports one value");
    updates[0].folded_value().expect("the total is set")
}

/// A paid order arriving at stream position `lsn`.
fn paid(orders: TableId, id: i64, amount: Value<Postgres>, lsn: u64) -> Event {
    TestEvent::insert(
        orders,
        vec![Value::Int(id), amount, Value::String("paid".into())],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(lsn))
}

/// The same order leaving the table.
fn deleted(orders: TableId, id: i64, amount: Value<Postgres>, lsn: u64) -> Event {
    TestEvent::delete(
        orders,
        vec![Value::Int(id), amount, Value::String("paid".into())],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(lsn))
}

/// The finding: over no rows the engines answer NULL, not zero.
#[test]
fn empty_set_sums_to_null() {
    let (_engine, _orders, _subscription, value) = seeded(Value::Null, 0);
    assert_eq!(
        value,
        AggValue::Sum(None),
        "no row contributes a value, so there is no sum"
    );
}

/// And it goes back to NULL: deleting the only contributing row is not a
/// sum of zero, measured.
#[test]
fn deleting_the_last_row_returns_to_null() {
    let (mut engine, orders, _subscription, value) = seeded(Value::Int(250), 1);
    assert_eq!(value, AggValue::Sum(Some(SumValue::Integer(250))));

    let updates = engine
        .aggregate_updates(&deleted(orders, 1, Value::Int(250), 20))
        .unwrap();
    assert_eq!(
        folded(&updates),
        AggValue::Sum(None),
        "the set is empty again, which is NULL and not 0"
    );
}

/// A row worth zero still contributes, so the sum becomes 0 rather than
/// staying NULL. This is what the zero-delta early-out destroyed: the row
/// changed the answer without changing the total.
#[test]
fn a_zero_valued_row_still_counts() {
    let (mut engine, orders, _subscription, value) = seeded(Value::Null, 0);
    assert_eq!(value, AggValue::Sum(None));

    let updates = engine
        .aggregate_updates(&paid(orders, 1, Value::Int(0), 20))
        .unwrap();
    assert_eq!(
        folded(&updates),
        AggValue::Sum(Some(SumValue::Integer(0))),
        "one row of zero sums to zero, which is not the same as no rows"
    );
}

/// Removing a zero-valued row is reported for the same reason, in the
/// other direction.
#[test]
fn removing_a_zero_valued_row_empties_the_sum() {
    let (mut engine, orders, _subscription, value) = seeded(Value::Int(0), 1);
    assert_eq!(value, AggValue::Sum(Some(SumValue::Integer(0))));

    let updates = engine
        .aggregate_updates(&deleted(orders, 1, Value::Int(0), 20))
        .unwrap();
    assert_eq!(
        folded(&updates),
        AggValue::Sum(None),
        "the last contributor left, so the sum is NULL again"
    );
}

/// A NULL seed stays NULL rather than being flattened to zero on the way
/// in.
#[test]
fn a_null_seed_stays_null() {
    let (_engine, _orders, _subscription, value) = seeded(Value::Null, 0);
    assert_eq!(value, AggValue::Sum(None), "NULL in is NULL out");
}

/// A row whose summed column is NULL contributes nothing at all, so it
/// neither starts a sum nor counts toward one, measured: `sum(a)` and
/// `count(a)` are both empty over a single all-NULL row.
#[test]
fn a_null_valued_row_contributes_nothing() {
    let (mut engine, orders, _subscription, _value) = seeded(Value::Null, 0);
    let updates = engine
        .aggregate_updates(&paid(orders, 1, Value::Null, 20))
        .unwrap();
    assert!(
        updates.is_empty() || folded(&updates) == AggValue::Sum(None),
        "a NULL value is not a contributor, got {updates:?}"
    );
}

/// Adding then removing an ordinary row returns to NULL rather than
/// settling on zero, which is the composition of the two corrections.
#[test]
fn a_row_added_and_removed_returns_to_null() {
    let (mut engine, orders, _subscription, _value) = seeded(Value::Null, 0);
    let added = engine
        .aggregate_updates(&paid(orders, 1, Value::Int(250), 20))
        .unwrap();
    assert_eq!(folded(&added), AggValue::Sum(Some(SumValue::Integer(250))));

    let removed = engine
        .aggregate_updates(&deleted(orders, 1, Value::Int(250), 30))
        .unwrap();
    assert_eq!(
        folded(&removed),
        AggValue::Sum(None),
        "back to an empty set, which is NULL"
    );
}
