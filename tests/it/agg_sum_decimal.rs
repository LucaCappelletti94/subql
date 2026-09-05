//! A `NUMERIC` column's aggregate folds, rather than being served and then
//! ignored.
//!
//! The worst shape a defect can take here: the number was not imprecise,
//! it never moved. `SELECT SUM(amount)` over a `NUMERIC` column registered
//! as served in process, its seed installed, and every subsequent event was
//! dropped, so the subscription reported the seed forever. The probe
//! matched only `Value::Int` and `Value::Float` and routed everything else,
//! `Value::Decimal` included, to `AggCellRead::NonNumeric`, which the delta
//! then skipped.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15 and SQLite 3.51.1, over a
//! `NUMERIC` column holding `10.25`:
//!
//! ```text
//! aggregate      pg                      sqlite
//! SUM            10.25, numeric, scale 2 10.25, real
//! AVG            10.2500000000000000     -
//! VAR_POP        15.0156250000000000     -
//! then + 2.5     SUM 12.75, scale 2      -
//! ```
//!
//! So every one of them has an answer, and reporting the seed forever is
//! not it. The rule this phase pins is that being served and being folded
//! are the same thing: registration admits `Int`, `Float` and `Decimal`
//! columns, and a column it admits must move the value.
#![allow(clippy::unwrap_used)]

use bigdecimal::BigDecimal;
use core::str::FromStr as _;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, NumericValue, PgLsn, SubscriptionEngine,
    SubscriptionRequest, TableId, Tier,
};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount NUMERIC, \
                   whole INT, approx DOUBLE PRECISION)";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

/// Register `sql`, assert the engine serves it in process, and seed it
/// empty so that every number the test sees came from a fold.
fn served(sql: &str, components: Vec<Value<Postgres>>) -> (Engine, TableId) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: Engine = SubscriptionEngine::new(database, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(7, sql))
        .expect("the aggregate registers");
    assert!(
        matches!(registered.tier, Tier::InProcess(_)),
        "`{sql}` is served in process, got {:?}",
        registered.tier
    );
    assert!(
        registered.not_served_because.is_none(),
        "`{sql}` is served, so it carries no reason not to be: {:?}",
        registered.not_served_because
    );
    subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![components],
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("the empty seed lands");
    (engine, table)
}

/// `(id, amount, whole, approx)`.
fn row(amount: Value<Postgres>) -> Vec<Value<Postgres>> {
    vec![Value::Int(1), amount, Value::Int(1), Value::Float(1.0)]
}

fn inserted(
    engine: &mut Engine,
    table: TableId,
    amount: Value<Postgres>,
    lsn: u64,
) -> Option<AggValue> {
    let event = TestEvent::insert(table, row(amount))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn));
    engine
        .aggregate_updates(&event)
        .expect("the event folds")
        .updates
        .first()
        .and_then(subql::AggregateValueUpdate::folded_value)
}

fn decimal(text: &str) -> Value<Postgres> {
    Value::Decimal(BigDecimal::from_str(text).unwrap())
}

/// The finding: an insert over a `NUMERIC` column reports the new sum
/// rather than nothing at all.
#[test]
fn decimal_column_sum_folds() {
    let (mut engine, table) = served(
        "SELECT SUM(amount) FROM t",
        vec![Value::Null, Value::Int(0)],
    );
    assert_eq!(
        inserted(&mut engine, table, decimal("10.25"), 10),
        Some(AggValue::Sum(Some(NumericValue::Decimal(
            BigDecimal::from_str("10.25").unwrap()
        )))),
        "measured: the server answers 10.25, so silence is not an answer"
    );
}

/// And it keeps folding: a second row moves the total again, at the scale
/// the values carry.
#[test]
fn a_second_decimal_row_moves_the_total() {
    let (mut engine, table) = served(
        "SELECT SUM(amount) FROM t",
        vec![Value::Null, Value::Int(0)],
    );
    inserted(&mut engine, table, decimal("10.25"), 10);
    assert_eq!(
        inserted(&mut engine, table, decimal("2.5"), 20),
        Some(AggValue::Sum(Some(NumericValue::Decimal(
            BigDecimal::from_str("12.75").unwrap()
        )))),
        "measured: 12.75 at scale two"
    );
}

/// The folded cell keeps every digit it carried, not the nearest double.
///
/// `9007199254740993.25` is unrepresentable in `f64`, which would answer
/// `9007199254740992`, so this separates "the decimal reaches the fold"
/// from "the decimal reaches the fold intact".
#[test]
fn a_decimal_row_folds_exactly() {
    let (mut engine, table) = served(
        "SELECT SUM(amount) FROM t",
        vec![Value::Null, Value::Int(0)],
    );
    assert_eq!(
        inserted(&mut engine, table, decimal("9007199254740993.25"), 10),
        Some(AggValue::Sum(Some(NumericValue::Decimal(
            BigDecimal::from_str("9007199254740993.25").unwrap()
        )))),
        "the cell is added as itself rather than through a double"
    );
}

/// `AVG` over the same column was ignored for the same reason, so it is
/// pinned too, now at the scale PostgreSQL's own division picks.
#[test]
fn decimal_column_avg_folds() {
    let (mut engine, table) = served(
        "SELECT AVG(amount) FROM t",
        vec![Value::Null, Value::Int(0)],
    );
    assert_eq!(
        inserted(&mut engine, table, decimal("10.25"), 10),
        Some(AggValue::Avg(Some(subql::NumericValue::Decimal(
            BigDecimal::from_str("10.2500000000000000").unwrap()
        )))),
        "measured: PostgreSQL answers 10.2500000000000000"
    );
}

/// So was the variance family, which reads the same cell.
#[test]
fn decimal_column_variance_folds() {
    let (mut engine, table) = served(
        "SELECT VAR_POP(amount) FROM t",
        vec![Value::Null, Value::Null, Value::Int(0)],
    );
    assert_eq!(
        inserted(&mut engine, table, decimal("10.25"), 10),
        Some(AggValue::Real(Some(0.0))),
        "one row has no spread, which is 0 rather than nothing"
    );
}

/// The rule, stated directly: every column type registration admits must
/// move the value. Registration accepts `Int`, `Float` and `Decimal`
/// columns and refuses the rest with a typed reason, so a served
/// subscription that reports nothing is the defect this phase closes.
#[test]
fn every_served_numeric_column_folds() {
    for (column, cell) in [
        ("whole", Value::Int(3)),
        ("approx", Value::Float(2.5)),
        ("amount", decimal("10.25")),
    ] {
        let (mut engine, table) = served(
            &format!("SELECT SUM({column}) FROM t"),
            vec![Value::Null, Value::Int(0)],
        );
        assert!(
            inserted(&mut engine, table, cell, 10).is_some(),
            "`SUM({column})` is served, so it has to fold"
        );
    }
}

/// A column type registration refuses is refused at registration, with the
/// reason on the registration, rather than being served and then skipped.
#[test]
fn an_unfoldable_column_is_refused_rather_than_ignored() {
    let database =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE u (id INT PRIMARY KEY, label TEXT)")
            .unwrap();
    let mut engine: Engine = SubscriptionEngine::new(database, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(7, "SELECT SUM(label) FROM u"))
        .expect("it registers on a re-read tier");
    assert!(
        matches!(
            registered.not_served_because,
            Some(subql::NotServed::UnfoldableAggregate { .. })
        ),
        "expected a typed reason naming the column, got {:?}",
        registered.not_served_because
    );
}
