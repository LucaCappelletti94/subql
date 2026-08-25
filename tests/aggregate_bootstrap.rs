//! Bootstrap and reset path for in-process delta aggregators.
//!
//! `Served::aggregate_bootstrap` bundles a runnable component seed
//! query per `AggSpec` with its decode kinds, and
//! `Install<AggregateSeedInstall>` decodes the returned component
//! row into the running total the engine holds. Together they let a caller
//! start an in-process aggregate and start it over after the resets subql
//! mandates (a permission change, through `reset_aggregate_value`), the same
//! courtesy `Tier::Scalar` already gives `MIN`/`MAX`.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, ScalarKind, Value};
use subql::testing::TestEvent;
use subql::{AggValue, AggregateBootstrap, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> Engine {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    SubscriptionEngine::new(db, PostgreSqlDialect {})
}

fn bootstrap_of(sql: &str) -> Option<AggregateBootstrap> {
    engine()
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap()
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
}

/// Register `sql`, hand it `row` as its starting numbers, and answer with the
/// value it then holds. Nothing has been folded against it, so the read cannot
/// have raced a change and needs no stream position.
fn seeded_value(sql: &str, row: &[Value<Postgres>]) -> AggValue {
    let mut engine = engine();
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap();
    let updates = subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![row.to_vec()],
            read_at: None,
        },
    )
    .expect("the starting numbers land");
    assert_eq!(updates.len(), 1);
    updates[0]
        .folded_value()
        .expect("ungrouped install sets a value")
}

#[test]
fn bootstrap_sql_per_aggspec() {
    // (subscription SQL, expected component seed SQL).
    let cases = [
        ("SELECT COUNT(*) FROM t", "SELECT COUNT(*) AS c0 FROM t"),
        (
            "SELECT COUNT(amount) FROM t",
            "SELECT COUNT(amount) AS c0 FROM t",
        ),
        (
            "SELECT SUM(amount) FROM t",
            "SELECT SUM(amount) AS c0 FROM t",
        ),
        (
            "SELECT AVG(amount) FROM t",
            "SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t",
        ),
        (
            "SELECT VAR_POP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT VAR_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT STDDEV_POP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT STDDEV_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            bootstrap_of(sql).map(|b| b.sql).as_deref(),
            Some(expected),
            "bootstrap SQL mismatch for `{sql}`"
        );
    }
}

#[test]
fn bootstrap_sql_preserves_where() {
    assert_eq!(
        bootstrap_of("SELECT SUM(amount) FROM t WHERE amount > 10")
            .map(|b| b.sql)
            .as_deref(),
        Some("SELECT SUM(amount) AS c0 FROM t WHERE amount > 10"),
    );
    assert_eq!(
        bootstrap_of("SELECT COUNT(*) FROM t WHERE status = 'open'")
            .map(|b| b.sql)
            .as_deref(),
        Some("SELECT COUNT(*) AS c0 FROM t WHERE status = 'open'"),
    );
}

/// The per-column decode kinds are a pure function of the AggSpec and line
/// up one-to-one with the seed SQL columns.
#[test]
fn bootstrap_kinds_per_aggspec() {
    use ScalarKind::{Float, Int};
    let cases: [(&str, Vec<BuiltinKind>); 8] = [
        ("SELECT COUNT(*) FROM t", vec![Int]),
        ("SELECT COUNT(amount) FROM t", vec![Int]),
        ("SELECT SUM(amount) FROM t", vec![Float]),
        ("SELECT AVG(amount) FROM t", vec![Float, Int]),
        ("SELECT VAR_POP(amount) FROM t", vec![Float, Float, Int]),
        ("SELECT VAR_SAMP(amount) FROM t", vec![Float, Float, Int]),
        ("SELECT STDDEV_POP(amount) FROM t", vec![Float, Float, Int]),
        ("SELECT STDDEV_SAMP(amount) FROM t", vec![Float, Float, Int]),
    ];
    for (sql, expected) in cases {
        let bundle = bootstrap_of(sql).expect("aggregate registration has a bootstrap");
        assert_eq!(bundle.kinds, expected, "kinds mismatch for `{sql}`");
        let column_count = bundle.sql.matches(" AS c").count();
        assert_eq!(
            bundle.kinds.len(),
            column_count,
            "kinds length must match seed column count for `{sql}`"
        );
    }
}

#[test]
fn bootstrap_sql_is_row_subscription_safe() {
    assert_eq!(bootstrap_of("SELECT * FROM t WHERE amount > 1"), None);
    assert_eq!(bootstrap_of("SELECT * FROM t"), None);
}

#[test]
fn a_seed_row_decodes_into_the_value_it_describes() {
    // COUNT family: single `c` component.
    assert_eq!(
        seeded_value("SELECT COUNT(*) FROM t", &[Value::Int(5)]),
        AggValue::Count(5),
    );
    assert_eq!(
        seeded_value("SELECT COUNT(amount) FROM t", &[Value::Int(3)]),
        AggValue::Count(3),
    );
    // SUM: single `s` component, from an integer or float column.
    assert_eq!(
        seeded_value("SELECT SUM(amount) FROM t", &[Value::Int(10)]),
        AggValue::Sum(10.0),
    );
    assert_eq!(
        seeded_value("SELECT SUM(amount) FROM t", &[Value::Float(2.5)]),
        AggValue::Sum(2.5),
    );
    // AVG: `(s, c)` components.
    assert_eq!(
        seeded_value(
            "SELECT AVG(amount) FROM t",
            &[Value::Float(10.0), Value::Int(4)],
        ),
        AggValue::Real(Some(2.5)),
    );
    // VAR_POP: `(s, sq, c)`. amounts [2, 4, 6] -> sum=12, sum_sq=56, n=3.
    // var_pop = 56/3 - (12/3)^2 = 2.6666666666666665.
    assert_eq!(
        seeded_value(
            "SELECT VAR_POP(amount) FROM t",
            &[Value::Float(12.0), Value::Float(56.0), Value::Int(3)],
        ),
        AggValue::Real(Some(56.0 / 3.0 - 16.0)),
    );
    // STDDEV_POP over the same components is sqrt(var_pop).
    assert_eq!(
        seeded_value(
            "SELECT STDDEV_POP(amount) FROM t",
            &[Value::Float(12.0), Value::Float(56.0), Value::Int(3)],
        ),
        AggValue::Real(Some((56.0f64 / 3.0 - 16.0).sqrt())),
    );
}

#[test]
fn a_seed_over_an_empty_table_is_the_empty_value() {
    // Zero matching rows: COUNT returns 0, SUM/variance components are NULL.
    assert_eq!(
        seeded_value("SELECT COUNT(*) FROM t", &[Value::Int(0)]),
        AggValue::Count(0),
    );
    assert_eq!(
        seeded_value("SELECT SUM(amount) FROM t", &[Value::Null]),
        AggValue::Sum(0.0),
    );
    assert_eq!(
        seeded_value("SELECT AVG(amount) FROM t", &[Value::Null, Value::Int(0)],),
        AggValue::Real(None),
    );
    assert_eq!(
        seeded_value(
            "SELECT VAR_POP(amount) FROM t",
            &[Value::Null, Value::Null, Value::Int(0)],
        ),
        AggValue::Real(None),
    );
}

/// The reset contract is actually runnable: seeding again from the bootstrap
/// components computed over the current table equals a direct recompute.
#[test]
fn reseed_matches_recompute() {
    // A registration must expose runnable bootstrap SQL to seed again after a
    // reset for a permission change.
    assert!(bootstrap_of("SELECT AVG(amount) FROM t").is_some());

    let mut engine = engine();
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(
            1u64,
            "SELECT AVG(amount) FROM t",
        ))
        .unwrap();
    let subscription = registered.subscription_id;
    subql::Install::install(
        &mut engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![vec![Value::Float(3.0), Value::Int(1)]],
            read_at: None,
        },
    )
    .expect("the first numbers land");

    // A permission change moved the answer without any event saying so, so the
    // caller resets and reads again. Current table amounts: [2, 4, 6].
    assert!(engine.reset_aggregate_value(subscription));
    let updates = subql::Install::install(
        &mut engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![vec![Value::Float(12.0), Value::Int(3)]],
            read_at: None,
        },
    )
    .expect("the new starting numbers land");
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].folded_value(), Some(AggValue::Real(Some(4.0))));
}
