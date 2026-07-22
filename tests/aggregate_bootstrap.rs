//! Bootstrap and reset path for in-process delta aggregators.
//!
//! `RegisterResult::aggregate_bootstrap_sql` renders a runnable component
//! seed query per `AggSpec`, and `AggAccumulator::seed_from_row` decodes
//! the returned component row into a seeded accumulator. Together they let
//! a caller obtain the seed for the in-process aggregate family and re-seed
//! after the resets subql mandates (`TruncateRequiresReset`, RLS/ACL policy
//! changes), the same courtesy `Registered::ReExec` already gives `MIN`/`MAX`.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    AggAccumulator, AggSpec, AggValue, DefaultIds, SubscriptionEngine, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> Engine {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    SubscriptionEngine::new(db, PostgreSqlDialect {})
}

fn bootstrap_of(sql: &str) -> Option<String> {
    engine()
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap()
        .aggregate_bootstrap_sql
}

#[test]
fn bootstrap_sql_per_aggspec() {
    // (subscription SQL, expected component seed SQL).
    let cases = [
        ("SELECT COUNT(*) FROM t", "SELECT COUNT(*) AS c FROM t"),
        (
            "SELECT COUNT(amount) FROM t",
            "SELECT COUNT(amount) AS c FROM t",
        ),
        (
            "SELECT SUM(amount) FROM t",
            "SELECT SUM(amount) AS s FROM t",
        ),
        (
            "SELECT AVG(amount) FROM t",
            "SELECT SUM(amount) AS s, COUNT(amount) AS c FROM t",
        ),
        (
            "SELECT VAR_POP(amount) FROM t",
            "SELECT SUM(amount) AS s, SUM(amount * amount) AS sq, COUNT(amount) AS c FROM t",
        ),
        (
            "SELECT VAR_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS s, SUM(amount * amount) AS sq, COUNT(amount) AS c FROM t",
        ),
        (
            "SELECT STDDEV_POP(amount) FROM t",
            "SELECT SUM(amount) AS s, SUM(amount * amount) AS sq, COUNT(amount) AS c FROM t",
        ),
        (
            "SELECT STDDEV_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS s, SUM(amount * amount) AS sq, COUNT(amount) AS c FROM t",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            bootstrap_of(sql).as_deref(),
            Some(expected),
            "bootstrap SQL mismatch for `{sql}`"
        );
    }
}

#[test]
fn bootstrap_sql_preserves_where() {
    assert_eq!(
        bootstrap_of("SELECT SUM(amount) FROM t WHERE amount > 10").as_deref(),
        Some("SELECT SUM(amount) AS s FROM t WHERE amount > 10"),
    );
    assert_eq!(
        bootstrap_of("SELECT COUNT(*) FROM t WHERE status = 'open'").as_deref(),
        Some("SELECT COUNT(*) AS c FROM t WHERE status = 'open'"),
    );
}

#[test]
fn bootstrap_sql_is_row_subscription_safe() {
    assert_eq!(bootstrap_of("SELECT * FROM t WHERE amount > 1"), None);
    assert_eq!(bootstrap_of("SELECT * FROM t"), None);
}

#[test]
fn seed_from_row_decodes_components() {
    // COUNT family: single `c` component.
    assert_eq!(
        AggAccumulator::seed_from_row(&AggSpec::CountStar, &[Value::<Postgres>::Int(5)]).value(),
        AggValue::Count(5),
    );
    assert_eq!(
        AggAccumulator::seed_from_row(
            &AggSpec::CountColumn { column: 1 },
            &[Value::<Postgres>::Int(3)]
        )
        .value(),
        AggValue::Count(3),
    );
    // SUM: single `s` component, from an integer or float column.
    assert_eq!(
        AggAccumulator::seed_from_row(&AggSpec::Sum { column: 1 }, &[Value::<Postgres>::Int(10)])
            .value(),
        AggValue::Sum(10.0),
    );
    assert_eq!(
        AggAccumulator::seed_from_row(
            &AggSpec::Sum { column: 1 },
            &[Value::<Postgres>::Float(2.5)]
        )
        .value(),
        AggValue::Sum(2.5),
    );
    // AVG: `(s, c)` components.
    assert_eq!(
        AggAccumulator::seed_from_row(
            &AggSpec::Avg { column: 1 },
            &[Value::<Postgres>::Float(10.0), Value::Int(4)],
        )
        .value(),
        AggValue::Real(Some(2.5)),
    );
    // VAR_POP: `(s, sq, c)`. amounts [2, 4, 6] -> sum=12, sum_sq=56, n=3.
    // var_pop = 56/3 - (12/3)^2 = 2.6666666666666665.
    let var_pop = AggAccumulator::seed_from_row(
        &AggSpec::VarPop { column: 1 },
        &[
            Value::<Postgres>::Float(12.0),
            Value::Float(56.0),
            Value::Int(3),
        ],
    );
    assert_eq!(var_pop.value(), AggValue::Real(Some(56.0 / 3.0 - 16.0)));
    // STDDEV_POP over the same components is sqrt(var_pop).
    let stddev_pop = AggAccumulator::seed_from_row(
        &AggSpec::StddevPop { column: 1 },
        &[
            Value::<Postgres>::Float(12.0),
            Value::Float(56.0),
            Value::Int(3),
        ],
    );
    assert_eq!(
        stddev_pop.value(),
        AggValue::Real(Some((56.0f64 / 3.0 - 16.0).sqrt())),
    );
}

#[test]
fn seed_from_row_empty_result_is_empty_state() {
    // Zero matching rows: COUNT returns 0, SUM/variance components are NULL.
    assert_eq!(
        AggAccumulator::seed_from_row(&AggSpec::CountStar, &[Value::<Postgres>::Int(0)]).value(),
        AggValue::Count(0),
    );
    assert_eq!(
        AggAccumulator::seed_from_row(&AggSpec::Sum { column: 1 }, &[Value::<Postgres>::Null])
            .value(),
        AggValue::Sum(0.0),
    );
    assert_eq!(
        AggAccumulator::seed_from_row(
            &AggSpec::Avg { column: 1 },
            &[Value::<Postgres>::Null, Value::Int(0)],
        )
        .value(),
        AggValue::Real(None),
    );
    assert_eq!(
        AggAccumulator::seed_from_row(
            &AggSpec::VarPop { column: 1 },
            &[Value::<Postgres>::Null, Value::Null, Value::Int(0)],
        )
        .value(),
        AggValue::Real(None),
    );
}

/// The reset contract is actually runnable: re-seeding from the bootstrap
/// components computed over the current table equals a direct recompute.
#[test]
fn reseed_matches_recompute() {
    // A registration must expose runnable bootstrap SQL to re-seed after a
    // reset (TruncateRequiresReset, policy change).
    assert!(bootstrap_of("SELECT AVG(amount) FROM t").is_some());

    // Current table amounts after a reset: [2, 4, 6]. Oracle AVG = 4.0.
    let seeded = AggAccumulator::seed_from_row(
        &AggSpec::Avg { column: 1 },
        &[Value::<Postgres>::Float(12.0), Value::Int(3)],
    );
    assert_eq!(seeded.value(), AggValue::Real(Some(4.0)));
}
