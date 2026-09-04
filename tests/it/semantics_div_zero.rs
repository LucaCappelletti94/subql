//! Division and modulo by zero, measured.
//!
//! subql answers no-match today, which is right for two engines and wrong
//! for the third. Measured 2026-09-04 on PostgreSQL 16.11, MySQL 8.4.11 and
//! SQLite 3.51.1:
//!
//! ```text
//! expression              pg                     mysql   sqlite
//! 1 / 0                   ERROR division by zero  NULL    NULL
//! 1 % 0                   ERROR division by zero  NULL    NULL
//! 1.0::float8 / 0.0       ERROR division by zero  NULL    NULL
//! 1.0::numeric / 0        ERROR division by zero  NULL    NULL
//! ```
//!
//! So PostgreSQL raises for every numeric type and both operators, which is
//! the per-subscription evaluation failure this reuses from the overflow
//! work, while MySQL and SQLite answer `NULL`, which composes to
//! `Tri::Unknown` and therefore to no-match. The MySQL row is worth naming:
//! `ERROR_FOR_DIVISION_BY_ZERO` is in its `sql_mode`, and a `SELECT` still
//! answers `NULL` with warning 1365. That mode raises on `INSERT` and
//! `UPDATE`, which is a write path subql does not evaluate.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::compiler::vm::refusal::{ArithmeticOp, EvaluationRefusal};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT, rate DOUBLE PRECISION)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER, rate REAL)";

macro_rules! dispatch {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $cells:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("the predicate registers");
        engine
            .consumers(&TestEvent::insert(table, $cells))
            .expect("dispatch succeeds")
    }};
}

/// `(id, qty, rate)`.
fn row(qty: i64, rate: f64) -> Vec<Value<Postgres>> {
    vec![Value::Int(1), Value::Int(qty), Value::Float(rate)]
}

/// The finding: PostgreSQL raises, so the subscription's evaluation fails
/// rather than quietly not matching.
#[test]
fn pg_division_by_zero_fails_the_subscription() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE 100 / qty > 0",
        row(0, 1.0)
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.refusal)
            .collect::<Vec<_>>(),
        vec![EvaluationRefusal::DivisionByZero {
            operation: ArithmeticOp::Divide,
        }],
        "the report names the operator that could not be evaluated"
    );
    assert!(notifications.inserted().is_empty());
}

/// Modulo raises the same error, measured, so it is not left behind.
#[test]
fn pg_modulo_by_zero_fails_the_subscription() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE 100 % qty > 0",
        row(0, 1.0)
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.refusal)
            .collect::<Vec<_>>(),
        vec![EvaluationRefusal::DivisionByZero {
            operation: ArithmeticOp::Modulo,
        }],
    );
}

/// Float division raises too: the rule is the operator's, not the integer
/// type's.
#[test]
fn pg_float_division_by_zero_fails_the_subscription() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE 100.0 / rate > 0",
        row(1, 0.0)
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.refusal)
            .collect::<Vec<_>>(),
        vec![EvaluationRefusal::DivisionByZero {
            operation: ArithmeticOp::Divide,
        }],
        "measured as ERROR for float8 as well as for bigint"
    );
}

/// The guard: MySQL and SQLite answer `NULL`, which is unknown and
/// therefore no-match, and no failure is reported. This is not a RED test;
/// it exists so the correction cannot overreach to the other two engines.
#[test]
fn mysql_and_sqlite_division_by_zero_is_null() {
    let mysql = dispatch!(
        MySql,
        MySqlDialect,
        DDL,
        "SELECT * FROM t WHERE 100 / qty > 0",
        vec![Value::<MySql>::Int(1), Value::Int(0), Value::Float(1.0)]
    );
    assert!(
        mysql.evaluation_failures().is_empty() && mysql.inserted().is_empty(),
        "MySQL answers NULL, so the row is unknown rather than refused"
    );

    let sqlite = dispatch!(
        SQLite,
        SQLiteDialect,
        SQLITE_DDL,
        "SELECT * FROM t WHERE 100 / qty > 0",
        vec![Value::<SQLite>::Int(1), Value::Int(0), Value::Float(1.0)]
    );
    assert!(
        sqlite.evaluation_failures().is_empty() && sqlite.inserted().is_empty(),
        "and so does SQLite"
    );
}

/// The control: a divisor that is not zero is unaffected on every backend.
#[test]
fn sound_division_still_answers() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE 100 / qty > 9",
        row(10, 1.0)
    );
    assert!(notifications.evaluation_failures().is_empty());
    assert_eq!(notifications.inserted(), &[1], "100 / 10 is above 9");
}

/// Unifying the integer path caught a second case: `i64::MIN / -1` has no
/// quotient that fits. Measured, both raising engines answer
/// `out of range` for it and SQLite promotes to a real, which is the same
/// rule as any other overflow, so it is reported as one rather than as a
/// division failure.
#[test]
fn the_one_quotient_that_does_not_fit_is_an_overflow() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE qty / -1 > 0",
        row(i64::MIN, 1.0)
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.refusal)
            .collect::<Vec<_>>(),
        vec![EvaluationRefusal::IntegerOverflow {
            operation: ArithmeticOp::Divide,
        }],
        "measured as `bigint out of range`, not as a division error"
    );

    let sqlite = dispatch!(
        SQLite,
        SQLiteDialect,
        SQLITE_DDL,
        "SELECT * FROM t WHERE qty / -1 > 0",
        vec![
            Value::<SQLite>::Int(1),
            Value::Int(i64::MIN),
            Value::Float(1.0)
        ]
    );
    assert!(
        sqlite.evaluation_failures().is_empty(),
        "SQLite promotes it to a real, measured as 9.22e18"
    );
    assert_eq!(
        sqlite.inserted(),
        &[1],
        "and the promoted quotient is above zero"
    );
}

/// Modulo has no overflow case, which the checked-arithmetic table would
/// otherwise invent: Rust's `checked_rem` reports `None` for
/// `i64::MIN % -1` because the *division* would overflow, while the
/// remainder itself is 0 and fits. Measured as 0 on PostgreSQL, MySQL and
/// SQLite alike.
#[test]
fn modulo_has_no_overflow_case() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE qty % -1 = 0",
        row(i64::MIN, 1.0)
    );
    assert!(
        notifications.evaluation_failures().is_empty(),
        "no engine refuses this: {:?}",
        notifications.evaluation_failures()
    );
    assert_eq!(notifications.inserted(), &[1], "the remainder is 0");

    let mysql = dispatch!(
        MySql,
        MySqlDialect,
        DDL,
        "SELECT * FROM t WHERE qty % -1 = 0",
        vec![
            Value::<MySql>::Int(1),
            Value::Int(i64::MIN),
            Value::Float(1.0)
        ]
    );
    assert!(mysql.evaluation_failures().is_empty());
    assert_eq!(mysql.inserted(), &[1]);

    let sqlite = dispatch!(
        SQLite,
        SQLiteDialect,
        SQLITE_DDL,
        "SELECT * FROM t WHERE qty % -1 = 0",
        vec![
            Value::<SQLite>::Int(1),
            Value::Int(i64::MIN),
            Value::Float(1.0)
        ]
    );
    assert!(sqlite.evaluation_failures().is_empty());
    assert_eq!(sqlite.inserted(), &[1]);
}
