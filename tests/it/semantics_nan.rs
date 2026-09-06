//! NaN comparison, measured against each engine rather than against IEEE.
//!
//! PostgreSQL defines its own order for `float4`/`float8`: NaN equals itself
//! and sorts above every non-NaN value, so `value = value` and `value > 100`
//! are both true for a NaN row. IEEE says the opposite, and IEEE is what
//! Rust's `PartialOrd` implements, so the in-process comparator answered
//! no-match where the database answers a row.
//!
//! The MySQL and SQLite cases here are guards on the split, not stream
//! reproductions: MySQL rejects NaN on the way into a `DOUBLE` column and
//! SQLite binds it as `NULL`, so neither can emit a NaN cell. They exist so
//! that applying PostgreSQL's rule to every backend fails.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

type PgEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;
type MySqlEngine = SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB>;
type SqliteEngine = SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB>;

const PG_DDL: &str = "CREATE TABLE readings (id INT PRIMARY KEY, value DOUBLE PRECISION, \
                      whole INT, exact NUMERIC)";
const MYSQL_DDL: &str = "CREATE TABLE readings (id INT PRIMARY KEY, value DOUBLE)";
const SQLITE_DDL: &str = "CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)";

fn pg_engine() -> (PgEngine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "readings").expect("readings is in the catalog");
    (SubscriptionEngine::new(db, PostgreSqlDialect {}), table)
}

/// One NaN row: the cell PostgreSQL orders above every number.
///
/// The PostgreSQL schema carries an `int` and a `numeric` beside the
/// float so a comparison can cross kinds; the other two engines cannot
/// hold a NaN at all, so their rows stay two columns wide.
fn nan_row<B: subql::backend::Backend<Int = i64, Float = f64>>() -> Vec<Value<B>> {
    vec![Value::Int(1), Value::Float(f64::NAN)]
}

/// The same row over the wider PostgreSQL schema.
fn pg_nan_row() -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::Float(f64::NAN),
        Value::Int(1),
        Value::Decimal(bigdecimal::BigDecimal::from(1)),
    ]
}

/// Whether the one subscription registered for `predicate` sees the NaN row.
fn pg_notifies(predicate: &str) -> bool {
    let (mut engine, table) = pg_engine();
    let registered = engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    assert!(
        registered.not_served_because.is_none(),
        "`{predicate}` is served in process, and this one was refused: {:?}",
        registered.not_served_because
    );
    let notifications = engine
        .consumers(&TestEvent::insert(table, pg_nan_row()))
        .expect("dispatch succeeds");
    !notifications.inserted().is_empty()
}

/// `WHERE value = value` holds for a NaN row in PostgreSQL, and `<>` does not.
#[test]
fn pg_nan_equals_itself() {
    assert!(
        pg_notifies("SELECT * FROM readings WHERE value = value"),
        "PostgreSQL answers `NaN = NaN` true, so the row is in the answer"
    );
    assert!(
        !pg_notifies("SELECT * FROM readings WHERE value <> value"),
        "the negation must not also hold, which a blanket `true` would give"
    );
}

/// NaN is PostgreSQL's largest float value, so it is above a literal bound
/// and never below one. A fractional literal is not an index range hint, so
/// this pair reaches the comparator with no pruning in front of it.
#[test]
fn pg_nan_sorts_above_every_value() {
    assert!(
        pg_notifies("SELECT * FROM readings WHERE value > 100.5"),
        "PostgreSQL sorts NaN above every non-NaN value"
    );
    assert!(
        !pg_notifies("SELECT * FROM readings WHERE value < 100.5"),
        "nothing is above NaN, so the row is not below the bound"
    );
}

/// The same question through the range index. An integer literal bound is an
/// index range hint, so the prefilter decides whether the row is a candidate
/// at all; if it prunes the NaN cell the comparator is never asked, and the
/// answer is wrong however correct the comparator is.
#[test]
fn pg_nan_range_probe_agrees_with_the_comparator() {
    assert!(
        pg_notifies("SELECT * FROM readings WHERE value >= 100"),
        "the range probe must keep a NaN candidate for the comparator to judge"
    );
    assert!(
        !pg_notifies("SELECT * FROM readings WHERE value <= 100"),
        "the probe may over-include, but the comparator still answers no"
    );
}

/// MySQL keeps the IEEE rule, because its engine has no NaN to order: the
/// server rejects one on the way into a `DOUBLE` column. This guards the
/// split, so a PostgreSQL rule applied to every backend fails here.
#[test]
fn mysql_keeps_the_ieee_rule() {
    let db = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "readings").expect("readings is in the catalog");
    let mut engine: MySqlEngine = SubscriptionEngine::new(db, MySqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM readings WHERE value = value",
        ))
        .expect("the predicate registers");
    let notifications = engine
        .consumers(&TestEvent::insert(table, nan_row()))
        .expect("dispatch succeeds");
    assert!(
        notifications.inserted().is_empty(),
        "MySQL has no NaN in a DOUBLE column, so the IEEE rule stands"
    );
}

/// SQLite's real counterpart: a bound NaN is stored as `NULL`, so the cell
/// that actually arrives off a SQLite stream is `Null`, and `value = value`
/// on it is unknown rather than a row. This is the case the engine can
/// really be handed.
#[test]
fn sqlite_stores_a_nan_as_null_and_null_is_not_self_equal() {
    let db = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "readings").expect("readings is in the catalog");
    let mut engine: SqliteEngine = SubscriptionEngine::new(db, SQLiteDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM readings WHERE value = value",
        ))
        .expect("the predicate registers");
    let notifications = engine
        .consumers(&TestEvent::insert(table, vec![Value::Int(1), Value::Null]))
        .expect("dispatch succeeds");
    assert!(
        notifications.inserted().is_empty(),
        "a NULL cell compares unknown, so the row is not in the answer"
    );
}

/// And the synthetic case, kept as a guard on the split: were PostgreSQL's
/// rule applied to every backend, a NaN cell would become self-equal here
/// too. No SQLite stream can produce this row.
#[test]
fn sqlite_keeps_the_ieee_rule() {
    let db = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "readings").expect("readings is in the catalog");
    let mut engine: SqliteEngine = SubscriptionEngine::new(db, SQLiteDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM readings WHERE value = value",
        ))
        .expect("the predicate registers");
    let notifications = engine
        .consumers(&TestEvent::insert(table, nan_row()))
        .expect("dispatch succeeds");
    assert!(
        notifications.inserted().is_empty(),
        "SQLite has no NaN of its own, so the IEEE rule stands"
    );
}

/// PostgreSQL's NaN order survives a widened comparison, which is where
/// the rule stopped short.
///
/// Measured on PostgreSQL 16.15 with `f = 'NaN'::float8`, `i = 1::int`
/// and `x = 1::numeric`:
///
/// ```text
/// f > i    t        f > x    t        i < f    t
/// ```
///
/// The engine widens the other operand and then applies its own float
/// order, so every one of those is true. `537ea04` wrote that order into
/// `Postgres::compare_scalars`, whose float arm only matches when both
/// operands are already floats: a float against an `int` or a `numeric`
/// fell through to the cross-kind path, which widened both sides and then
/// asked IEEE `partial_cmp`, getting `None` and dropping the row.
#[test]
fn pg_nan_outranks_a_widened_operand() {
    for predicate in [
        "value > whole",
        "value >= whole",
        "value > exact",
        "value >= exact",
    ] {
        assert!(
            pg_notifies(&format!("SELECT * FROM readings WHERE {predicate}")),
            "`{predicate}` is true for a NaN row, because NaN outranks every number \
             the engine widens to a double"
        );
    }
    for predicate in ["value < whole", "value <= whole", "value < exact"] {
        assert!(
            !pg_notifies(&format!("SELECT * FROM readings WHERE {predicate}")),
            "`{predicate}` is false for a NaN row: nothing is above NaN"
        );
    }
}

/// The same comparison with the operands the other way round, because an
/// order is only reproduced when it is antisymmetric.
///
/// Measured: `1::int < 'NaN'::float8` is true.
#[test]
fn a_widened_operand_ranks_below_pg_nan() {
    for predicate in ["whole < value", "whole <= value", "exact < value"] {
        assert!(
            pg_notifies(&format!("SELECT * FROM readings WHERE {predicate}")),
            "`{predicate}` is true: every number is below NaN"
        );
    }
    for predicate in ["whole > value", "whole >= value", "exact > value"] {
        assert!(
            !pg_notifies(&format!("SELECT * FROM readings WHERE {predicate}")),
            "`{predicate}` is false, which is the same fact read from the other side"
        );
    }
}
