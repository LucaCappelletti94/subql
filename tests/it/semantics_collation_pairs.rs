//! Two operands, two collations, and three engines that disagree about
//! what that even means.
//!
//! Both `text_rule` implementations proved each side reproducible and
//! then answered from one of them: the SQLite loop overwrote its rule per
//! operand so the *last* one decided, and the MySQL one took
//! `left.or(right)` so the first did. Neither asked whether the two
//! agree, and two of the three engines refuse the statement outright.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.0.46 and SQLite
//! 3.51.1:
//!
//! ```text
//! pg      c ("C")    = p ("POSIX")      ERROR could not determine which collation
//! pg      c ("C")    = d (default)      t, the default side yields
//! pg      c ("C")    = 'ab'             t, a literal carries no collation
//! mysql   b (_bin)   = n (_0900_bin)    ERROR 1267 illegal mix of collations
//! mysql   b (_bin)   = d (default)      1, the binary collation wins
//! sqlite  nocase_col = binary_col       1
//! sqlite  binary_col = nocase_col       0, so the leftmost collation decides
//! ```
//!
//! Three different right answers, and none of them is "whichever side we
//! looked at last". PostgreSQL's and MySQL's refusals are not database
//! reads either: `EXPLAIN` of the PostgreSQL statement plans without
//! complaint and the error arrives at execution, so a re-read raises the
//! identical error and `NotServedInProcess` would be a false promise.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::RegisterError;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// Register `predicate` against `ddl` and answer what registration said.
macro_rules! registers {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine.register(SubscriptionRequest::new(1u64, $predicate))
    }};
}

/// Dispatch one row through `predicate` and answer whether it matched.
macro_rules! notifies {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $cells:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        let registered = engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("the predicate registers");
        assert!(
            registered.not_served_because.is_none(),
            "this predicate is served in process, and it was refused: {:?}",
            registered.not_served_because
        );
        !engine
            .consumers(&TestEvent::insert(table, $cells))
            .expect("dispatch succeeds")
            .inserted()
            .is_empty()
    }};
}

/// PostgreSQL will not run a comparison between two differing named
/// collations, so registration says so rather than promising a read.
#[test]
fn pg_refuses_two_different_named_collations() {
    let ddl = "CREATE TABLE t (id INT PRIMARY KEY, c TEXT COLLATE \"C\", \
               p TEXT COLLATE \"POSIX\")";
    let refused = registers!(
        Postgres,
        PostgreSqlDialect,
        ddl,
        "SELECT * FROM t WHERE c = p"
    );
    match refused {
        Err(RegisterError::RefusedByEngine { reason, .. }) => assert!(
            reason.contains("could not determine which collation"),
            "the engine's own account travels with the refusal: {reason}"
        ),
        other => panic!("expected a refusal by the engine, got {other:?}"),
    }
}

/// And it runs one against the database default, which yields to the
/// named side.
#[test]
fn pg_resolves_a_named_collation_against_the_default() {
    let ddl = "CREATE TABLE t (id INT PRIMARY KEY, c TEXT COLLATE \"C\", d TEXT)";
    let cells = vec![
        Value::<Postgres>::Int(1),
        Value::String("ab".to_string()),
        Value::String("ab".to_string()),
    ];
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            ddl,
            "SELECT * FROM t WHERE c = d",
            cells
        ),
        "measured: PostgreSQL answers t, so this is served rather than refused"
    );
}

/// MySQL refuses the same shape, with its own error.
#[test]
fn mysql_refuses_two_different_named_collations() {
    let ddl = "CREATE TABLE t (id INT PRIMARY KEY, b VARCHAR(9) COLLATE utf8mb4_bin, \
               n VARCHAR(9) COLLATE utf8mb4_0900_bin)";
    let refused = registers!(MySql, MySqlDialect, ddl, "SELECT * FROM t WHERE b = n");
    match refused {
        Err(RegisterError::RefusedByEngine { reason, .. }) => assert!(
            reason.contains("Illegal mix of collations"),
            "MySQL's own account: {reason}"
        ),
        other => panic!("expected a refusal by the engine, got {other:?}"),
    }
}

/// SQLite runs it, and the leftmost collation decides.
///
/// Measured: `nocase_col = binary_col` is 1 and the reverse is 0. The
/// loop that resolved this overwrote its rule per operand, so the last
/// side decided and both directions answered alike.
#[test]
fn sqlite_takes_the_leftmost_collation() {
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, nc TEXT COLLATE NOCASE, \
               b TEXT COLLATE BINARY)";
    let cells = vec![
        Value::<SQLite>::Int(1),
        Value::String("a".to_string()),
        Value::String("A".to_string()),
    ];
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            ddl,
            "SELECT * FROM t WHERE nc = b",
            cells.clone()
        ),
        "measured: with NOCASE on the left, 'a' = 'A' is 1"
    );
    assert!(
        !notifies!(
            SQLite,
            SQLiteDialect,
            ddl,
            "SELECT * FROM t WHERE b = nc",
            cells
        ),
        "measured: with BINARY on the left it is 0, which is the asymmetry"
    );
}

/// A literal carries no collation, so the column's applies from either
/// side.
#[test]
fn sqlite_reads_a_literal_under_the_columns_collation() {
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, nc TEXT COLLATE NOCASE)";
    let cells = vec![Value::<SQLite>::Int(1), Value::String("a".to_string())];
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            ddl,
            "SELECT * FROM t WHERE nc = 'A'",
            cells.clone()
        ),
        "measured: the column's NOCASE applies"
    );
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            ddl,
            "SELECT * FROM t WHERE 'A' = nc",
            cells
        ),
        "and from the other side too, because the literal has none to contribute"
    );
}
