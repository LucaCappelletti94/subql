//! Case handling in a pattern is the collation's, not Rust's.
//!
//! `ILIKE` lowercased both operands per row with Rust's full Unicode
//! folding, which is no engine's rule and diverges in both directions.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15 in a `en_US.utf8` database,
//! MySQL 8.4.11 and SQLite 3.51.1:
//!
//! ```text
//! expression                        pg default  pg COLLATE "C"  rust to_lowercase
//! 'ABC' ILIKE 'abc'                 true        true            true
//! 'İ'   ILIKE 'i_'  (U+0130)        false       false           true
//! 'İ'   ILIKE '_'                   true        true            false
//! 'Ä'   ILIKE 'ä'                   true        false           true
//! 'ΣΟΦΟΣ' ILIKE 'σοφος'             false       false           true
//! ```
//!
//! PostgreSQL's `lower('İ')` is one character, `i`, where Rust's is two,
//! `i` plus a combining dot, so a length-changing fold makes `_` match a
//! character the server never produced. In the other direction PostgreSQL
//! folds a trailing sigma to `σ` while Rust folds it to `ς`, so a pattern
//! written with the final form matches in process and not on the server.
//! Neither is reproducible without the locale's own tables, and under
//! `C`/`POSIX` the server folds ASCII only, which is exactly reproducible.
//!
//! So `ILIKE` is served where the collation folds ASCII only, and refused
//! otherwise, which is the treatment Phase C6 gave a collation whose
//! ordering could not be reproduced.
//!
//! The other engines have no `ILIKE` at all: MySQL answers a syntax error,
//! and SQLite has no such keyword. What SQLite does have is a `LIKE` that
//! folds ASCII case by default, measured: `'ABC' LIKE 'abc'` is `1` and
//! `'Ä' LIKE 'ä'` is `0`. subql answered case-sensitively there, which is
//! a wrong answer at every mixed-case input, and it is the same
//! instruction and the same descriptor field, so it is corrected here.
//! MySQL's `LIKE` follows its collation, and subql serves only the binary
//! collations, where it is case-sensitive: measured, `_bin` answers `0`
//! and `_0900_ai_ci` answers `1` for the same comparison.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

/// `label` carries the database default collation, `strict` carries `C`.
const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, label TEXT, \
                      strict TEXT COLLATE \"C\")";
const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, \
                         label VARCHAR(64) COLLATE utf8mb4_bin)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)";

/// A backend on the standard carriers.
trait Matching: Backend<Int = i64, String = String> + subql::compiler::SqlLiteralParse {}

impl<B> Matching for B where
    B: Backend<Int = i64, String = String> + subql::compiler::SqlLiteralParse
{
}

/// Register `predicate`, insert one row whose text column holds `text`,
/// and answer whether the row matched. `None` when the predicate is not
/// served in process.
fn matched<B, D>(ddl: &str, dialect: D, predicate: &str, column: usize, text: &str) -> Option<bool>
where
    B: Matching<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let table = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    let registered = engine
        .register(SubscriptionRequest::new(7, predicate))
        .expect("the predicate registers on some tier");
    if registered.not_served_because.is_some() {
        return None;
    }
    let arity = if ddl == PG_DDL { 3 } else { 2 };
    let mut cells: Vec<Value<B>> = (0..arity).map(|_| Value::Null).collect();
    cells[0] = Value::Int(1);
    cells[column] = Value::String(text.to_string());
    Some(
        !engine
            .consumers(&TestEvent::insert(table, cells))
            .expect("dispatch succeeds")
            .inserted()
            .is_empty(),
    )
}

/// Why a predicate is not served, or `None` when it is.
fn refusal<B, D>(ddl: &str, dialect: D, predicate: &str) -> Option<NotServed<B>>
where
    B: Matching<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    engine
        .register(SubscriptionRequest::new(7, predicate))
        .expect("the predicate registers on some tier")
        .not_served_because
}

fn pg(predicate: &str, column: usize, text: &str) -> Option<bool> {
    matched::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, predicate, column, text)
}

/// The finding: `ILIKE` is served only where the collation folds ASCII
/// only, since no other folding is reproducible.
#[test]
fn ilike_is_restricted_to_a_reproducible_collation() {
    let refused = refusal::<Postgres, _>(
        PG_DDL,
        PostgreSqlDialect {},
        "SELECT * FROM t WHERE label ILIKE 'abc'",
    );
    assert!(
        matches!(refused, Some(NotServed::CollationNotReproducible { .. })),
        "the database default folds by locale, which is not reproducible, got {refused:?}"
    );
    assert!(
        refusal::<Postgres, _>(
            PG_DDL,
            PostgreSqlDialect {},
            "SELECT * FROM t WHERE strict ILIKE 'abc'",
        )
        .is_none(),
        "a `C` collation folds ASCII only, which is exactly reproducible"
    );
}

/// The length-changing fold: `lower('İ')` is one character on the server
/// and two in Rust, so `_` must not match what Rust's folding produced.
#[test]
fn ilike_underscore_survives_a_length_changing_fold() {
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE 'i_'", 2, "İ"),
        Some(false),
        "measured: the server answers false, and Rust's two-character fold answered true"
    );
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE '_'", 2, "İ"),
        Some(true),
        "measured: one character, so one wildcard matches it"
    );
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE 'i'", 2, "İ"),
        Some(false),
        "measured: a `C` collation folds ASCII only, so this letter is not `i`"
    );
}

/// Non-ASCII stays untouched under the collation subql serves, measured.
#[test]
fn ilike_leaves_non_ascii_alone_under_the_c_collation() {
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE 'ä'", 2, "Ä"),
        Some(false),
        "measured: `C` folds ASCII only, so these two are not equal"
    );
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE 'σοφος'", 2, "ΣΟΦΟΣ"),
        Some(false),
        "measured: the server answers false here even under its own locale"
    );
}

/// ASCII `ILIKE` is the control, and it matches in either direction.
#[test]
fn ascii_ilike_matches_either_way() {
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE 'abc'", 2, "ABC"),
        Some(true)
    );
    assert_eq!(
        pg("SELECT * FROM t WHERE strict ILIKE 'A%'", 2, "abc"),
        Some(true)
    );
}

/// PostgreSQL's `LIKE` is case-sensitive under every collation, measured,
/// so it is untouched.
#[test]
fn pg_like_stays_case_sensitive() {
    assert_eq!(
        pg("SELECT * FROM t WHERE label LIKE 'abc'", 1, "ABC"),
        Some(false),
        "measured: `'ABC' LIKE 'abc'` is false on PostgreSQL"
    );
}

/// SQLite's `LIKE` folds ASCII case by default, measured, which subql
/// answered case-sensitively.
#[test]
fn sqlite_like_folds_ascii_case() {
    assert_eq!(
        matched::<SQLite, _>(
            SQLITE_DDL,
            SQLiteDialect {},
            "SELECT * FROM t WHERE label LIKE 'abc'",
            1,
            "ABC",
        ),
        Some(true),
        "measured: SQLite answers 1 for `'ABC' LIKE 'abc'`"
    );
}

/// And only ASCII: its folding stops there, measured.
#[test]
fn sqlite_like_leaves_non_ascii_alone() {
    assert_eq!(
        matched::<SQLite, _>(
            SQLITE_DDL,
            SQLiteDialect {},
            "SELECT * FROM t WHERE label LIKE 'ä'",
            1,
            "Ä",
        ),
        Some(false),
        "measured: SQLite answers 0 for `'Ä' LIKE 'ä'`"
    );
}

/// MySQL's `LIKE` under a binary collation is case-sensitive, measured,
/// and the binary collations are the ones subql serves.
#[test]
fn mysql_like_under_a_binary_collation_stays_case_sensitive() {
    assert_eq!(
        matched::<MySql, _>(
            MYSQL_DDL,
            MySqlDialect {},
            "SELECT * FROM t WHERE label LIKE 'abc'",
            1,
            "ABC",
        ),
        Some(false),
        "measured: `_bin` answers 0 where `_0900_ai_ci` answers 1"
    );
}

/// An engine with no `ILIKE` has no answer to reproduce, so subql serves
/// none either.
#[test]
fn an_engine_without_ilike_does_not_serve_one() {
    assert!(
        refusal::<MySql, _>(
            MYSQL_DDL,
            MySqlDialect {},
            "SELECT * FROM t WHERE label ILIKE 'abc'",
        )
        .is_some(),
        "measured: MySQL answers a syntax error for ILIKE, so there is nothing to fold"
    );
    assert!(
        refusal::<SQLite, _>(
            SQLITE_DDL,
            SQLiteDialect {},
            "SELECT * FROM t WHERE label ILIKE 'abc'",
        )
        .is_some(),
        "SQLite has no ILIKE keyword either"
    );
}
