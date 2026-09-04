//! The default `LIKE` escape character, measured against each engine.
//!
//! PostgreSQL and MySQL both give `LIKE` a default escape character,
//! backslash, with no `ESCAPE` clause present: `'a%b' LIKE 'a\%b'` is a row
//! in PostgreSQL and `1` in MySQL, because the escaped `%` is a literal
//! percent sign. SubQL served the form in process and treated the backslash
//! as an ordinary character, so it answered no-match.
//!
//! SQLite has no default escape at all. There the backslash is literal, so
//! the same pattern matches a string that really contains one, and the fix
//! is per dialect rather than global.
//!
//! Measured on PostgreSQL 16.11, MySQL 8.4.11 and SQLite 3.51.1. The
//! escape applies to any following character, not only to a wildcard:
//! `'ab' LIKE 'a\b'` is true on both engines that have the escape.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE names (id INT PRIMARY KEY, name TEXT)";

/// MySQL matches `LIKE` under the column's collation, and its server
/// default folds case, which no in-process comparison reproduces. A binary
/// collation is reproducible, so that is where the escape rule is asserted.
const MYSQL_DDL: &str = "CREATE TABLE names (id INT PRIMARY KEY, name TEXT COLLATE utf8mb4_bin)";

/// One `names` row carrying `name`, for a backend whose text payload is a
/// `String`.
macro_rules! notifies {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $name:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "names").expect("names is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("the predicate registers");
        let row = vec![Value::Int(1), Value::String($name.to_string())];
        let notifications = engine
            .consumers(&TestEvent::insert(table, row))
            .expect("dispatch succeeds");
        !notifications.inserted().is_empty()
    }};
}

fn pg_notifies(predicate: &str, name: &str) -> bool {
    notifies!(Postgres, PostgreSqlDialect, DDL, predicate, name)
}

fn mysql_notifies(predicate: &str, name: &str) -> bool {
    notifies!(MySql, MySqlDialect, MYSQL_DDL, predicate, name)
}

fn sqlite_notifies(predicate: &str, name: &str) -> bool {
    notifies!(SQLite, SQLiteDialect, DDL, predicate, name)
}

/// The finding: an escaped wildcard is a literal wildcard character.
#[test]
fn like_backslash_escapes_a_wildcard() {
    assert!(
        pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\%b'", "a%b"),
        "the escaped percent sign matches a literal percent sign"
    );
    assert!(
        !pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\%b'", "axxb"),
        "an escaped percent sign is no longer a wildcard"
    );
    assert!(
        pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\_b'", "a_b"),
        "the same rule holds for the single-character wildcard"
    );
    assert!(
        !pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\_b'", "axb"),
        "an escaped underscore is no longer a wildcard"
    );
}

/// An unescaped wildcard is still a wildcard: the escape changes one
/// character, not the pattern language.
#[test]
fn unescaped_wildcards_still_match() {
    assert!(
        pg_notifies("SELECT * FROM names WHERE name LIKE 'a%b'", "axxb"),
        "an ordinary pattern is unaffected"
    );
    assert!(
        pg_notifies("SELECT * FROM names WHERE name LIKE 'a_b'", "axb"),
        "and so is the single-character wildcard"
    );
}

/// The escape applies to whatever follows it, including an ordinary
/// character, and an escaped escape is one literal escape character.
#[test]
fn the_escape_applies_to_any_following_character() {
    assert!(
        pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\b'", "ab"),
        "escaping an ordinary character yields that character"
    );
    assert!(
        !pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\b'", r"a\b"),
        "the escape is consumed, so it does not match itself"
    );
    assert!(
        pg_notifies(r"SELECT * FROM names WHERE name LIKE 'a\\b'", r"a\b"),
        "an escaped escape matches one literal escape character"
    );
}

/// The case-insensitive form honours the escape too.
#[test]
fn case_insensitive_like_honours_the_escape() {
    assert!(
        pg_notifies(r"SELECT * FROM names WHERE name ILIKE 'a\%b'", "A%B"),
        "ILIKE folds case and still escapes the wildcard"
    );
    assert!(
        !pg_notifies(r"SELECT * FROM names WHERE name ILIKE 'a\%b'", "AxxB"),
        "folding case does not restore the wildcard"
    );
}

/// MySQL has the same default escape and answers `1` for the same pattern.
#[test]
fn mysql_has_the_same_default_escape() {
    assert!(
        mysql_notifies(r"SELECT * FROM names WHERE name LIKE 'a\%b'", "a%b"),
        "MySQL answers 1 for the escaped wildcard"
    );
    assert!(
        !mysql_notifies(r"SELECT * FROM names WHERE name LIKE 'a\%b'", "axxb"),
        "and 0 once the wildcard is escaped"
    );
}

/// SQLite has no default escape: the backslash is an ordinary character
/// there, so the pattern matches a string that contains one and not the
/// string PostgreSQL matches.
#[test]
fn sqlite_has_no_default_like_escape() {
    assert!(
        !sqlite_notifies(r"SELECT * FROM names WHERE name LIKE 'a\%b'", "a%b"),
        "SQLite does not escape, so a literal percent sign is not a match"
    );
    assert!(
        sqlite_notifies(r"SELECT * FROM names WHERE name LIKE 'a\%b'", r"a\%b"),
        "the backslash is matched literally and the percent sign is a wildcard"
    );
}
