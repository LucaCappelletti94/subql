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
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::compiler::vm::refusal::DanglingEscape;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, EvaluationRefusal, SubscriptionEngine, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE names (id INT PRIMARY KEY, name TEXT)";

/// `ILIKE` folds by collation, and only an ASCII-only folding is
/// reproducible, so the case-insensitive escape is asserted on a `C`
/// column. Phase D5 records the measurements behind that.
const PG_FOLDING_DDL: &str = "CREATE TABLE names (id INT PRIMARY KEY, name TEXT COLLATE \"C\")";

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

/// As [`pg_notifies`], on a column whose collation folds ASCII only.
fn pg_folding_notifies(predicate: &str, name: &str) -> bool {
    notifies!(Postgres, PostgreSqlDialect, PG_FOLDING_DDL, predicate, name)
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
        pg_folding_notifies(r"SELECT * FROM names WHERE name ILIKE 'a\%b'", "A%B"),
        "ILIKE folds case and still escapes the wildcard"
    );
    assert!(
        !pg_folding_notifies(r"SELECT * FROM names WHERE name ILIKE 'a\%b'", "AxxB"),
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

/// A pattern whose last character is the escape character escapes nothing.
/// Measured on 16.11: PostgreSQL raises `LIKE pattern must not end with
/// escape character` once its matcher reaches that character with input
/// still to read, and answers false when the input ran out first. So the
/// row that still has input to match is a per-subscription evaluation
/// failure, and the row that does not is a plain no-match.
///
/// The walk has reproduced that condition since this phase landed; what was
/// missing was a channel to report it, which the overflow work built.
#[test]
fn a_pattern_ending_with_the_escape_fails_the_subscription() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "names").expect("names is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let subscription = engine
        .register(SubscriptionRequest::new(
            1u64,
            r"SELECT * FROM names WHERE name LIKE 'a\'",
        ))
        .expect("the predicate registers")
        .subscription_id;

    let dispatch = |engine: &mut SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
                    name: &str| {
        engine
            .consumers(&TestEvent::insert(
                table,
                vec![Value::Int(1), Value::String(name.to_string())],
            ))
            .expect("dispatch succeeds")
    };

    // Input remains when the matcher arrives, which is what PostgreSQL
    // refuses.
    let refused = dispatch(&mut engine, "ab");
    assert_eq!(
        refused
            .evaluation_failures()
            .iter()
            .map(|failure| (failure.subscription_id, failure.refusal))
            .collect::<Vec<_>>(),
        vec![(subscription, EvaluationRefusal::LikePatternEndsWithEscape)],
        "the report names the subscription and the malformed pattern"
    );
    assert!(refused.inserted().is_empty());

    // The input ran out first, so the server answers false and so does this.
    let exhausted = dispatch(&mut engine, "a");
    assert!(
        exhausted.evaluation_failures().is_empty(),
        "PostgreSQL answers false here rather than raising"
    );
    assert!(exhausted.inserted().is_empty());
}

/// MySQL never raises for a dangling escape: measured, it answers 0
/// whether or not input remains, so its rule is no-match rather than
/// PostgreSQL's refusal. The constants are asserted because they are the
/// per-backend rule the walk consults.
///
/// The end-to-end half is a different answer from PostgreSQL's, and it is
/// not the escape rule that decides it. MySQL's own literal rules make a
/// backslash escape the closing quote, so the pattern is written `'a\\'`
/// there, and rendering that back out yields SQL MySQL itself rejects,
/// which the server confirms with error 1064. subql's canonicalizer sees
/// exactly that and declines to serve a predicate whose spelling does not
/// read back as itself, so the subscription becomes a database read. That
/// is the honest answer, not a wrong one, and it means no in-process
/// dangling escape arises under MySQL at all.
#[test]
fn mysql_pattern_ending_with_the_escape_is_a_no_match() {
    assert_eq!(
        <MySql as Backend>::LIKE_ESCAPE
            .expect("MySQL has a default escape")
            .dangling,
        DanglingEscape::NoMatch,
        "measured as 0 on 8.4.11, not an error"
    );
    assert_eq!(
        <Postgres as Backend>::LIKE_ESCAPE
            .expect("PostgreSQL has a default escape")
            .dangling,
        DanglingEscape::Fails,
        "and PostgreSQL raises, which is why the rule is per backend"
    );
    assert!(
        <SQLite as Backend>::LIKE_ESCAPE.is_none(),
        "SQLite has no default escape, so it can have no dangling one"
    );

    let db = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).expect("DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM names WHERE name LIKE 'a\\\\'",
        ))
        .expect("a read answers it, so registration succeeds");
    assert!(
        registered.served().is_none(),
        "the pattern has no canonical spelling MySQL would accept back"
    );
    assert!(
        registered
            .not_served_because
            .as_ref()
            .map(std::string::ToString::to_string)
            .is_some_and(|reason| reason.contains("canonical spelling")),
        "got {:?}",
        registered.not_served_because
    );
}
