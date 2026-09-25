//! `LIKE ... ESCAPE`, the escape character written into the statement.
//!
//! Measured 2026-09-24 on PostgreSQL 16, MySQL 8.0.46 and SQLite 3.51:
//!
//! ```text
//! expression                              pg        mysql      sqlite
//! 'a%b' LIKE 'a!%b' ESCAPE '!'            t         1          1
//! 'axb' LIKE 'a!%b' ESCAPE '!'            f         0          0
//! 'ab'  LIKE 'a!b'  ESCAPE '!'            t         1          1
//! 'a%'  LIKE 'a%%'  ESCAPE '%'            t         1          1
//! 'ab'  LIKE 'a%%'  ESCAPE '%'            f         0          0
//! 'a'   LIKE 'a!'   ESCAPE '!'            f         0          0
//! 'ab'  LIKE 'a!'   ESCAPE '!'            raises    0          0
//! 'axb' LIKE 'a%b'  ESCAPE ''             t         see below  error
//! 'ab'  LIKE 'ab'   ESCAPE 'xy'           error     error      error
//! 'a%b' LIKE 'aé%b' ESCAPE 'é'            t         error      1
//! ```
//!
//! The written character replaces the engine's default escape, so a
//! backslash is ordinary under `ESCAPE '!'`. A pattern ending with the
//! escape answers as the engine's own dangling rule says, which SQLite has
//! too once a clause names an escape. `ESCAPE ''` means no escape on
//! PostgreSQL, and on MySQL it depends on the session's
//! `NO_BACKSLASH_ESCAPES`, which subql cannot see, so there it is routed.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, label TEXT COLLATE \"C\")";
const MYSQL_DDL: &str =
    "CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(64) COLLATE utf8mb4_bin)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)";

/// A backend on the standard carriers.
trait Matching: Backend<Int = i64, String = String> + subql::compiler::SqlLiteralParse {}

impl<B> Matching for B where
    B: Backend<Int = i64, String = String> + subql::compiler::SqlLiteralParse
{
}

/// What an engine did with `predicate` over one row whose `label` is `text`.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Selected,
    NotSelected,
    /// The row was refused, as an engine that raises refuses it.
    Refused,
    /// Not served in process.
    Routed,
}

fn outcome<B, D>(ddl: &str, dialect: D, predicate: &str, text: &str) -> Outcome
where
    B: Matching<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let table = catalog_helpers::table_id::<Postgres, _>(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    let registered = engine
        .register(SubscriptionRequest::new(
            7u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        ))
        .unwrap_or_else(|error| panic!("{predicate} registers on some tier, got {error:?}"));
    if let Some(reason) = registered.not_served_because {
        assert!(
            matches!(reason, NotServed::UnsupportedSql(_)),
            "{predicate}: {reason}"
        );
        return Outcome::Routed;
    }
    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::String(text.to_string())],
        ))
        .unwrap();
    if !notifications.evaluation_failures().is_empty() {
        Outcome::Refused
    } else if notifications.inserted().is_empty() {
        Outcome::NotSelected
    } else {
        Outcome::Selected
    }
}

fn pg(predicate: &str, text: &str) -> Outcome {
    outcome::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, predicate, text)
}

fn mysql(predicate: &str, text: &str) -> Outcome {
    outcome::<MySql, _>(MYSQL_DDL, MySqlDialect {}, predicate, text)
}

fn sqlite(predicate: &str, text: &str) -> Outcome {
    outcome::<SQLite, _>(SQLITE_DDL, SQLiteDialect {}, predicate, text)
}

/// The rows of the measured table every engine answers alike.
#[test]
fn an_escape_clause_answers_as_every_engine_does() {
    use Outcome::{NotSelected, Selected};
    for (predicate, text, expected) in [
        ("label LIKE 'a!%b' ESCAPE '!'", "a%b", Selected),
        ("label LIKE 'a!%b' ESCAPE '!'", "axb", NotSelected),
        ("label LIKE 'a!_b' ESCAPE '!'", "axb", NotSelected),
        ("label LIKE 'a!b' ESCAPE '!'", "ab", Selected),
        ("label LIKE 'a%%' ESCAPE '%'", "a%", Selected),
        ("label LIKE 'a%%' ESCAPE '%'", "ab", NotSelected),
        ("label LIKE 'a!' ESCAPE '!'", "a", NotSelected),
        ("label NOT LIKE 'a!%b' ESCAPE '!'", "axb", Selected),
    ] {
        assert_eq!(
            pg(predicate, text),
            expected,
            "PostgreSQL {predicate} over {text}"
        );
        assert_eq!(
            mysql(predicate, text),
            expected,
            "MySQL {predicate} over {text}"
        );
        assert_eq!(
            sqlite(predicate, text),
            expected,
            "SQLite {predicate} over {text}"
        );
    }
}

/// The written escape replaces the default one, so a backslash is an
/// ordinary character under `ESCAPE '!'` on the engines where it escapes by
/// default.
#[test]
fn the_written_escape_replaces_the_default_one() {
    assert_eq!(
        pg(r"label LIKE 'a\%' ESCAPE '!'", r"a\xyz"),
        Outcome::Selected
    );
    assert_eq!(
        pg(r"label LIKE 'a\%' ESCAPE '!'", "a%"),
        Outcome::NotSelected
    );
    assert_eq!(pg("label LIKE 'a%b' ESCAPE ''", "axb"), Outcome::Selected);
    assert_eq!(pg(r"label LIKE 'a\%' ESCAPE ''", r"a\x"), Outcome::Selected);
}

/// A pattern ending with the written escape, reached with input left,
/// answers as the engine's dangling rule says.
#[test]
fn a_dangling_written_escape_answers_as_the_engine_does() {
    assert_eq!(pg("label LIKE 'a!' ESCAPE '!'", "ab"), Outcome::Refused);
    assert_eq!(
        mysql("label LIKE 'a!' ESCAPE '!'", "ab"),
        Outcome::NotSelected
    );
    assert_eq!(
        sqlite("label LIKE 'a!' ESCAPE '!'", "ab"),
        Outcome::NotSelected
    );
}

/// `ILIKE` takes the clause too, and folds as it does without one.
#[test]
fn ilike_reads_the_written_escape() {
    assert_eq!(
        pg("label ILIKE 'A!%B' ESCAPE '!'", "a%b"),
        Outcome::Selected
    );
    assert_eq!(
        pg("label ILIKE 'A!%B' ESCAPE '!'", "axb"),
        Outcome::NotSelected
    );
}

/// A clause an engine rejects or reads by a setting subql cannot see is
/// routed to the engine.
#[test]
fn a_clause_the_engine_rejects_or_reads_by_setting_is_routed() {
    for predicate in ["label LIKE 'ab' ESCAPE 'xy'", "label LIKE 'ab' ESCAPE NULL"] {
        assert_eq!(
            pg(predicate, "ab"),
            Outcome::Routed,
            "PostgreSQL {predicate}"
        );
        assert_eq!(mysql(predicate, "ab"), Outcome::Routed, "MySQL {predicate}");
        assert_eq!(
            sqlite(predicate, "ab"),
            Outcome::Routed,
            "SQLite {predicate}"
        );
    }
    assert_eq!(mysql("label LIKE 'a%b' ESCAPE ''", "axb"), Outcome::Routed);
    assert_eq!(sqlite("label LIKE 'a%b' ESCAPE ''", "axb"), Outcome::Routed);
    assert_eq!(
        mysql("label LIKE 'aé%b' ESCAPE 'é'", "a%b"),
        Outcome::Routed
    );
    assert_eq!(pg("label LIKE 'aé%b' ESCAPE 'é'", "a%b"), Outcome::Selected);
    assert_eq!(
        sqlite("label LIKE 'aé%b' ESCAPE 'é'", "a%b"),
        Outcome::Selected
    );
}

/// The escape is part of what a filter asks, so two escapes over one pattern
/// are two predicates and never share an answer.
#[test]
fn two_escapes_over_one_pattern_are_two_predicates() {
    let database = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, PostgreSqlDialect {});
    let mut hashes = Vec::new();
    for (consumer, predicate) in [
        (1u64, "label LIKE 'a!%' ESCAPE '!'"),
        (2u64, r"label LIKE 'a!%' ESCAPE '\'"),
        (3u64, "label LIKE 'a!%'"),
    ] {
        let registered = engine
            .register(SubscriptionRequest::new(
                consumer,
                format!("SELECT * FROM t WHERE {predicate}"),
            ))
            .unwrap();
        let subql::Tier::InProcess(served) = registered.tier else {
            panic!("{predicate} is served in process");
        };
        assert!(served.created_new_predicate, "{predicate}");
        hashes.push(served.predicate_hash);
    }
    hashes.sort_unstable();
    hashes.dedup();
    assert_eq!(hashes.len(), 3);
    let table = catalog_helpers::table_id::<Postgres, _>(&engine_database(), "t").unwrap();
    let notified = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::String("a%".into())],
        ))
        .unwrap()
        .inserted()
        .to_vec();
    assert_eq!(notified, vec![1], "only `!` escapes the `%`");
}

fn engine_database() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap()
}
