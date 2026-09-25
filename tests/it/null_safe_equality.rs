//! Null-safe equality, `IS [NOT] DISTINCT FROM` and MySQL's `<=>`.
//!
//! Each engine accepts one spelling and rejects the other, measured
//! 2026-09-24:
//!
//! ```text
//! spelling                    PostgreSQL 16   MySQL 8.0.46   SQLite 3.51
//! a IS NOT DISTINCT FROM b    accepted        ERROR 1064     accepted
//! a <=> b                     no operator     accepted       syntax error
//! ```
//!
//! So each backend serves its own spelling in process and routes the other
//! to a database read, where the engine's own error surfaces. Agreement
//! with the engines themselves is the differential sweep's job, which runs
//! every NULL pairing past each server.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n INT, m INT)";

/// Why `predicate` over `t` is not served in process, `None` when it is.
fn not_served<B, D>(ddl: &str, dialect: D, predicate: &str) -> Option<NotServed<B>>
where
    B: Backend<Dialect = D> + subql::compiler::SqlLiteralParse,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    engine
        .register(SubscriptionRequest::new(
            7u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        ))
        .unwrap_or_else(|error| panic!("{predicate} registers on some tier, got {error:?}"))
        .not_served_because
}

/// The spelling an engine accepts is served in process and the one it
/// rejects is routed to the database, whose own error then surfaces.
#[test]
fn each_engine_routes_the_spelling_it_rejects() {
    for predicate in [
        "n IS NOT DISTINCT FROM m",
        "n IS DISTINCT FROM 3",
        "NOT (n IS DISTINCT FROM NULL)",
    ] {
        assert_eq!(
            not_served::<Postgres, _>(DDL, PostgreSqlDialect {}, predicate),
            None,
            "PostgreSQL serves {predicate}"
        );
        assert_eq!(
            not_served::<SQLite, _>(DDL, SQLiteDialect {}, predicate),
            None,
            "SQLite serves {predicate}"
        );
    }
    for predicate in ["n IS NOT DISTINCT FROM m", "n IS DISTINCT FROM 3"] {
        let mysql = not_served::<MySql, _>(DDL, MySqlDialect {}, predicate);
        assert!(
            matches!(&mysql, Some(NotServed::UnsupportedSql(message)) if message.contains("<=>")),
            "MySQL rejects {predicate} and is told its own spelling, got {mysql:?}"
        );
    }
    for predicate in ["n <=> m", "NOT (n <=> 3)", "n <=> NULL"] {
        assert!(
            not_served::<Postgres, _>(DDL, PostgreSqlDialect {}, predicate).is_some(),
            "PostgreSQL rejects {predicate}"
        );
        assert!(
            not_served::<SQLite, _>(DDL, SQLiteDialect {}, predicate).is_some(),
            "SQLite rejects {predicate}"
        );
    }
}

/// MySQL serves `<=>`, and a routed `<=>` is told the spelling its engine
/// accepts.
#[test]
fn mysql_serves_its_own_spelling_and_each_engine_names_its_own() {
    for predicate in ["n <=> m", "NOT (n <=> 3)", "n <=> NULL"] {
        assert_eq!(
            not_served::<MySql, _>(DDL, MySqlDialect {}, predicate),
            None,
            "MySQL serves {predicate}",
        );
    }
    // Each engine names its own spelling, which the canonicalizer's
    // refusal of `<=>` hides from PostgreSQL and SQLite as well.
    for predicate in ["n <=> m", "NOT (n <=> 3)"] {
        let postgres = not_served::<Postgres, _>(DDL, PostgreSqlDialect {}, predicate);
        assert!(
            matches!(&postgres, Some(NotServed::UnsupportedSql(message)) if message.contains("IS [NOT] DISTINCT FROM")),
            "PostgreSQL is told its own spelling for {predicate}, got {postgres:?}"
        );
        let sqlite = not_served::<SQLite, _>(DDL, SQLiteDialect {}, predicate);
        assert!(
            matches!(&sqlite, Some(NotServed::UnsupportedSql(message)) if message.contains("IS [NOT] DISTINCT FROM")),
            "SQLite is told its own spelling for {predicate}, got {sqlite:?}"
        );
    }
}

/// Null-safe equality asks the question `=` asks, so a pair `=` cannot
/// compare is refused alike, and a comparison `=` leaves to the database
/// is left to it here too.
#[test]
fn null_safe_equality_is_classified_as_equality_is() {
    const PG_DDL: &str = "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', \
                          deterministic = false); \
                          CREATE TABLE t (id INT PRIMARY KEY, label TEXT, \
                          strict TEXT COLLATE \"C\", folded TEXT COLLATE \"ci\", n INT, big BIGINT, \
                          doc JSON, bin JSONB)";
    // Served in process, left to a read, or refused, with the refusal's words.
    let outcome = |predicate: &str| -> Result<bool, String> {
        let database = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        engine
            .register(SubscriptionRequest::new(
                7u64,
                format!("SELECT * FROM t WHERE {predicate}"),
            ))
            .map(|registered| registered.not_served_because.is_none())
            .map_err(|error| error.to_string())
    };
    for (left, right) in [
        ("label", "'a'"),
        ("strict", "'a'"),
        ("label", "strict"),
        // `=` is a database read here, so null-safe equality must be too.
        ("folded", "'a'"),
        ("n", "big"),
        ("doc", "'{}'"),
        ("bin", "'{}'"),
    ] {
        let equality = outcome(&format!("{left} = {right}"));
        let null_safe = outcome(&format!("{left} IS NOT DISTINCT FROM {right}"));
        assert_eq!(null_safe, equality, "{left} against {right}");
    }
}

/// A cell the event did not carry is not `NULL`, so a null-safe comparison
/// reading it has no answer, in either polarity, and names the column.
#[test]
fn a_missing_cell_leaves_null_safe_equality_unanswered() {
    for predicate in [
        "n IS NOT DISTINCT FROM NULL",
        "n IS DISTINCT FROM NULL",
        "n IS DISTINCT FROM 3",
        "NOT (n IS NOT DISTINCT FROM m)",
    ] {
        let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let table = catalog_helpers::table_id::<Postgres, _>(&database, "t").unwrap();
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        engine
            .register(SubscriptionRequest::new(
                1u64,
                format!("SELECT * FROM t WHERE {predicate}"),
            ))
            .unwrap();
        let notifications = engine
            .consumers(&TestEvent::insert(
                table,
                vec![Value::Int(1), Value::Missing, Value::Null],
            ))
            .unwrap();
        assert!(notifications.inserted().is_empty(), "{predicate}");
        assert_eq!(
            notifications
                .unanswered()
                .iter()
                .map(|entry| entry.column)
                .collect::<Vec<_>>(),
            vec![1],
            "{predicate} is unanswered for want of `n`"
        );
    }
}

/// Two spellings of one null-safe equality or truth test are one predicate,
/// as two spellings of `=` are.
#[test]
fn spellings_of_one_null_safe_equality_share_a_predicate() {
    fn created<B, D>(dialect: D, spellings: [&str; 2]) -> Vec<bool>
    where
        B: Backend<Dialect = D> + subql::compiler::SqlLiteralParse,
        D: sqlparser::dialect::Dialect + Default,
    {
        let database = ParserDB::parse::<D>(DDL).unwrap();
        let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, dialect);
        spellings
            .iter()
            .enumerate()
            .map(|(consumer, predicate)| {
                let registered = engine
                    .register(SubscriptionRequest::new(
                        u64::try_from(consumer).unwrap(),
                        format!("SELECT * FROM t WHERE {predicate}"),
                    ))
                    .unwrap();
                let subql::Tier::InProcess(served) = registered.tier else {
                    panic!("{predicate} is served in process");
                };
                served.created_new_predicate
            })
            .collect()
    }
    for spellings in [
        ["n IS NOT DISTINCT FROM m", "M IS NOT DISTINCT FROM (N)"],
        ["n IS DISTINCT FROM 3", "(N) IS DISTINCT FROM 3"],
    ] {
        assert_eq!(
            created::<Postgres, _>(PostgreSqlDialect {}, spellings),
            vec![true, false],
            "{spellings:?}"
        );
    }
    assert_eq!(
        created::<MySql, _>(MySqlDialect {}, ["n <=> m", "M <=> N"]),
        vec![true, false]
    );
}
