//! Two strings compared with no column between them.
//!
//! A string carries no collation, so two of them compare under the
//! database default. MySQL's default is not a fact the catalog names and
//! usually ignores case, so every comparison of two strings there is a
//! database read. PostgreSQL's default is deterministic, so equality and
//! `LIKE` are the bytes, and ordering is the locale's, a read. SQLite's
//! default is `BINARY`, and its `LIKE` folds ASCII case whatever the
//! collation.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT)";

/// `None` when `predicate` is not served in process, otherwise whether it
/// delivers the row `(1, 0)`.
fn delivers<B, D>(dialect: D, predicate: &str) -> Option<bool>
where
    B: Backend<Dialect = D, Int = i64> + subql::compiler::SqlLiteralParse,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(DDL).unwrap();
    let table = catalog_helpers::table_id::<B, _>(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    let registered = engine
        .register(SubscriptionRequest::new(
            7u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        ))
        .ok()?;
    if registered.not_served_because.is_some() {
        return None;
    }
    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Int(0)],
        ))
        .unwrap();
    Some(!notifications.inserted().is_empty())
}

/// MySQL reads every comparison of two strings under a default the catalog
/// cannot name, and PostgreSQL orders them by its locale.
#[test]
fn two_strings_are_a_read_where_the_default_is_unnamed() {
    for predicate in ["'a' = 'A'", "'a%' < 'A'", "'a' LIKE 'A'", "'a' <=> 'A'"] {
        assert_eq!(
            delivers::<MySql, _>(MySqlDialect {}, predicate),
            None,
            "MySQL routes {predicate}"
        );
    }
    for predicate in ["'a%' < 'A'", "'b' >= 'B'"] {
        assert_eq!(
            delivers::<Postgres, _>(PostgreSqlDialect {}, predicate),
            None,
            "PostgreSQL routes {predicate}"
        );
    }
    for (predicate, selected) in [("'a' = 'A'", false), ("'a' LIKE 'a%'", true)] {
        assert_eq!(
            delivers::<Postgres, _>(PostgreSqlDialect {}, predicate),
            Some(selected),
            "PostgreSQL compares the bytes of {predicate}"
        );
    }
}

/// SQLite compares two strings by bytes, and its `LIKE` folds ASCII case.
#[test]
fn sqlite_compares_two_strings_as_sqlite_does() {
    for (predicate, selected) in [
        ("'a' = 'A'", false),
        ("'a' < 'b'", true),
        ("'a%' < 'A'", false),
        ("'a' LIKE 'A'", true),
        ("'ab' NOT LIKE 'A%'", false),
    ] {
        assert_eq!(
            delivers::<SQLite, _>(SQLiteDialect {}, predicate),
            Some(selected),
            "SQLite {predicate}"
        );
    }
}
