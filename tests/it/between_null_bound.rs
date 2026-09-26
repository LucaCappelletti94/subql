//! `BETWEEN` with a `NULL` bound.
//!
//! `x BETWEEN low AND high` is `x >= low AND x <= high` on every engine, so a
//! `NULL` bound leaves that side unknown and the other side can still decide.
//! With `b = 0`, `b BETWEEN 1 AND NULL` is `FALSE AND UNKNOWN`, which is
//! `FALSE`, and `b NOT BETWEEN 1 AND NULL` selects the row.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, b BIGINT, n BIGINT)";

/// Whether a row with `b = 0` and `n` NULL is delivered, the filter served in
/// process.
fn delivers<B, D>(dialect: D, predicate: &str) -> bool
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
        .unwrap();
    assert!(
        registered.not_served_because.is_none(),
        "{predicate} is served in process"
    );
    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Int(0), Value::Null],
        ))
        .unwrap();
    assert!(notifications.evaluation_failures().is_empty());
    !notifications.inserted().is_empty()
}

/// The decided side answers, and a `NULL` bound on the undecided side
/// leaves the whole unknown, on every backend.
#[test]
fn a_null_bound_leaves_only_its_own_side_unknown() {
    for (predicate, selected) in [
        ("b NOT BETWEEN 1 AND NULL", true),
        ("b NOT BETWEEN NULL AND (-1)", true),
        ("b NOT BETWEEN 1 AND n", true),
        ("b BETWEEN 1 AND NULL", false),
        ("b NOT BETWEEN (-1) AND NULL", false),
        ("b BETWEEN (-1) AND NULL", false),
        ("b NOT BETWEEN NULL AND 1", false),
        ("n NOT BETWEEN 1 AND 2", false),
    ] {
        assert_eq!(
            delivers::<Postgres, _>(PostgreSqlDialect {}, predicate),
            selected,
            "PostgreSQL {predicate}"
        );
        assert_eq!(
            delivers::<MySql, _>(MySqlDialect {}, predicate),
            selected,
            "MySQL {predicate}"
        );
        assert_eq!(
            delivers::<SQLite, _>(SQLiteDialect {}, predicate),
            selected,
            "SQLite {predicate}"
        );
    }
}
