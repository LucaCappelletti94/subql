//! An integer bound on a float column.
//!
//! `r > 0` over `r = 0.0025` is a row on every engine. The candidate index
//! reads a range as integers, where `> 0` starts at `1`, so a float between
//! two integers must not be filed under it.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// Whether a row with `r = 0.0025` is delivered.
fn delivers<B, D>(ddl: &str, dialect: D, predicate: &str) -> bool
where
    B: Backend<Dialect = D, Int = i64, Float = f64> + subql::compiler::SqlLiteralParse,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
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
        "{predicate} is served"
    );
    !engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Float(0.0025)],
        ))
        .unwrap()
        .inserted()
        .is_empty()
}

#[test]
fn an_integer_bound_on_a_float_column_keeps_the_fraction() {
    for (predicate, selected) in [
        ("r > 0", true),
        ("0 < r", true),
        ("r < 1", true),
        ("r <= 0", false),
        ("r BETWEEN 0 AND 1", true),
        ("r NOT BETWEEN 1 AND 2", true),
        ("NOT (r > 0)", false),
    ] {
        assert_eq!(
            delivers::<Postgres, _>(
                "CREATE TABLE t (id INT PRIMARY KEY, r DOUBLE PRECISION)",
                PostgreSqlDialect {},
                predicate
            ),
            selected,
            "PostgreSQL {predicate}"
        );
        assert_eq!(
            delivers::<SQLite, _>(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, r REAL)",
                SQLiteDialect {},
                predicate
            ),
            selected,
            "SQLite {predicate}"
        );
    }
}
