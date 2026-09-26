//! An integer bound on a float column.
//!
//! `r > 0` over `r = 0.0025` is a row on every engine. The candidate index
//! reads a range as integers, where `> 0` starts at `1`, so a float between
//! two integers must not be filed under it.
use subql::backend::{Backend, Postgres, SQLite, Value};

/// Whether a row with `r = 0.0025` is delivered.
fn delivers<B>(ddl: &str, predicate: &str) -> bool
where
    B: Backend<Int = i64, Float = f64> + subql::compiler::SqlLiteralParse + core::fmt::Debug,
    B::Dialect: sqlparser::dialect::Dialect + Default + 'static,
{
    crate::common::semantics::notifies::<B>(
        ddl,
        "t",
        &format!("SELECT * FROM t WHERE {predicate}"),
        vec![Value::Int(1), Value::Float(0.0025)],
    )
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
            delivers::<Postgres>(
                "CREATE TABLE t (id INT PRIMARY KEY, r DOUBLE PRECISION)",
                predicate
            ),
            selected,
            "PostgreSQL {predicate}"
        );
        assert_eq!(
            delivers::<SQLite>("CREATE TABLE t (id INTEGER PRIMARY KEY, r REAL)", predicate),
            selected,
            "SQLite {predicate}"
        );
    }
}
