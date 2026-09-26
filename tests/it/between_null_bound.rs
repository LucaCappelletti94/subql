//! `BETWEEN` with a `NULL` bound.
//!
//! `x BETWEEN low AND high` is `x >= low AND x <= high` on every engine, so a
//! `NULL` bound leaves that side unknown and the other side can still decide.
//! With `b = 0`, `b BETWEEN 1 AND NULL` is `FALSE AND UNKNOWN`, which is
//! `FALSE`, and `b NOT BETWEEN 1 AND NULL` selects the row.
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, b BIGINT, n BIGINT)";

/// Whether a row with `b = 0` and `n` NULL is delivered, the filter served in
/// process.
fn delivers<B>(predicate: &str) -> bool
where
    B: Backend<Int = i64> + subql::compiler::SqlLiteralParse + core::fmt::Debug,
    B::Dialect: sqlparser::dialect::Dialect + Default + 'static,
{
    crate::common::semantics::notifies::<B>(
        DDL,
        "t",
        &format!("SELECT * FROM t WHERE {predicate}"),
        vec![Value::Int(1), Value::Int(0), Value::Null],
    )
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
            delivers::<Postgres>(predicate),
            selected,
            "PostgreSQL {predicate}"
        );
        assert_eq!(delivers::<MySql>(predicate), selected, "MySQL {predicate}");
        assert_eq!(
            delivers::<SQLite>(predicate),
            selected,
            "SQLite {predicate}"
        );
    }
}
