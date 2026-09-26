//! `IS [NOT] NULL` over a condition.
//!
//! A condition answers true, false or unknown, and `IS NULL` asks whether it
//! answered unknown, on every engine. `(a > 0) IS NOT NULL` failed every
//! dispatch on its table with a VM type error, taking every other
//! subscription on that table down with it.

use subql::backend::{Backend, MySql, Postgres, SQLite, Value};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT)";

/// Whether a row with `a` is delivered, the filter served in process.
fn delivers<B>(predicate: &str, a: Value<B>) -> bool
where
    B: Backend<Int = i64> + subql::compiler::SqlLiteralParse + core::fmt::Debug,
    B::Dialect: sqlparser::dialect::Dialect + Default + 'static,
{
    crate::common::semantics::notifies::<B>(
        DDL,
        "t",
        &format!("SELECT * FROM t WHERE {predicate}"),
        vec![Value::Int(1), a],
    )
}

#[test]
fn a_null_test_of_a_condition_asks_whether_it_is_unknown() {
    for (predicate, on_five, on_null) in [
        ("(a > 0) IS NOT NULL", true, false),
        ("(a > 0) IS NULL", false, true),
        ("((a > 0) IS NULL) IS NOT NULL", true, true),
        ("NOT ((a < 0) IS NULL)", true, false),
    ] {
        assert_eq!(
            delivers::<Postgres>(predicate, Value::Int(5)),
            on_five,
            "PostgreSQL {predicate} over 5"
        );
        assert_eq!(
            delivers::<Postgres>(predicate, Value::Null),
            on_null,
            "PostgreSQL {predicate} over NULL"
        );
        assert_eq!(
            delivers::<MySql>(predicate, Value::Int(5)),
            on_five,
            "MySQL {predicate} over 5"
        );
        assert_eq!(
            delivers::<MySql>(predicate, Value::Null),
            on_null,
            "MySQL {predicate} over NULL"
        );
        assert_eq!(
            delivers::<SQLite>(predicate, Value::Int(5)),
            on_five,
            "SQLite {predicate} over 5"
        );
        assert_eq!(
            delivers::<SQLite>(predicate, Value::Null),
            on_null,
            "SQLite {predicate} over NULL"
        );
    }
}
