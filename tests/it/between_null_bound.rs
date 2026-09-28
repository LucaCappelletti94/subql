//! `BETWEEN` with a `NULL` bound.
//!
//! `x BETWEEN low AND high` is `x >= low AND x <= high` on every engine, so a
//! `NULL` bound leaves that side unknown and the other side can still decide.
//! With `b = 0`, `b BETWEEN 1 AND NULL` is `FALSE AND UNKNOWN`, which is
//! `FALSE`, and `b NOT BETWEEN 1 AND NULL` selects the row.
//!
//! MySQL also types the range by all three operands, and a bound whose type
//! is `NULL`, a bare `NULL` or arithmetic over one, sends the whole range to
//! doubles. The other side then compares rounded, which decides whether the
//! answer is `NULL` or false. Measured 2026-09-28 with `b` a `BIGINT`
//! holding 9007199254740993:
//!
//! ```text
//! filter                              PostgreSQL 15   MySQL 8.0.46   SQLite 3.51
//! b BETWEEN NULL AND (b - 1)          f               NULL           0
//! b BETWEEN (NULL + 0) AND (b - 1)    f               NULL           0
//! b BETWEEN 0 AND (b - 1)             f               0              0
//! ```
//!
//! So MySQL routes such a range to a database read. PostgreSQL and SQLite
//! type a bare `NULL` bound by the tested side and serve it.
use crate::common::semantics::served;
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
/// leaves the whole unknown.
#[test]
fn a_null_bound_leaves_only_its_own_side_unknown() {
    for (predicate, selected) in [
        ("b NOT BETWEEN 1 AND NULL", true),
        ("b NOT BETWEEN NULL AND (-1)", true),
        ("b BETWEEN 1 AND NULL", false),
        ("b NOT BETWEEN (-1) AND NULL", false),
        ("b BETWEEN (-1) AND NULL", false),
        ("b NOT BETWEEN NULL AND 1", false),
    ] {
        assert_eq!(
            delivers::<Postgres>(predicate),
            selected,
            "PostgreSQL {predicate}"
        );
        assert_eq!(
            delivers::<SQLite>(predicate),
            selected,
            "SQLite {predicate}"
        );
    }
    for (predicate, selected) in [
        ("b NOT BETWEEN 1 AND n", true),
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

/// A bound that is `NULL`, or arithmetic over one, however it is nested.
#[test]
fn a_null_typed_bound_is_routed_on_mysql() {
    for predicate in [
        "b BETWEEN NULL AND (b - 1)",
        "b BETWEEN (b - 1) AND NULL",
        "b BETWEEN ((NULL)) AND n",
        "b BETWEEN (- NULL) AND n",
        "b NOT BETWEEN (NULL + 0) AND (b - 1)",
        "b BETWEEN (n * (1 - NULL)) AND (b - 1)",
        "(b + 1) NOT BETWEEN (- (NULL % 1)) AND b",
    ] {
        let sql = format!("SELECT * FROM t WHERE {predicate}");
        assert!(!served::<MySql>(DDL, &sql), "MySQL routes {predicate}");
    }
}

/// The other engines type a `NULL` bound by the tested side, and serve it.
#[test]
fn a_null_bound_is_served_on_postgres_and_sqlite() {
    for predicate in [
        "b BETWEEN NULL AND (b - 1)",
        "b BETWEEN (b - 1) AND NULL",
        "b BETWEEN ((NULL)) AND n",
    ] {
        let sql = format!("SELECT * FROM t WHERE {predicate}");
        assert!(
            served::<Postgres>(DDL, &sql),
            "PostgreSQL serves {predicate}"
        );
        assert!(served::<SQLite>(DDL, &sql), "SQLite serves {predicate}");
    }
}

/// A `NULL` the range is not typed by, on the tested side or inside a
/// `COALESCE` beside a column, leaves MySQL comparing integers.
#[test]
fn mysql_serves_a_range_its_bounds_type() {
    for predicate in [
        "b BETWEEN 0 AND (b - 1)",
        "b BETWEEN COALESCE(n, NULL) AND (b - 1)",
        "(b + NULL) BETWEEN n AND (b - 1)",
    ] {
        let sql = format!("SELECT * FROM t WHERE {predicate}");
        assert!(served::<MySql>(DDL, &sql), "MySQL serves {predicate}");
    }
}
