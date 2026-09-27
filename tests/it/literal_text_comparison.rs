//! Two strings compared with no column between them.
//!
//! A string carries no collation, so two of them compare under the
//! database default. MySQL's default is not a fact the catalog names and
//! usually ignores case, so every comparison of two strings there is a
//! database read. PostgreSQL's default is deterministic, so equality and
//! `LIKE` are the bytes, and ordering is the locale's, a read. SQLite's
//! default is `BINARY`, and its `LIKE` folds ASCII case whatever the
//! collation.
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT)";

/// `None` when `predicate` is not served in process, otherwise whether it
/// delivers the row `(1, 0)`.
fn delivers<B>(predicate: &str) -> Option<bool>
where
    B: Backend<Int = i64> + subql::compiler::SqlLiteralParse + core::fmt::Debug,
    B::Dialect: sqlparser::dialect::Dialect + Default + 'static,
{
    let sql = format!("SELECT * FROM t WHERE {predicate}");
    crate::common::semantics::served::<B>(DDL, &sql).then(|| {
        crate::common::semantics::notifies::<B>(DDL, "t", &sql, vec![Value::Int(1), Value::Int(0)])
    })
}

/// MySQL reads every comparison of two strings under a default the catalog
/// cannot name, and PostgreSQL orders them by its locale.
#[test]
fn two_strings_are_a_read_where_the_default_is_unnamed() {
    for predicate in ["'a' = 'A'", "'a%' < 'A'", "'a' LIKE 'A'", "'a' <=> 'A'"] {
        assert_eq!(
            delivers::<MySql>(predicate),
            None,
            "MySQL routes {predicate}"
        );
    }
    for predicate in ["'a%' < 'A'", "'b' >= 'B'"] {
        assert_eq!(
            delivers::<Postgres>(predicate),
            None,
            "PostgreSQL routes {predicate}"
        );
    }
    for (predicate, selected) in [("'a' = 'A'", false), ("'a' LIKE 'a%'", true)] {
        assert_eq!(
            delivers::<Postgres>(predicate),
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
            delivers::<SQLite>(predicate),
            Some(selected),
            "SQLite {predicate}"
        );
    }
}
