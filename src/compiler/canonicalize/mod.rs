//! SQL normalization and hashing for predicate deduplication.
//!
//! The rules live in the `sqlparser-canonicalize` crate. This module keeps the names subql
//! calls and maps that crate's error onto [`RegisterError`].

use crate::RegisterError;
use alloc::format;
use alloc::string::{String, ToString};
use sqlparser::ast::Expr;
use sqlparser::dialect::Dialect;
use sqlparser_canonicalize::{CanonicalizeError, Canonicalizer};

/// Predicate hash (128-bit, deterministic)
pub type PredicateHash = u128;

/// Normalize SQL WHERE clause for consistent deduplication
///
/// Two spellings of one predicate produce identical text, so they hash alike and
/// deduplicate. The dialect decides which spellings those are.
///
/// # Example
/// ```
/// # use subql::compiler::canonicalize::normalize_sql;
/// # use sqlparser::dialect::PostgreSqlDialect;
/// let sql1 = "SELECT * FROM t WHERE a = 1 AND b = 2";
/// let sql2 = "SELECT * FROM t WHERE b = 2 AND a = 1";
/// let dialect = PostgreSqlDialect {};
///
/// let norm1 = normalize_sql(sql1, &dialect).unwrap();
/// let norm2 = normalize_sql(sql2, &dialect).unwrap();
///
/// assert_eq!(norm1, norm2); // Same predicate
/// ```
pub fn normalize_sql(sql: &str, dialect: &dyn Dialect) -> Result<String, RegisterError> {
    Canonicalizer::new(dialect)
        .normalize_sql(sql)
        .map_err(map_error)
}

/// Normalize an already-parsed WHERE clause (or absence of one) without reparsing SQL text.
///
/// `canonicalizer` MUST carry the dialect that parsed `where_expr`.
pub(crate) fn normalize_where_clause(
    where_expr: Option<&Expr>,
    canonicalizer: &Canonicalizer<'_>,
) -> Result<String, RegisterError> {
    canonicalizer
        .normalize_where_clause(where_expr)
        .map_err(map_error)
}

/// Hash normalized SQL for fast predicate lookup
#[must_use]
pub fn hash_sql(normalized: &str) -> PredicateHash {
    sqlparser_canonicalize::hash_canonical(normalized)
}

fn map_error(error: CanonicalizeError) -> RegisterError {
    match error {
        // subql's own parse errors carry the same placeholder position, see `sql_shape`.
        CanonicalizeError::Parse(message) => RegisterError::ParseError {
            line: 1,
            column: 0,
            message,
        },
        CanonicalizeError::InputTooLong { limit } => {
            RegisterError::UnsupportedSql(format!("SQL input is longer than {limit} bytes"))
        }
        CanonicalizeError::TooDeep { limit } => {
            RegisterError::UnsupportedSql(format!("Expression nests deeper than {limit} levels"))
        }
        CanonicalizeError::NotRoundTrippable(text) => RegisterError::UnsupportedSql(format!(
            "Predicate has no canonical spelling that reads back as itself: {text}"
        )),
        other => RegisterError::UnsupportedSql(other.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    //! What subql owns at this boundary. The canonicalization rules themselves are the
    //! crate's, tested there against 129 pinned cases, property tests, and two fuzz targets,
    //! so re-testing them here would only give one suite to drift from the other.

    use super::{hash_sql, normalize_sql, normalize_where_clause};
    use crate::RegisterError;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser_canonicalize::Canonicalizer;

    /// One filter written two ways is one predicate, which is what lets two subscribers
    /// share a registration.
    #[test]
    fn a_membership_term_normalizes_the_same_under_two_spellings() {
        let dialect = PostgreSqlDialect {};

        let one = normalize_sql(
            "SELECT * FROM t WHERE x IN (SELECT id FROM m WHERE owner = 'a')",
            &dialect,
        )
        .unwrap();
        let two = normalize_sql(
            "SELECT   *  FROM t\n  where   x   in   ( select id from m where owner = 'a' )",
            &dialect,
        )
        .unwrap();

        assert_eq!(one, two, "one filter, two spellings, one predicate");
    }

    /// Two different relationships are two predicates. This is the half a canonical
    /// rendering can get wrong in the direction that matters: two filters collapsed onto one
    /// predicate answer each other's subscribers.
    #[test]
    fn two_different_membership_terms_are_two_predicates() {
        let dialect = PostgreSqlDialect {};
        let norm = |sql: &str| normalize_sql(sql, &dialect).unwrap();

        let base = norm("SELECT * FROM t WHERE x IN (SELECT id FROM m WHERE owner = 'a')");

        for other in [
            // A different inner table.
            "SELECT * FROM t WHERE x IN (SELECT id FROM n WHERE owner = 'a')",
            // A different projected column.
            "SELECT * FROM t WHERE x IN (SELECT ref FROM m WHERE owner = 'a')",
            // A different inner filter.
            "SELECT * FROM t WHERE x IN (SELECT id FROM m WHERE owner = 'b')",
            // A different tested column.
            "SELECT * FROM t WHERE y IN (SELECT id FROM m WHERE owner = 'a')",
            // A clause that changes which rows are members.
            "SELECT * FROM t WHERE x IN (SELECT id FROM m WHERE owner = 'a' LIMIT 1)",
            // The negation, which is refused later but must not share either.
            "SELECT * FROM t WHERE x NOT IN (SELECT id FROM m WHERE owner = 'a')",
        ] {
            assert_ne!(
                base,
                norm(other),
                "{other} names a different relationship and must not share the predicate"
            );
        }
    }

    /// The clause entry point answers the same as the whole-statement one, since slot
    /// ordering canonicalizes expressions while registration canonicalizes text.
    #[test]
    fn both_entry_points_agree() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE a = 1 AND b = 2";
        let statement = crate::compiler::sql_shape::parse_single_statement(sql, &dialect).unwrap();
        let where_expr = match &statement {
            sqlparser::ast::Statement::Query(query) => match query.body.as_ref() {
                sqlparser::ast::SetExpr::Select(select) => select.selection.clone(),
                _ => panic!("test SQL is a plain SELECT"),
            },
            _ => panic!("test SQL is a query"),
        };

        let canonicalizer = Canonicalizer::new(&dialect as &dyn sqlparser::dialect::Dialect);
        assert_eq!(
            normalize_where_clause(where_expr.as_ref(), &canonicalizer).unwrap(),
            normalize_sql(sql, &dialect).unwrap()
        );
    }

    #[test]
    fn a_parse_failure_maps_to_parse_error() {
        let dialect = PostgreSqlDialect {};
        assert!(matches!(
            normalize_sql("NOT VALID SQL ;;;", &dialect),
            Err(RegisterError::ParseError { .. })
        ));
    }

    #[test]
    fn a_refusal_maps_to_unsupported_sql() {
        let dialect = PostgreSqlDialect {};
        for sql in [
            // More than one statement.
            "SELECT * FROM t WHERE a = 1; SELECT * FROM t WHERE b = 2",
            // A join.
            "SELECT * FROM t JOIN u ON t.id = u.id WHERE a = 1",
            // A derived table.
            "SELECT * FROM (SELECT * FROM t) sub WHERE a = 1",
            // Not a SELECT.
            "INSERT INTO t VALUES (1)",
            // A literal the parser cannot print without changing its value, which the crate
            // reports as having no canonical spelling.
            "SELECT * FROM t WHERE a = ''''''",
        ] {
            assert!(
                matches!(
                    normalize_sql(sql, &dialect),
                    Err(RegisterError::UnsupportedSql(_))
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn hashing_is_deterministic() {
        let dialect = PostgreSqlDialect {};
        let normalized = normalize_sql("SELECT * FROM t WHERE a = 1", &dialect).unwrap();
        assert_eq!(hash_sql(&normalized), hash_sql(&normalized));
    }
}
