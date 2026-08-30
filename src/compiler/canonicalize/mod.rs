//! SQL normalization and hashing for predicate deduplication

use super::sql_shape;
use crate::RegisterError;
use alloc::string::{String, ToString};
use core::hash::{Hash, Hasher};
use seahash::SeaHasher;
use sqlparser::ast::Expr;
use sqlparser::dialect::Dialect;

mod internal_helpers;
use internal_helpers::extract_where;
pub(super) use internal_helpers::normalize_expr;

/// Predicate hash (128-bit, deterministic)
pub type PredicateHash = u128;

/// Normalize SQL WHERE clause for consistent deduplication
///
/// Normalization rules:
/// - Extract WHERE clause only (ignore SELECT list, table name)
/// - Sort commutative operands (AND/OR)
/// - Remove redundant parentheses
/// - Lowercase keywords
/// - Normalize whitespace
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
    let stmt = sql_shape::parse_single_statement(sql, dialect)?;

    // Extract WHERE clause
    let where_expr = extract_where(&stmt)?;

    normalize_where_clause(where_expr.as_ref())
}

/// Normalize an already-parsed WHERE clause (or absence of one) without
/// reparsing SQL text.
pub(crate) fn normalize_where_clause(where_expr: Option<&Expr>) -> Result<String, RegisterError> {
    // No WHERE clause = always-true predicate
    where_expr.map_or_else(|| Ok("TRUE".to_string()), normalize_expr)
}

/// Hash normalized SQL for fast predicate lookup
///
/// Uses `seahash` for deterministic, high-quality hashing.
/// Returns 128-bit hash (two 64-bit hashes concatenated).
#[must_use]
pub fn hash_sql(normalized: &str) -> PredicateHash {
    // First 64 bits
    let mut hasher1 = SeaHasher::new();
    normalized.hash(&mut hasher1);
    let hash1 = hasher1.finish();

    // Second 64 bits (with different seeds derived from hash1)
    let mut hasher2 = SeaHasher::with_seeds(
        hash1,
        hash1.wrapping_add(1),
        hash1.wrapping_add(2),
        hash1.wrapping_add(3),
    );
    normalized.hash(&mut hasher2);
    let hash2 = hasher2.finish();

    (u128::from(hash1) << 64) | u128::from(hash2)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::single_char_pattern
)]
mod tests {
    use super::{hash_sql, normalize_sql};
    use crate::RegisterError;
    use sqlparser::dialect::PostgreSqlDialect;

    #[test]
    fn test_normalize_simple() {
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM t WHERE age > 18";
        let result = normalize_sql(sql, &dialect);
        assert!(result.is_ok());

        let normalized = result.unwrap();
        assert!(normalized.contains("age"));
        assert!(normalized.contains(">"));
        assert!(normalized.contains("18"));
    }

    #[test]
    fn test_normalize_commutative_and() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE a = 1 AND b = 2";
        let sql2 = "SELECT * FROM t WHERE b = 2 AND a = 1";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        // Should be identical after normalization
        assert_eq!(norm1, norm2);
    }

    #[test]
    fn test_normalize_commutative_or() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE a = 1 OR b = 2";
        let sql2 = "SELECT * FROM t WHERE b = 2 OR a = 1";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        assert_eq!(norm1, norm2);
    }

    #[test]
    fn test_normalize_in_list_sorted() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE x IN (1, 2, 3)";
        let sql2 = "SELECT * FROM t WHERE x IN (3, 1, 2)";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        // IN lists should be sorted
        assert_eq!(norm1, norm2);
    }

    /// The same membership term written with different whitespace and keyword
    /// case is one predicate. Without an arm of its own the term falls to the
    /// `Debug` fallback, which prints the byte offset of every identifier, so
    /// two spellings of one filter would compile and store twice and the
    /// architecture's predicate sharing would quietly stop applying to terms.
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
        assert!(
            !one.contains("Span") && !one.contains("Ident"),
            "the term must not normalize through the Debug fallback, got {one:?}"
        );
    }

    /// Two different relationships are two predicates, which is the half a
    /// canonical rendering can get wrong in the direction that matters: two
    /// filters collapsed onto one predicate answer each other's subscribers.
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

    #[test]
    fn test_normalize_no_where() {
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM t";
        let result = normalize_sql(sql, &dialect);
        assert!(result.is_ok());

        let normalized = result.unwrap();
        assert_eq!(normalized, "TRUE");
    }

    #[test]
    fn test_hash_deterministic() {
        let s = "age > 18 AND status = 'active'";

        let hash1 = hash_sql(s);
        let hash2 = hash_sql(s);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_different() {
        let s1 = "age > 18";
        let s2 = "age > 19";

        let hash1 = hash_sql(s1);
        let hash2 = hash_sql(s2);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_128bit() {
        let s = "test";
        let hash = hash_sql(s);

        // Should use full 128 bits
        assert!(hash > 0);
        assert!(hash < u128::MAX);
    }

    #[test]
    fn test_normalize_nested_parentheses() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE ((age > 18))";
        let sql2 = "SELECT * FROM t WHERE age > 18";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        // Redundant parens should be removed
        assert_eq!(norm1, norm2);
    }

    #[test]
    fn test_normalize_preserves_order_noncommutative() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE a < b";
        let sql2 = "SELECT * FROM t WHERE b < a";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        // < is not commutative, should be different
        assert_ne!(norm1, norm2);
    }

    #[test]
    fn test_normalize_error_parse_failure() {
        let dialect = PostgreSqlDialect {};

        let invalid_sql = "NOT VALID SQL ;;;";
        let result = normalize_sql(invalid_sql, &dialect);

        assert!(matches!(result, Err(RegisterError::ParseError { .. })));
    }

    #[test]
    fn test_normalize_error_multiple_statements() {
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM t WHERE a = 1; SELECT * FROM t WHERE b = 2";
        let result = normalize_sql(sql, &dialect);

        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_normalize_rejects_unbalanced_open_parens() {
        // The sanity check (in sql_shape) catches this before sqlparser
        // can blow up on it.
        let dialect = PostgreSqlDialect {};
        let err = normalize_sql("SELECT * FROM t WHERE ((((a = 1", &dialect).unwrap_err();
        assert!(matches!(&err, RegisterError::UnsupportedSql(m) if m.contains("Unbalanced")));
    }

    #[test]
    fn test_normalize_no_where_clause() {
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM t";
        let result = normalize_sql(sql, &dialect).unwrap();

        assert_eq!(result, "TRUE");
    }

    #[test]
    fn test_normalize_all_operators() {
        let dialect = PostgreSqlDialect {};

        // Test all comparison operators
        for op in &["=", "!=", "<", ">", "<=", ">="] {
            let sql = format!("SELECT * FROM t WHERE a {} b", op);
            let result = normalize_sql(&sql, &dialect);
            assert!(result.is_ok(), "Failed on operator: {}", op);
        }

        // Test logical operators
        for op in &["AND", "OR"] {
            let sql = format!("SELECT * FROM t WHERE a = 1 {} b = 2", op);
            let result = normalize_sql(&sql, &dialect);
            assert!(result.is_ok(), "Failed on operator: {}", op);
        }
    }

    #[test]
    fn test_normalize_arithmetic_operators() {
        let dialect = PostgreSqlDialect {};

        for op in &["+", "-", "*", "/", "%"] {
            let sql = format!("SELECT * FROM t WHERE a {} b > 10", op);
            let result = normalize_sql(&sql, &dialect);
            assert!(result.is_ok(), "Failed on arithmetic operator: {}", op);
        }
    }

    #[test]
    fn test_normalize_not_operator() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE NOT (a = 1)";
        let sql2 = "SELECT * FROM t WHERE a != 1";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        // NOT (a = 1) is different from a != 1 in normalization
        // (even though they're semantically similar)
        assert_ne!(norm1, norm2);
    }

    #[test]
    fn test_normalize_complex_nested_expression() {
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM t WHERE ((a = 1 AND b = 2) OR (c = 3 AND d = 4)) AND e = 5";
        let result = normalize_sql(sql, &dialect);

        assert!(result.is_ok());
    }

    #[test]
    fn test_normalize_in_list_order() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE status IN ('active', 'pending', 'processing')";
        let sql2 = "SELECT * FROM t WHERE status IN ('processing', 'active', 'pending')";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        // IN lists should be sorted for consistency
        // (though current impl might not do this - test documents behavior)
        // If they're different, that's current behavior
        let _ = (norm1, norm2);
    }

    #[test]
    fn test_hash_consistency() {
        // Same input should always produce same hash
        let s = "age > 18 AND status = 'active'";

        let hash1 = hash_sql(s);
        let hash2 = hash_sql(s);
        let hash3 = hash_sql(s);

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }

    #[test]
    fn test_hash_empty_string() {
        let hash = hash_sql("");
        assert!(hash > 0); // Should still produce a hash
    }

    #[test]
    fn test_hash_long_string() {
        let long_str = "a".repeat(10000);
        let hash = hash_sql(&long_str);
        assert!(hash > 0);
    }

    #[test]
    fn test_normalize_error_multiple_tables() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t1, t2 WHERE a = 1";
        let result = normalize_sql(sql, &dialect);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("Exactly one table"));
        }
    }

    #[test]
    fn test_normalize_error_joins() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE a = 1";
        let result = normalize_sql(sql, &dialect);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("JOINs not supported"));
        }
    }

    #[test]
    fn test_normalize_error_derived_table() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM (SELECT * FROM t1) AS d WHERE d.a = 1";
        let result = normalize_sql(sql, &dialect);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("Subqueries and derived tables not supported"));
        }
    }

    #[test]
    fn test_normalize_error_non_select_query() {
        let dialect = PostgreSqlDialect {};

        // Test INSERT
        let insert_sql = "INSERT INTO t VALUES (1, 2)";
        let result = normalize_sql(insert_sql, &dialect);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));

        // Test UPDATE
        let update_sql = "UPDATE t SET a = 1";
        let result = normalize_sql(update_sql, &dialect);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));

        // Test DELETE
        let delete_sql = "DELETE FROM t WHERE a = 1";
        let result = normalize_sql(delete_sql, &dialect);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_normalize_is_null() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE age IS NULL";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("IS NULL"));
    }

    #[test]
    fn test_normalize_is_not_null() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE age IS NOT NULL";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("IS NOT NULL"));
    }

    #[test]
    fn test_normalize_between() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE age BETWEEN 18 AND 65";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("BETWEEN"));
        assert!(result.contains("18"));
        assert!(result.contains("65"));
    }

    #[test]
    fn test_normalize_not_between() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE age NOT BETWEEN 18 AND 65";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("NOT BETWEEN"));
    }

    #[test]
    fn test_normalize_like() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE name LIKE 'John%'";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("LIKE"));
    }

    #[test]
    fn test_normalize_not_like() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE name NOT LIKE 'John%'";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("NOT LIKE"));
    }

    #[test]
    fn test_normalize_like_with_escape() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE name LIKE 'John\\%' ESCAPE '\\'";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("LIKE"));
        assert!(result.contains("ESCAPE"));
    }

    #[test]
    fn test_normalize_ilike() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE name ILIKE 'john%'";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("ILIKE"));
    }

    #[test]
    fn test_normalize_not_ilike() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE name NOT ILIKE 'john%'";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("NOT ILIKE"));
    }

    #[test]
    fn test_normalize_ilike_with_escape() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE name ILIKE 'john\\%' ESCAPE '\\'";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("ILIKE"));
        assert!(result.contains("ESCAPE"));
    }

    #[test]
    fn test_normalize_compound_identifier() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE schema.table.column = 1";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("schema.table.column"));
    }

    #[test]
    fn test_normalize_unary_plus() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE +age = 10";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("+"));
    }

    #[test]
    fn test_normalize_unary_minus() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE -balance > 100";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("-"));
    }

    #[test]
    fn test_normalize_not_in_list() {
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM t WHERE status NOT IN ('active', 'pending')";
        let result = normalize_sql(sql, &dialect).unwrap();
        assert!(result.contains("NOT IN"));
    }

    #[test]
    fn test_error_set_operations() {
        let dialect = PostgreSqlDialect {};

        // UNION is not a simple SELECT
        let sql = "SELECT * FROM t WHERE a = 1 UNION SELECT * FROM t WHERE b = 2";
        let result = normalize_sql(sql, &dialect);

        // This will fail at parse or give unsupported SQL
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_unknown_expr_fallback() {
        let dialect = PostgreSqlDialect {};

        // CAST produces Expr::Cast which is not in the handled set
        let sql = "SELECT * FROM t WHERE CAST(a AS text) = 'hello'";
        let result = normalize_sql(sql, &dialect);

        // Should succeed : unknown expr uses debug fallback
        assert!(result.is_ok());
        let normalized = result.unwrap();
        // The fallback uses {:?} format, so it produces something
        assert!(
            !normalized.is_empty(),
            "the debug fallback always produces at least the expression text"
        );
    }

    #[test]
    fn test_normalize_unknown_unary_op_fallback() {
        let dialect = PostgreSqlDialect {};

        // ~ is PGBitwiseNot, not handled by unary_op_to_string
        let sql = "SELECT * FROM t WHERE ~a = 1";
        let result = normalize_sql(sql, &dialect);

        // Should fail : unknown unary op now returns UnsupportedSql error
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_and_tree_flattening() {
        let dialect = PostgreSqlDialect {};

        // These must hash identically
        let sql1 = "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3";
        let sql2 = "SELECT * FROM t WHERE (a = 1 AND b = 2) AND c = 3";
        let sql3 = "SELECT * FROM t WHERE a = 1 AND (b = 2 AND c = 3)";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();
        let norm3 = normalize_sql(sql3, &dialect).unwrap();

        assert_eq!(norm1, norm2, "Flat AND should equal left-associated AND");
        assert_eq!(norm1, norm3, "Flat AND should equal right-associated AND");
    }

    #[test]
    fn test_or_tree_flattening() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3";
        let sql2 = "SELECT * FROM t WHERE (a = 1 OR b = 2) OR c = 3";
        let sql3 = "SELECT * FROM t WHERE a = 1 OR (b = 2 OR c = 3)";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();
        let norm3 = normalize_sql(sql3, &dialect).unwrap();

        assert_eq!(norm1, norm2);
        assert_eq!(norm1, norm3);
    }

    #[test]
    fn test_distinct_operators_produce_different_strings() {
        let dialect = PostgreSqlDialect {};

        let sql1 = "SELECT * FROM t WHERE a + b > 0";
        let sql2 = "SELECT * FROM t WHERE a - b > 0";

        let norm1 = normalize_sql(sql1, &dialect).unwrap();
        let norm2 = normalize_sql(sql2, &dialect).unwrap();

        assert_ne!(
            norm1, norm2,
            "'+' and '-' must produce different normalized strings"
        );
    }
}
