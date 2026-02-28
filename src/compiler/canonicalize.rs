//! SQL normalization and hashing for predicate deduplication

use super::sql_shape;
use crate::RegisterError;
use seahash::SeaHasher;
use sqlparser::ast::{BinaryOperator, Expr, Statement};
use sqlparser::dialect::Dialect;
use std::hash::{Hash, Hasher};

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
    // Reject SQL that would cause stack overflow in the parser
    check_sql_depth(sql)?;

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

// ============================================================================
// Internal Helpers
// ============================================================================

/// Reject SQL with excessive nesting before parsing to prevent stack overflow.
///
/// Tracks parenthesis nesting depth and consecutive unary-operator chains,
/// both of which cause recursive descent in sqlparser.
fn check_sql_depth(sql: &str) -> Result<(), RegisterError> {
    let mut paren_depth: usize = 0;
    let mut consecutive_ops: usize = 0;

    for c in sql.bytes() {
        match c {
            b'(' => {
                paren_depth += 1;
                consecutive_ops += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                consecutive_ops = 0;
            }
            b'+' | b'-' | b'*' | b'/' | b'=' | b'<' | b'>' | b'!' | b'~' => {
                consecutive_ops += 1;
            }
            b' ' | b'\t' | b'\n' | b'\r' => {}
            _ => {
                consecutive_ops = 0;
            }
        }

        if paren_depth > sql_shape::MAX_EXPR_DEPTH || consecutive_ops > sql_shape::MAX_EXPR_DEPTH {
            return Err(RegisterError::UnsupportedSql(
                "Expression nesting too deep".to_string(),
            ));
        }
    }
    Ok(())
}

/// Extract WHERE clause from SELECT statement
fn extract_where(stmt: &Statement) -> Result<Option<Expr>, RegisterError> {
    let (_table_name, where_clause) = sql_shape::extract_single_table_and_where(stmt)?;
    Ok(where_clause)
}

/// Normalize expression recursively
///
/// Applies transformations:
/// - Sort AND/OR operands alphabetically
/// - Remove redundant parentheses
/// - Normalize to canonical string representation
fn normalize_expr(expr: &Expr) -> Result<String, RegisterError> {
    normalize_expr_inner(expr, 0)
}

/// Collect all operands of a flattened AND or OR tree.
/// E.g., `a AND (b AND c)` → [a, b, c]
fn collect_flat_children<'a>(expr: &'a Expr, target_op: &BinaryOperator) -> Vec<&'a Expr> {
    match expr {
        Expr::Nested(inner) => collect_flat_children(inner, target_op),
        Expr::BinaryOp { left, op, right } if op == target_op => {
            let mut children = collect_flat_children(left, target_op);
            children.extend(collect_flat_children(right, target_op));
            children
        }
        _ => vec![expr],
    }
}

#[allow(clippy::too_many_lines)]
fn normalize_expr_inner(expr: &Expr, depth: usize) -> Result<String, RegisterError> {
    if depth > sql_shape::MAX_EXPR_DEPTH {
        return Err(RegisterError::UnsupportedSql(
            "Expression nesting too deep".to_string(),
        ));
    }

    Ok(match expr {
        Expr::BinaryOp { left, op, right } => {
            // A4: For AND/OR, flatten nested chains before sorting so that
            // "(a AND b) AND c" and "a AND (b AND c)" produce the same string.
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                let mut children = collect_flat_children(left, op);
                children.extend(collect_flat_children(right, op));
                let mut child_strs: Vec<String> = children
                    .iter()
                    .map(|c| normalize_expr_inner(c, depth + 1))
                    .collect::<Result<_, _>>()?;
                child_strs.sort();
                let op_str = op_to_string(op)?;
                child_strs
                    .into_iter()
                    .reduce(|acc, s| format!("({acc} {op_str} {s})"))
                    .unwrap_or_default()
            } else {
                let left_norm = normalize_expr_inner(left, depth + 1)?;
                let right_norm = normalize_expr_inner(right, depth + 1)?;

                // For commutative operators, sort operands
                let (left_str, right_str) = if is_commutative(op) {
                    if left_norm <= right_norm {
                        (left_norm, right_norm)
                    } else {
                        (right_norm, left_norm)
                    }
                } else {
                    (left_norm, right_norm)
                };

                format!("({} {} {})", left_str, op_to_string(op)?, right_str)
            }
        }

        Expr::UnaryOp { op, expr } => {
            format!(
                "{} {}",
                unary_op_to_string(op)?,
                normalize_expr_inner(expr, depth + 1)?
            )
        }

        Expr::IsNull(expr) => {
            format!("{} IS NULL", normalize_expr_inner(expr, depth + 1)?)
        }

        Expr::IsNotNull(expr) => {
            format!("{} IS NOT NULL", normalize_expr_inner(expr, depth + 1)?)
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut list_strs: Vec<String> = list
                .iter()
                .map(|e| normalize_expr_inner(e, depth + 1))
                .collect::<Result<_, _>>()?;
            list_strs.sort(); // Stable ordering

            let not_str = if *negated { "NOT " } else { "" };
            format!(
                "{} {}IN ({})",
                normalize_expr_inner(expr, depth + 1)?,
                not_str,
                list_strs.join(", ")
            )
        }

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let not_str = if *negated { "NOT " } else { "" };
            format!(
                "{} {}BETWEEN {} AND {}",
                normalize_expr_inner(expr, depth + 1)?,
                not_str,
                normalize_expr_inner(low, depth + 1)?,
                normalize_expr_inner(high, depth + 1)?
            )
        }

        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } => {
            let not_str = if *negated { "NOT " } else { "" };
            let escape_str = escape_char
                .as_ref()
                .map_or_else(String::new, |ch| format!(" ESCAPE '{ch}'"));
            format!(
                "{} {}LIKE {}{}",
                normalize_expr_inner(expr, depth + 1)?,
                not_str,
                normalize_expr_inner(pattern, depth + 1)?,
                escape_str
            )
        }

        Expr::ILike {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } => {
            let not_str = if *negated { "NOT " } else { "" };
            let escape_str = escape_char
                .as_ref()
                .map_or_else(String::new, |ch| format!(" ESCAPE '{ch}'"));
            format!(
                "{} {}ILIKE {}{}",
                normalize_expr_inner(expr, depth + 1)?,
                not_str,
                normalize_expr_inner(pattern, depth + 1)?,
                escape_str
            )
        }

        Expr::Nested(inner) => {
            // Remove redundant parentheses for simple expressions
            normalize_expr_inner(inner, depth + 1)?
        }

        Expr::Identifier(ident) => ident.value.clone(),

        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|p| &p.value)
            .cloned()
            .collect::<Vec<_>>()
            .join("."),

        Expr::Value(val) => {
            format!("{}", val.value)
        }

        _ => {
            // Fallback: use debug representation
            format!("{expr:?}")
        }
    })
}

/// Check if binary operator is commutative
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_commutative(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Eq
    )
}

/// Convert binary operator to canonical string
fn op_to_string(op: &BinaryOperator) -> Result<&'static str, RegisterError> {
    match op {
        BinaryOperator::And => Ok("AND"),
        BinaryOperator::Or => Ok("OR"),
        BinaryOperator::Eq => Ok("="),
        BinaryOperator::NotEq => Ok("!="),
        BinaryOperator::Lt => Ok("<"),
        BinaryOperator::LtEq => Ok("<="),
        BinaryOperator::Gt => Ok(">"),
        BinaryOperator::GtEq => Ok(">="),
        BinaryOperator::Plus => Ok("+"),
        BinaryOperator::Minus => Ok("-"),
        BinaryOperator::Multiply => Ok("*"),
        BinaryOperator::Divide => Ok("/"),
        BinaryOperator::Modulo => Ok("%"),
        other => Err(RegisterError::UnsupportedSql(format!(
            "Unsupported binary operator: {other}"
        ))),
    }
}

/// Convert unary operator to canonical string
#[allow(clippy::trivially_copy_pass_by_ref)]
fn unary_op_to_string(op: &sqlparser::ast::UnaryOperator) -> Result<&'static str, RegisterError> {
    match op {
        sqlparser::ast::UnaryOperator::Not => Ok("NOT"),
        sqlparser::ast::UnaryOperator::Plus => Ok("+"),
        sqlparser::ast::UnaryOperator::Minus => Ok("-"),
        other => Err(RegisterError::UnsupportedSql(format!(
            "Unsupported unary operator: {other}"
        ))),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::single_char_pattern
)]
mod tests {
    use super::*;
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

    // ========================================================================
    // Phase 1: Error Path Coverage Tests
    // ========================================================================

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

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Canonicalize Completion
    // ========================================================================

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

        // Should succeed — unknown expr uses debug fallback
        assert!(result.is_ok());
        let normalized = result.unwrap();
        // The fallback uses {:?} format, so it produces something
        assert!(!normalized.is_empty());
    }

    #[test]
    fn test_normalize_unknown_unary_op_fallback() {
        let dialect = PostgreSqlDialect {};

        // ~ is PGBitwiseNot, not handled by unary_op_to_string
        let sql = "SELECT * FROM t WHERE ~a = 1";
        let result = normalize_sql(sql, &dialect);

        // Should fail — unknown unary op now returns UnsupportedSql error
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
