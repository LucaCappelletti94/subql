//! SQL normalization and hashing for predicate deduplication

use sqlparser::ast::{Expr, Statement, BinaryOperator};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use std::hash::{Hash, Hasher};
use seahash::SeaHasher;
use crate::RegisterError;

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
    // Parse SQL
    let statements = Parser::parse_sql(dialect, sql)
        .map_err(|e| RegisterError::ParseError {
            line: 1,
            column: 0,
            message: e.to_string(),
        })?;

    if statements.len() != 1 {
        return Err(RegisterError::UnsupportedSql(
            "Expected exactly one SELECT statement".to_string()
        ));
    }

    // Extract WHERE clause
    let where_expr = extract_where(&statements[0])?;

    // No WHERE clause = always-true predicate
    let normalized_expr = if let Some(expr) = where_expr {
        normalize_expr(&expr)
    } else {
        "TRUE".to_string()
    };

    Ok(normalized_expr)
}

/// Hash normalized SQL for fast predicate lookup
///
/// Uses `seahash` for deterministic, high-quality hashing.
/// Returns 128-bit hash (two 64-bit hashes concatenated).
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

    ((hash1 as u128) << 64) | (hash2 as u128)
}

// ============================================================================
// Internal Helpers
// ============================================================================

/// Extract WHERE clause from SELECT statement
fn extract_where(stmt: &Statement) -> Result<Option<Expr>, RegisterError> {
    use sqlparser::ast::SetExpr;

    match stmt {
        Statement::Query(query) => {
            match query.body.as_ref() {
                SetExpr::Select(select) => {
                    // Validate single table (no joins)
                    if select.from.len() != 1 {
                        return Err(RegisterError::UnsupportedSql(
                            "Exactly one table required".to_string()
                        ));
                    }

                    if !select.from[0].joins.is_empty() {
                        return Err(RegisterError::UnsupportedSql(
                            "Joins not supported".to_string()
                        ));
                    }

                    Ok(select.selection.clone())
                }
                _ => Err(RegisterError::UnsupportedSql(
                    "Only SELECT supported".to_string()
                )),
            }
        }
        _ => Err(RegisterError::UnsupportedSql(
            "Only SELECT supported".to_string()
        )),
    }
}

/// Normalize expression recursively
///
/// Applies transformations:
/// - Sort AND/OR operands alphabetically
/// - Remove redundant parentheses
/// - Normalize to canonical string representation
fn normalize_expr(expr: &Expr) -> String {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_norm = normalize_expr(left);
            let right_norm = normalize_expr(right);

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

            format!("({} {} {})", left_str, op_to_string(op), right_str)
        }

        Expr::UnaryOp { op, expr } => {
            format!("{} {}", unary_op_to_string(op), normalize_expr(expr))
        }

        Expr::IsNull(expr) => {
            format!("{} IS NULL", normalize_expr(expr))
        }

        Expr::IsNotNull(expr) => {
            format!("{} IS NOT NULL", normalize_expr(expr))
        }

        Expr::InList { expr, list, negated } => {
            let mut list_strs: Vec<String> = list.iter()
                .map(normalize_expr)
                .collect();
            list_strs.sort(); // Stable ordering

            let not_str = if *negated { "NOT " } else { "" };
            format!("{} {}IN ({})",
                    normalize_expr(expr),
                    not_str,
                    list_strs.join(", "))
        }

        Expr::Between { expr, low, high, negated } => {
            let not_str = if *negated { "NOT " } else { "" };
            format!("{} {}BETWEEN {} AND {}",
                    normalize_expr(expr),
                    not_str,
                    normalize_expr(low),
                    normalize_expr(high))
        }

        Expr::Like { expr, pattern, negated, escape_char } => {
            let not_str = if *negated { "NOT " } else { "" };
            let escape_str = if let Some(ch) = escape_char {
                format!(" ESCAPE '{}'", ch)
            } else {
                String::new()
            };
            format!("{} {}LIKE {}{}",
                    normalize_expr(expr),
                    not_str,
                    normalize_expr(pattern),
                    escape_str)
        }

        Expr::ILike { expr, pattern, negated, escape_char } => {
            let not_str = if *negated { "NOT " } else { "" };
            let escape_str = if let Some(ch) = escape_char {
                format!(" ESCAPE '{}'", ch)
            } else {
                String::new()
            };
            format!("{} {}ILIKE {}{}",
                    normalize_expr(expr),
                    not_str,
                    normalize_expr(pattern),
                    escape_str)
        }

        Expr::Nested(inner) => {
            // Remove redundant parentheses for simple expressions
            normalize_expr(inner)
        }

        Expr::Identifier(ident) => {
            ident.value.clone()
        }

        Expr::CompoundIdentifier(parts) => {
            parts.iter().map(|p| &p.value).cloned().collect::<Vec<_>>().join(".")
        }

        Expr::Value(val) => {
            format!("{}", val)
        }

        _ => {
            // Fallback: use debug representation
            format!("{:?}", expr)
        }
    }
}

/// Check if binary operator is commutative
fn is_commutative(op: &BinaryOperator) -> bool {
    matches!(op, BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Eq)
}

/// Convert binary operator to canonical string
fn op_to_string(op: &BinaryOperator) -> &'static str {
    match op {
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "!=",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        _ => "?",
    }
}

/// Convert unary operator to canonical string
fn unary_op_to_string(op: &sqlparser::ast::UnaryOperator) -> &'static str {
    match op {
        sqlparser::ast::UnaryOperator::Not => "NOT",
        sqlparser::ast::UnaryOperator::Plus => "+",
        sqlparser::ast::UnaryOperator::Minus => "-",
        _ => "?",
    }
}

#[cfg(test)]
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
}
