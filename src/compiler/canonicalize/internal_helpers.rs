//! Internal normalization helpers split out of `canonicalize`.

use crate::compiler::sql_shape;
use crate::RegisterError;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sqlparser::ast::{BinaryOperator, Expr, Statement};

/// Extract WHERE clause from SELECT statement.
pub(super) fn extract_where(stmt: &Statement) -> Result<Option<Expr>, RegisterError> {
    let (_table_name, where_clause) = sql_shape::extract_single_table_and_where(stmt)?;
    Ok(where_clause)
}

/// Normalize expression recursively.
///
/// Applies transformations:
/// - Sort AND/OR operands alphabetically
/// - Remove redundant parentheses
/// - Normalize to canonical string representation
pub fn normalize_expr(expr: &Expr) -> Result<String, RegisterError> {
    normalize_expr_inner(expr, 0)
}

/// Collect all operands of a flattened AND or OR tree.
/// E.g., `a AND (b AND c)` -> [a, b, c]
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

        // A membership subquery needs an arm of its own rather than the Debug
        // fallback below, because a `Debug` rendering carries the byte offset of
        // every identifier (`Ident` derives `Debug` over its `span` while its
        // `PartialEq` ignores it). Predicate identity would then depend on where
        // in the statement text the filter was written, so the same filter under
        // two spellings would compile twice and never share a predicate.
        //
        // The inner query is rendered by sqlparser rather than normalized
        // clause by clause. The cost is that two spellings of the inner WHERE
        // (`a = 1 AND b = 2` against `b = 2 AND a = 1`) do not share, which is a
        // missed sharing rather than a wrong one. Normalizing it would mean
        // listing every clause that changes which rows are members (`LIMIT`,
        // `ORDER BY`, `GROUP BY`, `DISTINCT`, a `WITH`), and a clause left off
        // that list silently maps two different filters onto one predicate.
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let not_str = if *negated { "NOT " } else { "" };
            format!(
                "{} {not_str}IN ({subquery})",
                normalize_expr_inner(expr, depth + 1)?,
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

        // A call is rendered by sqlparser rather than by the Debug fallback
        // below, for the same reason the membership subquery above is: `Ident`
        // derives `Debug` over its byte span while its `PartialEq` ignores it,
        // so a Debug rendering would make predicate identity depend on where in
        // the statement the call was written.
        Expr::Function(function) => format!("{function}"),

        _ => {
            // Fallback: use debug representation
            format!("{expr:?}")
        }
    })
}

/// Check if binary operator is commutative.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_commutative(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Eq
    )
}

/// Convert binary operator to canonical string.
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

/// Convert unary operator to canonical string.
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
