use crate::RegisterError;
use sqlparser::ast::{Expr, ObjectName, SetExpr, Statement, TableFactor};

/// Maximum expression nesting depth to prevent stack overflow from fuzzer-crafted SQL.
pub(super) const MAX_EXPR_DEPTH: usize = 128;

/// Maximum SQL input length (defense-in-depth against pathological inputs).
pub(super) const MAX_SQL_LEN: usize = 8192;

/// Extract the single table name and WHERE clause from a supported SELECT.
///
/// This enforces SubQL's statement-shape constraints (single table, no joins,
/// no set operations, no derived tables) so parser and canonicalizer stay in sync.
pub(super) fn extract_single_table_and_where(
    stmt: &Statement,
) -> Result<(ObjectName, Option<Expr>), RegisterError> {
    match stmt {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                if select.from.len() != 1 {
                    return Err(RegisterError::UnsupportedSql(
                        "Exactly one table required (no joins)".to_string(),
                    ));
                }

                if !select.from[0].joins.is_empty() {
                    return Err(RegisterError::UnsupportedSql(
                        "JOINs not supported - SubQL is for single-table CDC event filtering. \
                         For multi-table queries, run this as a regular SQL query in your database."
                            .to_string(),
                    ));
                }

                match &select.from[0].relation {
                    TableFactor::Table { name, .. } => Ok((name.clone(), select.selection.clone())),
                    _ => Err(RegisterError::UnsupportedSql(
                        "Subqueries and derived tables not supported - SubQL is for single-table WHERE clauses. \
                         Run this as a regular SQL query in your database instead."
                            .to_string(),
                    )),
                }
            }
            _ => Err(RegisterError::UnsupportedSql(
                "Set operations (UNION, INTERSECT, EXCEPT) not supported - SubQL is for single-table CDC event filtering. \
                 For queries combining multiple result sets, run this as a regular SQL query in your database."
                    .to_string(),
            )),
        },
        _ => Err(RegisterError::UnsupportedSql(
            "Only SELECT statements supported - SubQL is for querying CDC events, not modifying data. \
             For INSERT, UPDATE, DELETE, or DDL operations, use your database directly."
                .to_string(),
        )),
    }
}
