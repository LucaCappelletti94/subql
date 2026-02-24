use crate::RegisterError;
use sqlparser::ast::{
    DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName,
    SelectItem, SetExpr, Statement, TableFactor,
};

/// Projection kind for a subscription SQL statement.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryProjection {
    /// `SELECT *` — deliver row events (default, current behaviour).
    Rows,
    /// `SELECT <aggregate>` — deliver signed count deltas.
    Aggregate(AggSpec),
}

/// Aggregate function specification.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AggSpec {
    /// `SELECT COUNT(*)`
    CountStar,
    // Future: Sum { column: ColumnId }, Min { column: ColumnId }, …
}

/// Extract the `QueryProjection` from a parsed SELECT statement.
///
/// Accepts:
/// - `SELECT *`                         → `QueryProjection::Rows`
/// - `SELECT COUNT(*) [AS alias]`       → `QueryProjection::Aggregate(AggSpec::CountStar)`
///
/// Returns `Err(UnsupportedSql)` for any other projection.
pub(super) fn extract_projection(stmt: &Statement) -> Result<QueryProjection, RegisterError> {
    let select = match stmt {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => {
                return Ok(QueryProjection::Rows);
            }
        },
        _ => return Ok(QueryProjection::Rows),
    };

    let items = &select.projection;

    // SELECT * — single wildcard item
    if items.len() == 1 {
        if let SelectItem::Wildcard(_) = &items[0] {
            return Ok(QueryProjection::Rows);
        }
    }

    // Single expression (with or without alias)
    if items.len() == 1 {
        let expr = match &items[0] {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(RegisterError::UnsupportedSql(
                    "Qualified wildcard (e.g. table.*) not supported in projection".to_string(),
                ));
            }
            SelectItem::Wildcard(_) => unreachable!("handled above"),
        };

        if let Expr::Function(f) = expr {
            // Get the (unqualified) function name — last ObjectName part.
            let func_name = f
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.to_lowercase());

            match func_name.as_deref() {
                Some("count") => {
                    // Must be COUNT(*) — no FILTER, no OVER, no DISTINCT.
                    if f.filter.is_some() {
                        return Err(RegisterError::UnsupportedSql(
                            "COUNT(*) FILTER (WHERE ...) not supported".to_string(),
                        ));
                    }
                    if f.over.is_some() {
                        return Err(RegisterError::UnsupportedSql(
                            "Window functions not supported".to_string(),
                        ));
                    }

                    match &f.args {
                        FunctionArguments::List(list) => {
                            // Reject DISTINCT
                            if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
                                return Err(RegisterError::UnsupportedSql(
                                    "COUNT(DISTINCT ...) not supported — only COUNT(*) is"
                                        .to_string(),
                                ));
                            }
                            // Must be exactly one arg: the wildcard `*`
                            if list.args.len() == 1
                                && matches!(
                                    &list.args[0],
                                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                                )
                            {
                                return Ok(QueryProjection::Aggregate(AggSpec::CountStar));
                            }
                            Err(RegisterError::UnsupportedSql(
                                "Only COUNT(*) is supported, not COUNT(expression)".to_string(),
                            ))
                        }
                        _ => Err(RegisterError::UnsupportedSql(
                            "Only COUNT(*) is supported".to_string(),
                        )),
                    }
                }
                Some(name @ ("sum" | "avg" | "min" | "max")) => {
                    Err(RegisterError::UnsupportedSql(format!(
                        "{} aggregate not yet supported — only COUNT(*) is implemented",
                        name.to_uppercase()
                    )))
                }
                _ => Err(RegisterError::UnsupportedSql(
                    "Unsupported projection: only SELECT * or SELECT COUNT(*) are supported"
                        .to_string(),
                )),
            }
        } else {
            Err(RegisterError::UnsupportedSql(
                "Unsupported projection: only SELECT * or SELECT COUNT(*) are supported"
                    .to_string(),
            ))
        }
    } else {
        Err(RegisterError::UnsupportedSql(
            "Unsupported projection: only SELECT * or SELECT COUNT(*) are supported".to_string(),
        ))
    }
}

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
