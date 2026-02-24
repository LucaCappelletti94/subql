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
    /// `SELECT COUNT(column_name)` — counts non-NULL values; column resolved at registration.
    CountColumn { column: crate::ColumnId },
    /// `SELECT SUM(column_name)` — column resolved to ColumnId at registration.
    Sum { column: crate::ColumnId },
    /// `SELECT AVG(column_name)` — emits both sum and count deltas; column resolved at registration.
    Avg { column: crate::ColumnId },
}

/// Extract a plain column name from a function argument, if it is a bare
/// identifier or a two-part `table.column` compound identifier.
/// Returns `None` for wildcards, expressions, or anything else.
fn extract_column_arg(arg: &FunctionArg) -> Option<String> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            Some(ident.value.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            parts.last().map(|p| p.value.clone())
        }
        _ => None,
    }
}

/// Extract the `QueryProjection` from a parsed SELECT statement.
///
/// Accepts:
/// - `SELECT *`                         → `QueryProjection::Rows`
/// - `SELECT COUNT(*) [AS alias]`       → `QueryProjection::Aggregate(AggSpec::CountStar)`
/// - `SELECT COUNT(col) [AS alias]`     → `QueryProjection::Aggregate(AggSpec::CountColumn { column })`
/// - `SELECT SUM(col) [AS alias]`       → `QueryProjection::Aggregate(AggSpec::Sum { column })`
/// - `SELECT AVG(col) [AS alias]`       → `QueryProjection::Aggregate(AggSpec::Avg { column })`
///
/// Returns `Err(UnsupportedSql)` for any other projection.
/// Returns `Err(UnknownColumn)` when the aggregate column does not exist in the catalog.
/// Returns `Err(UnsupportedSql)` when `SUM`/`AVG` is used on a non-numeric column type
/// (only when the catalog implements [`crate::SchemaCatalog::column_type`]).
#[allow(clippy::too_many_lines)]
pub(super) fn extract_projection(
    stmt: &Statement,
    table_id: crate::TableId,
    catalog: &dyn crate::SchemaCatalog,
) -> Result<QueryProjection, RegisterError> {
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
                    // Supports COUNT(*) and COUNT(column) — no FILTER, OVER, or DISTINCT.
                    if f.filter.is_some() {
                        return Err(RegisterError::UnsupportedSql(
                            "COUNT FILTER (WHERE ...) not supported".to_string(),
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
                                    "COUNT(DISTINCT ...) not supported".to_string(),
                                ));
                            }
                            if list.args.len() != 1 {
                                return Err(RegisterError::UnsupportedSql(
                                    "COUNT requires exactly one argument".to_string(),
                                ));
                            }
                            // COUNT(*) — wildcard arg
                            if matches!(
                                &list.args[0],
                                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                            ) {
                                return Ok(QueryProjection::Aggregate(AggSpec::CountStar));
                            }
                            // COUNT(column) — plain column identifier
                            let col_name = extract_column_arg(&list.args[0]).ok_or_else(|| {
                                RegisterError::UnsupportedSql(
                                    "COUNT argument must be * or a plain column name, not an expression"
                                        .to_string(),
                                )
                            })?;
                            let column = catalog
                                .column_id(table_id, &col_name)
                                .ok_or(RegisterError::UnknownColumn {
                                    table_id,
                                    column: col_name,
                                })?;
                            Ok(QueryProjection::Aggregate(AggSpec::CountColumn { column }))
                        }
                        _ => Err(RegisterError::UnsupportedSql(
                            "COUNT requires an argument".to_string(),
                        )),
                    }
                }
                Some(func @ ("sum" | "avg")) => {
                    // SUM(col) and AVG(col) — no FILTER, OVER, or DISTINCT; numeric columns only.
                    if f.filter.is_some() {
                        return Err(RegisterError::UnsupportedSql(format!(
                            "{}(...) FILTER (WHERE ...) not supported",
                            func.to_uppercase()
                        )));
                    }
                    if f.over.is_some() {
                        return Err(RegisterError::UnsupportedSql(
                            "Window functions not supported".to_string(),
                        ));
                    }

                    let column = match &f.args {
                        FunctionArguments::List(list) => {
                            if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
                                return Err(RegisterError::UnsupportedSql(format!(
                                    "{}(DISTINCT ...) not supported",
                                    func.to_uppercase()
                                )));
                            }
                            if list.args.len() != 1 {
                                return Err(RegisterError::UnsupportedSql(format!(
                                    "{} requires exactly one argument",
                                    func.to_uppercase()
                                )));
                            }
                            if matches!(
                                &list.args[0],
                                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                            ) {
                                return Err(RegisterError::UnsupportedSql(format!(
                                    "{}(*) is not supported — use {}(column_name)",
                                    func.to_uppercase(),
                                    func.to_uppercase()
                                )));
                            }
                            let col_name =
                                extract_column_arg(&list.args[0]).ok_or_else(|| {
                                    RegisterError::UnsupportedSql(format!(
                                        "{} argument must be a plain column name, not an expression",
                                        func.to_uppercase()
                                    ))
                                })?;
                            catalog
                                .column_id(table_id, &col_name)
                                .ok_or(RegisterError::UnknownColumn {
                                    table_id,
                                    column: col_name,
                                })?
                        }
                        _ => {
                            return Err(RegisterError::UnsupportedSql(format!(
                                "{} requires a column argument",
                                func.to_uppercase()
                            )));
                        }
                    };

                    // Reject non-numeric column types when the catalog provides type info.
                    if let Some(col_type) = catalog.column_type(table_id, column) {
                        match col_type {
                            crate::ColumnType::Bool | crate::ColumnType::String => {
                                return Err(RegisterError::UnsupportedSql(format!(
                                    "{} requires a numeric column (Int or Float), \
                                     but column {} has type {:?}",
                                    func.to_uppercase(),
                                    column,
                                    col_type,
                                )));
                            }
                            crate::ColumnType::Int
                            | crate::ColumnType::Float
                            | crate::ColumnType::Unknown => {}
                        }
                    }

                    if func == "sum" {
                        Ok(QueryProjection::Aggregate(AggSpec::Sum { column }))
                    } else {
                        Ok(QueryProjection::Aggregate(AggSpec::Avg { column }))
                    }
                }
                Some(name @ ("min" | "max")) => {
                    Err(RegisterError::UnsupportedSql(format!(
                        "{} aggregate not supported — not delta-composable; \
                         see src/todo.md for design notes",
                        name.to_uppercase()
                    )))
                }
                _ => Err(RegisterError::UnsupportedSql(
                    "Unsupported projection: only SELECT *, COUNT(*), COUNT(col), SUM(col), or AVG(col) are supported"
                        .to_string(),
                )),
            }
        } else {
            Err(RegisterError::UnsupportedSql(
                "Unsupported projection: only SELECT *, COUNT(*), COUNT(col), SUM(col), or AVG(col) are supported"
                    .to_string(),
            ))
        }
    } else {
        Err(RegisterError::UnsupportedSql(
            "Unsupported projection: only SELECT *, COUNT(*), COUNT(col), SUM(col), or AVG(col) are supported".to_string(),
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
