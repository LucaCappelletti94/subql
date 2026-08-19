use crate::{catalog_helpers, RegisterError};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
};

const WINDOW_FUNCTIONS_NOT_SUPPORTED: &str = "Window functions not supported";
const UNSUPPORTED_PROJECTION: &str =
    "Unsupported projection: only SELECT *, COUNT(*), COUNT(col), SUM(col), AVG(col), \
     VAR_POP(col), VAR_SAMP(col), STDDEV_POP(col), or STDDEV_SAMP(col) are supported \
     (VARIANCE/STDDEV are accepted as aliases for VAR_SAMP/STDDEV_SAMP)";

/// Projection kind for a subscription SQL statement.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryProjection {
    /// `SELECT *`: deliver row events (default, current behaviour).
    Rows,
    /// `SELECT <aggregate>`: deliver signed count deltas.
    Aggregate(AggSpec),
}

/// Aggregate function specification.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AggSpec {
    /// `SELECT COUNT(*)`
    CountStar,
    /// `SELECT COUNT(column_name)`. Counts non-NULL values. Column resolved
    /// at registration.
    CountColumn { column: crate::ColumnId },
    /// `SELECT SUM(column_name)`. Column resolved to `ColumnId` at registration.
    Sum { column: crate::ColumnId },
    /// `SELECT AVG(column_name)`. Emits both sum and count deltas. Column
    /// resolved at registration.
    Avg { column: crate::ColumnId },
    /// `SELECT VAR_POP(column_name)`. Population variance. Emits `Stats`
    /// deltas (`sum`, `sum_sq`, `count`). Consumer computes
    /// `sum_sq / N - (sum / N).powi(2)`.
    VarPop { column: crate::ColumnId },
    /// `SELECT VAR_SAMP(column_name)` (alias `VARIANCE`). Sample variance.
    /// Emits `Stats` deltas. Consumer computes
    /// `(sum_sq - sum.powi(2) / N) / (N - 1)`, requires `N >= 2`.
    VarSamp { column: crate::ColumnId },
    /// `SELECT STDDEV_POP(column_name)`. Population standard deviation.
    /// Same `Stats` deltas as `VarPop`. Consumer takes `sqrt(var_pop)`.
    StddevPop { column: crate::ColumnId },
    /// `SELECT STDDEV_SAMP(column_name)` (alias `STDDEV`). Sample standard
    /// deviation. Same `Stats` deltas as `VarSamp`. Consumer takes
    /// `sqrt(var_samp)`.
    StddevSamp { column: crate::ColumnId },
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

/// Resolve a single bare-column aggregate argument (`SUM(col)`, `MIN(col)`,
/// etc.) to a `ColumnId`. Rejects `FILTER`, `OVER`, `DISTINCT`, wildcard
/// arguments, multi-argument calls, and non-column expressions. Does not
/// constrain the column type. Callers needing a numeric column add that
/// check on top (see [`resolve_numeric_agg_column`]).
///
/// `display` is the upper-cased function name used in error messages.
pub(crate) fn resolve_single_column_arg<DB: DatabaseLike>(
    display: &str,
    f: &sqlparser::ast::Function,
    table_id: crate::TableId,
    database: &DB,
) -> Result<crate::ColumnId, RegisterError> {
    if f.filter.is_some() {
        return Err(RegisterError::UnsupportedSql(format!(
            "{display}(...) FILTER (WHERE ...) not supported"
        )));
    }
    if f.over.is_some() {
        return Err(RegisterError::UnsupportedSql(
            WINDOW_FUNCTIONS_NOT_SUPPORTED.to_string(),
        ));
    }

    match &f.args {
        FunctionArguments::List(list) => {
            if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
                return Err(RegisterError::UnsupportedSql(format!(
                    "{display}(DISTINCT ...) not supported"
                )));
            }
            if list.args.len() != 1 {
                return Err(RegisterError::UnsupportedSql(format!(
                    "{display} requires exactly one argument"
                )));
            }
            if matches!(
                &list.args[0],
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            ) {
                return Err(RegisterError::UnsupportedSql(format!(
                    "{display}(*) is not supported, use {display}(column_name)"
                )));
            }
            let col_name = extract_column_arg(&list.args[0]).ok_or_else(|| {
                RegisterError::UnsupportedSql(format!(
                    "{display} argument must be a plain column name, not an expression"
                ))
            })?;
            catalog_helpers::column_id(database, table_id, &col_name).ok_or(
                RegisterError::UnknownColumn {
                    table_id,
                    column: col_name,
                },
            )
        }
        _ => Err(RegisterError::UnsupportedSql(format!(
            "{display} requires a column argument"
        ))),
    }
}

/// Resolve a single-column numeric-aggregate argument (`SUM`, `AVG`,
/// `VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, `STDDEV_SAMP`, plus the `VARIANCE`
/// and `STDDEV` aliases). Layers a numeric-type constraint on top of
/// [`resolve_single_column_arg`]: rejects `Bool`/`String` columns when the
/// catalog exposes type information.
fn resolve_numeric_agg_column<DB: DatabaseLike>(
    func: &str,
    f: &sqlparser::ast::Function,
    table_id: crate::TableId,
    database: &DB,
) -> Result<crate::ColumnId, RegisterError> {
    let display = func.to_uppercase();
    let column = resolve_single_column_arg(&display, f, table_id, database)?;

    if let Some(kind) = catalog_helpers::column_scalar_kind(database, table_id, column) {
        match kind {
            // Numeric scalars: SUM/AVG/variance/stddev accept these.
            crate::backend::ScalarKind::Int
            | crate::backend::ScalarKind::Float
            | crate::backend::ScalarKind::Decimal => {}
            // Everything else is rejected. Give the caller the concrete
            // kind in the error so the message matches the aggregate's
            // requirement.
            other => {
                return Err(RegisterError::UnsupportedSql(format!(
                    "{display} requires a numeric column (Int, Float, or Decimal), \
                     but column {column} has type {other:?}"
                )));
            }
        }
    }

    Ok(column)
}

/// Extract the `QueryProjection` from a parsed SELECT statement.
///
/// Accepts:
/// - `SELECT *`                            -> `QueryProjection::Rows`
/// - `SELECT COUNT(*) [AS alias]`          -> `Aggregate(CountStar)`
/// - `SELECT COUNT(col) [AS alias]`        -> `Aggregate(CountColumn { column })`
/// - `SELECT SUM(col) [AS alias]`          -> `Aggregate(Sum { column })`
/// - `SELECT AVG(col) [AS alias]`          -> `Aggregate(Avg { column })`
/// - `SELECT VAR_POP(col) [AS alias]`      -> `Aggregate(VarPop { column })`
/// - `SELECT VAR_SAMP(col) [AS alias]`     -> `Aggregate(VarSamp { column })`
/// - `SELECT STDDEV_POP(col) [AS alias]`   -> `Aggregate(StddevPop { column })`
/// - `SELECT STDDEV_SAMP(col) [AS alias]`  -> `Aggregate(StddevSamp { column })`
/// - `VARIANCE(col)` is accepted as a `VAR_SAMP` alias.
/// - `STDDEV(col)` is accepted as a `STDDEV_SAMP` alias.
///
/// Returns `Err(UnsupportedSql)` for any other projection.
/// Returns `Err(UnknownColumn)` when the aggregate column does not exist in the catalog.
/// Returns `Err(UnsupportedSql)` when `SUM`/`AVG`/`VAR_*`/`STDDEV_*` is used on a
/// non-numeric column type (only when the catalog exposes type information via
/// [`catalog_helpers::column_type`]).
/// Whether `items` is a complete, duplicate-free list of the table's columns,
/// each projected as a bare or table-qualified column reference. Such a
/// projection is equivalent to `SELECT *` for subql (which delivers full row
/// images). Returns false for partial lists, duplicates, aliases, expressions,
/// wildcards, or when the catalog cannot report the table's arity.
fn is_complete_column_list<DB: DatabaseLike>(
    items: &[SelectItem],
    table_id: crate::TableId,
    database: &DB,
) -> bool {
    let Some(arity) = catalog_helpers::table_arity(database, table_id) else {
        return false;
    };
    if items.is_empty() {
        return false;
    }
    let mut seen: Vec<crate::ColumnId> = Vec::with_capacity(items.len());
    for item in items {
        let SelectItem::UnnamedExpr(expr) = item else {
            return false;
        };
        let name = match expr {
            Expr::Identifier(ident) => ident.value.as_str(),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => parts[1].value.as_str(),
            _ => return false,
        };
        let Some(col) = catalog_helpers::column_id(database, table_id, name) else {
            return false;
        };
        if seen.contains(&col) {
            return false;
        }
        seen.push(col);
    }
    seen.len() == arity
}

#[allow(clippy::too_many_lines)]
pub(super) fn extract_projection<DB: DatabaseLike>(
    stmt: &Statement,
    table_id: crate::TableId,
    database: &DB,
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

    // SELECT *: single wildcard item
    if items.len() == 1 {
        if let SelectItem::Wildcard(_) = &items[0] {
            return Ok(QueryProjection::Rows);
        }
    }

    // A complete, duplicate-free list of the table's columns is equivalent to
    // `SELECT *`: subql delivers full row images regardless of the projection,
    // and diesel renders row queries as an explicit all-columns list. Partial
    // lists, aliases, or expressions fall through to the aggregate checks below.
    if is_complete_column_list(items, table_id, database) {
        return Ok(QueryProjection::Rows);
    }

    // Single expression (with or without alias)
    if items.len() == 1 {
        let expr = match &items[0] {
            // `expr`, `expr AS alias`, and Spark-SQL's `expr AS (a1, a2, ...)`
            // all project the same underlying expression. subql does not care
            // about the alias names, only the expression shape (COUNT/SUM/AVG/
            // wildcard).
            SelectItem::UnnamedExpr(e)
            | SelectItem::ExprWithAlias { expr: e, .. }
            | SelectItem::ExprWithAliases { expr: e, .. } => e,
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(RegisterError::UnsupportedSql(
                    "Qualified wildcard (e.g. table.*) not supported in projection".to_string(),
                ));
            }
            SelectItem::Wildcard(_) => unreachable!("handled above"),
        };

        if let Expr::Function(f) = expr {
            // Get the (unqualified) function name (last ObjectName part).
            let func_name = f
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.to_lowercase());

            match func_name.as_deref() {
                Some("count") => {
                    // Supports COUNT(*) and COUNT(column): no FILTER, OVER, or DISTINCT.
                    if f.filter.is_some() {
                        return Err(RegisterError::UnsupportedSql(
                            "COUNT FILTER (WHERE ...) not supported".to_string(),
                        ));
                    }
                    if f.over.is_some() {
                        return Err(RegisterError::UnsupportedSql(
                            WINDOW_FUNCTIONS_NOT_SUPPORTED.to_string(),
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
                            // COUNT(*): wildcard arg
                            if matches!(
                                &list.args[0],
                                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                            ) {
                                return Ok(QueryProjection::Aggregate(AggSpec::CountStar));
                            }
                            // COUNT(column): plain column identifier
                            let col_name = extract_column_arg(&list.args[0]).ok_or_else(|| {
                                RegisterError::UnsupportedSql(
                                    "COUNT argument must be * or a plain column name, not an expression"
                                        .to_string(),
                                )
                            })?;
                            let column = catalog_helpers::column_id(database, table_id, &col_name)
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
                Some(
                    func @ ("sum" | "avg" | "var_pop" | "var_samp" | "variance" | "stddev_pop"
                    | "stddev_samp" | "stddev"),
                ) => {
                    let column = resolve_numeric_agg_column(func, f, table_id, database)?;
                    let spec = match func {
                        "sum" => AggSpec::Sum { column },
                        "avg" => AggSpec::Avg { column },
                        "var_pop" => AggSpec::VarPop { column },
                        "var_samp" | "variance" => AggSpec::VarSamp { column },
                        "stddev_pop" => AggSpec::StddevPop { column },
                        "stddev_samp" | "stddev" => AggSpec::StddevSamp { column },
                        _ => unreachable!("matched function name above"),
                    };
                    Ok(QueryProjection::Aggregate(spec))
                }
                Some(name @ ("min" | "max")) => Err(RegisterError::UnsupportedSql(format!(
                    "{} aggregate not supported, not delta-composable. \
                     See MILESTONES.md for design notes.",
                    name.to_uppercase()
                ))),
                _ => Err(RegisterError::UnsupportedSql(
                    UNSUPPORTED_PROJECTION.to_string(),
                )),
            }
        } else {
            Err(RegisterError::UnsupportedSql(
                UNSUPPORTED_PROJECTION.to_string(),
            ))
        }
    } else {
        Err(RegisterError::UnsupportedSql(
            UNSUPPORTED_PROJECTION.to_string(),
        ))
    }
}

/// A scalar `MIN`/`MAX` aggregate, which the core engine cannot evaluate
/// incrementally (not delta-composable). Used by the reexec wrapper, which
/// handles these by re-querying the database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScalarAggKind {
    /// `MIN(column)`
    Min,
    /// `MAX(column)`
    Max,
}

/// Detect a single-column scalar `MIN`/`MAX` projection.
///
/// Returns:
/// - `Ok(Some((kind, column)))` for `SELECT MIN(col)` / `SELECT MAX(col)`
///   (with or without an alias), the column resolved against the catalog.
/// - `Ok(None)` when the projection is anything else (`SELECT *`, `COUNT`,
///   `SUM`, multiple items, a non-function expression, ...). Callers treat
///   this as "not a scalar MIN/MAX".
/// - `Err(UnsupportedSql)` for a `MIN`/`MAX` call with an unsupported argument
///   shape (`DISTINCT`, `FILTER`, `OVER`, wildcard, expression), and
///   `Err(UnknownColumn)` when the argument names a column not in the table.
///
/// Unlike [`extract_projection`], the column type is not constrained: `MIN`
/// and `MAX` are well-defined on any orderable type.
pub(crate) fn extract_scalar_aggregate<DB: DatabaseLike>(
    stmt: &Statement,
    table_id: crate::TableId,
    database: &DB,
) -> Result<Option<(ScalarAggKind, crate::ColumnId)>, RegisterError> {
    let select = match stmt {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let items = &select.projection;
    if items.len() != 1 {
        return Ok(None);
    }

    let expr = match &items[0] {
        SelectItem::UnnamedExpr(e)
        | SelectItem::ExprWithAlias { expr: e, .. }
        | SelectItem::ExprWithAliases { expr: e, .. } => e,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(None),
    };

    let Expr::Function(f) = expr else {
        return Ok(None);
    };

    let func_name = f
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_lowercase());

    let kind = match func_name.as_deref() {
        Some("min") => ScalarAggKind::Min,
        Some("max") => ScalarAggKind::Max,
        _ => return Ok(None),
    };

    let display = match kind {
        ScalarAggKind::Min => "MIN",
        ScalarAggKind::Max => "MAX",
    };
    let column = resolve_single_column_arg(display, f, table_id, database)?;
    Ok(Some((kind, column)))
}

/// Parse and validate a single SQL statement from text.
///
/// Encapsulates the common sequence: length / sanity check -> parse ->
/// single-statement assertion.
pub(super) fn parse_single_statement(
    sql: &str,
    dialect: &dyn sqlparser::dialect::Dialect,
) -> Result<sqlparser::ast::Statement, crate::RegisterError> {
    if sql.len() > MAX_SQL_LEN {
        return Err(crate::RegisterError::UnsupportedSql(
            "SQL input too long".to_string(),
        ));
    }

    check_sql_sanity(sql)?;

    let statements = sqlparser::parser::Parser::parse_sql(dialect, sql).map_err(|e| {
        crate::RegisterError::ParseError {
            line: 1,
            column: 0,
            message: e.to_string(),
        }
    })?;

    if statements.len() != 1 {
        return Err(crate::RegisterError::UnsupportedSql(
            "Expected exactly one SELECT statement".to_string(),
        ));
    }

    // SAFETY: we just checked len == 1
    Ok(statements
        .into_iter()
        .next()
        .expect("len == 1 checked above"))
}

/// Render the runnable component-seed bundle for an in-process aggregate.
///
/// Reuses the parsed statement's FROM and WHERE verbatim and rewrites only
/// the projection to the accumulator's seed components, aliased
/// positionally (`c0`, `c1`, ...) in the order [`crate::AggAccumulator::seed_from_row`]
/// consumes them, paired with the per-column decode kinds. Returns `None`
/// when `sql` is not the single-aggregate SELECT shape
/// [`extract_projection`] already validated.
pub(crate) fn render_aggregate_bootstrap(
    sql: &str,
    spec: &AggSpec,
    dialect: &dyn sqlparser::dialect::Dialect,
) -> Option<crate::AggregateBootstrap> {
    let mut stmt = parse_single_statement(sql, dialect).ok()?;
    let col = aggregate_arg_text(&stmt)?;
    let components = match spec {
        AggSpec::CountStar => "COUNT(*) AS c0".to_string(),
        AggSpec::CountColumn { .. } => format!("COUNT({col}) AS c0"),
        AggSpec::Sum { .. } => format!("SUM({col}) AS c0"),
        AggSpec::Avg { .. } => format!("SUM({col}) AS c0, COUNT({col}) AS c1"),
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => {
            // The `* 1.0 *` forces the squared term into floating/decimal
            // arithmetic on every backend, so `col * col` cannot overflow
            // the source integer type before it is summed. Portable across
            // PostgreSQL (numeric), MySQL (decimal), and SQLite (real),
            // and it needs no dialect-specific cast keyword.
            format!("SUM({col}) AS c0, SUM({col} * 1.0 * {col}) AS c1, COUNT({col}) AS c2")
        }
    };
    let template = parse_single_statement(&format!("SELECT {components}"), dialect).ok()?;
    let projection = select_projection(&template)?.to_vec();
    *select_projection_mut(&mut stmt)? = projection;
    Some(crate::AggregateBootstrap {
        sql: stmt.to_string(),
        kinds: aggregate_bootstrap_kinds(spec),
    })
}

/// Per-column decode kinds for [`render_aggregate_bootstrap`], in column
/// order. `COUNT` components are exact integers ([`crate::backend::ScalarKind::Int`]);
/// `SUM` and `SUM(x*x)` components are decoded as double
/// ([`crate::backend::ScalarKind::Float`]) regardless of the source column
/// type, matching the `f64` accumulator, since `SUM` promotes to
/// `bigint`/`numeric`/`DECIMAL` depending on the backend.
pub(crate) fn aggregate_bootstrap_kinds(spec: &AggSpec) -> Vec<crate::backend::ScalarKind> {
    use crate::backend::ScalarKind::{Float, Int};
    match spec {
        AggSpec::CountStar | AggSpec::CountColumn { .. } => alloc::vec![Int],
        AggSpec::Sum { .. } => alloc::vec![Float],
        AggSpec::Avg { .. } => alloc::vec![Float, Int],
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => alloc::vec![Float, Float, Int],
    }
}

/// The column-argument text of the single aggregate projection, or `"*"`
/// for `COUNT(*)`. `None` when `stmt` is not that shape.
fn aggregate_arg_text(stmt: &Statement) -> Option<String> {
    let [item] = select_projection(stmt)? else {
        return None;
    };
    let expr = match item {
        SelectItem::UnnamedExpr(e)
        | SelectItem::ExprWithAlias { expr: e, .. }
        | SelectItem::ExprWithAliases { expr: e, .. } => e,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
    };
    let Expr::Function(f) = expr else {
        return None;
    };
    match &f.args {
        FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Some("*".to_string()),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.to_string()),
            _ => None,
        },
        _ => None,
    }
}

fn select_projection(stmt: &Statement) -> Option<&[SelectItem]> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(&select.projection)
}

fn select_projection_mut(stmt: &mut Statement) -> Option<&mut Vec<SelectItem>> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    Some(&mut select.projection)
}

/// Maximum expression nesting depth to prevent stack overflow from fuzzer-crafted SQL.
pub(super) const MAX_EXPR_DEPTH: usize = 128;

/// Maximum SQL input length (defense-in-depth against pathological inputs).
pub(super) const MAX_SQL_LEN: usize = 8192;

/// Reject SQL likely to drive sqlparser into pathological backtracking.
///
/// Tracks parenthesis nesting and consecutive-operator runs, requires
/// balanced parens at EOF, and rejects non-whitespace control characters
/// (NUL, vertical tab, form feed, etc.). Real SQL contains none of those.
/// Fuzz-found inputs that hit them have driven sqlparser to near-exponential
/// parse times.
fn check_sql_sanity(sql: &str) -> Result<(), crate::RegisterError> {
    let mut paren_depth: usize = 0;
    let mut bracket_depth: usize = 0;
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
            // Square brackets: PostgreSQL array subscripts (`arr[1]`) and
            // SQL Server delimited identifiers (`[col]`). Both balanced
            // in well-formed input. Unmatched `[` runs drove GenericDialect
            // into hundreds of ms of array-subscript backtracking.
            b'[' => {
                bracket_depth += 1;
                consecutive_ops = 0;
            }
            b']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                consecutive_ops = 0;
            }
            b'+' | b'-' | b'*' | b'/' | b'=' | b'<' | b'>' | b'!' | b'~' => {
                consecutive_ops += 1;
            }
            b' ' | b'\t' | b'\n' | b'\r' => {}
            // ASCII control characters other than tab/LF/CR have no place
            // in SQL and are a strong adversarial-input signal.
            0x00..=0x08 | 0x0B | 0x0C | 0x0E..=0x1F | 0x7F => {
                return Err(crate::RegisterError::UnsupportedSql(
                    "Control character in SQL".to_string(),
                ));
            }
            _ => {
                consecutive_ops = 0;
            }
        }

        if paren_depth > MAX_EXPR_DEPTH
            || bracket_depth > MAX_EXPR_DEPTH
            || consecutive_ops > MAX_EXPR_DEPTH
        {
            return Err(crate::RegisterError::UnsupportedSql(
                "Expression nesting too deep".to_string(),
            ));
        }
    }

    if paren_depth != 0 {
        return Err(crate::RegisterError::UnsupportedSql(
            "Unbalanced parentheses".to_string(),
        ));
    }
    if bracket_depth != 0 {
        return Err(crate::RegisterError::UnsupportedSql(
            "Unbalanced square brackets".to_string(),
        ));
    }

    Ok(())
}

/// The one `SELECT` a query reduces to, with the table it reads.
///
/// This is SubQL's statement-shape rule: single table, no joins, no set
/// operations, no derived tables. It is stated once and applied both to the
/// subscription statement and to the inner query of a membership subquery, so
/// the two cannot drift apart.
fn single_table_select(query: &Query) -> Result<(&Select, &ObjectName), RegisterError> {
    match query.body.as_ref() {
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
                TableFactor::Table { name, .. } => Ok((select, name)),
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
    }
}

/// Extract the single table name and WHERE clause from a supported SELECT.
///
/// This enforces SubQL's statement-shape constraints (single table, no joins,
/// no set operations, no derived tables) so parser and canonicalizer stay in sync.
pub(super) fn extract_single_table_and_where(
    stmt: &Statement,
) -> Result<(ObjectName, Option<Expr>), RegisterError> {
    match stmt {
        Statement::Query(query) => {
            let (select, name) = single_table_select(query)?;
            Ok((name.clone(), select.selection.clone()))
        }
        _ => Err(RegisterError::UnsupportedSql(
            "Only SELECT statements supported - SubQL is for querying CDC events, not modifying data. \
             For INSERT, UPDATE, DELETE, or DDL operations, use your database directly."
                .to_string(),
        )),
    }
}

/// Does any expression under `expr` satisfy `matches`?
///
/// Recurses through exactly the composite variants the predicate compiler
/// handles, so the two agree on what SubQL's predicate language contains. A
/// variant outside that set is a leaf here, and the compiler refuses it anyway.
fn contains_matching(expr: &Expr, matches: fn(&Expr) -> bool) -> bool {
    if matches(expr) {
        return true;
    }
    match expr {
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => contains_matching(inner, matches),
        Expr::BinaryOp { left, right, .. } => {
            contains_matching(left, matches) || contains_matching(right, matches)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_matching(expr, matches)
                || contains_matching(low, matches)
                || contains_matching(high, matches)
        }
        Expr::InList { expr, list, .. } => {
            contains_matching(expr, matches)
                || list.iter().any(|item| contains_matching(item, matches))
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            contains_matching(expr, matches) || contains_matching(pattern, matches)
        }
        _ => false,
    }
}

/// Is there a subquery of any kind anywhere in `expr`?
fn contains_subquery(expr: &Expr) -> bool {
    contains_matching(expr, |inner| {
        matches!(
            inner,
            Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_)
        )
    })
}

/// Is there a membership subquery anywhere in `expr`?
///
/// Narrower than [`contains_subquery`] on purpose: a `NOT` over one of these is
/// refused as subtraction, and saying that about an `EXISTS` would describe the
/// wrong thing.
pub(super) fn contains_membership_subquery(expr: &Expr) -> bool {
    contains_matching(expr, |inner| matches!(inner, Expr::InSubquery { .. }))
}

/// Whether `expr` is a call carrying no column reference: a bare keyword
/// function, or a function of literal arguments only.
///
/// The shape a session accessor takes (`current_setting('app.user_id', true)`,
/// `current_user`), recognised structurally. Which calls actually name the
/// caller is registration's question, answered by `rls2fga`'s registry.
fn is_columnless_call(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return false;
    }
    match &function.args {
        FunctionArguments::None => true,
        FunctionArguments::Subquery(_) => false,
        FunctionArguments::List(list) => {
            list.duplicate_treatment.is_none()
                && list.clauses.is_empty()
                && list.args.iter().all(|arg| {
                    matches!(
                        arg,
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(_)))
                    )
                })
        }
    }
}

/// Is `expr` a comparison of a column to the caller, structurally: equality
/// between an identifier and a columnless call, in either operand order?
///
/// Structural on purpose, mirroring the membership subquery: the form is
/// recognised in every build, and whether the call names the caller is
/// registration's question.
pub(crate) fn is_caller_comparison(expr: &Expr) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return false;
    };
    let column_beside_call = |column: &Expr, call: &Expr| {
        matches!(column, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            && is_columnless_call(call)
    };
    column_beside_call(left, right) || column_beside_call(right, left)
}

/// Is there a caller comparison anywhere in `expr`?
///
/// Same purpose as [`contains_membership_subquery`]: a `NOT` over one is
/// refused before the compiler recurses into it.
pub(super) fn contains_caller_comparison(expr: &Expr) -> bool {
    contains_matching(expr, is_caller_comparison)
}

/// Enforce the bounded form a membership subquery has to take.
///
/// The inner query is bounded exactly as the outer statement is, by the same
/// [`single_table_select`] rule, plus two things only a subquery can get wrong:
/// it projects exactly one value, since the tested value is matched against
/// that one value, and it nests nothing, since one subscription tracks one
/// relationship.
///
/// Whether the projected value is a column, and whether the relationship is one
/// SubQL can serve, are not decided here.
pub(super) fn check_membership_subquery_bound(query: &Query) -> Result<(), RegisterError> {
    if query.with.is_some() {
        return Err(RegisterError::UnsupportedSql(
            "A membership subquery cannot contain another subquery. SubQL tracks one \
             relationship for the subscription, and a WITH clause names another query it \
             cannot follow."
                .to_string(),
        ));
    }

    let (select, _) = single_table_select(query)?;

    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }] => {}
        _ => {
            return Err(RegisterError::UnsupportedSql(
                "A membership subquery must select exactly one column. SubQL matches the \
                 tested value against that one column, so a wildcard or a second column has \
                 nothing to match."
                    .to_string(),
            ))
        }
    }

    if select.selection.as_ref().is_some_and(contains_subquery) {
        return Err(RegisterError::UnsupportedSql(
            "A membership subquery cannot contain another subquery. SubQL tracks one \
             relationship for the subscription, and a nested subquery names a second one it \
             cannot follow."
                .to_string(),
        ));
    }

    Ok(())
}

/// Derive the follow-subscription SELECT from an UPDATE statement:
/// `SELECT * FROM <table> WHERE <the UPDATE's WHERE>`. The consumer follows the
/// rows the UPDATE targets, as a standing predicate (a moving set).
///
/// Rejects a non-UPDATE statement, a multi-table / joined / `ORDER BY` / `LIMIT`
/// UPDATE, and an UPDATE with no WHERE (a whole-table follow is better expressed
/// as an explicit SELECT).
pub(super) fn derive_update_follow_sql(stmt: &Statement) -> Result<String, RegisterError> {
    Ok(derive_update_follow_sql_and_set_binds(stmt)?.0)
}

/// Derive the follow-subscription SELECT AND report how many positional /
/// numbered bind placeholders belonged to the UPDATE's SET clause.
///
/// The caller uses that count to slice the diesel-collected bind list to
/// WHERE-only binds before compiling. Positional (`?`) placeholders are
/// consumed left-to-right so trimming from the front is sufficient. Numbered
/// (`$N`) placeholders are renumbered so the surviving SELECT begins at `$1`.
pub(super) fn derive_update_follow_sql_and_set_binds(
    stmt: &Statement,
) -> Result<(String, usize), RegisterError> {
    let Statement::Update(update) = stmt else {
        return Err(RegisterError::FollowUnsupportedStatement(
            "expected an UPDATE statement".to_string(),
        ));
    };
    if update.from.is_some() || !update.order_by.is_empty() || update.limit.is_some() {
        return Err(RegisterError::UnsupportedUpdateShape(
            "UPDATE with FROM, ORDER BY, or LIMIT is not supported for a follow".to_string(),
        ));
    }
    if !update.table.joins.is_empty() {
        return Err(RegisterError::UnsupportedUpdateShape(
            "UPDATE with joins is not supported for a follow".to_string(),
        ));
    }
    let TableFactor::Table { name, .. } = &update.table.relation else {
        return Err(RegisterError::UnsupportedUpdateShape(
            "UPDATE target must be a plain table".to_string(),
        ));
    };
    let selection = update.selection.as_ref().ok_or_else(|| {
        RegisterError::UnsupportedUpdateShape(
            "UPDATE without a WHERE clause would follow the whole table; \
             add a WHERE or register an explicit SELECT"
                .to_string(),
        )
    })?;

    let set_bind_count = update
        .assignments
        .iter()
        .map(|a| count_placeholders_in_expr(&a.value))
        .sum::<usize>();

    let mut selection = selection.clone();
    renumber_placeholders(&mut selection, set_bind_count);

    Ok((
        alloc::format!("SELECT * FROM {name} WHERE {selection}"),
        set_bind_count,
    ))
}

/// Walk `expr` and count every `SqlValue::Placeholder` leaf (positional `?`
/// and numbered `$N` alike). Recurses through the same expression shapes the
/// compiler already supports.
fn count_placeholders_in_expr(expr: &Expr) -> usize {
    use sqlparser::ast::Value as SqlValue;
    match expr {
        Expr::Value(v) => usize::from(matches!(&v.value, SqlValue::Placeholder(_))),
        Expr::BinaryOp { left, right, .. } => {
            count_placeholders_in_expr(left) + count_placeholders_in_expr(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::Nested(expr) => count_placeholders_in_expr(expr),
        Expr::InList { expr, list, .. } => {
            count_placeholders_in_expr(expr)
                + list.iter().map(count_placeholders_in_expr).sum::<usize>()
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            count_placeholders_in_expr(expr)
                + count_placeholders_in_expr(low)
                + count_placeholders_in_expr(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            count_placeholders_in_expr(expr) + count_placeholders_in_expr(pattern)
        }
        _ => 0,
    }
}

/// Renumber numbered (`$N`) placeholders in `expr` so the smallest surviving
/// index is `$1`, subtracting `set_bind_count`. Positional (`?`) placeholders
/// are left untouched; the compiler consumes them left-to-right and the
/// caller trims the bind vector from the front to match.
fn renumber_placeholders(expr: &mut Expr, set_bind_count: usize) {
    use sqlparser::ast::Value as SqlValue;
    match expr {
        Expr::Value(v) => {
            if let SqlValue::Placeholder(token) = &mut v.value {
                if let Some(rest) = token.strip_prefix('$') {
                    if let Ok(idx) = rest.parse::<usize>() {
                        let new_idx = idx.saturating_sub(set_bind_count);
                        *token = alloc::format!("${new_idx}");
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            renumber_placeholders(left, set_bind_count);
            renumber_placeholders(right, set_bind_count);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::Nested(expr) => renumber_placeholders(expr, set_bind_count),
        Expr::InList { expr, list, .. } => {
            renumber_placeholders(expr, set_bind_count);
            for item in list {
                renumber_placeholders(item, set_bind_count);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            renumber_placeholders(expr, set_bind_count);
            renumber_placeholders(low, set_bind_count);
            renumber_placeholders(high, set_bind_count);
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            renumber_placeholders(expr, set_bind_count);
            renumber_placeholders(pattern, set_bind_count);
        }
        _ => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod sanity_tests {
    use super::check_sql_sanity;
    use crate::RegisterError;

    #[test]
    fn rejects_vertical_tab() {
        let err = check_sql_sanity("SELECT * FROM t WHERE a\x0b= 1").unwrap_err();
        assert!(matches!(err, RegisterError::UnsupportedSql(ref m) if m.contains("Control")));
    }

    #[test]
    fn rejects_nul_byte() {
        let err = check_sql_sanity("SELECT * FROM t WHERE a\x00= 1").unwrap_err();
        assert!(matches!(err, RegisterError::UnsupportedSql(ref m) if m.contains("Control")));
    }

    #[test]
    fn rejects_unbalanced_open_parens() {
        let err = check_sql_sanity("SELECT * FROM t WHERE ((((a = 1").unwrap_err();
        assert!(matches!(err, RegisterError::UnsupportedSql(ref m) if m.contains("Unbalanced")));
    }

    #[test]
    fn rejects_unbalanced_open_brackets() {
        let err = check_sql_sanity("SELECT * FROM t WHERE a[[[[ = 1").unwrap_err();
        assert!(
            matches!(err, RegisterError::UnsupportedSql(ref m) if m.contains("square brackets"))
        );
    }

    #[test]
    fn accepts_well_formed_sql() {
        check_sql_sanity("SELECT * FROM t WHERE a = 1\nAND b > 2").unwrap();
        check_sql_sanity("SELECT * FROM t WHERE x IN (1, 2, 3)").unwrap();
        // PG array subscript with balanced brackets is fine.
        check_sql_sanity("SELECT * FROM t WHERE arr[1] = 5").unwrap();
    }
}
