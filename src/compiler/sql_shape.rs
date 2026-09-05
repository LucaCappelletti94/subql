use crate::{catalog_helpers, RegisterError};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, LimitClause, ObjectName, Query,
    Select, SelectItem, SelectModifiers, SetExpr, Statement, TableFactor,
};

const WINDOW_FUNCTIONS_NOT_SUPPORTED: &str = "Window functions not supported";
const UNSUPPORTED_PROJECTION: &str =
    "Unsupported projection: only SELECT *, COUNT(*), COUNT(col), SUM(col), AVG(col), \
     VAR_POP(col), VAR_SAMP(col), STDDEV_POP(col), or STDDEV_SAMP(col) are supported \
     (VARIANCE/STDDEV are accepted as aliases for VAR_SAMP/STDDEV_SAMP)";

/// Whether a `GROUP BY` is a served shape where the clause gate is applied.
///
/// A parameter rather than a default, so a new call site has to say which it
/// is. Letting one inherit the permissive answer is how a grouping reaches
/// code that ignores it, which answers a different query than the caller
/// wrote and is the failure this whole gate exists to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Grouping {
    /// The outer statement of a subscription, where `extract_projection`
    /// decides whether this particular grouping is one the fold can maintain.
    Served,
    /// Everywhere else. A membership subquery matches its inner query value by
    /// value and a keyed capture asks about one changed row, and a grouping
    /// invalidates both without failing.
    Refused,
}

/// Projection kind for a subscription SQL statement.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryProjection {
    /// `SELECT *`: deliver row events (default, current behaviour).
    Rows,
    /// `SELECT <aggregate>`: deliver one maintained value.
    Aggregate(AggSpec),
    /// `SELECT g1, ..., gn, <aggregate> FROM t WHERE p GROUP BY g1, ..., gn`:
    /// deliver one maintained value per group.
    ///
    /// Every group column is bare, so a changed row names its own group. The
    /// backend selects a canonical encoder from each column's scalar and
    /// comparison metadata. `groups` preserves `GROUP BY` order.
    ///
    /// The enum-level `non_exhaustive` does NOT cover this variant's fields:
    /// match it with `..` or a future field addition breaks the match, as
    /// `having`'s arrival did.
    GroupedAggregate {
        /// The grouping columns, in `GROUP BY` order.
        groups: Vec<crate::ColumnId>,
        /// The single aggregate maintained per group.
        agg: AggSpec,
        /// The `HAVING` comparison checked per group after each fold, when
        /// the statement carries one the fold can evaluate in process.
        having: Option<AggHaving>,
    },
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
    /// deltas (`sum`, `squared_deviations`, `count`). The value is
    /// `squared_deviations / N`.
    VarPop { column: crate::ColumnId },
    /// `SELECT VAR_SAMP(column_name)` (alias `VARIANCE`). Sample variance.
    /// Emits `Stats` deltas. The value is
    /// `squared_deviations / (N - 1)`, and requires `N >= 2`.
    VarSamp { column: crate::ColumnId },
    /// `SELECT STDDEV_POP(column_name)`. Population standard deviation.
    /// Same `Stats` deltas as `VarPop`. Consumer takes `sqrt(var_pop)`.
    StddevPop { column: crate::ColumnId },
    /// `SELECT STDDEV_SAMP(column_name)` (alias `STDDEV`). Sample standard
    /// deviation. Same `Stats` deltas as `VarSamp`. Consumer takes
    /// `sqrt(var_samp)`.
    StddevSamp { column: crate::ColumnId },
}

impl AggSpec {
    /// The column this aggregate reads, `None` for `COUNT(*)`.
    #[must_use]
    pub const fn column(&self) -> Option<crate::ColumnId> {
        match self {
            Self::CountStar => None,
            Self::CountColumn { column }
            | Self::Sum { column }
            | Self::Avg { column }
            | Self::VarPop { column }
            | Self::VarSamp { column }
            | Self::StddevPop { column }
            | Self::StddevSamp { column } => Some(*column),
        }
    }
}

/// Comparison operator of a fast-path `HAVING`, subject on the left.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum HavingOp {
    /// `=`
    Eq,
    /// `<>`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
}

impl HavingOp {
    /// The same comparison with its operands swapped.
    #[must_use]
    pub const fn mirrored(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::NotEq => Self::NotEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }

    /// Whether `left op right` holds under this comparison.
    #[must_use]
    pub const fn admits(self, ordering: core::cmp::Ordering) -> bool {
        match self {
            Self::Eq => ordering.is_eq(),
            Self::NotEq => !ordering.is_eq(),
            Self::Lt => ordering.is_lt(),
            Self::LtEq => ordering.is_le(),
            Self::Gt => ordering.is_gt(),
            Self::GtEq => ordering.is_ge(),
        }
    }

    /// The served subset of comparison operators, `None` for the rest.
    pub(crate) const fn from_operator(op: &sqlparser::ast::BinaryOperator) -> Option<Self> {
        use sqlparser::ast::BinaryOperator;
        match op {
            BinaryOperator::Eq => Some(Self::Eq),
            BinaryOperator::NotEq => Some(Self::NotEq),
            BinaryOperator::Lt => Some(Self::Lt),
            BinaryOperator::LtEq => Some(Self::LtEq),
            BinaryOperator::Gt => Some(Self::Gt),
            BinaryOperator::GtEq => Some(Self::GtEq),
            _ => None,
        }
    }

    /// Stable spelling inside a predicate's identity hash. Attached to the
    /// variant so a new operator cannot ship without one.
    pub(crate) const fn as_hash_str(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

/// The accumulable-family function a fast-path `HAVING` reads. Always over
/// the same column the projection aggregates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum HavingFunction {
    /// `COUNT(column)`: the non-null contribution count.
    CountColumn,
    /// `SUM(column)`
    Sum,
    /// `AVG(column)`
    Avg,
    /// `VAR_POP(column)`
    VarPop,
    /// `VAR_SAMP(column)`
    VarSamp,
    /// `STDDEV_POP(column)`
    StddevPop,
    /// `STDDEV_SAMP(column)`
    StddevSamp,
}

impl HavingFunction {
    /// The function a projected spec maintains, `None` for `COUNT(*)`,
    /// which aggregates no column.
    #[must_use]
    pub const fn of(spec: &AggSpec) -> Option<Self> {
        match spec {
            AggSpec::CountStar => None,
            AggSpec::CountColumn { .. } => Some(Self::CountColumn),
            AggSpec::Sum { .. } => Some(Self::Sum),
            AggSpec::Avg { .. } => Some(Self::Avg),
            AggSpec::VarPop { .. } => Some(Self::VarPop),
            AggSpec::VarSamp { .. } => Some(Self::VarSamp),
            AggSpec::StddevPop { .. } => Some(Self::StddevPop),
            AggSpec::StddevSamp { .. } => Some(Self::StddevSamp),
        }
    }

    /// Stable spelling inside a predicate's identity hash. Attached to the
    /// variant so a new function cannot ship without one.
    pub(crate) const fn as_hash_str(self) -> &'static str {
        match self {
            Self::CountColumn => "COUNT(col)",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::VarPop => "VAR_POP",
            Self::VarSamp => "VAR_SAMP",
            Self::StddevPop => "STDDEV_POP",
            Self::StddevSamp => "STDDEV_SAMP",
        }
    }
}

/// The column name a bare identifier or a two-part `table.column` spells.
/// `None` for wildcards, longer qualifications, and expressions, which
/// keeps the two-part assumption in one place.
fn ident_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(&ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(&parts[1].value),
        _ => None,
    }
}

/// The `Select` of a plain single-`SELECT` statement, `None` for set
/// operations and non-queries. The one place that unwraps the
/// statement-to-select nesting, so a future statement shape lands here.
pub(crate) fn select_of(stmt: &Statement) -> Option<&Select> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select)
}

/// Mutable twin of [`select_of`].
pub(crate) fn select_mut(stmt: &mut Statement) -> Option<&mut Select> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    Some(select)
}

/// What a fast-path `HAVING` comparison reads from a group's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum HavingSubject {
    /// `COUNT(*)`: the group's source-row count.
    RowCount,
    /// A family function over the column the projection aggregates.
    Aggregate(HavingFunction),
}

/// The `HAVING` comparison a grouped fold checks per group after each fold.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AggHaving {
    /// What the comparison reads.
    pub subject: HavingSubject,
    /// Comparison with the subject on the left.
    pub op: HavingOp,
    /// The constant compared against, as its SQL spelling. Validated to
    /// parse as a float at registration.
    pub threshold: String,
}

impl AggHaving {
    /// Whether evaluating this condition needs the complete component set
    /// (sum, sum of squares, count) rather than the projected function's own.
    #[must_use]
    pub fn widens(&self, projected: &AggSpec) -> bool {
        match self.subject {
            HavingSubject::RowCount => false,
            // `Sum`'s running value cannot express NULL (the empty sum reads
            // 0.0), so a `SUM` subject always needs the contribution count.
            HavingSubject::Aggregate(HavingFunction::Sum) => true,
            HavingSubject::Aggregate(function) => HavingFunction::of(projected) != Some(function),
        }
    }
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

    if let Some(kind) = catalog_helpers::column_builtin_kind(database, table_id, column) {
        match kind {
            // Numeric scalars: SUM/AVG/variance/stddev accept these.
            crate::backend::BuiltinKind::Int
            | crate::backend::BuiltinKind::Float
            | crate::backend::BuiltinKind::Decimal => {}
            // Everything else is rejected. Give the caller the concrete
            // kind in the error so the message matches the aggregate's
            // requirement.
            other => {
                return Err(RegisterError::NotServedInProcess(
                    crate::errors::Refusal::UnfoldableAggregate {
                        column,
                        kind: other,
                        function: display,
                    },
                ));
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
    let Ok(arity) = catalog_helpers::table_arity(database, table_id) else {
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
        let Some(name) = ident_name(expr) else {
            return false;
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
pub(super) fn extract_projection<B: crate::backend::Backend, DB: DatabaseLike>(
    stmt: &Statement,
    table_id: crate::TableId,
    database: &DB,
) -> Result<QueryProjection, RegisterError> {
    let Some(select) = select_of(stmt) else {
        return Ok(QueryProjection::Rows);
    };
    // A grouped statement is a different shape with different rules, and the
    // clause gate deliberately let it through for this to decide. Handled
    // before anything else so no ungrouped path can accept one by accident: a
    // wildcard projection alongside a `GROUP BY` used to look like a plain row
    // subscription, which is the silent drop this whole surface exists to stop.
    if let Some(groups) = group_columns(select, table_id, database)? {
        return grouped_projection::<B, DB>(select, &groups, table_id, database);
    }

    // A `HAVING` filters grouped results. The grouped path above parses it
    // into the per-group check, so reaching here with one means the shape
    // cannot serve it and silence would drop the clause.
    if select.having.is_some() {
        return Err(RegisterError::UnsupportedSql(
            "HAVING is not supported outside a grouped aggregate subscription".to_string(),
        ));
    }

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
        aggregate_from_expr(expr, table_id, database).map(QueryProjection::Aggregate)
    } else {
        Err(RegisterError::UnsupportedSql(
            UNSUPPORTED_PROJECTION.to_string(),
        ))
    }
}

/// Classify one projected expression as an aggregate of the accumulable family.
///
/// Shared by the plain and grouped paths so the two cannot drift on which
/// aggregates are served or on how their arguments are read.
fn aggregate_from_expr<DB: DatabaseLike>(
    expr: &Expr,
    table_id: crate::TableId,
    database: &DB,
) -> Result<AggSpec, RegisterError> {
    let Expr::Function(f) = expr else {
        return Err(RegisterError::UnsupportedSql(
            UNSUPPORTED_PROJECTION.to_string(),
        ));
    };
    // The (unqualified) function name, its last `ObjectName` part.
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
                        return Ok(AggSpec::CountStar);
                    }
                    // COUNT(column): plain column identifier
                    let col_name = extract_column_arg(&list.args[0]).ok_or_else(|| {
                        RegisterError::UnsupportedSql(
                            "COUNT argument must be * or a plain column name, not an expression"
                                .to_string(),
                        )
                    })?;
                    let column = catalog_helpers::column_id(database, table_id, &col_name).ok_or(
                        RegisterError::UnknownColumn {
                            table_id,
                            column: col_name,
                        },
                    )?;
                    Ok(AggSpec::CountColumn { column })
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
            Ok(match func {
                "sum" => AggSpec::Sum { column },
                "avg" => AggSpec::Avg { column },
                "var_pop" => AggSpec::VarPop { column },
                "var_samp" | "variance" => AggSpec::VarSamp { column },
                "stddev_pop" => AggSpec::StddevPop { column },
                "stddev_samp" | "stddev" => AggSpec::StddevSamp { column },
                _ => unreachable!("matched function name above"),
            })
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
}

/// Resolve the bare columns named by `GROUP BY`.
/// `Ok(None)` when the statement is not grouped. Refuses a grouping that is
/// anything other than bare columns of this table: the fold works by letting a
/// changed row name its own group, and it can only do that when the group is
/// read from the row rather than computed from it.
pub(crate) fn group_columns<DB: DatabaseLike>(
    select: &Select,
    table_id: crate::TableId,
    database: &DB,
) -> Result<Option<Vec<crate::ColumnId>>, RegisterError> {
    let exprs = match &select.group_by {
        // Refused by the clause gate before this runs.
        GroupByExpr::All(_) => return Ok(None),
        GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => return Ok(None),
        GroupByExpr::Expressions(exprs, _) => exprs,
    };

    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let Some(name) = ident_name(expr) else {
            return Err(RegisterError::UnsupportedSql(
                "GROUP BY must name bare columns: a grouped fold works by letting a \
                 changed row say which group it is in, which it cannot do for a group \
                 computed from the row"
                    .to_string(),
            ));
        };
        let column = catalog_helpers::column_id(database, table_id, name).ok_or_else(|| {
            RegisterError::UnknownColumn {
                table_id,
                column: name.to_string(),
            }
        })?;
        if columns.contains(&column) {
            return Err(RegisterError::UnsupportedSql(
                "GROUP BY names the same column twice".to_string(),
            ));
        }
        columns.push(column);
    }
    Ok(Some(columns))
}

/// Classify a grouped statement, given the columns its `GROUP BY` names.
///
/// Served only in the one shape the fold maintains: every group column
/// projected as a bare column, plus exactly one aggregate from the accumulable
/// family, and nothing else. Anything wider is a question about rows the fold
/// does not hold, so it is refused here and re-read by the tier above.
///
/// [`extract_grouped_extreme`] walks the same projection shape with `Ok(None)`
/// where this errors. A change to what either accepts belongs in both.
fn grouped_projection<B: crate::backend::Backend, DB: DatabaseLike>(
    select: &Select,
    groups: &[crate::ColumnId],
    table_id: crate::TableId,
    database: &DB,
) -> Result<QueryProjection, RegisterError> {
    let columns: Vec<_> = groups
        .iter()
        .map(|column| catalog_helpers::column_comparison::<B, _>(database, table_id, *column))
        .collect::<Option<_>>()
        .ok_or_else(|| {
            RegisterError::UnsupportedSql(
                "a GROUP BY column has no complete scalar or comparison metadata".to_string(),
            )
        })?;
    if B::group_key_encoder(columns).is_none() {
        return Err(RegisterError::UnsupportedSql(
            "a GROUP BY column has no canonical key for this database comparison".to_string(),
        ));
    }

    let mut projected_groups = Vec::with_capacity(groups.len());
    let mut agg = None;
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e)
            | SelectItem::ExprWithAlias { expr: e, .. }
            | SelectItem::ExprWithAliases { expr: e, .. } => e,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(RegisterError::UnsupportedSql(
                    "GROUP BY with a wildcard projection not supported - a grouped \
                     subscription projects its group columns and one aggregate, and a \
                     wildcard projects rows a grouped answer does not have"
                        .to_string(),
                ));
            }
        };
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let Some(name) = ident_name(expr) else {
                    return Err(RegisterError::UnsupportedSql(
                        UNSUPPORTED_PROJECTION.to_string(),
                    ));
                };
                let column =
                    catalog_helpers::column_id(database, table_id, name).ok_or_else(|| {
                        RegisterError::UnknownColumn {
                            table_id,
                            column: name.to_string(),
                        }
                    })?;
                if !groups.contains(&column) {
                    return Err(RegisterError::UnsupportedSql(alloc::format!(
                        "{name} is projected but not grouped by, so its value differs between \
                         the rows of a group and the answer would depend on which row was read"
                    )));
                }
                projected_groups.push(column);
            }
            Expr::Function(_) => {
                if agg.is_some() {
                    return Err(RegisterError::UnsupportedSql(
                        "a grouped subscription maintains one aggregate per group, so it \
                         projects exactly one"
                            .to_string(),
                    ));
                }
                agg = Some(aggregate_from_expr(expr, table_id, database)?);
            }
            _ => {
                return Err(RegisterError::UnsupportedSql(
                    UNSUPPORTED_PROJECTION.to_string(),
                ))
            }
        }
    }

    let Some(agg) = agg else {
        return Err(RegisterError::UnsupportedSql(
            "a grouped subscription projects one aggregate, and this projects none".to_string(),
        ));
    };
    // Every group column has to be delivered, or a value could not be
    // attributed to anything the caller asked for.
    for column in groups {
        if !projected_groups.contains(column) {
            let name = catalog_helpers::column_name(database, table_id, *column)
                .unwrap_or_else(|| alloc::format!("column {column}"));
            return Err(RegisterError::UnsupportedSql(alloc::format!(
                "{name} is grouped by but not projected, so a delivered value could not be \
                 attributed to a group"
            )));
        }
    }

    let having = select
        .having
        .as_ref()
        .map(|expr| having_from_expr(expr, &agg, table_id, database))
        .transpose()?;
    Ok(QueryProjection::GroupedAggregate {
        groups: groups.to_vec(),
        agg,
        having,
    })
}

/// The numeric text of a constant comparison operand, sign folded in.
/// `None` for anything that is not a plain, non-null numeric literal.
fn having_literal_text(expr: &Expr) -> Option<String> {
    match extreme_literal(expr)? {
        sqlparser::ast::Value::Number(text, _) => Some(text),
        _ => None,
    }
}

/// Parse the one `HAVING` comparison a grouped fold can check in process:
/// `COUNT(*)` or a family function over the projected column, against a
/// numeric constant, either operand order.
fn having_from_expr<DB: DatabaseLike>(
    having: &Expr,
    projected: &AggSpec,
    table_id: crate::TableId,
    database: &DB,
) -> Result<AggHaving, RegisterError> {
    let Expr::BinaryOp { left, op, right } = having else {
        return Err(RegisterError::UnsupportedSql(
            "HAVING is served as one comparison between an aggregate and a constant".to_string(),
        ));
    };
    let Some(op) = HavingOp::from_operator(op) else {
        return Err(RegisterError::UnsupportedSql(
            "HAVING is served as one comparison between an aggregate and a constant".to_string(),
        ));
    };
    let (subject_expr, op, threshold) =
        match (having_literal_text(left), having_literal_text(right)) {
            (None, Some(text)) => (left.as_ref(), op, text),
            (Some(text), None) => (right.as_ref(), op.mirrored(), text),
            _ => {
                return Err(RegisterError::UnsupportedSql(
                    "HAVING compares the aggregate against one numeric constant".to_string(),
                ))
            }
        };
    if sql_scalar_text::parse_f64(&threshold).is_none() {
        return Err(RegisterError::UnsupportedSql(alloc::format!(
            "HAVING threshold {threshold} is not a numeric constant"
        )));
    }
    let compared = aggregate_from_expr(subject_expr, table_id, database)?;
    let subject = match &compared {
        AggSpec::CountStar => HavingSubject::RowCount,
        spec => {
            let Some(projected_column) = projected.column() else {
                return Err(RegisterError::UnsupportedSql(
                    "COUNT(*) aggregates no column, so its HAVING can compare only COUNT(*)"
                        .to_string(),
                ));
            };
            let column = spec
                .column()
                .expect("every non-COUNT(*) family spec names its column");
            if column != projected_column {
                return Err(RegisterError::UnsupportedSql(
                    "HAVING is evaluable only over the column the projection aggregates"
                        .to_string(),
                ));
            }
            HavingSubject::Aggregate(HavingFunction::of(spec).expect("checked non-COUNT(*) above"))
        }
    };
    Ok(AggHaving {
        subject,
        op,
        threshold,
    })
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

/// Grouped `MIN` or `MAX` projection served by the hybrid read tier.
pub(crate) struct GroupedExtremeProjection {
    pub groups: Vec<crate::ColumnId>,
    pub kind: ScalarAggKind,
    pub column: crate::ColumnId,
    pub having: Option<ExtremeHaving>,
}

/// What a grouped extreme's `HAVING` comparison reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtremeHavingSubject {
    /// The projected extreme itself.
    Extreme,
    /// `COUNT(*)`: the group's source-row count.
    RowCount,
}

/// The `HAVING` comparison a grouped extreme checks per group. The literal
/// stays as parsed SQL: the plan parses it to a backend value against the
/// extreme column's kind, or as an integer for a row count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExtremeHaving {
    pub subject: ExtremeHavingSubject,
    pub op: HavingOp,
    pub literal: sqlparser::ast::Value,
}

/// A constant comparison operand for an extreme's `HAVING`, sign folded
/// into a numeric literal. `None` for NULL and for anything non-constant.
fn extreme_literal(expr: &Expr) -> Option<sqlparser::ast::Value> {
    match expr {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::Null => None,
            other => Some(other.clone()),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(value) => match &value.value {
                sqlparser::ast::Value::Number(text, long) => Some(sqlparser::ast::Value::Number(
                    alloc::format!("-{text}"),
                    *long,
                )),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Parse the one `HAVING` comparison a grouped extreme can check in
/// process: the projected extreme or `COUNT(*)` against a constant, either
/// operand order. `None` sends the statement to the capture tier.
fn extreme_having<DB: DatabaseLike>(
    having: &Expr,
    kind: ScalarAggKind,
    extreme_column: crate::ColumnId,
    table_id: crate::TableId,
    database: &DB,
) -> Option<ExtremeHaving> {
    let Expr::BinaryOp { left, op, right } = having else {
        return None;
    };
    let op = HavingOp::from_operator(op)?;
    let (subject_expr, op, literal) = match (extreme_literal(left), extreme_literal(right)) {
        (None, Some(literal)) => (left.as_ref(), op, literal),
        (Some(literal), None) => (right.as_ref(), op.mirrored(), literal),
        _ => return None,
    };
    let Expr::Function(function) = subject_expr else {
        return None;
    };
    let name = function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_lowercase());
    let subject = match name.as_deref() {
        Some("count") => {
            let FunctionArguments::List(list) = &function.args else {
                return None;
            };
            let [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] = list.args.as_slice() else {
                return None;
            };
            ExtremeHavingSubject::RowCount
        }
        Some(called @ ("min" | "max")) => {
            let called_kind = if called == "min" {
                ScalarAggKind::Min
            } else {
                ScalarAggKind::Max
            };
            if called_kind != kind {
                return None;
            }
            let display = match kind {
                ScalarAggKind::Min => "MIN",
                ScalarAggKind::Max => "MAX",
            };
            let column = resolve_single_column_arg(display, function, table_id, database).ok()?;
            if column != extreme_column {
                return None;
            }
            ExtremeHavingSubject::Extreme
        }
        _ => return None,
    };
    Some(ExtremeHaving {
        subject,
        op,
        literal,
    })
}

/// Detect and validate one grouped bare-column extreme.
///
/// [`grouped_projection`] walks the same projection shape with pinned errors
/// where this answers `Ok(None)`. A change to what either accepts belongs in
/// both. Unifying them was tried and rejected: keeping the fold's error
/// precedence through a shared walker needs a three-outcome callback and a
/// refusal-to-message map longer than the loops themselves.
pub(crate) fn extract_grouped_extreme<B: crate::backend::Backend, DB: DatabaseLike>(
    stmt: &Statement,
    table_id: crate::TableId,
    database: &DB,
) -> Result<Option<GroupedExtremeProjection>, RegisterError> {
    let Some(select) = select_of(stmt) else {
        return Ok(None);
    };
    let Some(groups) = group_columns(select, table_id, database)? else {
        return Ok(None);
    };
    let Some(columns) = groups
        .iter()
        .map(|column| catalog_helpers::column_comparison::<B, _>(database, table_id, *column))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    if B::group_key_encoder(columns).is_none() {
        return Ok(None);
    }

    let mut projected_groups = Vec::with_capacity(groups.len());
    let mut extreme = None;
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. }
            | SelectItem::ExprWithAliases { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(None),
        };
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let Some(name) = ident_name(expr) else {
                    return Ok(None);
                };
                let Some(column) = catalog_helpers::column_id(database, table_id, name) else {
                    return Err(RegisterError::UnknownColumn {
                        table_id,
                        column: name.to_string(),
                    });
                };
                if !groups.contains(&column) {
                    return Ok(None);
                }
                projected_groups.push(column);
            }
            Expr::Function(function) => {
                if extreme.is_some() {
                    return Ok(None);
                }
                let name = function
                    .name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.to_lowercase());
                let kind = match name.as_deref() {
                    Some("min") => ScalarAggKind::Min,
                    Some("max") => ScalarAggKind::Max,
                    _ => return Ok(None),
                };
                let display = match kind {
                    ScalarAggKind::Min => "MIN",
                    ScalarAggKind::Max => "MAX",
                };
                let column = resolve_single_column_arg(display, function, table_id, database)?;
                extreme = Some((kind, column));
            }
            _ => return Ok(None),
        }
    }
    if groups
        .iter()
        .any(|column| !projected_groups.contains(column))
    {
        return Ok(None);
    }
    let Some((kind, column)) = extreme else {
        return Ok(None);
    };
    let having = match &select.having {
        None => None,
        Some(expr) => match extreme_having(expr, kind, column, table_id, database) {
            Some(having) => Some(having),
            // Outside the fast path: the capture tier answers instead.
            None => return Ok(None),
        },
    };
    Ok(Some(GroupedExtremeProjection {
        groups,
        kind,
        column,
        having,
    }))
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
    let Some(select) = select_of(stmt) else {
        return Ok(None);
    };

    // The scalar tier maintains one value and re-queries for it, so a grouping
    // it kept in the SQL would return a row per group and the value it holds
    // would be one arbitrary group's. Answering `None` sends the statement to
    // the tier that re-reads it whole, which is the correct answer for it.
    if !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers)
        if exprs.is_empty() && modifiers.is_empty())
    {
        return Ok(None);
    }

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
pub(crate) fn parse_single_statement(
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
/// Reuses the parsed statement's FROM, WHERE and GROUP BY verbatim and
/// rewrites only the projection: the group columns as written, then the
/// accumulator's seed components aliased positionally (`c0`, `c1`, ...) in the
/// order [`crate::AggAccumulator::seed_from_row`] consumes them, paired with
/// the per-column decode kinds. Returns `None` when `sql` is not the SELECT
/// shape [`extract_projection`] already validated.
pub(crate) fn render_aggregate_bootstrap<B: crate::backend::Backend, DB: DatabaseLike>(
    sql: &str,
    binds: &[crate::backend::Value<B>],
    projection: &QueryProjection,
    dialect: &dyn sqlparser::dialect::Dialect,
    table_id: crate::TableId,
    database: &DB,
) -> Option<crate::AggregateBootstrap<B>> {
    let (spec, groups, having) = match projection {
        QueryProjection::Rows => return None,
        QueryProjection::Aggregate(spec) => (spec, &[][..], None),
        QueryProjection::GroupedAggregate {
            groups,
            agg,
            having,
        } => (agg, groups.as_slice(), having.as_ref()),
    };
    // A sibling condition reads components the projected function alone does
    // not maintain, so its seed carries the complete set.
    let widened = having.is_some_and(|having| having.widens(spec));
    let mut stmt = parse_single_statement(sql, dialect).ok()?;
    // The seed fetches every group. Groups failing the condition install
    // silently and are needed the moment they cross into the result.
    if let Some(select) = select_mut(&mut stmt) {
        select.having = None;
    }
    let arg = aggregate_arg(&stmt)?;
    let sum = || agg_call("SUM", arg.clone());
    let count = || agg_call("COUNT", arg.clone());
    // Group columns lead, in `GROUP BY` order rather than projection order, so
    // a seeded group's values line up with the order its key encodes in. Built
    // as `Ident`s carrying the dialect's quote style, so an embedded delimiter
    // is escaped by the same renderer that parsed it.
    let mut items = Vec::with_capacity(groups.len() + 4);
    let mut group_kinds = Vec::with_capacity(groups.len());
    for (slot, column) in groups.iter().enumerate() {
        let name = catalog_helpers::column_name(database, table_id, *column)?;
        items.push(component(
            Expr::Identifier(super::quoted_ident(dialect, &name)),
            slot,
        ));
        group_kinds.push(catalog_helpers::column_builtin_kind(
            database, table_id, *column,
        )?);
    }
    let components: Vec<Expr> = match spec {
        _ if widened => alloc::vec![sum(), sum_of_squared_deviations::<B>(&arg)?, count()],
        AggSpec::CountStar => alloc::vec![agg_call("COUNT", FunctionArgExpr::Wildcard)],
        AggSpec::CountColumn { .. } => alloc::vec![count()],
        // `SUM` and `AVG` read the same pair: the total and how many rows
        // contributed to it. `AVG` divides by that count and `SUM` reports
        // NULL when it is zero.
        AggSpec::Sum { .. } | AggSpec::Avg { .. } => alloc::vec![sum(), count()],
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => {
            alloc::vec![sum(), sum_of_squared_deviations::<B>(&arg)?, count()]
        }
    };
    let component_count = components.len();
    items.extend(
        components
            .into_iter()
            .enumerate()
            .map(|(slot, expr)| component(expr, groups.len() + slot)),
    );
    if !groups.is_empty() {
        // A grouped seed also reports how many source rows each group holds,
        // which is what lets a later change know whether it emptied one.
        items.push(component(
            agg_call("COUNT", FunctionArgExpr::Wildcard),
            groups.len() + component_count,
        ));
    }
    *select_projection_mut(&mut stmt)? = items;
    let mut kinds = group_kinds;
    if widened {
        // A widened seed still carries the projected function's own total
        // first, so it decodes in the type that function sums into. Only
        // the sum of squares is a double, being nobody's exact answer.
        kinds.extend([
            aggregate_bootstrap_kinds(
                spec,
                crate::catalog_helpers::total_rule::<B, DB>(spec, database, table_id),
            )
            .first()
            .copied()
            .unwrap_or(crate::backend::BuiltinKind::Float),
            crate::backend::BuiltinKind::Float,
            crate::backend::BuiltinKind::Int,
        ]);
    } else {
        kinds.extend(aggregate_bootstrap_kinds(
            spec,
            crate::catalog_helpers::total_rule::<B, DB>(spec, database, table_id),
        ));
    }
    if !groups.is_empty() {
        kinds.push(crate::backend::BuiltinKind::Int);
    }
    Some(crate::AggregateBootstrap {
        query: crate::reexec::BoundQuery::new(stmt.to_string(), binds.to_vec()),
        kinds,
        group_columns: groups.len(),
    })
}

/// Per-column decode kinds for an aggregate's seed components, in component
/// order.
///
/// `COUNT` components are exact integers
/// ([`crate::backend::BuiltinKind::Int`]). A `SUM` component decodes in the
/// type its engine sums into, so that the seed and the fold hold the same
/// number: PostgreSQL answers `bigint` for an `int` column and `numeric`
/// for a `bigint` one, MySQL answers a decimal for both, and SQLite an
/// integer. `SUM(x*x)` and `AVG`'s total keep their double, which is what
/// their `f64` components still are until Phase D2.
pub(crate) fn aggregate_bootstrap_kinds(
    spec: &AggSpec,
    rule: crate::backend::SumRule,
) -> Vec<crate::backend::BuiltinKind> {
    let total = match spec {
        // `AVG` holds the same exact total `SUM` does, since a mean is
        // that total divided by the count, so its seed component decodes
        // in the same type.
        AggSpec::Sum { .. } | AggSpec::Avg { .. } => match rule {
            crate::backend::SumRule::Integer
            | crate::backend::SumRule::IntegerPromotingToDouble => crate::backend::BuiltinKind::Int,
            crate::backend::SumRule::Decimal { .. } => crate::backend::BuiltinKind::Decimal,
            crate::backend::SumRule::Double => crate::backend::BuiltinKind::Float,
        },
        _ => crate::backend::BuiltinKind::Float,
    };
    aggregate_bootstrap_kinds_with_total(spec, total)
}

fn aggregate_bootstrap_kinds_with_total(
    spec: &AggSpec,
    total: crate::backend::BuiltinKind,
) -> Vec<crate::backend::BuiltinKind> {
    match spec {
        AggSpec::CountStar | AggSpec::CountColumn { .. } => {
            alloc::vec![crate::backend::BuiltinKind::Int]
        }
        AggSpec::Sum { .. } | AggSpec::Avg { .. } => {
            alloc::vec![total, crate::backend::BuiltinKind::Int]
        }
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => alloc::vec![
            crate::backend::BuiltinKind::Float,
            crate::backend::BuiltinKind::Float,
            crate::backend::BuiltinKind::Int,
        ],
    }
}
/// Returns [`FunctionArgExpr::Wildcard`] for `COUNT(*)`, and `None` when no
/// projected item is a function call of exactly one unnamed argument.
///
/// An AST node rather than its rendered text, because the seed projection is
/// built as AST. Rendering here and re-parsing there used to be how a
/// catalog-supplied identifier could break the seed query without anyone
/// hearing about it.
fn aggregate_arg(stmt: &Statement) -> Option<FunctionArgExpr> {
    // Scans rather than requiring a single projected item, because a grouped
    // statement projects its group columns alongside the aggregate.
    let f = select_projection(stmt)?.iter().find_map(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(e)
            | SelectItem::ExprWithAlias { expr: e, .. }
            | SelectItem::ExprWithAliases { expr: e, .. } => e,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        };
        match expr {
            Expr::Function(f) => Some(f),
            _ => None,
        }
    })?;
    match &f.args {
        FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
            FunctionArg::Unnamed(arg @ (FunctionArgExpr::Wildcard | FunctionArgExpr::Expr(_))) => {
                Some(arg.clone())
            }
            _ => None,
        },
        _ => None,
    }
}

/// `NAME(arg)`, with every optional clause empty.
///
/// The seed's components are plain aggregate calls: no `DISTINCT`, no `FILTER`,
/// no `OVER`, no `WITHIN GROUP`. Spelling each field out rather than cloning the
/// projected call is what keeps a clause the user wrote from riding along into a
/// component that must not carry it.
fn agg_call(name: &str, arg: FunctionArgExpr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(alloc::vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: alloc::vec![FunctionArg::Unnamed(arg)],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// `expr AS cN`, the alias a seed component is read back by.
fn component(expr: Expr, slot: usize) -> SelectItem {
    SelectItem::ExprWithAlias {
        expr,
        alias: Ident::new(alloc::format!("c{slot}")),
    }
}

/// The seed component for the variance family's spread: the engine's own
/// sum of squared deviations, spelled `VAR_POP(x) * COUNT(x)`.
///
/// Read back from the server rather than derived from a sum of squares,
/// because `sum_sq - sum^2/n` is the cancellation this fold exists to
/// avoid, and a seed carrying it would reintroduce the wrong answer the
/// moment a subscription starts from a database read. Measured over
/// `100000000.0`, `100000001.0` and `100000002.0`, PostgreSQL answers `2`
/// for `var_pop(x) * count(x)` exactly, while `sum(x*x)` is
/// `3.0000000600000004e+16` and loses the answer.
///
/// `VAR_POP` is NULL over no rows, so the product is NULL there, which
/// seeds the empty state exactly as the other components do.
fn sum_of_squared_deviations<B: crate::backend::Backend>(arg: &FunctionArgExpr) -> Option<Expr> {
    let FunctionArgExpr::Expr(col) = arg else {
        // `COUNT(*)` has no column to spread, and no spec that needs this
        // term accepts a wildcard argument.
        return None;
    };
    match B::VARIANCE_SEED {
        crate::backend::VarianceSeed::EnginesOwn => Some(Expr::BinaryOp {
            left: alloc::boxed::Box::new(agg_call("VAR_POP", arg.clone())),
            op: BinaryOperator::Multiply,
            right: alloc::boxed::Box::new(agg_call("COUNT", arg.clone())),
        }),
        // No variance function to ask, so the seed asks for a sum of
        // squares and the deviations are derived from it. The `* 1.0 *`
        // keeps the product out of the source integer type.
        crate::backend::VarianceSeed::SumOfSquares => {
            let one = Expr::Value(
                sqlparser::ast::Value::Number("1.0".to_string(), false).with_empty_span(),
            );
            let scaled = Expr::BinaryOp {
                left: alloc::boxed::Box::new(col.clone()),
                op: BinaryOperator::Multiply,
                right: alloc::boxed::Box::new(one),
            };
            Some(agg_call(
                "SUM",
                FunctionArgExpr::Expr(Expr::BinaryOp {
                    left: alloc::boxed::Box::new(scaled),
                    op: BinaryOperator::Multiply,
                    right: alloc::boxed::Box::new(col.clone()),
                }),
            ))
        }
    }
}

fn select_projection(stmt: &Statement) -> Option<&[SelectItem]> {
    select_of(stmt).map(|select| select.projection.as_slice())
}

fn select_projection_mut(stmt: &mut Statement) -> Option<&mut Vec<SelectItem>> {
    select_mut(stmt).map(|select| &mut select.projection)
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
/// This is SubQL's statement-shape rule: one table, and of the statement only
/// the projection, that table, and the WHERE clause. A subscription is the
/// rows of one table that satisfy one filter, delivered as they change, so
/// every other clause asks a question no single change event can answer, and
/// answering the reduced query instead is silent wrongness. Refusing is also
/// what routes a statement upward: the re-execution wrapper triggers on
/// [`RegisterError::UnsupportedSql`], so a clause accepted here is a clause no
/// tier above can serve.
///
/// Stated once and applied both to the subscription statement and to the inner
/// query of a membership subquery, so the two cannot drift apart.
fn single_table_select(
    query: &Query,
    grouping: Grouping,
) -> Result<(&Select, &ObjectName), RegisterError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(RegisterError::UnsupportedSql(
            "Set operations (UNION, INTERSECT, EXCEPT) not supported - SubQL is for single-table CDC event filtering. \
             For queries combining multiple result sets, run this as a regular SQL query in your database."
                .to_string(),
        ));
    };

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

    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Err(RegisterError::UnsupportedSql(
            "Subqueries and derived tables not supported - SubQL is for single-table WHERE clauses. \
             Run this as a regular SQL query in your database."
                .to_string(),
        ));
    };

    check_served_clauses(query, select, grouping)?;

    Ok((select, name))
}

/// Refuse every clause of `query` and `select` outside the projection, the
/// table, and the WHERE clause.
fn check_served_clauses(
    query: &Query,
    select: &Select,
    grouping: Grouping,
) -> Result<(), RegisterError> {
    // Destructured exhaustively, no `..`, on purpose: sqlparser is tracked by
    // branch, so a clause it learns to parse arrives here as a compile error
    // demanding a decision instead of as a silently dropped clause. sqlparser
    // guards `SelectModifiers::is_any_set` the same way.
    let Query {
        with,
        // Reduced to `select` by the caller.
        body: _,
        // Served, not refused: `ORDER BY` changes the sequence rows arrive in,
        // never which rows are members, so the in-process row engine matches the
        // same set and a caller applies the ordering to its own snapshot. A
        // window (`LIMIT`/`OFFSET`/`FETCH`) does change membership and is refused
        // on its own line below.
        order_by: _,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    let Select {
        // Span of the `SELECT` keyword, not a clause.
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        // Only meaningful alongside `top`, refused below.
        top_before_distinct: _,
        projection: _,
        exclude,
        into,
        // Reduced to the one table by the caller.
        from: _,
        lateral_views,
        prewhere,
        selection: _,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        // Only meaningful alongside `named_window` and `qualify`, both refused below.
        window_before_qualify: _,
        value_table_mode,
        // `FROM t SELECT *` spells the same query as `SELECT * FROM t`.
        flavor: _,
    } = select;

    // A `GROUP BY` is a served shape only where a grouped fold can be built
    // from it, which is the outer statement of a subscription. There
    // `extract_projection` decides, refusing every grouped shape the fold
    // cannot maintain, including a grouped row subscription. Everywhere else
    // it is refused here, because nothing downstream would look at it: a
    // membership subquery's inner query is matched value by value, and a keyed
    // capture asks about one changed row, both of which a grouping invalidates
    // without failing.
    //
    // `GROUP BY ALL` is refused even where grouping is served, since it names
    // its groups by position in the projection rather than by column.
    let (grouped, grouped_by_position) = match group_by {
        GroupByExpr::All(_) => (true, true),
        GroupByExpr::Expressions(exprs, modifiers) => {
            (!exprs.is_empty() || !modifiers.is_empty(), false)
        }
    };
    let grouping_refused = grouped_by_position || (grouped && grouping == Grouping::Refused);

    // `SELECT ALL` asks for every row and no deduplication, which is the served
    // shape spelled out, so only the deduplicating spellings are refused.
    let deduplicating = match distinct {
        None | Some(Distinct::All) => false,
        Some(Distinct::Distinct | Distinct::On(_)) => true,
    };

    // Ordered most-written first. Later entries name clauses no backend's
    // dialect here parses (ClickHouse, Hive, Snowflake, MSSQL, BigQuery), so no
    // test reaches them: they are the decision the exhaustive destructuring
    // demands, and refusing is the only safe one to make blind.
    for (present, clause) in [
        (with.is_some(), "WITH"),
        (grouping_refused, "GROUP BY"),
        // Like `GROUP BY`: on the served path the projection logic decides,
        // parsing the fast-path comparison and refusing everything else, so
        // no shape can silently drop the clause. Everywhere else it is
        // refused outright.
        (having.is_some() && grouping == Grouping::Refused, "HAVING"),
        (deduplicating, "DISTINCT"),
        (
            limit_clause.is_some(),
            limit_clause_name(limit_clause.as_ref()),
        ),
        (fetch.is_some(), "FETCH"),
        (!locks.is_empty(), "FOR UPDATE / FOR SHARE"),
        (for_clause.is_some(), "FOR XML / FOR JSON"),
        (settings.is_some(), "SETTINGS"),
        (format_clause.is_some(), "FORMAT"),
        (!pipe_operators.is_empty(), "a pipe operator"),
        (!optimizer_hints.is_empty(), "an optimizer hint"),
        (
            select_modifiers
                .as_ref()
                .is_some_and(SelectModifiers::is_any_set),
            "a SELECT modifier",
        ),
        (top.is_some(), "TOP"),
        (exclude.is_some(), "EXCLUDE"),
        (into.is_some(), "INTO"),
        (!lateral_views.is_empty(), "LATERAL VIEW"),
        (prewhere.is_some(), "PREWHERE"),
        (!connect_by.is_empty(), "CONNECT BY"),
        (!cluster_by.is_empty(), "CLUSTER BY"),
        (!distribute_by.is_empty(), "DISTRIBUTE BY"),
        (!sort_by.is_empty(), "SORT BY"),
        (!named_window.is_empty(), "WINDOW"),
        (qualify.is_some(), "QUALIFY"),
        (
            value_table_mode.is_some(),
            "SELECT AS VALUE / SELECT AS STRUCT",
        ),
    ] {
        if present {
            return Err(unserved_clause(clause));
        }
    }

    Ok(())
}

/// Which part of a `LIMIT` clause the caller actually wrote, so the refusal
/// quotes their own keyword.
const fn limit_clause_name(limit: Option<&LimitClause>) -> &'static str {
    match limit {
        Some(LimitClause::LimitOffset {
            limit: None,
            offset,
            limit_by,
        }) => {
            if offset.is_some() {
                "OFFSET"
            } else if limit_by.is_empty() {
                "LIMIT"
            } else {
                "LIMIT BY"
            }
        }
        None | Some(LimitClause::LimitOffset { .. } | LimitClause::OffsetCommaLimit { .. }) => {
            "LIMIT"
        }
    }
}

/// Refuse a clause outside the shape SubQL serves.
fn unserved_clause(clause: &str) -> RegisterError {
    RegisterError::UnsupportedSql(format!(
        "{clause} not supported - a SubQL subscription is the rows of one table that satisfy one \
         WHERE clause, delivered as they change, and nothing else in a SELECT survives that \
         reduction. Run this as a regular SQL query in your database."
    ))
}

/// Extract the single table name and WHERE clause from a supported SELECT,
/// where a grouped statement is one of the supported shapes.
///
/// This enforces SubQL's statement-shape constraints (see
/// [`single_table_select`]) so parser and canonicalizer stay in sync. Whether
/// this particular grouping is servable is [`extract_projection`]'s answer,
/// which every caller of this runs afterwards.
pub(crate) fn extract_single_table_and_where(
    stmt: &Statement,
) -> Result<(ObjectName, Option<Expr>), RegisterError> {
    extract_table_and_where_with(stmt, Grouping::Served)
}

/// As [`extract_single_table_and_where`], but refusing a grouped statement.
///
/// For callers that do something with the rows themselves rather than build a
/// fold: a keyed capture asks the database about one changed row, and a
/// grouping makes that row's membership depend on the others.
pub(crate) fn extract_ungrouped_table_and_where(
    stmt: &Statement,
) -> Result<(ObjectName, Option<Expr>), RegisterError> {
    extract_table_and_where_with(stmt, Grouping::Refused)
}

fn extract_table_and_where_with(
    stmt: &Statement,
    grouping: Grouping,
) -> Result<(ObjectName, Option<Expr>), RegisterError> {
    match stmt {
        Statement::Query(query) => {
            let (select, name) = single_table_select(query, grouping)?;
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
/// Narrower than [`contains_subquery`] on purpose: a `NOT` over one of these
/// is refused as subtraction, and saying that about a bare scalar subquery
/// would describe the wrong thing. `EXISTS` counts, since the bounded
/// membership `EXISTS` is a term and its negation is the same subtraction.
pub(super) fn contains_membership_subquery(expr: &Expr) -> bool {
    contains_matching(expr, |inner| {
        matches!(inner, Expr::InSubquery { .. } | Expr::Exists { .. })
    })
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
    let (select, _) = single_table_select(query, Grouping::Refused)?;

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

/// The parts of a bounded membership `EXISTS`, from one recognizer for both
/// the compiler and the seed synthesis, so the two cannot drift.
pub(crate) struct ExistsParts<'a> {
    /// One entry per pair equality, in written order.
    pub pairs: Vec<ExistsPair<'a>>,
    /// The caller comparison conjunct, verbatim.
    pub caller: &'a Expr,
    /// The membership table clause, verbatim, alias included.
    pub from: &'a sqlparser::ast::TableWithJoins,
    /// The membership table, resolved. Read only by the `membership-term`
    /// compiler half.
    #[cfg(feature = "membership-term")]
    pub member_table: crate::TableId,
}

/// One pair equality of a bounded membership `EXISTS`.
pub(crate) struct ExistsPair<'a> {
    /// The membership-side expression, verbatim, for the seed projection.
    pub inner_expr: &'a Expr,
    /// The membership column it names, resolved on the membership table. Read
    /// only by the `membership-term` compiler half.
    #[cfg(feature = "membership-term")]
    pub inner: crate::ColumnId,
    /// The compared column, resolved on the subscribed table.
    pub outer: crate::ColumnId,
}

/// The refusal every violation of the bounded `EXISTS` form shares its frame
/// with: name what the form is, then what this filter got wrong.
fn exists_refusal(what: &str) -> RegisterError {
    RegisterError::UnsupportedSql(alloc::format!(
        "A membership EXISTS is served in one bounded form: a single-table subquery whose \
         WHERE is one or more pair equalities (membership column against a column of the \
         subscribed table, written qualified as its table) plus exactly one comparison \
         naming the caller. This filter {what}."
    ))
}

/// Append `expr`'s AND-conjuncts to `out`, in written order.
fn conjuncts_of<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts_of(left, out);
            conjuncts_of(right, out);
        }
        Expr::Nested(inner) => conjuncts_of(inner, out),
        other => out.push(other),
    }
}

/// The qualifier and column of a one- or two-part column reference.
const fn qualified_parts(expr: &Expr) -> Option<(Option<&str>, &str)> {
    match expr {
        Expr::Identifier(ident) => Some((None, ident.value.as_str())),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [qualifier, column] => Some((Some(qualifier.value.as_str()), column.value.as_str())),
            _ => None,
        },
        _ => None,
    }
}

/// Recognize and bound the membership `EXISTS` form.
///
/// The membership side of a pair may be bare or qualified by the membership
/// table's name or alias. The subscribed side must be qualified by the
/// subscribed table's name: a bare name resolves to the membership table
/// inside the subquery under SQL's own rules, so accepting one as the
/// subscribed column would serve a filter the database reads differently.
#[allow(
    clippy::too_many_lines,
    reason = "one pass classifies every conjunct against resolvers that close over the \
              subquery's alias, and splitting it would hand the closures around"
)]
pub(crate) fn membership_exists_parts<'a, DB: DatabaseLike>(
    query: &'a Query,
    table_id: crate::TableId,
    database: &DB,
) -> Result<ExistsParts<'a>, RegisterError> {
    let (select, member_name) = single_table_select(query, Grouping::Refused)?;
    if select.selection.as_ref().is_some_and(contains_subquery) {
        return Err(RegisterError::UnsupportedSql(
            "A membership subquery cannot contain another subquery. SubQL tracks one \
             relationship for the subscription, and a nested subquery names a second one it \
             cannot follow."
                .to_string(),
        ));
    }
    let member_table_name = member_name
        .0
        .last()
        .and_then(sqlparser::ast::ObjectNamePart::as_ident)
        .map(|ident| ident.value.as_str())
        .ok_or_else(|| exists_refusal("names its membership table in a form SubQL cannot read"))?;
    let member_table = catalog_helpers::table_id(database, member_table_name)
        .ok_or_else(|| exists_refusal("reads a membership table the catalog does not know"))?;
    let alias = match &select.from[0].relation {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|alias| alias.name.value.as_str()),
        _ => None,
    };
    // A qualifier names the membership side when it is the alias or the
    // membership table's own name, and the subscribed side when it resolves to
    // the subscribed table. Checked in that order, because inside the subquery
    // the alias shadows everything else.
    let is_member_qualifier = |qualifier: &str| {
        Some(qualifier) == alias
            || catalog_helpers::table_id(database, qualifier) == Some(member_table)
    };
    let member_column = |expr: &Expr| -> Option<crate::ColumnId> {
        let (qualifier, column) = qualified_parts(expr)?;
        if qualifier.is_some_and(|qualifier| !is_member_qualifier(qualifier)) {
            return None;
        }
        catalog_helpers::column_id(database, member_table, column)
    };
    let subscribed_column = |expr: &Expr| -> Option<crate::ColumnId> {
        let (qualifier, column) = qualified_parts(expr)?;
        let qualifier = qualifier?;
        if is_member_qualifier(qualifier)
            || catalog_helpers::table_id(database, qualifier) != Some(table_id)
        {
            return None;
        }
        catalog_helpers::column_id(database, table_id, column)
    };
    // Both builds resolve the membership column to classify the pair. Only
    // the `membership-term` half stores it.
    let make_pair = |member: &'a Expr, subscribed: &'a Expr| -> Option<ExistsPair<'a>> {
        let inner = member_column(member)?;
        let outer = subscribed_column(subscribed)?;
        #[cfg(not(feature = "membership-term"))]
        let _ = inner;
        Some(ExistsPair {
            inner_expr: member,
            #[cfg(feature = "membership-term")]
            inner,
            outer,
        })
    };

    let Some(selection) = select.selection.as_ref() else {
        return Err(exists_refusal(
            "has no WHERE, so it correlates with nothing",
        ));
    };
    let mut flat = Vec::new();
    conjuncts_of(selection, &mut flat);

    let mut pairs = Vec::new();
    let mut caller = None;
    for conjunct in flat {
        if is_caller_comparison(conjunct) {
            if caller.is_some() {
                return Err(exists_refusal("compares the caller twice"));
            }
            let Expr::BinaryOp { left, right, .. } = conjunct else {
                unreachable!("a caller comparison is a binary comparison by construction");
            };
            let column_side = if qualified_parts(left).is_some() {
                left
            } else {
                right
            };
            if member_column(column_side).is_none() {
                return Err(exists_refusal(
                    "compares the caller against something other than a membership column",
                ));
            }
            caller = Some(conjunct);
            continue;
        }
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = conjunct
        else {
            return Err(exists_refusal(
                "carries a condition that is neither a pair equality nor a caller comparison",
            ));
        };
        let pair = make_pair(left, right).or_else(|| make_pair(right, left));
        let Some(pair) = pair else {
            return Err(exists_refusal(
                "carries an equality that does not pair a membership column with a qualified \
                 column of the subscribed table",
            ));
        };
        pairs.push(pair);
    }
    let Some(caller) = caller else {
        return Err(exists_refusal("never names the caller"));
    };
    if pairs.is_empty() {
        return Err(exists_refusal(
            "correlates no membership column with the subscribed table",
        ));
    }
    Ok(ExistsParts {
        pairs,
        caller,
        from: &select.from[0],
        #[cfg(feature = "membership-term")]
        member_table,
    })
}

/// The compared columns of a bounded membership `EXISTS`, in written order.
pub(super) fn check_membership_exists_bound<DB: DatabaseLike>(
    query: &Query,
    table_id: crate::TableId,
    database: &DB,
) -> Result<Vec<crate::ColumnId>, RegisterError> {
    Ok(membership_exists_parts(query, table_id, database)?
        .pairs
        .into_iter()
        .map(|pair| pair.outer)
        .collect())
}

/// The seed read of a bounded membership `EXISTS`: the membership columns the
/// pairs name, projected in pair order, for the rows naming the caller.
///
/// `None` when `query` is not the recognized form, which `resolve` reports as
/// a shape that lost its seed read.
pub(crate) fn exists_seed_select<DB: DatabaseLike>(
    query: &Query,
    table_id: crate::TableId,
    database: &DB,
) -> Option<String> {
    let parts = membership_exists_parts(query, table_id, database).ok()?;
    let mut projection = String::new();
    for (position, pair) in parts.pairs.iter().enumerate() {
        if position > 0 {
            projection.push_str(", ");
        }
        projection.push_str(&pair.inner_expr.to_string());
    }
    Some(alloc::format!(
        "SELECT {projection} FROM {} WHERE {}",
        parts.from,
        parts.caller
    ))
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
pub(crate) fn count_placeholders_in_expr(expr: &Expr) -> usize {
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
