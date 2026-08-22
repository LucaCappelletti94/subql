//! Layer 1: classification.
//!
//! Turns a SQL string the core engine rejected into a re-execution plan. The
//! engine itself decides the "native" case (it accepts the query). This layer
//! only runs for rejected queries and produces a [`QueryPlan`] describing how
//! the re-execution layer should maintain it.

use crate::backend::Backend;
use crate::compiler::literals::SqlLiteralParse;
use crate::compiler::parser;
use crate::compiler::sql_shape::{extract_scalar_aggregate, ScalarAggKind};
use crate::compiler::BytecodeProgram;
use crate::{ColumnId, RegisterError, TableId};
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{Ident, SelectItem, SetExpr, Statement};
use sqlparser::dialect::Dialect;

/// Column alias the re-execution query projects its scalar result under, so the
/// executor can load it back by a stable name.
const REEXEC_VALUE_ALIAS: &str = "v";

/// How the re-execution layer should serve a query the core engine rejected.
///
/// `Native` (the engine handles it) is decided upstream by the engine
/// accepting the query, so it never appears here. A future `Total` variant
/// (JOIN/HAVING/multi-table re-run on any relevant event) plugs in alongside
/// `Partial` once the row-set executor exists.
pub(super) enum QueryPlan<B: Backend> {
    /// A single-table scalar `MIN` / `MAX`, maintained incrementally
    /// with a database re-query only on extreme removal.
    Partial(MinMaxPlan<B>),
    /// A query nothing in process can maintain, re-read in full whenever a
    /// table it reads changes.
    Total(TotalPlan),
}

/// Plan for a query served by re-reading it whole.
pub(super) struct TotalPlan {
    /// Tables whose changes mean the answer may have moved.
    pub tables: Vec<TableId>,
    /// Every column of every one of those tables: a computed projection can
    /// depend on any of them, so narrowing this would mean guessing.
    pub dependency_columns: Vec<ColumnId>,
    /// The statement as written, which is what gets re-read. Unlike the scalar
    /// plan nothing is rewritten, because the caller asked for these rows and
    /// this tier promises exactly them.
    pub reexec_sql: String,
}

/// Plan for an incrementally-maintained single-table scalar `MIN`/`MAX`.
pub(super) struct MinMaxPlan<B: Backend> {
    /// Table the aggregate reads from.
    pub table_id: TableId,
    /// `MIN` or `MAX`.
    pub kind: ScalarAggKind,
    /// The aggregated column.
    pub agg_column: ColumnId,
    /// [`ScalarKind`](crate::backend::ScalarKind) of the aggregated
    /// column. Not consumed by in-process maintenance (MIN/MAX compares
    /// `Value` variants directly), but returned to the materializer via
    /// [`Registered::ReExec`](super::Registered::ReExec) as a decode hint.
    pub agg_kind: crate::backend::BuiltinKind,
    /// Columns whose change can alter the result: the aggregated column
    /// plus every column the WHERE clause reads (UPDATE routing
    /// optimization).
    pub dependency_columns: Vec<ColumnId>,
    /// The compiled WHERE clause, so maintenance can test row membership
    /// in-process via the VM (always-true when the query has no WHERE).
    pub where_program: Arc<BytecodeProgram<B>>,
    /// SQL the Subscription Materializer runs after a
    /// [`ReExecutionTrigger`](super::ReExecutionTrigger), with its
    /// projection aliased. Returned to the materializer at registration
    /// via [`Registered::ReExec`](super::Registered::ReExec). Subql
    /// itself never executes it.
    pub reexec_sql: String,
}

/// Classify a rejected query into a [`QueryPlan`].
///
/// Returns `Ok(QueryPlan::Partial(_))` for a single-table scalar `MIN`/`MAX`.
/// Returns `Err(UnsupportedSql)` for anything else (the caller surfaces the
/// engine's original rejection message instead of this one). This is where the
/// future `Total` plan will be built for JOIN/HAVING/multi-table queries.
pub(super) fn build_plan<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    // The scalar plan is the narrow, cheap case. Anything else the engine
    // refused is a candidate for a whole re-read, including the filters the
    // predicate language cannot compile, which is why that attempt's error is
    // discarded rather than surfaced.
    scalar_plan::<B, DB>(sql, dialect, database)
        .or_else(|_| total_plan::<B, DB>(sql, dialect, database))
}

/// Plan a query by re-reading it whole: find every table it reads, take every
/// column of those tables as a dependency, and keep the statement as written.
///
/// Deliberately weaker than [`scalar_plan`]: it neither compiles the WHERE
/// clause nor constrains the statement's shape, because the shapes this serves
/// are exactly the ones those checks refuse.
fn total_plan<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    use core::ops::ControlFlow;

    let statement =
        crate::compiler::sql_shape::parse_single_statement(sql, dialect as &dyn Dialect)?;
    if !matches!(statement, Statement::Query(_)) {
        return Err(RegisterError::UnsupportedSql(
            "only a SELECT can be captured for re-execution".to_string(),
        ));
    }

    // The derived walk rather than a hand-rolled one: a table missed here is a
    // table whose changes never trigger the query, so the subscription would go
    // quietly stale instead of failing.
    let mut names = Vec::new();
    let _: ControlFlow<()> = sqlparser::ast::visit_relations(&statement, |name| {
        names.push(name.clone());
        ControlFlow::Continue(())
    });

    let mut tables = Vec::new();
    for name in &names {
        let table_name = crate::compiler::parser::SqlTableName::from_object_name(name)?;
        let id = crate::table_resolution::resolve_table_reference(
            table_name.qualified.as_deref(),
            &table_name.unqualified,
            database,
        )
        .map_err(|_| RegisterError::UnknownTable(table_name.unqualified.clone()))?;
        if !tables.contains(&id) {
            tables.push(id);
        }
    }
    if tables.is_empty() {
        return Err(RegisterError::UnsupportedSql(
            "a captured query must read at least one table, or no change could \
             ever refresh it"
                .to_string(),
        ));
    }

    let mut dependency_columns = Vec::new();
    for table in &tables {
        let arity = crate::catalog_helpers::table_arity(database, *table).unwrap_or(0);
        for ordinal in 0..arity {
            // Column ids are ordinals in the table's own order.
            let Ok(column) = ColumnId::try_from(ordinal) else {
                continue;
            };
            if !dependency_columns.contains(&column) {
                dependency_columns.push(column);
            }
        }
    }

    Ok(QueryPlan::Total(TotalPlan {
        tables,
        dependency_columns,
        reexec_sql: sql.to_string(),
    }))
}

fn scalar_plan<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let parsed = parser::parse_table_and_where_deps::<B, DB>(sql, dialect, database)?;

    let Some((kind, agg_column)) =
        extract_scalar_aggregate(&parsed.statement, parsed.table_id, database)?
    else {
        return Err(RegisterError::UnsupportedSql(
            "query is not a single-table scalar MIN/MAX; cannot re-execute".to_string(),
        ));
    };

    let agg_kind =
        crate::catalog_helpers::column_builtin_kind(database, parsed.table_id, agg_column)
        .ok_or_else(|| {
            RegisterError::UnsupportedSql(format!(
                "aggregated column {agg_column} of table {table_id} has an unsupported SQL type for the maintenance layer",
                table_id = parsed.table_id,
            ))
        })?;

    let mut dependency_columns = parsed.where_dependency_columns.clone();
    if !dependency_columns.contains(&agg_column) {
        dependency_columns.push(agg_column);
    }

    let reexec_sql = render_aliased_scalar(&parsed.statement).ok_or_else(|| {
        RegisterError::UnsupportedSql("unable to render re-execution query".to_string())
    })?;

    Ok(QueryPlan::Partial(MinMaxPlan {
        table_id: parsed.table_id,
        kind,
        agg_column,
        dependency_columns,
        agg_kind,
        where_program: Arc::new(parsed.where_program),
        reexec_sql,
    }))
}

/// Re-render `stmt` with its single projection aliased as
/// [`REEXEC_VALUE_ALIAS`], producing the re-execution query. AST mutation and
/// rendering are `sqlparser`'s job. Subql only sets the alias. Returns `None`
/// if the statement is not the single-projection SELECT shape
/// [`extract_scalar_aggregate`] already validated.
fn render_aliased_scalar(stmt: &Statement) -> Option<String> {
    let mut stmt = stmt.clone();
    let Statement::Query(query) = &mut stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    let [item] = select.projection.as_mut_slice() else {
        return None;
    };
    let expr = match item {
        SelectItem::UnnamedExpr(e)
        | SelectItem::ExprWithAlias { expr: e, .. }
        | SelectItem::ExprWithAliases { expr: e, .. } => e.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
    };
    *item = SelectItem::ExprWithAlias {
        expr,
        alias: Ident::new(REEXEC_VALUE_ALIAS),
    };
    Some(stmt.to_string())
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.
