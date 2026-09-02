//! Layer 1: classification.
//!
//! Turns a SQL string the core engine rejected into a re-execution plan. The
//! engine itself decides the "native" case (it accepts the query). This layer
//! only runs for rejected queries and produces a [`QueryPlan`] describing how
//! the re-execution layer should maintain it.

use crate::backend::{Backend, BuiltinKind, Value};
use crate::compiler::literals::SqlLiteralParse;
use crate::compiler::parser;
use crate::compiler::sql_shape::{
    extract_grouped_extreme, extract_scalar_aggregate, ScalarAggKind,
};
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
pub enum QueryPlan<B: Backend> {
    /// A single-table scalar `MIN` / `MAX`, maintained incrementally
    /// with a database re-query only on extreme removal.
    Partial(MinMaxPlan<B>),
    /// Grouped extrema maintained in memory with reads scoped to one group.
    GroupedPartial(alloc::boxed::Box<GroupedMinMaxPlan<B>>),
    /// A query nothing in process can maintain, re-read in full whenever a
    /// table it reads changes.
    Total(TotalPlan<B>),
    /// A filter over one table, maintained by asking the database only about
    /// the rows that changed.
    Keyed(alloc::boxed::Box<KeyedPlan>),
}

/// Plan for a query served by re-reading it whole.
pub struct TotalPlan<B: Backend> {
    /// Tables whose changes mean the answer may have moved.
    pub tables: Vec<TableId>,
    /// Every column of every one of those tables: a computed projection can
    /// depend on any of them, so narrowing this would mean guessing.
    pub dependency_columns: Vec<ColumnId>,
    /// The executable query re-read without rewriting.
    pub read_query: crate::reexec::BoundQuery<B>,
}

/// Plan for an incrementally-maintained single-table scalar `MIN`/`MAX`.
pub struct MinMaxPlan<B: Backend> {
    /// Table the aggregate reads from.
    pub table_id: TableId,
    /// `MIN` or `MAX`.
    pub kind: ScalarAggKind,
    /// The aggregated column.
    pub agg_column: ColumnId,
    /// [`ScalarKind`](crate::backend::ScalarKind) of the aggregated
    /// column. Not consumed by in-process maintenance (MIN/MAX compares
    /// `Value` variants directly), but returned to the materializer via
    /// [`Tier::Scalar`](crate::Tier::Scalar) as a decode hint.
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
    /// via [`Tier::Scalar`](crate::Tier::Scalar). Subql
    /// itself never executes it.
    pub read_query: crate::reexec::BoundQuery<B>,
}

/// Plan for grouped `MIN` or `MAX`.
pub struct GroupedMinMaxPlan<B: Backend> {
    pub table_id: TableId,
    pub kind: ScalarAggKind,
    pub agg_column: ColumnId,
    pub agg_kind: crate::backend::BuiltinKind,
    pub group_columns: Vec<ColumnId>,
    pub group_idents: Vec<Ident>,
    pub dependency_columns: Vec<ColumnId>,
    pub where_dependency_columns: Vec<ColumnId>,
    pub where_program: Arc<BytecodeProgram<B>>,
    pub statement: Statement,
    pub read_projection: Vec<SelectItem>,
    pub bootstrap: crate::AggregateBootstrap<B>,
    pub having: Option<GroupedHavingCheck<B>>,
    pub bind_placeholder: crate::compiler::BindPlaceholder,
    pub positional_scope_bind_index: usize,
    pub source_query: crate::reexec::BoundQuery<B>,
    pub group_key_encoder: crate::backend::GroupKeyEncoder<B>,
}

/// A grouped extreme's `HAVING` comparison, its threshold parsed at plan
/// time so each event evaluates without touching SQL text.
pub enum GroupedHavingCheck<B: Backend> {
    /// Compare the group's current extreme against a constant.
    Extreme {
        op: crate::HavingOp,
        threshold: Value<B>,
    },
    /// Compare the group's source-row count against a constant.
    RowCount { op: crate::HavingOp, threshold: i64 },
}

/// Classify a rejected query into a [`QueryPlan`].
///
/// Returns `Ok(QueryPlan::Partial(_))` for a single-table scalar `MIN`/`MAX`.
/// Returns `Err(UnsupportedSql)` for anything else (the caller surfaces the
/// engine's original rejection message instead of this one). This is where the
/// future `Total` plan will be built for JOIN/HAVING/multi-table queries.
pub fn build_plan<B, DB>(
    query: &crate::reexec::BoundQuery<B>,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    // Cheapest first. The scalar plan is the narrowest, then a filter over one
    // table whose changed rows can be asked about individually, then a whole
    // re-read for everything else. An intermediate tier's error only means the
    // query belongs to the next tier, so it surfaces only when every tier
    // refused, where each tier's own reason is the whole diagnosis.
    let grouped = match grouped_scalar_plan::<B, DB>(query, dialect, database) {
        Ok(plan) => return Ok(plan),
        Err(error) => error,
    };
    let scalar = match scalar_plan::<B, DB>(query, dialect, database) {
        Ok(plan) => return Ok(plan),
        Err(error) => error,
    };
    let keyed = match keyed_plan::<B, DB>(query.sql(), dialect, database) {
        Ok(plan) => return Ok(plan),
        Err(error) => error,
    };
    total_plan::<B, DB>(query, dialect, database).map_err(|total| {
        RegisterError::UnsupportedSql(alloc::format!(
            "no read tier serves this statement: grouped extreme said \
             \"{grouped}\", scalar said \"{scalar}\", keyed said \"{keyed}\", \
             whole re-read said \"{total}\""
        ))
    })
}

/// Build the complete-row replacement tier directly.
///
/// Aggregate transitions call this instead of [`build_plan`], whose cheapest
/// first ordering would select the in-process aggregate tier again.
pub fn build_whole_rows_plan<B, DB>(
    query: &crate::reexec::BoundQuery<B>,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<TotalPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    match total_plan::<B, DB>(query, dialect, database)? {
        QueryPlan::Total(plan) => Ok(plan),
        QueryPlan::Partial(_) | QueryPlan::GroupedPartial(_) | QueryPlan::Keyed(_) => {
            unreachable!("total_plan returns Total")
        }
    }
}

/// Plan a query by re-reading it whole: find every table it reads, take every
/// column of those tables as a dependency, and keep the statement as written.
///
/// Deliberately weaker than [`scalar_plan`]: it neither compiles the WHERE
/// clause nor constrains the statement's shape, because the shapes this serves
/// are exactly the ones those checks refuse.
fn total_plan<B, DB>(
    query: &crate::reexec::BoundQuery<B>,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    use core::ops::ControlFlow;

    let statement =
        crate::compiler::sql_shape::parse_single_statement(query.sql(), dialect as &dyn Dialect)?;
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
        let arity = crate::catalog_helpers::table_arity(database, *table)?;
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
        read_query: query.clone(),
    }))
}

fn grouped_scalar_plan<B, DB>(
    query: &crate::reexec::BoundQuery<B>,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let parsed =
        parser::parse_table_and_where_deps::<B, DB>(query.sql(), dialect, database, query.binds())?;
    let projection =
        extract_grouped_extreme::<B, DB>(&parsed.statement, parsed.table_id, database)?
            .ok_or_else(|| {
                RegisterError::UnsupportedSql(
                    "query is not a grouped single-table MIN/MAX".to_string(),
                )
            })?;
    let agg_kind =
        crate::catalog_helpers::column_builtin_kind(database, parsed.table_id, projection.column)
            .ok_or_else(|| {
            RegisterError::UnsupportedSql(
                "the grouped extreme column has no supported decode kind".to_string(),
            )
        })?;
    let having = planned_having::<B>(projection.having.as_ref(), agg_kind)?;
    let group_key_columns = projection
        .groups
        .iter()
        .map(|column| {
            crate::catalog_helpers::group_key_column::<B, _>(database, parsed.table_id, *column)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            RegisterError::UnsupportedSql(
                "a grouped extreme column has incomplete key metadata".to_string(),
            )
        })?;
    let group_key_encoder = B::group_key_encoder(group_key_columns).ok_or_else(|| {
        RegisterError::UnsupportedSql("a grouped extreme column has no canonical key".to_string())
    })?;
    let (group_kinds, group_idents) =
        grouped_column_metadata::<B, DB>(&projection.groups, parsed.table_id, dialect, database)?;
    let agg_name =
        crate::catalog_helpers::column_name(database, parsed.table_id, projection.column)
            .ok_or_else(|| RegisterError::UnknownColumn {
                table_id: parsed.table_id,
                column: alloc::format!("column {}", projection.column),
            })?;
    let agg_ident = crate::compiler::quoted_ident(dialect as &dyn Dialect, &agg_name);
    let bootstrap_query = render_grouped_scalar_bootstrap_query::<B>(
        &parsed.statement,
        &group_idents,
        &agg_ident,
        projection.kind,
        query.binds(),
        dialect,
    )?;
    let mut bootstrap_kinds = group_kinds;
    bootstrap_kinds.extend([agg_kind, crate::backend::BuiltinKind::Int]);
    let bootstrap = crate::AggregateBootstrap {
        query: bootstrap_query,
        kinds: bootstrap_kinds,
        group_columns: group_idents.len(),
    };
    let read_projection =
        grouped_scalar_read_projection::<B>(projection.kind, &agg_ident, dialect)?;
    let positional_scope_bind_index = crate::compiler::sql_shape::select_of(&parsed.statement)
        .and_then(|select| select.selection.as_ref())
        .map_or(0, crate::compiler::sql_shape::count_placeholders_in_expr);
    let mut dependency_columns = parsed.where_dependency_columns.clone();
    for column in projection
        .groups
        .iter()
        .copied()
        .chain(core::iter::once(projection.column))
    {
        if !dependency_columns.contains(&column) {
            dependency_columns.push(column);
        }
    }
    Ok(QueryPlan::GroupedPartial(alloc::boxed::Box::new(
        GroupedMinMaxPlan {
            table_id: parsed.table_id,
            kind: projection.kind,
            agg_column: projection.column,
            agg_kind,
            group_columns: projection.groups,
            group_idents,
            dependency_columns,
            where_dependency_columns: parsed.where_dependency_columns,
            read_projection,
            where_program: Arc::new(parsed.where_program),
            statement: parsed.statement,
            bootstrap,
            having,
            bind_placeholder: crate::compiler::bind_placeholder(dialect),
            positional_scope_bind_index,
            source_query: query.clone(),
            group_key_encoder,
        },
    )))
}

fn grouped_column_metadata<B, DB>(
    columns: &[crate::ColumnId],
    table_id: crate::TableId,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<(Vec<crate::backend::BuiltinKind>, Vec<Ident>), RegisterError>
where
    B: Backend,
    DB: DatabaseLike,
{
    let mut kinds = Vec::with_capacity(columns.len());
    let mut idents = Vec::with_capacity(columns.len());
    for column in columns {
        let kind = crate::catalog_helpers::column_builtin_kind(database, table_id, *column)
            .ok_or_else(|| {
                RegisterError::UnsupportedSql(
                    "a grouped extreme column has no supported decode kind".to_string(),
                )
            })?;
        let name =
            crate::catalog_helpers::column_name(database, table_id, *column).ok_or_else(|| {
                RegisterError::UnknownColumn {
                    table_id,
                    column: alloc::format!("column {column}"),
                }
            })?;
        kinds.push(kind);
        idents.push(crate::compiler::quoted_ident(
            dialect as &dyn Dialect,
            &name,
        ));
    }
    Ok((kinds, idents))
}

/// Parse an extreme's `HAVING` threshold: to the extreme column's kind for a
/// value comparison, to a plain integer for a row count.
fn planned_having<B: Backend + SqlLiteralParse>(
    having: Option<&crate::compiler::sql_shape::ExtremeHaving>,
    agg_kind: crate::backend::BuiltinKind,
) -> Result<Option<GroupedHavingCheck<B>>, RegisterError> {
    let Some(having) = having else {
        return Ok(None);
    };
    Ok(Some(match having.subject {
        crate::compiler::sql_shape::ExtremeHavingSubject::Extreme => GroupedHavingCheck::Extreme {
            op: having.op,
            threshold: B::parse_literal(
                &having.literal,
                crate::backend::ScalarKind::from(agg_kind),
            )?,
        },
        crate::compiler::sql_shape::ExtremeHavingSubject::RowCount => {
            let sqlparser::ast::Value::Number(text, _) = &having.literal else {
                return Err(RegisterError::UnsupportedSql(
                    "a row-count HAVING needs an integer constant".to_string(),
                ));
            };
            GroupedHavingCheck::RowCount {
                op: having.op,
                threshold: text.parse().map_err(|_| {
                    RegisterError::UnsupportedSql(
                        "a row-count HAVING needs an integer constant".to_string(),
                    )
                })?,
            }
        }
    }))
}

fn render_grouped_scalar_bootstrap_query<B: Backend>(
    statement: &Statement,
    group_idents: &[Ident],
    agg_ident: &Ident,
    kind: ScalarAggKind,
    binds: &[Value<B>],
    dialect: &B::Dialect,
) -> Result<crate::reexec::BoundQuery<B>, RegisterError> {
    use core::fmt::Write as _;
    let function = match kind {
        ScalarAggKind::Min => "MIN",
        ScalarAggKind::Max => "MAX",
    };
    let mut selected = group_idents
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    if !selected.is_empty() {
        selected.push_str(", ");
    }
    let _ = write!(selected, "{function}({agg_ident}) AS c0, COUNT(*) AS c1");
    let template = crate::compiler::sql_shape::parse_single_statement(
        &alloc::format!("SELECT {selected}"),
        dialect,
    )?;
    let Statement::Query(template_query) = template else {
        unreachable!("the projection template is a SELECT")
    };
    let SetExpr::Select(template_select) = *template_query.body else {
        unreachable!("the projection template is a plain SELECT")
    };
    let mut statement = statement.clone();
    let select = crate::compiler::sql_shape::select_mut(&mut statement).ok_or_else(|| {
        RegisterError::UnsupportedSql("a grouped extreme seed needs a plain SELECT".to_string())
    })?;
    select.projection = template_select.projection;
    // The seed fetches every group. A hidden group's numbers are needed the
    // moment it crosses into the result.
    select.having = None;
    Ok(crate::reexec::BoundQuery::new(
        statement.to_string(),
        binds.to_vec(),
    ))
}

fn grouped_scalar_read_projection<B: Backend>(
    kind: ScalarAggKind,
    agg_ident: &Ident,
    dialect: &B::Dialect,
) -> Result<Vec<SelectItem>, RegisterError> {
    let function = match kind {
        ScalarAggKind::Min => "MIN",
        ScalarAggKind::Max => "MAX",
    };
    let template = crate::compiler::sql_shape::parse_single_statement(
        &alloc::format!("SELECT {function}({agg_ident}) AS {REEXEC_VALUE_ALIAS}, COUNT(*) AS c1"),
        dialect,
    )?;
    let Statement::Query(query) = template else {
        unreachable!("the projection template is a SELECT")
    };
    let SetExpr::Select(select) = *query.body else {
        unreachable!("the projection template is a plain SELECT")
    };
    Ok(select.projection)
}

/// Render the extreme and source-row count for one group.
pub fn render_grouped_scalar_read<B: Backend>(
    plan: &GroupedMinMaxPlan<B>,
    group_values: &[Value<B>],
) -> Result<crate::reexec::BoundQuery<B>, RegisterError> {
    use sqlparser::ast::{BinaryOperator, Expr, GroupByExpr, Value as SqlValue};

    if group_values.len() != plan.group_idents.len() {
        return Err(RegisterError::UnsupportedSql(
            "a grouped read received the wrong number of group values".to_string(),
        ));
    }
    let registration_binds = plan.source_query.binds();
    let non_null_group_values = group_values.iter().filter(|value| !value.is_null()).count();
    let mut binds = Vec::with_capacity(registration_binds.len() + non_null_group_values);
    match plan.bind_placeholder {
        crate::compiler::BindPlaceholder::Numbered => {
            binds.extend(registration_binds.iter().cloned());
        }
        crate::compiler::BindPlaceholder::Positional => {
            let (leading, trailing) = registration_binds.split_at(plan.positional_scope_bind_index);
            binds.extend(leading.iter().cloned());
            binds.extend(
                group_values
                    .iter()
                    .filter(|value| !value.is_null())
                    .cloned(),
            );
            binds.extend(trailing.iter().cloned());
        }
    }
    let mut scoped = None;
    for (ident, value) in plan.group_idents.iter().zip(group_values) {
        let predicate = if value.is_null() {
            Expr::IsNull(alloc::boxed::Box::new(Expr::Identifier(ident.clone())))
        } else {
            let placeholder = match plan.bind_placeholder {
                crate::compiler::BindPlaceholder::Numbered => {
                    let placeholder = alloc::format!("${}", binds.len() + 1);
                    binds.push(value.clone());
                    placeholder
                }
                crate::compiler::BindPlaceholder::Positional => "?".to_string(),
            };
            Expr::BinaryOp {
                left: alloc::boxed::Box::new(Expr::Identifier(ident.clone())),
                op: BinaryOperator::Eq,
                right: alloc::boxed::Box::new(Expr::Value(
                    SqlValue::Placeholder(placeholder).with_empty_span(),
                )),
            }
        };
        scoped = Some(match scoped {
            Some(left) => Expr::BinaryOp {
                left: alloc::boxed::Box::new(left),
                op: BinaryOperator::And,
                right: alloc::boxed::Box::new(predicate),
            },
            None => predicate,
        });
    }
    let mut statement = plan.statement.clone();
    let Some(select) = crate::compiler::sql_shape::select_mut(&mut statement) else {
        unreachable!("a grouped extreme plan holds a plain SELECT")
    };
    select.projection.clone_from(&plan.read_projection);
    select.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
    // The scoped read replaces the whole group, so HAVING must not filter it.
    select.having = None;
    let scoped = scoped.expect("a grouped plan has at least one group column");
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: alloc::boxed::Box::new(Expr::Nested(alloc::boxed::Box::new(existing))),
            op: BinaryOperator::And,
            right: alloc::boxed::Box::new(scoped),
        },
        None => scoped,
    });
    Ok(crate::reexec::BoundQuery::new(statement.to_string(), binds))
}

fn scalar_plan<B, DB>(
    query: &crate::reexec::BoundQuery<B>,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let parsed =
        parser::parse_table_and_where_deps::<B, DB>(query.sql(), dialect, database, query.binds())?;
    // The shared parse admits `HAVING` because the grouped plan serves it.
    // A scalar read takes one value and cannot honour the clause, so a
    // statement carrying one falls through to the whole re-read, which
    // re-runs it verbatim.
    if crate::compiler::sql_shape::select_of(&parsed.statement)
        .is_some_and(|select| select.having.is_some())
    {
        return Err(RegisterError::UnsupportedSql(
            "a scalar extreme cannot honour HAVING".to_string(),
        ));
    }

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

    let read_sql = render_aliased_scalar(&parsed.statement).ok_or_else(|| {
        RegisterError::UnsupportedSql("unable to render re-execution query".to_string())
    })?;

    Ok(QueryPlan::Partial(MinMaxPlan {
        table_id: parsed.table_id,
        kind,
        agg_column,
        dependency_columns,
        agg_kind,
        where_program: Arc::new(parsed.where_program),
        read_query: crate::reexec::BoundQuery::new(read_sql, query.binds().to_vec()),
    }))
}

/// Re-render `stmt` with its single projection aliased as
/// [`REEXEC_VALUE_ALIAS`], producing the re-execution query. AST mutation and
/// rendering are `sqlparser`'s job. Subql only sets the alias. Returns `None`
/// if the statement is not the single-projection SELECT shape
/// [`extract_scalar_aggregate`] already validated.
fn render_aliased_scalar(stmt: &Statement) -> Option<String> {
    let mut stmt = stmt.clone();
    let select = crate::compiler::sql_shape::select_mut(&mut stmt)?;
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

/// Plan for a query maintained by asking the database only about the rows that
/// changed.
///
/// The cheap tier. A change to one row cannot move any other row of the answer
/// when the answer is a filter over one table, so the maintenance is that
/// filter applied to the changed keys, which is the standard incremental view
/// maintenance rule rather than an optimisation invented here. The filter
/// itself is whatever the caller wrote, evaluated by the database, which is why
/// this serves filters the in-process language cannot compile.
#[derive(Clone)]
pub struct KeyedPlan {
    /// Table the query reads.
    pub table: TableId,
    /// Where each key column lands in the projection, so the resolver reads a
    /// delivered row's key by position without assuming the projection is the
    /// table's own column order.
    pub key_positions: Vec<usize>,
    /// The key columns as identifiers to render, quoted the way the dialect
    /// wants them.
    ///
    /// Built here rather than in the resolver because an unquoted identifier is
    /// not always the same column: Postgres folds `OrderId` to `orderid`, so a
    /// column created as `"OrderId"` is not found, and the subscription
    /// registers cleanly and then fails on every change.
    pub key_idents: Vec<Ident>,
    /// The statement as parsed, so a scoped read can be built by adding a
    /// predicate to its WHERE rather than by rebuilding the query from parts.
    pub statement: Statement,
    /// Every column of the table: a filter the engine could not compile may
    /// read any of them, so narrowing this would mean guessing which UPDATEs
    /// matter.
    pub dependency_columns: Vec<ColumnId>,
}

/// Plan a filter over one table whose changed rows can be asked about one by
/// one.
///
/// Qualifies when the statement is a clause-free single-table `SELECT` whose
/// projection carries the table's whole primary key. Clause-free matters
/// because a `DISTINCT`, a row bound or a grouping makes one row's membership
/// depend on other rows, and then asking about the changed row alone answers
/// the wrong question. The projection has to carry the key because that key is
/// how a delivered row is identified and how a removal is expressed.
///
/// The filter is deliberately unconstrained: a filter the in-process language
/// cannot compile is the reason the query is here, and the database evaluates
/// it either way.
fn keyed_plan<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<QueryPlan<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let statement =
        crate::compiler::sql_shape::parse_single_statement(sql, dialect as &dyn Dialect)?;
    // Succeeds only for a clause-free single-table SELECT, which is exactly the
    // shape whose rows can be asked about individually.
    let (table_name, _) =
        crate::compiler::sql_shape::extract_ungrouped_table_and_where(&statement)?;
    let name = crate::compiler::parser::SqlTableName::from_object_name(&table_name)?;
    let table = crate::table_resolution::resolve_table_reference(
        name.qualified.as_deref(),
        &name.unqualified,
        database,
    )
    .map_err(|_| RegisterError::UnknownTable(name.unqualified.clone()))?;

    let key_columns = crate::catalog_helpers::primary_key_columns(database, table)?;
    if key_columns.is_empty() {
        return Err(RegisterError::UnsupportedSql(
            "a keyed read needs a primary key to identify a delivered row by".to_string(),
        ));
    }
    let Some(key_positions) = key_projection_positions(&statement, table, &key_columns, database)
    else {
        return Err(RegisterError::UnsupportedSql(
            "a keyed read needs the projection to carry the primary key, since that key is \
             both how a delivered row is identified and how its removal is expressed"
                .to_string(),
        ));
    };
    // A key value is rendered back into the scoped read as a SQL literal, so a
    // key whose type has no literal spelling would register cleanly and then
    // fail on every single change. Refused here so it falls to a tier that
    // needs no key.
    for column in &key_columns {
        let kind = crate::catalog_helpers::column_builtin_kind(database, table, *column);
        if !key_kind_has_literal_spelling(kind) {
            return Err(RegisterError::UnsupportedSql(alloc::format!(
                "a keyed read renders its key as a SQL literal, and {kind:?} has no literal \
                 spelling, so this query cannot be scoped to its changed rows"
            )));
        }
    }
    // One table in the whole statement, not just in FROM. A subquery reading
    // another table makes membership depend on rows this tier never watches, so
    // a change over there would never be delivered at all.
    if statement_reads_more_than(&statement, &table_name) {
        return Err(RegisterError::UnsupportedSql(
            "a keyed read must depend on one table only: membership that also depends on \
             another table cannot be maintained by asking about this table's changed rows"
                .to_string(),
        ));
    }

    let mut key_idents = Vec::with_capacity(key_columns.len());
    for column in &key_columns {
        let name = crate::catalog_helpers::column_name(database, table, *column)
            .ok_or_else(|| RegisterError::UnknownTable(name.unqualified.clone()))?;
        key_idents.push(crate::compiler::quoted_ident(
            dialect as &dyn Dialect,
            &name,
        ));
    }

    let arity = crate::catalog_helpers::table_arity(database, table)?;
    let dependency_columns = (0..arity)
        .filter_map(|o| ColumnId::try_from(o).ok())
        .collect();

    Ok(QueryPlan::Keyed(alloc::boxed::Box::new(KeyedPlan {
        table,
        key_positions,
        key_idents,
        statement,
        dependency_columns,
    })))
}

/// Can a key of this kind be written back into SQL as a literal?
///
/// Mirrors [`crate::compiler::parser::value_to_sql_value`], which is what
/// actually renders it. Exhaustive rather than a wildcard, so a new kind has to
/// be classified here instead of silently joining whichever side is the
/// default.
const fn key_kind_has_literal_spelling(kind: Option<crate::backend::BuiltinKind>) -> bool {
    match kind {
        Some(BuiltinKind::Int | BuiltinKind::String | BuiltinKind::Bytes) => true,
        // Float is refused despite being spellable, because it cannot identify
        // a row: Infinity and negative infinity have no literal spelling, and
        // `{f:?}` renders them as bare tokens no backend parses as numerics,
        // so a scoped read on such a key would be broken SQL. (NaN equality is
        // no argument: Postgres documents `'NaN'::float8 = 'NaN'::float8` as
        // TRUE.) Bool has no uniform spelling across backends, and the rest
        // render through typed constructors rather than literals. An unknown
        // column type is not a licence to guess, so `None` joins them.
        Some(
            BuiltinKind::Float
            | BuiltinKind::Bool
            | BuiltinKind::Uuid
            | BuiltinKind::Timestamp
            | BuiltinKind::TimestampTz
            | BuiltinKind::Date
            | BuiltinKind::Time
            | BuiltinKind::Decimal
            | BuiltinKind::Json
            | BuiltinKind::Jsonb,
        )
        | None => false,
    }
}

/// Does the statement read any table other than `table_name`?
///
/// Walks every relation the statement names, including ones inside subqueries,
/// which is the point: `FROM` alone would miss `WHERE id IN (SELECT ...)`, and
/// a change to that other table can move a row of this answer in or out.
fn statement_reads_more_than(
    statement: &Statement,
    table_name: &sqlparser::ast::ObjectName,
) -> bool {
    let mut other = false;
    let _ = sqlparser::ast::visit_relations(statement, |name| {
        if name != table_name {
            other = true;
        }
        core::ops::ControlFlow::<()>::Continue(())
    });
    other
}

/// Where in the projection each primary key column lands.
///
/// `None` when the projection does not carry the whole key. The positions
/// matter as much as their presence: the resolver reads keys out of returned
/// rows by index, so a key column's table ordinal is the wrong answer for any
/// projection that is not a wildcard in table order.
fn key_projection_positions<DB: DatabaseLike>(
    statement: &Statement,
    table: TableId,
    key_columns: &[ColumnId],
    database: &DB,
) -> Option<Vec<usize>> {
    let select = crate::compiler::sql_shape::select_of(statement)?;
    // A wildcard returns the table's columns in the table's own order, so a key
    // column's ordinal is its position.
    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return (select.projection.len() == 1).then(|| {
            key_columns
                .iter()
                .map(|column| *column as usize)
                .collect::<Vec<_>>()
        });
    }
    let mut named: Vec<Option<String>> = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        named.push(match item {
            SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(ident)) => {
                Some(ident.value.clone())
            }
            SelectItem::UnnamedExpr(sqlparser::ast::Expr::CompoundIdentifier(parts)) => {
                parts.last().map(|last| last.value.clone())
            }
            // An expression, an alias over one, or a qualified wildcard: the
            // delivered column is not the table's key column even if it
            // computes the same value.
            _ => None,
        });
    }
    key_columns
        .iter()
        .map(|column| {
            let name = crate::catalog_helpers::column_name(database, table, *column)?;
            named.iter().position(|got| {
                got.as_ref()
                    .is_some_and(|got| got.eq_ignore_ascii_case(&name))
            })
        })
        .collect()
}

/// Render the caller's own query restricted to the rows whose keys changed.
///
/// `SELECT ... FROM t WHERE (<the caller's filter>) AND (k1, k2) IN ((..), (..))`,
/// built as AST and rendered by sqlparser so the quoting is the parser's own
/// rather than hand-rolled. A key value with no SQL literal spelling refuses,
/// because guessing one is how an injection or a silently wrong comparison gets
/// in.
///
/// Returns `None` when `keys` is empty: there is nothing to ask about.
pub fn render_scoped_read<B: Backend>(
    plan: &KeyedPlan,
    keys: &[Vec<Value<B>>],
) -> Result<Option<String>, RegisterError> {
    use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr};
    let key_names = &plan.key_idents;

    if keys.is_empty() {
        return Ok(None);
    }

    let mut statement = plan.statement.clone();
    let Statement::Query(query) = &mut statement else {
        return Err(RegisterError::UnsupportedSql(
            "a keyed read needs a SELECT to restrict".to_string(),
        ));
    };
    let Query { body, .. } = &mut **query;
    let SetExpr::Select(select) = &mut **body else {
        return Err(RegisterError::UnsupportedSql(
            "a keyed read needs a plain SELECT to restrict".to_string(),
        ));
    };

    // One key column reads as `k IN (a, b)`; several as a row comparison, which
    // is what a composite key needs and what sqlparser spells as a tuple. The
    // identifiers come from the plan already quoted the way the dialect wants,
    // because an unquoted name is not always the same column.
    let key_expr = if key_names.len() == 1 {
        Expr::Identifier(key_names[0].clone())
    } else {
        Expr::Tuple(
            key_names
                .iter()
                .map(|ident| Expr::Identifier(ident.clone()))
                .collect(),
        )
    };

    let mut list = Vec::with_capacity(keys.len());
    for key in keys {
        if key.len() != key_names.len() {
            return Err(RegisterError::UnsupportedSql(alloc::format!(
                "a changed row carried {} key value(s) for a {}-column key",
                key.len(),
                key_names.len()
            )));
        }
        let mut parts = Vec::with_capacity(key.len());
        for value in key {
            parts.push(Expr::Value(
                crate::compiler::parser::value_to_sql_value(value)?.into(),
            ));
        }
        // A single key column compares directly; several compare as a tuple,
        // which is what a compound key needs.
        list.push(match <[Expr; 1]>::try_from(parts) {
            Ok([only]) => only,
            Err(parts) => Expr::Tuple(parts),
        });
    }

    let scoped = Expr::InList {
        expr: alloc::boxed::Box::new(key_expr),
        list,
        negated: false,
    };
    // The caller's filter is kept whole and parenthesised: `AND`-ing into an
    // `OR` without brackets would widen the answer rather than narrow it.
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: alloc::boxed::Box::new(Expr::Nested(alloc::boxed::Box::new(existing))),
            op: BinaryOperator::And,
            right: alloc::boxed::Box::new(scoped),
        },
        None => scoped,
    });

    Ok(Some(alloc::format!("{statement}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod scoped_read_tests {
    use super::{render_scoped_read, KeyedPlan};
    use crate::backend::{Postgres, Value};

    /// A plan over `sql` whose key columns are `columns`, rendered unquoted.
    fn plan_keyed(sql: &str, columns: &[&str]) -> KeyedPlan {
        let statement = crate::compiler::sql_shape::parse_single_statement(
            sql,
            &sqlparser::dialect::PostgreSqlDialect {} as &dyn sqlparser::dialect::Dialect,
        )
        .unwrap();
        KeyedPlan {
            table: 0,
            key_positions: (0..columns.len()).collect(),
            key_idents: columns
                .iter()
                .map(|c| sqlparser::ast::Ident::new(*c))
                .collect(),
            statement,
            dependency_columns: alloc::vec![0],
        }
    }

    /// The caller's filter survives whole and the key restriction is added, so
    /// the read answers "which of these rows belong" rather than a new question.
    #[test]
    fn a_scoped_read_keeps_the_callers_filter_and_adds_the_keys() {
        let plan = plan_keyed("SELECT * FROM t WHERE lower(name) = 'x'", &["id"]);
        let sql = render_scoped_read::<Postgres>(
            &plan,
            &alloc::vec![alloc::vec![Value::Int(7)], alloc::vec![Value::Int(9)]],
        )
        .unwrap()
        .unwrap();
        assert!(sql.contains("lower(name) = 'x'"), "filter kept: {sql}");
        assert!(sql.contains("id IN (7, 9)"), "keys added: {sql}");
    }

    /// An `OR` filter is bracketed before the key restriction is `AND`-ed on.
    /// Without the brackets `a OR b AND keys` binds as `a OR (b AND keys)`,
    /// which widens the answer to every row satisfying `a` instead of
    /// narrowing it to the changed ones.
    #[test]
    fn an_or_filter_is_bracketed_so_the_keys_narrow_rather_than_widen() {
        let plan = plan_keyed("SELECT * FROM t WHERE a = 1 OR b = 2", &["id"]);
        let sql = render_scoped_read::<Postgres>(&plan, &alloc::vec![alloc::vec![Value::Int(1)]])
            .unwrap()
            .unwrap();
        assert!(
            sql.contains("(a = 1 OR b = 2) AND id IN (1)"),
            "the filter must be bracketed: {sql}"
        );
    }

    /// A filterless query still gets restricted, and gets no stray `AND`.
    #[test]
    fn a_query_with_no_filter_is_restricted_by_the_keys_alone() {
        let plan = plan_keyed("SELECT * FROM t", &["id"]);
        let sql = render_scoped_read::<Postgres>(&plan, &alloc::vec![alloc::vec![Value::Int(3)]])
            .unwrap()
            .unwrap();
        assert!(sql.ends_with("WHERE id IN (3)"), "{sql}");
    }

    /// A composite key reads as a tuple comparison, which is what lets one
    /// read cover several changed rows of a compound-keyed table.
    #[test]
    fn a_composite_key_reads_as_a_tuple() {
        let plan = plan_keyed("SELECT * FROM t", &["country", "code"]);
        let sql = render_scoped_read::<Postgres>(
            &plan,
            &alloc::vec![
                alloc::vec![Value::String("it".into()), Value::Int(1)],
                alloc::vec![Value::String("fr".into()), Value::Int(2)],
            ],
        )
        .unwrap()
        .unwrap();
        assert!(
            sql.contains("(country, code) IN (('it', 1), ('fr', 2))"),
            "{sql}"
        );
    }

    /// Quoting is sqlparser's, not subql's. A key carrying a quote must come
    /// back escaped rather than closing the literal, which is the difference
    /// between a filter and an injection.
    #[test]
    fn a_key_containing_a_quote_is_escaped_by_the_parser() {
        let plan = plan_keyed("SELECT * FROM t", &["name"]);
        let sql = render_scoped_read::<Postgres>(
            &plan,
            &alloc::vec![alloc::vec![Value::String(
                "o'brien'; DROP TABLE t --".into()
            )]],
        )
        .unwrap()
        .unwrap();
        assert!(
            sql.contains("'o''brien''; DROP TABLE t --'"),
            "the quote must be doubled, got {sql}"
        );
        // And the statement still parses as one statement, so nothing escaped
        // the literal.
        let reparsed = crate::compiler::sql_shape::parse_single_statement(
            &sql,
            &sqlparser::dialect::PostgreSqlDialect {} as &dyn sqlparser::dialect::Dialect,
        );
        assert!(reparsed.is_ok(), "round trip: {sql}");
    }

    /// No changed keys means no question to ask, which is not the same as a
    /// query that returns nothing.
    #[test]
    fn no_keys_renders_no_read() {
        let plan = plan_keyed("SELECT * FROM t", &["id"]);
        assert!(render_scoped_read::<Postgres>(&plan, &[])
            .unwrap()
            .is_none());
    }

    /// A key value with no SQL literal spelling refuses rather than guessing
    /// one, following the same rule the bind path already applies.
    #[test]
    fn a_key_with_no_literal_spelling_is_refused() {
        let plan = plan_keyed("SELECT * FROM t", &["id"]);
        assert!(
            render_scoped_read::<Postgres>(&plan, &alloc::vec![alloc::vec![Value::Missing]],)
                .is_err()
        );
    }

    /// A row whose key arity disagrees with the table's is refused, because
    /// building a tuple comparison from it would compare the wrong columns.
    #[test]
    fn a_key_of_the_wrong_arity_is_refused() {
        let plan = plan_keyed("SELECT * FROM t", &["country", "code"]);
        assert!(
            render_scoped_read::<Postgres>(&plan, &alloc::vec![alloc::vec![Value::Int(1)]],)
                .is_err()
        );
    }
}
