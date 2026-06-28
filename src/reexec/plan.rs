//! Layer 1: classification.
//!
//! Turns a SQL string the core engine rejected into a re-execution plan. The
//! engine itself decides the "native" case (it accepts the query). This layer
//! only runs for rejected queries and produces a [`QueryPlan`] describing how
//! the re-execution layer should maintain it.

use crate::compiler::parser;
use crate::compiler::sql_shape::{extract_scalar_aggregate, ScalarAggKind};
use crate::compiler::BytecodeProgram;
use crate::{ColumnId, ColumnType, RegisterError, TableId};
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
pub(super) enum QueryPlan {
    /// A single-table scalar `MIN`/`MAX`, maintained incrementally with a
    /// database re-query only on extreme removal.
    Partial(MinMaxPlan),
}

/// Plan for an incrementally-maintained single-table scalar `MIN`/`MAX`.
pub(super) struct MinMaxPlan {
    /// Table the aggregate reads from.
    pub table_id: TableId,
    /// `MIN` or `MAX`.
    pub kind: ScalarAggKind,
    /// The aggregated column.
    pub agg_column: ColumnId,
    /// Type of the aggregated column (`Unknown` when the catalog is silent).
    /// Returned to the materializer so it can decode the re-executed scalar.
    pub column_type: ColumnType,
    /// Columns whose change can alter the result: the aggregated column plus
    /// every column the WHERE clause reads (UPDATE routing optimization).
    pub dependency_columns: Vec<ColumnId>,
    /// The compiled WHERE clause, so maintenance can test row membership
    /// in-process via the VM (always-true when the query has no WHERE).
    pub where_program: Arc<BytecodeProgram>,
    /// SQL the Subscription Materializer runs after a
    /// [`ReExecutionTrigger`](super::ReExecutionTrigger), with its projection
    /// aliased. Returned to the materializer at registration via
    /// [`Registered::ReExec`](super::Registered::ReExec). Subql itself never
    /// executes it.
    pub reexec_sql: String,
}

/// Classify a rejected query into a [`QueryPlan`].
///
/// Returns `Ok(QueryPlan::Partial(_))` for a single-table scalar `MIN`/`MAX`.
/// Returns `Err(UnsupportedSql)` for anything else (the caller surfaces the
/// engine's original rejection message instead of this one). This is where the
/// future `Total` plan will be built for JOIN/HAVING/multi-table queries.
pub(super) fn build_plan<D: Dialect, DB: DatabaseLike>(
    sql: &str,
    dialect: &D,
    database: &DB,
) -> Result<QueryPlan, RegisterError> {
    let parsed = parser::parse_table_and_where_deps(sql, dialect, database)?;

    let Some((kind, agg_column)) =
        extract_scalar_aggregate(&parsed.statement, parsed.table_id, database)?
    else {
        return Err(RegisterError::UnsupportedSql(
            "query is not a single-table scalar MIN/MAX; cannot re-execute".to_string(),
        ));
    };

    let column_type = crate::catalog_helpers::column_type(database, parsed.table_id, agg_column)
        .unwrap_or(ColumnType::Unknown);

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
        column_type,
        dependency_columns,
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::catalog_helpers;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    fn plan(sql: &str) -> Result<QueryPlan, RegisterError> {
        build_plan(sql, &PostgreSqlDialect {}, &catalog())
    }

    fn col(name: &str) -> ColumnId {
        let cat = catalog();
        catalog_helpers::column_id(
            &cat,
            catalog_helpers::table_id(&cat, "orders").unwrap(),
            name,
        )
        .unwrap()
    }

    #[test]
    fn min_builds_partial_plan() {
        let QueryPlan::Partial(p) = plan("SELECT MIN(price) FROM orders").unwrap();
        assert_eq!(p.kind, ScalarAggKind::Min);
        assert_eq!(p.agg_column, col("price"));
        assert_eq!(p.column_type, ColumnType::Float);
        assert_eq!(p.dependency_columns, vec![col("price")]);
        assert!(p.reexec_sql.contains("AS v") || p.reexec_sql.contains("AS \"v\""));
    }

    #[test]
    fn max_with_where_includes_where_deps_and_program() {
        let QueryPlan::Partial(p) =
            plan("SELECT MAX(quantity) FROM orders WHERE status = 'paid'").unwrap();
        assert_eq!(p.kind, ScalarAggKind::Max);
        assert!(p.dependency_columns.contains(&col("quantity")));
        assert!(p.dependency_columns.contains(&col("status")));
        // The WHERE program depends on `status`.
        assert!(p.where_program.dependency_columns.contains(&col("status")));
    }

    #[test]
    fn no_where_has_always_true_program() {
        let QueryPlan::Partial(p) = plan("SELECT MIN(price) FROM orders").unwrap();
        assert!(p.where_program.dependency_columns.is_empty());
    }

    #[test]
    fn non_minmax_is_err() {
        // SUM is engine-handled, not a reexec plan.
        assert!(plan("SELECT SUM(price) FROM orders").is_err());
        // SELECT * is engine-handled.
        assert!(plan("SELECT * FROM orders").is_err());
    }

    #[test]
    fn join_is_err() {
        assert!(plan("SELECT MIN(orders.price) FROM orders JOIN u ON orders.id = u.id").is_err());
    }
}
