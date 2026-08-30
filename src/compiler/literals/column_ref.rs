//! Column-reference helpers used by the parser and prefilter.

use crate::{catalog_helpers, ColumnId, TableId};
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::Expr;

/// Resolve simple column references used by parser / prefilter.
///
/// Supports `col` and `table.col` (table qualifier ignored after
/// SQL-shape validation).
#[must_use]
pub fn resolve_column_ref<DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ColumnId> {
    match expr {
        Expr::Identifier(ident) => catalog_helpers::column_id(database, table_id, &ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            catalog_helpers::column_id(database, table_id, &parts[1].value)
        }
        _ => None,
    }
}
