//! Column-reference helpers used by the parser and prefilter.

use crate::{catalog_helpers, ColumnId, TableId};
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::Expr;

/// Resolve simple column references used by parser / prefilter.
///
/// Supports `col` and `table.col` (table qualifier ignored after
/// SQL-shape validation).
///
/// The identifier's quoting decides which column it names, so it is read
/// off the node rather than from the text: a written `"Owner"` names the
/// column stored under that spelling, and an unquoted `owner` names the
/// folded one, which are two columns whenever a table declares both.
#[must_use]
pub fn resolve_column_ref<DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ColumnId> {
    let ident = match expr {
        Expr::Identifier(ident) => ident,
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => &parts[1],
        _ => return None,
    };
    catalog_helpers::column_id_for_name(
        database,
        table_id,
        &ident.value,
        ident.quote_style.is_some(),
    )
}
