//! Column-reference helpers used by the parser and prefilter.

use crate::{backend::Backend, catalog_helpers, ColumnId, TableId};
use alloc::vec::Vec;
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
pub fn resolve_column_ref<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ColumnId> {
    let ident = match expr {
        Expr::Identifier(ident) => ident,
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => &parts[1],
        _ => return None,
    };
    catalog_helpers::column_id_for_name::<B, DB>(
        database,
        table_id,
        &ident.value,
        ident.quote_style.is_some(),
    )
}

/// The column a `COALESCE` over `expr` reads as, its first column argument,
/// or `expr` itself when it is not one.
///
/// Registration serves a `COALESCE` only when its column arguments share one
/// declared type and collation, so the first one's facts are the call's.
/// Purely syntactic: whether the call is served is the compiler's question.
#[must_use]
pub fn value_column(expr: &Expr) -> &Expr {
    coalesce_arguments(expr)
        .and_then(|arguments| {
            arguments.into_iter().find(|argument| {
                matches!(argument, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            })
        })
        .unwrap_or(expr)
}

/// The arguments of `expr` when it is a plain `COALESCE(...)` call, parentheses
/// aside, and `None` for anything else, including a call carrying a clause.
#[must_use]
pub fn coalesce_arguments(expr: &Expr) -> Option<Vec<&Expr>> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let mut bare = expr;
    while let Expr::Nested(inner) = bare {
        bare = inner;
    }
    let Expr::Function(function) = bare else {
        return None;
    };
    let [name] = function.name.0.as_slice() else {
        return None;
    };
    if !name
        .as_ident()
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("coalesce"))
        || function.filter.is_some()
        || function.over.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    list.args
        .iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(argument)) => Some(argument),
            _ => None,
        })
        .collect()
}
