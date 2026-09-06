use alloc::vec::Vec;
use core::cell::Cell;

use rls2fga_types::{
    records_from_row, ColumnKind, ColumnRead, Guard, Record, RecordDerivation, RecordDescription,
    ValueSource,
};
use rls2fga_types::{AttributeLiteral, AttributeOperator, AttributePredicate};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{ScalarFamily, ScalarKindOf};
use crate::catalog_helpers;
use crate::visibility::RowView;
use crate::TableId;

use super::adapter::RowValuesView;
use super::RowRecordError;

/// The records `description` says `row` implies.
///
/// # Errors
///
/// [`RowRecordError::Refused`] for a description rls2fga will not evaluate,
/// [`RowRecordError::UnsupportedValueSource`] for a shape whose columns a
/// row view cannot read, and [`RowRecordError::Undecodable`] for a cell
/// the row carried but could not decode. None is an empty record set, on
/// purpose.
pub fn records_from_row_view<R, DB>(
    description: &RecordDescription,
    row: &R,
    db: &DB,
) -> Result<Vec<Record>, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    if let Some(refusal) = unsupported_description::<R::Backend, DB>(description, db) {
        return Err(refusal);
    }
    let view = RowValuesView {
        row,
        db,
        undecodable: Cell::new(None),
    };
    let records = records_from_row(description, &view)?;
    if let Some(error) = view.undecodable.into_inner() {
        return Err(RowRecordError::Undecodable(error));
    }
    Ok(records)
}

/// Whether [`records_from_row_view`] can evaluate `description` at all.
///
/// Depends only on the description's shape and the catalog, never on a
/// row, so a caller holding the descriptions for a whole schema settles
/// this once at setup rather than rediscovering the refusal on every
/// changed row.
#[must_use]
pub fn is_evaluable<B: crate::backend::Backend, DB: DatabaseLike>(
    description: &RecordDescription,
    db: &DB,
) -> bool {
    unsupported_description::<B, DB>(description, db).is_none()
}

/// Why a row view cannot answer `description`, or [`None`].
fn unsupported_description<B: crate::backend::Backend, DB: DatabaseLike>(
    description: &RecordDescription,
    db: &DB,
) -> Option<RowRecordError> {
    let RecordDerivation::FromRow {
        table,
        template,
        guards,
        ..
    } = &description.derivation
    else {
        return None;
    };
    let Some(table) = catalog_helpers::contract_table_id(db, table) else {
        return Some(RowRecordError::UnreadableColumn(alloc::format!(
            "any column of {table:?}, a table the catalog does not know"
        )));
    };
    template
        .object_key
        .parts()
        .iter()
        .find_map(|part| unsupported_value::<B, DB>(part, db, table))
        .or_else(|| unsupported_value::<B, DB>(template.subject_key.part(), db, table))
        .or_else(|| {
            guards
                .iter()
                .find_map(|guard| unsupported_guard::<B, DB>(guard, db, table))
        })
        .or_else(|| {
            template.context.as_ref().and_then(|context| {
                context
                    .entries
                    .iter()
                    .find_map(|entry| unsupported_value::<B, DB>(&entry.value, db, table))
            })
        })
}

fn unsupported_value<B: crate::backend::Backend, DB: DatabaseLike>(
    source: &ValueSource,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match source {
        ValueSource::Column(column) => direct_column_read::<B, DB>(column, db, table),
        ValueSource::JsonPath { column, .. } => document_read::<B, DB>(column, db, table),
        ValueSource::Literal(_) => None,
        ValueSource::ListElements(_) => Some(RowRecordError::UnsupportedValueSource("list")),
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised")),
    }
}

fn unsupported_guard<B: crate::backend::Backend, DB: DatabaseLike>(
    guard: &Guard,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match guard {
        Guard::NotNull(column) => direct_column_read::<B, DB>(column, db, table),
        Guard::IsTrue(column) => bool_read::<B, DB>(column, db, table),
        Guard::Compare { column, predicate } => {
            comparison_read::<B, DB>(column, predicate, db, table)
        }
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised guard")),
    }
}

fn comparison_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    predicate: &AttributePredicate,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    match (&predicate.value, kind) {
        (AttributeLiteral::Boolean(_), ColumnKind::Bool)
        | (AttributeLiteral::Number(_), ColumnKind::Integer | ColumnKind::Decimal) => None,
        (AttributeLiteral::Text(_), ColumnKind::Text)
            if matches!(
                predicate.operator,
                AttributeOperator::Eq | AttributeOperator::NotEq
            ) =>
        {
            None
        }
        (AttributeLiteral::Text(_), ColumnKind::Text) => Some(RowRecordError::UnreadableColumn(
            alloc::format!("comparison over column {column} needs a query"),
        )),
        (
            AttributeLiteral::Boolean(_) | AttributeLiteral::Number(_) | AttributeLiteral::Text(_),
            _,
        ) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} cannot be compared as {:?}",
            predicate.value
        ))),
        _ => Some(RowRecordError::UnsupportedValueSource(
            "unrecognised comparison",
        )),
    }
}

fn direct_column_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if direct_kind(kind) {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} has unsupported kind {kind:?}"
        )))
    }
}

fn bool_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if kind == ColumnKind::Bool {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {kind:?}, not Bool"
        )))
    }
}

fn document_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if kind == ColumnKind::Json {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {kind:?}, not Json"
        )))
    }
}

pub(super) const fn direct_kind(kind: ColumnKind) -> bool {
    !matches!(kind, ColumnKind::Json | ColumnKind::Unsupported)
}

fn column_read_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Result<ColumnKind, RowRecordError> {
    let refuse = || {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column}, which the catalog does not know or cannot type"
        ))
    };
    let id = catalog_helpers::column_id(db, table, column.as_str()).ok_or_else(refuse)?;
    let scalar = catalog_helpers::column_scalar_kind::<B, DB>(db, table, id).ok_or_else(refuse)?;
    // A custom type answers `None`: it has no renderable column kind, so a
    // shape that reads this column is refused rather than served a spelling
    // subql cannot prove (R1).
    let actual = column_kind_from_scalar::<B>(scalar).ok_or_else(|| {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} holds a custom type, which has no spelling this side can prove"
        ))
    })?;
    if actual == column.kind() {
        Ok(actual)
    } else {
        Err(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {actual:?} in the catalog but the shape reads {:?}",
            column.kind()
        )))
    }
}

pub(super) fn column_kind_from_scalar<B: crate::backend::Backend>(
    kind: ScalarKindOf<B>,
) -> Option<ColumnKind> {
    // A custom type has no column kind here on purpose: rendering it would
    // mean asserting a text form subql cannot prove matches the loading SQL
    // (R1), so a shape that renders such a column is reported uncovered.
    Some(match kind.family()? {
        ScalarFamily::Bool => ColumnKind::Bool,
        ScalarFamily::Int => ColumnKind::Integer,
        ScalarFamily::Float => ColumnKind::Unsupported,
        ScalarFamily::String => ColumnKind::Text,
        ScalarFamily::Bytes => ColumnKind::Bytea,
        ScalarFamily::Uuid => ColumnKind::Uuid,
        ScalarFamily::Timestamp => ColumnKind::Timestamp,
        ScalarFamily::TimestampTz => ColumnKind::TimestampTz,
        ScalarFamily::Date => ColumnKind::Date,
        ScalarFamily::Time => ColumnKind::Time,
        ScalarFamily::Decimal => ColumnKind::Decimal,
        ScalarFamily::Json | ScalarFamily::Jsonb => ColumnKind::Json,
    })
}
