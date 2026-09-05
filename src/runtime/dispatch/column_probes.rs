use crate::backend::{CdcEvent, RowKind, Value};
use crate::runtime::{agg::AggCellRead, indexes::IndexableCell, partition::ColumnProbe};
use sql_traits::prelude::DatabaseLike;

/// Probe column `col` at the `row` view of `event` for the equality /
/// range / null indexes tracked by [`TablePartition::select_candidates`].
///
/// Values whose scalar payload downcasts to one of the four indexable
/// primitives (`bool` / `i64` / `f64` / `String`) become
/// [`IndexableCell`] variants. Every other scalar returns
/// [`ColumnProbe::present`] with `value: None`, causing the caller to
/// consult only the fallback index for that column.
pub(super) fn probe_column_for_index<E: CdcEvent, DB: DatabaseLike>(
    event: &E,
    row: RowKind,
    col: crate::ColumnId,
    arity: usize,
    db: &DB,
) -> ColumnProbe {
    if col as usize >= arity {
        return ColumnProbe::missing();
    }
    match event.value_at(db, row, col) {
        Ok(Value::Missing) => ColumnProbe::missing(),
        Ok(Value::Null) => ColumnProbe::null(),
        Ok(v) => ColumnProbe::present(IndexableCell::from_value::<E::Backend>(&v)),
        Err(_) => ColumnProbe::undecodable(),
    }
}

/// Probe column `col` at the `row` view of `event` for aggregate delta
/// computation.
///
/// Downcasts the scalar payload to `f64` when the column carries a
/// numeric type (`Value::Int` or `Value::Float`). Every other scalar is
/// reported as [`AggCellRead::NonNumeric`] when present.
pub(super) fn probe_column_for_agg<E: CdcEvent, DB: DatabaseLike>(
    event: &E,
    row: RowKind,
    col: crate::ColumnId,
    arity: usize,
    db: &DB,
) -> Result<AggCellRead, crate::ValueError> {
    use core::any::Any;
    if usize::from(col) >= arity {
        return Ok(AggCellRead::Missing);
    }
    let value = event.value_at(db, row, col)?;
    Ok(match &value {
        Value::Missing => AggCellRead::Missing,
        Value::Null => AggCellRead::Null,
        Value::Int(i) => (i as &dyn Any)
            .downcast_ref::<i64>()
            .map_or(AggCellRead::NonNumeric, |int| AggCellRead::Integer(*int)),
        // `Infinity` and `NaN` fold like any other float, because the
        // engines answer with them: measured, PostgreSQL sums `1.0` and
        // `Infinity` to `Infinity` and answers `NaN` once a `NaN`
        // participates, and SQLite answers `Inf`. Both are representable
        // in the carrier the float family already holds, so dropping one
        // only hid a row that moved the answer.
        Value::Float(f) => (f as &dyn Any)
            .downcast_ref::<f64>()
            .map_or(AggCellRead::NonNumeric, |float| AggCellRead::Real(*float)),
        // A decimal cell folds exactly, which is what an engine that sums
        // one into a decimal answers.
        Value::Decimal(d) => (d as &dyn Any)
            .downcast_ref::<bigdecimal::BigDecimal>()
            .map_or(AggCellRead::NonNumeric, |decimal| {
                AggCellRead::Decimal(decimal.clone())
            }),
        _ => AggCellRead::NonNumeric,
    })
}
