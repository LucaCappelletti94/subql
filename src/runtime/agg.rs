//! Streaming aggregate utilities for `COUNT(*)`, `COUNT(col)`, `SUM(col)`,
//! `AVG(col)`, `VAR_POP(col)`, `VAR_SAMP(col)`, `STDDEV_POP(col)`, and
//! `STDDEV_SAMP(col)`.

use super::aggregate::AggDelta;
use crate::{compiler::AggSpec, ColumnId};

/// Three-valued numeric read for aggregate delta computation.
///
/// Composed by the caller (dispatch) against the current `E: CdcEvent`
/// and the target column's [`crate::backend::ScalarKind`]. Reflects both
/// the presence tri-state and the numeric convertibility of the cell.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum AggCellRead {
    /// The source did not carry this cell (row image partial).
    Missing,
    /// The cell carries SQL `NULL`.
    Null,
    /// The cell is present and its value convertible to `f64`
    /// (finite; `NaN` and `Inf` are represented as `NonNumeric`).
    Numeric(f64),
    /// The cell is present but does not participate in numeric aggregates
    /// (Bool, String, non-finite Float, etc.).
    NonNumeric,
}

impl AggCellRead {
    /// Whether the cell has any observable presence (Null or a value).
    pub const fn is_present(self) -> bool {
        !matches!(self, Self::Missing | Self::Null)
    }

    /// The finite `f64` value if the cell is `Numeric`, else `None`.
    pub const fn numeric(self) -> Option<f64> {
        match self {
            Self::Numeric(v) => Some(v),
            _ => None,
        }
    }
}

/// Which delta shape dispatch asks for.
///
/// A sibling `HAVING` reads components the projected function alone does
/// not maintain, so its registrations fold the complete set. Spelled as its
/// own type rather than borrowing a variance spec, so the intent survives
/// the call.
#[derive(Copy, Clone, Debug)]
pub enum DeltaSpec<'a> {
    /// The projected function's own components.
    Projected(&'a AggSpec),
    /// Sum, sum of squares and contribution count over the column,
    /// whatever the projected function.
    FullStats(ColumnId),
}

/// Compute the per-row aggregate delta for a matched row image.
///
/// Returns `None` when the row contributes no delta under SQL semantics
/// (for example `COUNT(col)` with `NULL`, `SUM`/`AVG` with non-numeric values).
#[allow(clippy::cast_precision_loss)]
pub fn agg_delta_for_row<F>(spec: DeltaSpec<'_>, weight: i64, mut read: F) -> Option<AggDelta>
where
    F: FnMut(ColumnId) -> AggCellRead,
{
    let spec = match spec {
        DeltaSpec::Projected(spec) => spec,
        DeltaSpec::FullStats(column) => {
            let value = read(column).numeric()?;
            let w = weight as f64;
            return Some(AggDelta::Stats {
                sum_delta: value * w,
                sum_sq_delta: value * value * w,
                count_delta: weight,
            });
        }
    };
    match spec {
        AggSpec::CountStar => Some(AggDelta::Count(weight)),
        AggSpec::CountColumn { column } => {
            if read(*column).is_present() {
                Some(AggDelta::Count(weight))
            } else {
                None
            }
        }
        AggSpec::Sum { column } => {
            // No early-out on a zero delta: a row worth zero moves the
            // answer from NULL to 0, or back, without moving the total.
            let value = read(*column).numeric()?;
            Some(AggDelta::Totalled {
                sum_delta: value * weight as f64,
                count_delta: weight,
            })
        }
        AggSpec::Avg { column } => {
            let value = read(*column).numeric()?;
            Some(AggDelta::Totalled {
                sum_delta: value * weight as f64,
                count_delta: weight,
            })
        }
        AggSpec::VarPop { column }
        | AggSpec::VarSamp { column }
        | AggSpec::StddevPop { column }
        | AggSpec::StddevSamp { column } => {
            let value = read(*column).numeric()?;
            let w = weight as f64;
            Some(AggDelta::Stats {
                sum_delta: value * w,
                sum_sq_delta: value * value * w,
                count_delta: weight,
            })
        }
    }
}

// The kernels this module used to export were an unused second spelling of
// `agg_delta_for_row`, and their only reason to be public was handing a caller
// an `AggDelta`. The engine holds the running value now, so nothing outside
// this crate ever sees one.
