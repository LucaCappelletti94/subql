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
/// Not `Copy`: an exact decimal cell owns its digits.
#[derive(Clone, Debug, PartialEq)]
pub enum AggCellRead {
    /// The source did not carry this cell (row image partial).
    Missing,
    /// The cell carries SQL `NULL`.
    Null,
    /// An integer cell, exactly as the row carried it. No engine sums
    /// integers in `f64`, so the value is kept rather than widened.
    Integer(i64),
    /// An exact decimal cell.
    Decimal(bigdecimal::BigDecimal),
    /// A floating cell, `Infinity` and `NaN` included, because the
    /// engines answer with them: measured, PostgreSQL sums `1.0` and
    /// `Infinity` to `Infinity`.
    Real(f64),
    /// The cell is present but does not participate in numeric aggregates
    /// (Bool, String, non-finite Float, etc.).
    NonNumeric,
}

impl AggCellRead {
    /// Whether the cell has any observable presence (Null or a value).
    pub const fn is_present(&self) -> bool {
        !matches!(self, Self::Missing | Self::Null)
    }

    /// This cell's contribution to a total, exactly as it was carried, or
    /// `None` when the cell contributes nothing.
    pub fn contribution(&self, weight: i64) -> Option<crate::runtime::aggregate::TotalDelta> {
        use crate::runtime::aggregate::TotalDelta;

        #[allow(clippy::cast_precision_loss)]
        match self {
            Self::Integer(value) => {
                Some(TotalDelta::Integer(i128::from(*value) * i128::from(weight)))
            }
            Self::Decimal(value) => Some(TotalDelta::Decimal(
                value * bigdecimal::BigDecimal::from(weight),
            )),
            Self::Real(value) => Some(TotalDelta::Real(*value * weight as f64)),
            Self::Missing | Self::Null | Self::NonNumeric => None,
        }
    }

    /// The `f64` value if the cell is numeric, else `None`. Lossy above
    /// `2^53` for an integer or decimal cell, which is what Phase D2
    /// removes for `AVG` and the variance family.
    #[allow(clippy::cast_precision_loss)]
    pub fn numeric(&self) -> Option<f64> {
        match self {
            Self::Integer(value) => Some(*value as f64),
            Self::Decimal(value) => {
                <bigdecimal::BigDecimal as bigdecimal::ToPrimitive>::to_f64(value)
            }
            Self::Real(value) => Some(*value),
            Self::Missing | Self::Null | Self::NonNumeric => None,
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

/// This row read as a pair of one-row sets: the one joining and the one
/// leaving.
///
/// A variance is not a sum, so a row cannot be folded as a signed
/// contribution: taking a row back out has to know the set it is leaving.
/// A row is therefore summarised on the side it belongs to and the
/// accumulator combines or uncombines it.
fn spread_of(
    cell: &AggCellRead,
    weight: i64,
) -> Option<(
    crate::runtime::aggregate::Spread,
    crate::runtime::aggregate::Spread,
)> {
    use crate::runtime::aggregate::Spread;

    let one = Spread::of_one(cell.numeric()?);
    Some(match weight.signum() {
        1 => (one, Spread::EMPTY),
        -1 => (Spread::EMPTY, one),
        // A row that neither joins nor leaves moves no spread. Dispatch
        // weighs every matched row `+1` or `-1`, so this is the arm that
        // cannot happen rather than a case with an answer of its own.
        _ => (Spread::EMPTY, Spread::EMPTY),
    })
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
            let cell = read(column);
            let (added, removed) = spread_of(&cell, weight)?;
            return Some(AggDelta::Stats {
                value: cell.contribution(weight)?,
                added,
                removed,
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
            Some(AggDelta::Totalled {
                value: read(*column).contribution(weight)?,
                count_delta: weight,
            })
        }
        AggSpec::Avg { column } => Some(AggDelta::Totalled {
            value: read(*column).contribution(weight)?,
            count_delta: weight,
        }),
        AggSpec::VarPop { column }
        | AggSpec::VarSamp { column }
        | AggSpec::StddevPop { column }
        | AggSpec::StddevSamp { column } => {
            let cell = read(*column);
            let (added, removed) = spread_of(&cell, weight)?;
            Some(AggDelta::Stats {
                value: cell.contribution(weight)?,
                added,
                removed,
            })
        }
    }
}

// The kernels this module used to export were an unused second spelling of
// `agg_delta_for_row`, and their only reason to be public was handing a caller
// an `AggDelta`. The engine holds the running value now, so nothing outside
// this crate ever sees one.
