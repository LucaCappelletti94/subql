//! Streaming aggregate utilities for `COUNT(*)`, `COUNT(col)`, `SUM(col)`,
//! `AVG(col)`, `VAR_POP(col)`, `VAR_SAMP(col)`, `STDDEV_POP(col)`, and
//! `STDDEV_SAMP(col)`.

use crate::{compiler::AggSpec, AggDelta, ColumnId};

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
    #[must_use]
    pub const fn is_present(self) -> bool {
        !matches!(self, Self::Missing | Self::Null)
    }

    /// The finite `f64` value if the cell is `Numeric`, else `None`.
    #[must_use]
    pub const fn numeric(self) -> Option<f64> {
        match self {
            Self::Numeric(v) => Some(v),
            _ => None,
        }
    }
}

/// Compute the per-row aggregate delta for a matched row image.
///
/// Returns `None` when the row contributes no delta under SQL semantics
/// (for example `COUNT(col)` with `NULL`, `SUM`/`AVG` with non-numeric values).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn agg_delta_for_row<F>(spec: &AggSpec, weight: i64, mut read: F) -> Option<AggDelta>
where
    F: FnMut(ColumnId) -> AggCellRead,
{
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
            let value = read(*column).numeric()?;
            let delta = value * weight as f64;
            if delta == 0.0 {
                None
            } else {
                Some(AggDelta::Sum(delta))
            }
        }
        AggSpec::Avg { column } => {
            let value = read(*column).numeric()?;
            Some(AggDelta::Avg {
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

/// Trait for streaming aggregate kernels.
///
/// Kernels accumulate signed-weight deltas produced by the dispatch pipeline.
/// One kernel instance is created per evaluation pass. The result is returned
/// to the caller after all deltas have been applied.
///
/// # Caller contract
/// The engine handles only WAL-driven deltas. Callers must:
/// 1. **Bootstrap**: query the DB for the initial aggregate before subscribing.
/// 2. **Accumulate**: `running_value += delta` on each `aggregate_deltas` call.
/// 3. **Require old UPDATE images**: aggregate UPDATE deltas need both old and
///    new row images. CDC sources that omit `before`/`old` rows cannot produce
///    correct UPDATE deltas.
/// 4. **Reset on policy change**: RLS/ACL changes produce no WAL events. Re-query
///    the DB and replace the stored value.
/// 5. **Reset on TRUNCATE**: engine returns `Err(TruncateRequiresReset)`. Caller
///    must re-query and replace the stored value.
pub trait AggKernel: Send {
    /// Apply a signed-weight delta for a matched row view.
    ///
    /// `weight` is `+1` for INSERT and the new side of UPDATE, `-1` for
    /// DELETE and the old side of UPDATE. `read` yields the presence /
    /// numeric read for any column the kernel needs.
    fn apply(&mut self, read: &mut dyn FnMut(ColumnId) -> AggCellRead, weight: i64);

    /// Return the net delta accumulated so far.
    fn result(&self) -> AggDelta;

    /// Reset the kernel to zero (for reuse across calls).
    fn reset(&mut self);
}

/// COUNT(*) kernel: counts matching rows with signed weights.
#[derive(Default, Debug)]
pub struct CountKernel {
    delta: i64,
}

impl AggKernel for CountKernel {
    fn apply(&mut self, _read: &mut dyn FnMut(ColumnId) -> AggCellRead, weight: i64) {
        self.delta += weight;
    }

    fn result(&self) -> AggDelta {
        AggDelta::Count(self.delta)
    }

    fn reset(&mut self) {
        self.delta = 0;
    }
}

/// COUNT(column) kernel: counts non-NULL, non-Missing values with signed weights.
#[derive(Debug)]
pub struct CountColumnKernel {
    column: ColumnId,
    delta: i64,
}

impl CountColumnKernel {
    /// Create a new kernel for the given column ID.
    #[must_use]
    pub const fn new(column: ColumnId) -> Self {
        Self { column, delta: 0 }
    }
}

impl AggKernel for CountColumnKernel {
    fn apply(&mut self, read: &mut dyn FnMut(ColumnId) -> AggCellRead, weight: i64) {
        if read(self.column).is_present() {
            self.delta += weight;
        }
    }

    fn result(&self) -> AggDelta {
        AggDelta::Count(self.delta)
    }

    fn reset(&mut self) {
        self.delta = 0;
    }
}

/// SUM(column) kernel: accumulates signed weighted column values.
#[derive(Debug)]
pub struct SumKernel {
    column: ColumnId,
    delta: f64,
}

impl SumKernel {
    /// Create a new SumKernel for the given column ID.
    #[must_use]
    pub const fn new(column: ColumnId) -> Self {
        Self { column, delta: 0.0 }
    }
}

impl AggKernel for SumKernel {
    #[allow(clippy::cast_precision_loss)]
    fn apply(&mut self, read: &mut dyn FnMut(ColumnId) -> AggCellRead, weight: i64) {
        let Some(v) = read(self.column).numeric() else {
            return;
        };
        self.delta = v.mul_add(weight as f64, self.delta);
    }

    fn result(&self) -> AggDelta {
        AggDelta::Sum(self.delta)
    }

    fn reset(&mut self) {
        self.delta = 0.0;
    }
}

/// AVG(column) kernel: accumulates both sum and count deltas for running-average updates.
///
/// Emits `AggDelta::Avg { sum_delta, count_delta }`. The caller maintains
/// `running_sum` and `running_count` and computes `AVG = running_sum / running_count`.
#[derive(Debug)]
pub struct AvgKernel {
    column: ColumnId,
    sum_delta: f64,
    count_delta: i64,
}

impl AvgKernel {
    /// Create a new kernel for the given column ID.
    #[must_use]
    pub const fn new(column: ColumnId) -> Self {
        Self {
            column,
            sum_delta: 0.0,
            count_delta: 0,
        }
    }
}

impl AggKernel for AvgKernel {
    #[allow(clippy::cast_precision_loss)]
    fn apply(&mut self, read: &mut dyn FnMut(ColumnId) -> AggCellRead, weight: i64) {
        let Some(v) = read(self.column).numeric() else {
            return;
        };
        self.sum_delta = v.mul_add(weight as f64, self.sum_delta);
        self.count_delta += weight;
    }

    fn result(&self) -> AggDelta {
        AggDelta::Avg {
            sum_delta: self.sum_delta,
            count_delta: self.count_delta,
        }
    }

    fn reset(&mut self) {
        self.sum_delta = 0.0;
        self.count_delta = 0;
    }
}

/// Variance / standard-deviation kernel. Backs `VAR_POP`, `VAR_SAMP`,
/// `STDDEV_POP`, and `STDDEV_SAMP`.
///
/// Accumulates `sum`, `sum_sq`, and `count` deltas. The same numbers feed
/// every flavor, so a single kernel covers all four `AggSpec` variants.
/// The caller derives the final value from the running tuple using the
/// formulas documented on [`AggDelta::Stats`].
///
/// NULL, Missing, NaN, and infinite values are skipped, matching SQL
/// semantics and the policy used by `SumKernel` / `AvgKernel`.
#[derive(Debug)]
pub struct StatsKernel {
    column: ColumnId,
    sum_delta: f64,
    sum_sq_delta: f64,
    count_delta: i64,
}

impl StatsKernel {
    /// Create a new kernel for the given column ID.
    #[must_use]
    pub const fn new(column: ColumnId) -> Self {
        Self {
            column,
            sum_delta: 0.0,
            sum_sq_delta: 0.0,
            count_delta: 0,
        }
    }
}

impl AggKernel for StatsKernel {
    #[allow(clippy::cast_precision_loss)]
    fn apply(&mut self, read: &mut dyn FnMut(ColumnId) -> AggCellRead, weight: i64) {
        let Some(v) = read(self.column).numeric() else {
            return;
        };
        let w = weight as f64;
        self.sum_delta = v.mul_add(w, self.sum_delta);
        self.sum_sq_delta = (v * v).mul_add(w, self.sum_sq_delta);
        self.count_delta += weight;
    }

    fn result(&self) -> AggDelta {
        AggDelta::Stats {
            sum_delta: self.sum_delta,
            sum_sq_delta: self.sum_sq_delta,
            count_delta: self.count_delta,
        }
    }

    fn reset(&mut self) {
        self.sum_delta = 0.0;
        self.sum_sq_delta = 0.0;
        self.count_delta = 0;
    }
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.
