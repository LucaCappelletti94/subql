//! Streaming aggregate kernels for COUNT(*), COUNT(col), SUM(col), and AVG(col).

use crate::{AggDelta, ColumnId, RowImage};

/// Trait for streaming aggregate kernels.
///
/// Kernels accumulate signed-weight deltas produced by the dispatch pipeline.
/// One kernel instance is created per evaluation pass; the result is returned
/// to the caller after all deltas have been applied.
///
/// # Caller contract
/// The engine handles only WAL-driven deltas. Callers must:
/// 1. **Bootstrap** — query the DB for the initial aggregate before subscribing.
/// 2. **Accumulate** — `running_value += delta` on each `aggregate_deltas` call.
/// 3. **Reset on policy change** — RLS/ACL changes produce no WAL events;
///    re-query the DB and replace the stored value.
/// 4. **Reset on TRUNCATE** — engine returns `Err(TruncateRequiresReset)`;
///    caller must re-query and replace the stored value.
pub trait AggKernel: Send {
    /// Apply a signed-weight delta for a matched row.
    ///
    /// `weight` is `+1` for INSERT/new-side of UPDATE, `-1` for DELETE/old-side.
    /// `row` is inspected by SUM/MIN kernels for column values.
    fn apply(&mut self, row: &RowImage, weight: i64);

    /// Return the net delta accumulated so far.
    fn result(&self) -> AggDelta;

    /// Reset the kernel to zero (for reuse across calls).
    fn reset(&mut self);
}

/// COUNT(*) kernel — counts matching rows with signed weights.
#[derive(Default, Debug)]
pub struct CountKernel {
    delta: i64,
}

impl AggKernel for CountKernel {
    fn apply(&mut self, _row: &RowImage, weight: i64) {
        self.delta += weight;
    }

    fn result(&self) -> AggDelta {
        AggDelta::Count(self.delta)
    }

    fn reset(&mut self) {
        self.delta = 0;
    }
}

/// COUNT(column) kernel — counts non-NULL, non-Missing values with signed weights.
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
    fn apply(&mut self, row: &RowImage, weight: i64) {
        match row.get(self.column) {
            Some(crate::Cell::Null) | None => {} // SQL NULL semantics: do not count
            Some(_) => self.delta += weight,
        }
    }

    fn result(&self) -> AggDelta {
        AggDelta::Count(self.delta)
    }

    fn reset(&mut self) {
        self.delta = 0;
    }
}

/// SUM(column) kernel — accumulates signed weighted column values.
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
    fn apply(&mut self, row: &RowImage, weight: i64) {
        let v = match row.get(self.column) {
            Some(crate::Cell::Int(v)) => *v as f64,
            Some(crate::Cell::Float(v)) => {
                if v.is_finite() {
                    *v
                } else {
                    return;
                }
            }
            // NULL, Missing, Bool, String → SQL NULL semantics (skip)
            _ => return,
        };
        self.delta += v * weight as f64;
    }

    fn result(&self) -> AggDelta {
        AggDelta::Sum(self.delta)
    }

    fn reset(&mut self) {
        self.delta = 0.0;
    }
}

/// AVG(column) kernel — accumulates both sum and count deltas for running-average updates.
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
    fn apply(&mut self, row: &RowImage, weight: i64) {
        let v = match row.get(self.column) {
            Some(crate::Cell::Int(v)) => *v as f64,
            Some(crate::Cell::Float(v)) => {
                if v.is_finite() {
                    *v
                } else {
                    return;
                }
            }
            // NULL, Missing, Bool, String → skip (SQL AVG ignores NULLs)
            _ => return,
        };
        self.sum_delta += v * weight as f64;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::Cell;
    use std::sync::Arc;

    fn row(cells: Vec<Cell>) -> RowImage {
        RowImage {
            cells: Arc::from(cells),
        }
    }

    // --- CountKernel tests ---

    #[test]
    fn test_count_kernel_apply_positive_weight() {
        let mut k = CountKernel::default();
        k.apply(&row(vec![]), 1);
        assert_eq!(k.result(), AggDelta::Count(1));
    }

    #[test]
    fn test_count_kernel_apply_negative_weight() {
        let mut k = CountKernel::default();
        k.apply(&row(vec![]), -1);
        assert_eq!(k.result(), AggDelta::Count(-1));
    }

    #[test]
    fn test_count_kernel_reset() {
        let mut k = CountKernel::default();
        k.apply(&row(vec![]), 1);
        k.reset();
        assert_eq!(k.result(), AggDelta::Count(0));
    }

    // --- CountColumnKernel tests ---

    #[test]
    fn test_count_column_kernel_non_null_counted() {
        let mut k = CountColumnKernel::new(0);
        k.apply(&row(vec![Cell::Int(42)]), 1);
        assert_eq!(k.result(), AggDelta::Count(1));
    }

    #[test]
    fn test_count_column_kernel_null_skipped() {
        let mut k = CountColumnKernel::new(0);
        k.apply(&row(vec![Cell::Null]), 1);
        assert_eq!(k.result(), AggDelta::Count(0));
    }

    #[test]
    fn test_count_column_kernel_missing_skipped() {
        let mut k = CountColumnKernel::new(1); // col 1 absent in row
        k.apply(&row(vec![Cell::Int(1)]), 1);
        assert_eq!(k.result(), AggDelta::Count(0));
    }

    #[test]
    fn test_count_column_kernel_bool_counted() {
        let mut k = CountColumnKernel::new(0);
        k.apply(&row(vec![Cell::Bool(false)]), 1);
        assert_eq!(k.result(), AggDelta::Count(1));
    }

    #[test]
    fn test_count_column_kernel_string_counted() {
        let mut k = CountColumnKernel::new(0);
        k.apply(&row(vec![Cell::String("hi".into())]), 1);
        assert_eq!(k.result(), AggDelta::Count(1));
    }

    #[test]
    fn test_count_column_kernel_negative_weight() {
        let mut k = CountColumnKernel::new(0);
        k.apply(&row(vec![Cell::Int(5)]), -1);
        assert_eq!(k.result(), AggDelta::Count(-1));
    }

    #[test]
    fn test_count_column_kernel_reset() {
        let mut k = CountColumnKernel::new(0);
        k.apply(&row(vec![Cell::Int(5)]), 1);
        k.reset();
        assert_eq!(k.result(), AggDelta::Count(0));
    }

    // --- AvgKernel tests ---

    #[test]
    fn test_avg_kernel_int_cell() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Int(10)]), 1);
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 10.0,
                count_delta: 1
            }
        );
    }

    #[test]
    fn test_avg_kernel_float_cell() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Float(2.5)]), 1);
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 2.5,
                count_delta: 1
            }
        );
    }

    #[test]
    fn test_avg_kernel_null_skipped() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Null]), 1);
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 0.0,
                count_delta: 0
            }
        );
    }

    #[test]
    fn test_avg_kernel_missing_skipped() {
        let mut k = AvgKernel::new(1);
        k.apply(&row(vec![Cell::Int(5)]), 1);
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 0.0,
                count_delta: 0
            }
        );
    }

    #[test]
    fn test_avg_kernel_nan_skipped() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Float(f64::NAN)]), 1);
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 0.0,
                count_delta: 0
            }
        );
    }

    #[test]
    fn test_avg_kernel_negative_weight() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Int(20)]), -1);
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: -20.0,
                count_delta: -1
            }
        );
    }

    #[test]
    fn test_avg_kernel_update_net() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Int(10)]), -1); // old row
        k.apply(&row(vec![Cell::Int(20)]), 1); // new row
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 10.0,
                count_delta: 0
            }
        );
    }

    #[test]
    fn test_avg_kernel_reset() {
        let mut k = AvgKernel::new(0);
        k.apply(&row(vec![Cell::Int(100)]), 1);
        k.reset();
        assert_eq!(
            k.result(),
            AggDelta::Avg {
                sum_delta: 0.0,
                count_delta: 0
            }
        );
    }

    // --- SumKernel tests ---

    #[test]
    fn test_sum_kernel_int_cell() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Int(20)]), 1);
        assert_eq!(k.result(), AggDelta::Sum(20.0));
    }

    #[test]
    fn test_sum_kernel_float_cell() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Float(2.5)]), 1);
        assert_eq!(k.result(), AggDelta::Sum(2.5));
    }

    #[test]
    fn test_sum_kernel_null_skipped() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Null]), 1);
        assert_eq!(k.result(), AggDelta::Sum(0.0));
    }

    #[test]
    fn test_sum_kernel_missing_skipped() {
        let mut k = SumKernel::new(1); // col 1, but row only has col 0
        k.apply(&row(vec![Cell::Int(5)]), 1);
        assert_eq!(k.result(), AggDelta::Sum(0.0));
    }

    #[test]
    fn test_sum_kernel_negative_weight() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Int(20)]), -1);
        assert_eq!(k.result(), AggDelta::Sum(-20.0));
    }

    #[test]
    fn test_sum_kernel_nan_skipped() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Float(f64::NAN)]), 1);
        assert_eq!(k.result(), AggDelta::Sum(0.0));
    }

    #[test]
    fn test_sum_kernel_inf_skipped() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Float(f64::INFINITY)]), 1);
        assert_eq!(k.result(), AggDelta::Sum(0.0));
    }

    #[test]
    fn test_sum_kernel_update_net() {
        // Simulates old row weight=-1, new row weight=+1
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Int(15)]), -1);
        k.apply(&row(vec![Cell::Int(20)]), 1);
        assert_eq!(k.result(), AggDelta::Sum(5.0));
    }

    #[test]
    fn test_sum_kernel_reset() {
        let mut k = SumKernel::new(0);
        k.apply(&row(vec![Cell::Int(100)]), 1);
        k.reset();
        assert_eq!(k.result(), AggDelta::Sum(0.0));
    }
}
