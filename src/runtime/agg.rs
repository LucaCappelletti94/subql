//! Streaming aggregate kernels for COUNT(*) and future aggregates.

use crate::RowImage;

/// Trait for streaming aggregate kernels.
///
/// Kernels accumulate signed-weight deltas produced by the dispatch pipeline.
/// One kernel instance is created per evaluation pass; the result is returned
/// to the caller after all deltas have been applied.
///
/// # Caller contract
/// The engine handles only WAL-driven deltas. Callers must:
/// 1. **Bootstrap** — query the DB for the initial aggregate before subscribing.
/// 2. **Accumulate** — `running_value += delta` on each `count_deltas` call.
/// 3. **Reset on policy change** — RLS/ACL changes produce no WAL events;
///    re-query the DB and replace the stored value.
/// 4. **Reset on TRUNCATE** — engine returns `Err(TruncateRequiresReset)`;
///    caller must re-query and replace the stored value.
pub trait AggKernel: Send {
    /// Apply a signed-weight delta for a matched row.
    ///
    /// `weight` is `+1` for INSERT/new-side of UPDATE, `-1` for DELETE/old-side.
    /// `row` is passed for future SUM/MIN kernels that inspect column values.
    fn apply(&mut self, row: &RowImage, weight: i64);

    /// Return the net delta accumulated so far.
    fn result(&self) -> i64;

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

    fn result(&self) -> i64 {
        self.delta
    }

    fn reset(&mut self) {
        self.delta = 0;
    }
}
