//! Time abstraction for rate-limiting decisions.
//!
//! The auto-resolving engines use a [`Clock`] to debounce re-executions
//! per captured query. The trait is intentionally minimal: a single
//! `now_micros()` method returning a monotonically non-decreasing
//! microsecond counter from some implementation-defined origin.
//!
//! [`StdClock`] is the default, available under the `std` feature; it
//! reads `std::time::Instant`. [`ManualClock`] is for tests and ticks
//! only when the test calls [`ManualClock::advance`].

use alloc::sync::Arc;
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

/// Monotonic time source.
///
/// Implementations must return values that are non-decreasing across calls
/// from the same instance, but the origin of the count is up to the
/// implementation (process start, an absolute epoch, a test-supplied
/// counter, etc.). Engines use the trait only for *deltas* between calls.
///
/// `Send + Sync` so the trait works in both single-threaded sync engines
/// and multi-threaded async runtimes. Subql stores clocks behind
/// [`Arc<dyn Clock>`](alloc::sync::Arc) so the same clock can be shared
/// across multiple engines without ownership games.
pub trait Clock: Send + Sync {
    /// Current monotonic position in microseconds from this clock's
    /// origin. Calls are non-decreasing for a given clock instance.
    fn now_micros(&self) -> u64;
}

/// Default clock for `std` builds: reads `std::time::Instant`.
#[cfg(feature = "std")]
#[derive(Debug)]
pub struct StdClock {
    origin: std::time::Instant,
}

#[cfg(feature = "std")]
impl StdClock {
    /// Construct a clock anchored at "now" of the calling thread.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

#[cfg(feature = "std")]
impl Default for StdClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "std")]
impl Clock for StdClock {
    fn now_micros(&self) -> u64 {
        let elapsed = std::time::Instant::now().saturating_duration_since(self.origin);
        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
    }
}

/// Test-controlled clock. Starts at zero (or a caller-supplied origin)
/// and only advances when [`advance`](Self::advance) is called.
///
/// Thread-safe via an atomic counter; useful in async tests that want
/// deterministic timing across `block_on` boundaries.
#[derive(Debug)]
pub struct ManualClock {
    micros: AtomicU64,
}

impl ManualClock {
    /// Create a clock starting at `start_micros`.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::{Clock, ManualClock};
    ///
    /// let clock = ManualClock::new(1_000_000);
    /// assert_eq!(clock.now_micros(), 1_000_000);
    /// ```
    #[must_use]
    pub const fn new(start_micros: u64) -> Self {
        Self {
            micros: AtomicU64::new(start_micros),
        }
    }

    /// Advance the clock by `by`. Saturates at `u64::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use subql::{Clock, ManualClock};
    ///
    /// let clock = ManualClock::new(0);
    /// clock.advance(Duration::from_millis(250));
    /// assert_eq!(clock.now_micros(), 250_000);
    /// ```
    pub fn advance(&self, by: Duration) {
        let add = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
        self.micros.fetch_add(add, Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clock for ManualClock {
    fn now_micros(&self) -> u64 {
        self.micros.load(Ordering::SeqCst)
    }
}

/// Compute the elapsed [`Duration`] between two `now_micros` readings.
///
/// Saturates at zero when the clock somehow went backwards (the trait's
/// monotonicity contract should make this impossible, but the saturation
/// keeps debounce logic robust against badly-behaved custom impls).
#[must_use]
pub const fn duration_between(earlier_micros: u64, later_micros: u64) -> Duration {
    Duration::from_micros(later_micros.saturating_sub(earlier_micros))
}

/// Shared clock handle: an `Arc<dyn Clock>` typedef used by the engines
/// to store and clone clocks cheaply.
pub type ClockHandle = Arc<dyn Clock>;
