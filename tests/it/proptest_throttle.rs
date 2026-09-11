//! Property-based extension of `tests/it/reexec_throttle.rs`.
//!
//! The hand-written integration test exercises `cap = 1`, `cap = 2`,
//! and `cap = QUERIES.len()`. This proptest broadens the matrix to
//! arbitrary `(num_triggers, cap, per-call delay)` tuples and asserts
//! the same load-bearing invariants on every case:
//!
//! 1. **Cap respected.** `peak inflight <= cap` for every schedule the
//!    runtime produces.
//! 2. **No dropped work.** `total_calls == num_triggers` exactly. The
//!    throttle must not skip or double-count.
//! 3. **Clean shutdown.** `inflight` returns to 0 after the batch.
//! 4. **Per-trigger outcome.** `scalar_updates.len() == num_triggers`.
//!
//! Each case spins up its own `current_thread` tokio runtime. Proptest
//! itself is synchronous so we use `block_on`. To keep wall-clock
//! reasonable we cap the delay at 8 ms and run 32 cases.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::missing_const_for_fn,
    clippy::type_complexity,
    clippy::cast_precision_loss
)]

use core::time::Duration;

use crate::common::throttle::{delete_event, engine_with_first_n_queries, QUERIES};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        ..ProptestConfig::default()
    })]

    /// For every `(num_triggers, cap, delay)`:
    ///   peak inflight <= cap, total_calls == num_triggers,
    ///   scalar_updates.len() == num_triggers, inflight returns to 0.
    #[test]
    fn throttle_invariants_hold(
        num_triggers in 1usize..=QUERIES.len(),
        cap in 1usize..=QUERIES.len(),
        delay_ms in 1u64..=8,
    ) {
        let delay = Duration::from_millis(delay_ms);
        let (mut engine, tid) = engine_with_first_n_queries(num_triggers, cap, delay);
        let events = vec![delete_event(tid, 1, 7.0, 1)];

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        for event in &events {
            engine.apply(event).unwrap();
        }
        let outcome = runtime.block_on(engine.resolve_collect()).unwrap();

        let peak = engine.connector().peak();
        let total = engine.connector().total_calls();

        prop_assert!(
            peak <= cap,
            "peak ({peak}) exceeded cap ({cap}) for num_triggers={num_triggers} delay={delay:?}",
        );
        prop_assert_eq!(
            total,
            num_triggers,
            "total_calls ({}) != num_triggers ({})",
            total,
            num_triggers,
        );
        prop_assert_eq!(
            outcome.scalar_updates.len(),
            num_triggers,
            "scalar_updates count ({}) != num_triggers ({})",
            outcome.scalar_updates.len(),
            num_triggers,
        );
        prop_assert_eq!(
            engine.inflight(),
            0,
            "inflight ({}) did not return to 0 after batch",
            engine.inflight(),
        );
    }
}
