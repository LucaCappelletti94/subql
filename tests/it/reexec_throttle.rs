//! Integration test: peak concurrent `execute_scalar` calls during a
//! single resolve of an applied burst never exceed
//! `with_max_concurrent_reexecutions(cap)`.
//!
//! The unit tests in `src/reexec/async_auto.rs` cannot observe this
//! invariant because their `MockAsyncConnector` futures complete in one
//! poll (no parking, no real concurrency). This test wires up a tokio
//! `current_thread` runtime plus a `ConcurrencyProbingConnector` whose
//! `execute_scalar` actually awaits a `tokio::time::sleep`. Each call
//! bumps an `inflight` counter and updates a `peak` via `fetch_max`.
//! After the batch completes we assert `peak <= cap` and that every
//! captured query did get its update.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::missing_const_for_fn,
    clippy::type_complexity,
    clippy::cast_precision_loss
)]

use core::time::Duration;
use std::time::Instant;

use crate::common::throttle::{delete_event, engine_with_first_n_queries, QUERIES};

#[tokio::test(flavor = "current_thread")]
async fn throttle_peak_inflight_under_cap_for_cap_1() {
    run_peak_inflight_assertion(1).await;
}

#[tokio::test(flavor = "current_thread")]
async fn throttle_peak_inflight_under_cap_for_cap_2() {
    run_peak_inflight_assertion(2).await;
}

#[tokio::test(flavor = "current_thread")]
async fn throttle_peak_inflight_at_cap_equals_trigger_count() {
    run_peak_inflight_assertion(QUERIES.len()).await;
}

async fn run_peak_inflight_assertion(cap: usize) {
    let delay = Duration::from_millis(20);
    let n_triggers = QUERIES.len();
    let (mut engine, tid) = engine_with_first_n_queries(QUERIES.len(), cap, delay);

    let events = vec![delete_event(tid, 1, 7.0, 1)];

    let started = Instant::now();
    for event in &events {
        engine.apply(event).unwrap();
    }
    let outcome = engine.resolve_collect().await.unwrap();
    let elapsed = started.elapsed();

    let peak = engine.connector().peak();
    let total = engine.connector().total_calls();
    println!("cap={cap} n_triggers={n_triggers} peak={peak} total={total} elapsed={elapsed:?}");

    assert!(
        peak <= cap,
        "peak concurrent connector calls ({peak}) exceeded cap ({cap})"
    );
    assert_eq!(
        total, n_triggers,
        "every captured query should get exactly one connector call"
    );
    assert_eq!(
        outcome.scalar_updates.len(),
        n_triggers,
        "every captured query should emit one ScalarUpdate"
    );
    assert_eq!(
        engine.inflight(),
        0,
        "inflight must return to 0 after the batch completes"
    );

    if cap == 1 {
        let expected_min = delay.saturating_mul(n_triggers.try_into().unwrap_or(u32::MAX)) / 2;
        assert!(
            elapsed >= expected_min,
            "with cap=1 and {n_triggers} triggers, expected at least {expected_min:?}, got {elapsed:?}"
        );
    }
}
