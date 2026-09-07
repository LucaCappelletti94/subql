//! The debounce window, per subscription and per group.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn grouped_debounce_is_scoped_by_group_key_async() {
    let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
    let engine_clock: crate::ClockHandle = clock;
    let (engine, _) = engine_with_values(Vec::new());
    let mut engine = engine
        .with_clock(engine_clock)
        .with_debounce_per_query(core::time::Duration::from_secs(1));
    let first = crate::reexec::ReExecutionRead::GroupedScalar {
        group: vec![1],
        query: crate::reexec::BoundQuery::new(String::new(), Vec::new()),
        column_kinds: [ScalarFamily::Int, ScalarFamily::Int],
    };
    let second = crate::reexec::ReExecutionRead::GroupedScalar {
        group: vec![2],
        query: crate::reexec::BoundQuery::new(String::new(), Vec::new()),
        column_kinds: [ScalarFamily::Int, ScalarFamily::Int],
    };
    engine.stamp_reexec(7, &first);
    assert!(engine.debounce_skip(7, &first));
    assert!(!engine.debounce_skip(7, &second));
}
