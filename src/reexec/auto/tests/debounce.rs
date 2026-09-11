//! The debounce window, per subscription and per group.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// `debounced` counts the unanswered-cell path, not only the trigger
/// path.
#[expect(
    clippy::clone_on_ref_ptr,
    reason = "the clone below performs the Arc<ManualClock> to Arc<dyn Clock> \
              unsize coercion at the assignment site, which Arc::clone cannot \
              do from an uncoerced source"
)]
#[test]
fn a_debounced_unanswered_read_is_reported() {
    let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
    let engine_clock: crate::ClockHandle = clock.clone();
    let (e0, tid) = engine_with_values(alloc::vec![]);
    let mut engine = e0
        .with_clock(engine_clock)
        .with_debounce_per_query(core::time::Duration::from_millis(100));
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
            (),
        )
        .expect("the filter is served in process");

    // The row image omits `status`, which the predicate reads, so the
    // event cannot answer it and the read is queued.
    let unanswerable = |id: i64| {
        let mut cells = row(id, 5.0);
        cells[3] = Value::Missing;
        TestEvent::<Postgres>::update(tid, row(id, 5.0), cells)
            .with_pk_columns([0u16])
            .with_changed_columns([1u16])
    };

    engine
        .connector()
        .cursor_pages
        .borrow_mut()
        .push(crate::reexec::RowPage {
            columns: alloc::vec![String::from("id"), String::from("status")],
            rows: alloc::vec![alloc::vec![Value::Int(1), Value::String("paid".into())]],
            more: false,
        });
    let first = engine.apply(&unanswerable(1)).expect("the event applies");
    assert_eq!(
        first.debounced, 0,
        "nothing has run yet, so nothing is dropped"
    );
    assert!(first.outstanding > 0, "the unanswered cell queued a read");
    engine.resolve_collect().expect("the read runs and stamps");

    // Inside the window, the same subscription's read is discarded.
    clock.advance(core::time::Duration::from_millis(50));
    let second = engine.apply(&unanswerable(2)).expect("the event applies");
    assert_eq!(
        second.debounced, 1,
        "the window dropped the unanswered-cell read and the report says so"
    );
    assert_eq!(
        second.outstanding, 0,
        "which the queue depth alone cannot say"
    );
}

/// T6.1: a second displacing event within the debounce window is
/// dropped: connector is not called and no ScalarUpdate is emitted.
/// T6.2 in the same test: after the clock ticks past the window the
/// next trigger fires normally.
#[test]
// The `.clone()` below needs the Arc<ManualClock> -> Arc<dyn Clock>
// unsize coercion at the assignment site. `Arc::clone(&clock)` would
// need an already-coerced source. Allow the clippy lint here.
#[allow(clippy::clone_on_ref_ptr)]
fn debounce_skips_within_window_and_fires_after() {
    let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
    let engine_clock: crate::ClockHandle = clock.clone();
    // Two values in the connector queue: one for the first re-exec,
    // one for the post-window re-exec. The "within window" re-exec
    // is debounced and never reaches the connector.
    let (e0, tid) = engine_with_values(alloc::vec![Value::Float(20.0), Value::Float(7.0)]);
    let mut e = e0
        .with_clock(engine_clock)
        .with_debounce_per_query(core::time::Duration::from_millis(100));

    let qid = crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    // First displacing event: re-exec proceeds (no prior stamp).
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.connector().call_count(), 1);

    // Second displacing event within 50ms (window is 100ms): skipped.
    clock.advance(core::time::Duration::from_millis(50));
    // The engine's MIN is currently 7.0 from the prior re-exec. To
    // force a second trigger we delete a row matching 7.0.
    let dispatched = e.apply(&delete_event(tid, 2, 7.0)).unwrap();
    // The queue is empty because the trigger was discarded.
    assert_eq!(
        dispatched.debounced, 1,
        "the window dropped this event's read and the report says so"
    );
    assert_eq!(
        dispatched.outstanding, 0,
        "and nothing is queued, which on its own would look answered"
    );
    let n = e.resolve_collect().unwrap();
    assert!(
        n.scalar_updates.is_empty(),
        "debounced trigger must not emit a ScalarUpdate"
    );
    assert_eq!(
        e.connector().call_count(),
        1,
        "debounced trigger must not call the connector"
    );

    // Past the window now: 50ms + 100ms = 150ms total since first.
    clock.advance(core::time::Duration::from_millis(100));
    // Reinstall the value the engine thinks is current so the next
    // displacement is well-defined. The test exercises debounce,
    // not the state machine.
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    e.apply(&delete_event(tid, 3, 7.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1, "post-window trigger must fire");
    assert_eq!(n.scalar_updates[0].value, Value::Float(20.0));
    assert_eq!(e.connector().call_count(), 2);
}

/// Without a configured clock, `with_debounce_per_query` is a no-op:
/// triggers fire as normal.
#[test]
fn debounce_without_clock_is_a_noop() {
    let (e0, tid) = engine_with_values(alloc::vec![Value::Float(9.0), Value::Float(7.0)]);
    let mut e = e0.with_debounce_per_query(core::time::Duration::from_secs(3600));

    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let result = e.resolve_collect().unwrap();
    assert!(!result.scalar_updates.is_empty());
    // trigger. The debounce-without-clock case must NOT skip it.
    e.apply(&delete_event(tid, 2, 7.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1, "no clock -> no debounce");
    assert_eq!(e.connector().call_count(), 2);
}

#[test]
fn grouped_debounce_is_scoped_by_group_key() {
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
