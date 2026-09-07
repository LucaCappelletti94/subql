//! The queue of pending reads: coalescing, displacement and reclamation.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// The async twin of the sync drop-on-install-failure contract: a
/// grouped re-read answering the wrong row count can never install, so
/// the failing read is dropped in phase three rather than staying queued
/// and repeating the same malformed read on every resolve.
#[test]
fn a_failed_install_drops_the_read_instead_of_requeueing_it() {
    let (mut engine, table) = engine_with_values(Vec::new());
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders GROUP BY status",
            ),
            (),
        )
        .expect("grouped minimum registers")
        .subscription_id;
    crate::Install::install(
        &mut engine.inner,
        subscription,
        crate::GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(2),
            ]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("group map installs");
    engine
        .apply(
            &TestEvent::<Postgres>::delete(
                table,
                vec![
                    Value::Int(1),
                    Value::Float(5.0),
                    Value::Int(1),
                    Value::String("paid".into()),
                ],
            )
            .with_pk_columns([0u16]),
        )
        .expect("the displacing delete dispatches");
    assert_eq!(
        engine.pending_read_count(),
        1,
        "the displacement queues one read"
    );
    // The re-read answers two rows for a one-group read, which can never
    // install: retrying it would return the same malformed answer.
    engine.connector().push_page(crate::reexec::RowPage {
        columns: vec!["min".into(), "n".into()],
        rows: vec![
            vec![Value::Float(6.0), Value::Int(1)],
            vec![Value::Float(7.0), Value::Int(1)],
        ],
        more: false,
    });
    let error = block_on(engine.resolve_collect()).unwrap_err();
    assert!(
        matches!(error, crate::reexec::ReExecError::AggregateInstall(_)),
        "the row count mismatch reports as an aggregate install failure, got {error:?}"
    );
    assert_eq!(
        engine.pending_read_count(),
        0,
        "a non-retryable read is dropped, never requeued"
    );
    block_on(engine.resolve_collect()).expect("the next resolve is a clean no-op");
}

/// Mirrors `auto::tests::a_dispatch_reports_the_reads_it_queued`,
/// because a shared line tested on one side only is correct for the
/// other by luck.
#[test]
fn async_dispatch_reports_the_reads_it_queued() {
    let (mut e, tid) = engine_with_values(vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");

    let queued = e
        .apply(&insert_event(tid, 1, 5.0))
        .expect("the event applies");
    assert_eq!(
        queued.outstanding,
        e.pending_read_count(),
        "the report is the depth of the queue, not a guess about it"
    );
    assert!(
        queued.outstanding > 0,
        "a whole-result subscription queues a read for this event"
    );

    let again = e
        .apply(&insert_event(tid, 2, 6.0))
        .expect("the event applies");
    assert_eq!(
        again.outstanding, queued.outstanding,
        "a burst coalesces, so the report does not count events"
    );

    // Depth two, so a count collapsed to `min(1)` cannot pass.
    e.register(
        SubscriptionRequest::new(2u64, "SELECT DISTINCT quantity FROM orders"),
        (),
    )
    .expect("a second whole read registers");
    let two = e
        .apply(&insert_event(tid, 4, 8.0))
        .expect("the event applies");
    assert_eq!(
        two.outstanding, 2,
        "two subscriptions each queued one read, so the depth is two"
    );

    // The mock refuses a cursor it has no page for.
    e.connector().cursor_pages.lock().extend([
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: false,
        },
        crate::reexec::RowPage {
            columns: vec![String::from("quantity")],
            rows: vec![vec![Value::Int(1)]],
            more: false,
        },
    ]);
    block_on(e.resolve_collect()).expect("the reads run");
    let settled = e
        .apply(&update_status_only(tid, 3, 7.0))
        .expect("the event applies");
    assert_eq!(
        e.pending_read_count(),
        settled.outstanding,
        "and it still agrees with the queue after a drain"
    );
}

/// Async batch coalesces repeated triggers for the same query into a
/// single connector call. Mirrors the sync engine's T4.1 assertion.
#[test]
fn async_applied_burst_coalesces_repeated_triggers() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(99.0)]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    let events = [
        delete_event(tid, 1, 5.0),
        delete_event(tid, 2, 5.0),
        delete_event(tid, 3, 5.0),
    ];

    let per_event: Vec<_> = events.iter().map(|ev| e.apply(ev).unwrap()).collect();
    assert_eq!(per_event.len(), 3, "per_event positional alignment");
    let outcome = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        1,
        "three displacing events collapse to one connector call"
    );
    assert_eq!(outcome.scalar_updates.len(), 1);
    assert_eq!(outcome.scalar_updates[0].value, Value::Float(99.0));
}

/// Dropping a resolve mid-read loses nothing: the read stays queued, a
/// fresh resolve completes it, and the event is never reapplied.
#[test]
fn dropped_resolve_keeps_the_read_queued() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();

    let applied = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert!(
        applied.scalar_updates.is_empty(),
        "the read is queued, not run"
    );
    assert_eq!(e.pending_read_count(), 1);

    *e.connector().pend_next_read.lock() = true;
    {
        let mut ctx = Context::from_waker(core::task::Waker::noop());
        let mut sink =
            |_delivery: crate::reexec::ReadDelivery<DefaultIds, Postgres, NoCheckpoint>| {};
        let fut = e.resolve(&mut sink);
        let mut pinned = pin!(fut);
        assert!(
            pinned.as_mut().poll(&mut ctx).is_pending(),
            "the resolve suspends inside the connector read"
        );
        // Dropped here, mid-read.
    }
    assert_eq!(e.pending_read_count(), 1, "the dropped read stayed queued");
    assert_eq!(e.connector().call_count(), 0, "the read never completed");

    let resolved = block_on(e.resolve_collect()).unwrap();
    assert_eq!(resolved.scalar_updates.len(), 1);
    assert_eq!(resolved.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.pending_read_count(), 0);
    assert_eq!(
        e.connector().call_count(),
        1,
        "one completed read, no redispatch"
    );
}
