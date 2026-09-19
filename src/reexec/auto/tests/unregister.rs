//! What unregistering drops.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn unregister_drops_auth_context() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    let qid = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(qid));
    assert_eq!(e.contexts.len(), 0);
    assert!(!e.unregister_subscription(qid), "second drop is a no-op");
}

#[test]
fn unregister_subscription_resolves_either_registry() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    // Captured re-execution query: lands in the read registry with a context.
    let captured = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    // In-process row subscription: lands in the in-process registry, no context.
    let in_process = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE price > 100"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    assert!(
        e.unregister_subscription(captured),
        "read registry id resolves"
    );
    assert!(
        e.unregister_subscription(in_process),
        "in-process registry id resolves"
    );
    assert!(
        !e.unregister_subscription(999u64),
        "an unknown id resolves to neither registry"
    );
}

#[test]
fn unregister_subscription_drops_the_resolve_context() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);
    let captured = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    crate::Install::install(
        &mut e,
        captured,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(captured));
    assert_eq!(e.contexts.len(), 0, "the resolve context is dropped");
    // A later delete of the former extreme must not reach the connector.
    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no connector call after unregister"
    );
}

/// A queued read must not outlive its subscription: unregistering purges
/// it, so the next resolve is a clean no-op rather than a panic on the
/// missing resolve context.
#[test]
fn unregister_subscription_drops_the_queued_read() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(5.0)]);
    let captured = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    crate::Install::install(
        &mut e,
        captured,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    assert_eq!(
        e.pending_read_count(),
        1,
        "the displacement queues one read"
    );
    assert!(e.unregister_subscription(captured));
    assert_eq!(
        e.pending_read_count(),
        0,
        "the queued read left with its subscription"
    );
    e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no read runs for a dead subscription"
    );
}

/// The session twin: unregistering a session purges the queued reads of
/// every subscription it carried.
#[test]
fn unregister_session_drops_the_queued_reads() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(5.0)]);
    let session = 9u64;
    // Not the shared scaffold: this one registers into a session scope,
    // which is the whole subject of the test.
    let captured = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders")
                .scope(crate::SubscriptionScope::Session(session)),
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
        captured,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply_leaving_reads_queued(&delete_event(tid, 1, 5.0))
        .unwrap();
    assert_eq!(
        e.pending_read_count(),
        1,
        "the displacement queues one read"
    );
    e.unregister_session(session);
    assert_eq!(
        e.pending_read_count(),
        0,
        "the session took its queued reads with it"
    );
    e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no read runs for a dead session"
    );
}

/// A read outlives nothing, whichever way its subscription ended.
///
/// Ending by statement is the one removal path that dropped neither the
/// resolve context nor the queued read. The context is what the refusal
/// checks for, so an orphaned one is not refused: the next resolve runs a
/// real read for a subscription the engine no longer holds, fails, and
/// leaves the read queued for the next one to repeat.
#[test]
fn unregistering_by_statement_drops_the_queued_read() {
    const SQL: &str = "SELECT * FROM orders WHERE status = 'paid'";
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(SubscriptionRequest::new(1u64, SQL), ())
        .expect("the filter is served in process");

    // The row image omits `status`, which the filter reads, so the event
    // cannot answer it and a read is queued.
    let mut cells = row(1, 5.0);
    cells[3] = Value::Missing;
    let event = TestEvent::<Postgres>::update(tid, row(1, 5.0), cells)
        .with_pk_columns([0u16])
        .with_changed_columns([1u16]);
    e.connector().push_page(crate::reexec::RowPage {
        columns: alloc::vec![String::from("id"), String::from("status")],
        rows: alloc::vec![alloc::vec![Value::Int(1), Value::String("paid".into())]],
        more: false,
    });
    e.apply_leaving_reads_queued(&event)
        .expect("the event applies");
    assert_eq!(
        e.pending_read_count(),
        1,
        "the unanswered cell queues a read"
    );

    e.unregister_query(1u64, SQL)
        .expect("the statement names it");
    assert_eq!(
        e.pending_read_count(),
        0,
        "the queued read left with its subscription"
    );
    assert_eq!(e.contexts.len(), 0, "and so did its resolve context");

    e.resolve_collect().expect("the resolve is a clean no-op");
    assert!(
        e.connector().cursor_queries.borrow().is_empty(),
        "no read runs for a subscription the caller ended"
    );
}

/// Every kind of answer is visible to the predicate removal leans on.
///
/// Dropping the contexts of subscriptions neither registry holds is only
/// safe while every live answer is in one of them. An answer kept
/// somewhere else would be judged gone, lose its context, and have its
/// next read refused instead of run.
#[test]
fn every_registered_kind_is_held_by_one_registry() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    for (consumer, sql) in [
        (1u64, "SELECT * FROM orders WHERE status = 'paid'"),
        (2u64, "SELECT COUNT(*) FROM orders WHERE status = 'paid'"),
        (3u64, "SELECT MIN(price) FROM orders"),
        (4u64, "SELECT * FROM orders WHERE lower(status) = 'paid'"),
        (5u64, "SELECT * FROM orders"),
    ] {
        let registered = e
            .register(SubscriptionRequest::new(consumer, sql), ())
            .expect("the fixture catalog serves every statement here");
        assert!(
            e.inner.holds_subscription(registered.subscription_id),
            "a live answer for {sql} is in neither registry, so removal \
             would drop its context while it is still registered"
        );
    }
}
