//! What unregistering drops.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn unregister_drops_auth_context() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
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
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(qid));
    assert_eq!(e.contexts.len(), 0);
    assert!(!e.unregister_subscription(qid), "second drop is a no-op");
}

#[test]
fn unregister_subscription_resolves_either_registry() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    // Captured re-execution query: lands in the read registry with a context.
    let captured = match e
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
    let captured = match e
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
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
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
    let captured = match e
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
        captured,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
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
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
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
