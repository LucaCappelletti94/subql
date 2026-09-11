//! What unregistering drops.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// A queued read must not outlive its subscription, the async twin:
/// unregistering purges it, so the next resolve is a clean no-op rather
/// than a panic on the missing resolve context in `plan_job`.
#[test]
fn unregister_subscription_drops_the_queued_read() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(5.0)]);
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
    block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no read runs for a dead subscription"
    );
}

/// `unregister_subscription` drops the stored auth context.
#[test]
fn async_engine_unregister_drops_context() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let qid = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(qid));
    assert_eq!(e.contexts.len(), 0);
}

#[test]
fn async_unregister_subscription_resolves_either_registry() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let captured = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
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
fn async_unregister_subscription_drops_the_resolve_context() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
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
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(captured));
    assert_eq!(e.contexts.len(), 0, "the resolve context is dropped");
    let n = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(
        e.connector().call_count(),
        0,
        "no connector call after unregister"
    );
}
