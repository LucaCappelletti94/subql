//! The concurrency cap: accessors, occupancy and call counts.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// `with_max_concurrent_reexecutions` does not change the result of
/// one resolve of a burst. Correctness is preserved. The cap is a
/// throughput / fairness knob, not a semantic one.
#[test]
#[allow(clippy::similar_names)]
fn async_applied_burst_respects_max_concurrent_cap() {
    // Two distinct captured queries, each displaced once in the
    // batch. Both must resolve regardless of the cap.
    let (e0, tid) = engine_with_values(vec![Value::Float(22.0), Value::Float(11.0)]);
    let mut e = e0.with_max_concurrent_reexecutions(1);
    let qid1 = match e
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
        other => panic!("expected ReExec for MIN, got {other:?}"),
    };
    let qid2 = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec for MAX, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid1,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    assert!(crate::Install::install(
        &mut e,
        qid2,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    let events = vec![delete_event(tid, 1, 7.0)];
    for event in &events {
        e.apply(event).unwrap();
    }
    let outcome = block_on(e.resolve_collect()).unwrap();
    assert_eq!(e.connector().call_count(), 2);
    assert_eq!(outcome.scalar_updates.len(), 2);
    let qids: std::collections::BTreeSet<_> = outcome
        .scalar_updates
        .iter()
        .map(|u| u.subscription_id)
        .collect();
    assert!(qids.contains(&qid1));
    assert!(qids.contains(&qid2));
}

/// No cap by default: `inflight()` is 0, `concurrency_cap()` is `None`.
#[test]
fn throttle_disabled_by_default() {
    let (e, _tid) = engine_with_values(vec![]);
    assert_eq!(e.inflight(), 0);
    assert_eq!(e.concurrency_cap(), None);
}

/// `with_max_concurrent_reexecutions(n)` records `n` as the cap and
/// starts with `inflight() == 0`.
#[test]
fn throttle_set_cap_observable_via_accessors() {
    let (e0, _tid) = engine_with_values(vec![]);
    let e = e0.with_max_concurrent_reexecutions(4);
    assert_eq!(e.concurrency_cap(), Some(4));
    assert_eq!(e.inflight(), 0);
}

/// `cap = 0` is normalised to 1 to prevent a deadlock on first
/// `acquire`.
#[test]
fn throttle_zero_cap_normalised_to_one() {
    let (e0, _tid) = engine_with_values(vec![]);
    let e = e0.with_max_concurrent_reexecutions(0);
    assert_eq!(e.concurrency_cap(), Some(1));
}

/// Cleanup invariant: after a successful resolve of a burst the
/// inflight counter is back to 0. Tests that the `InflightGuard`
/// drop path actually fires when futures complete.
#[test]
fn throttle_inflight_returns_to_zero_after_batch() {
    let (e0, tid) = engine_with_values(vec![Value::Float(22.0), Value::Float(11.0)]);
    let mut e = e0.with_max_concurrent_reexecutions(1);
    let qid1 = match e
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
    let qid2 = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
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
        qid1,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    assert!(crate::Install::install(
        &mut e,
        qid2,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    e.apply(&delete_event(tid, 1, 7.0)).unwrap();
    let _ = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.inflight(),
        0,
        "every InflightGuard must drop after batch completes"
    );
}

/// Cleanup invariant on the error path: when the connector fails
/// mid-batch, every permit must still be released.
#[test]
fn throttle_inflight_returns_to_zero_after_connector_error() {
    // Two captured queries, only one value in the queue: the second
    // connector call hits "queue empty" and the batch aborts.
    let (e0, tid) = engine_with_values(vec![Value::Float(22.0)]);
    let mut e = e0.with_max_concurrent_reexecutions(2);
    let qid1 = match e
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
    let qid2 = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
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
        qid1,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    assert!(crate::Install::install(
        &mut e,
        qid2,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    e.apply(&delete_event(tid, 1, 7.0)).unwrap();
    assert!(block_on(e.resolve_collect()).is_err());
    assert_eq!(
        e.inflight(),
        0,
        "InflightGuards must drop even when the batch aborts on connector error"
    );
}

/// The throttle preserves correctness: total connector call count
/// equals the number of deduplicated triggers regardless of cap.
#[test]
fn throttle_total_call_count_unchanged_with_cap() {
    // Three queries, one trigger each, cap = 1.
    let values = vec![Value::Float(30.0), Value::Float(20.0), Value::Float(10.0)];
    let (e0, tid) = engine_with_values(values);
    let mut e = e0.with_max_concurrent_reexecutions(1);
    let qids: Vec<_> = (1u64..=3)
        .map(|c| {
            match e
                .register(
                    SubscriptionRequest::new(c, "SELECT MIN(price) FROM orders WHERE quantity = 1"),
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
            }
        })
        .collect();
    for q in &qids {
        assert!(crate::Install::install(
            &mut e,
            *q,
            crate::ScalarInstall {
                value: Value::Float(7.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());
    }
    e.apply(&delete_event(tid, 1, 7.0)).unwrap();
    let outcome = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        3,
        "three distinct queries each get one connector call regardless of cap"
    );
    assert_eq!(outcome.scalar_updates.len(), 3);
}
