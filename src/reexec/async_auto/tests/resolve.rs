//! The resolve path: what a triggered read answers, and how it fails.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// Full path through the async engine: register, snapshot (which
/// installs), unrelated insert (no connector call), delete of the
/// extreme (one connector call, ScalarUpdate emitted).
#[test]
fn async_engine_dispatch_round_trip() {
    // Two values for: snapshot bootstrap (5.0), delete re-execution (9.0).
    // Mock pops from the back so push in reverse order.
    let (mut e, tid) = engine_with_values(vec![Value::Float(9.0), Value::Float(5.0)]);

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

    // Snapshot bootstraps. Future is Send-bound and ready immediately.
    let snap = block_on(e.snapshot(qid))
        .unwrap()
        .expect("subscription_id exists");
    match snap {
        SnapshotResult::Scalar(Value::Float(v), None) => {
            assert!((v - 5.0).abs() < f64::EPSILON);
        }
        other => panic!("expected Scalar(5.0, None), got {other:?}"),
    }
    assert_eq!(e.connector().call_count(), 1);

    // Insert above the extreme: in-process Unchanged, no connector call.
    let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(e.connector().call_count(), 1);

    // Delete the extreme: trigger -> connector -> ScalarUpdate.
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let n = block_on(e.resolve_collect()).unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
    assert_eq!(e.connector().call_count(), 2);
}

/// `unrelated-column` UPDATE skip optimization still works under
/// the async engine.
#[test]
fn async_engine_unrelated_column_update_skips_connector() {
    let (mut e, tid) = engine_with_values(vec![]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MAX(price) FROM orders"),
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
            value: Value::Float(10.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    let event = update_status_only(tid, 1, 10.0);

    let n = e.apply(&event).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(n.outstanding, 0, "nothing was queued either");
    assert_eq!(e.connector().call_count(), 0);
}

/// `snapshot` on an unknown id returns `Ok(None)`.
#[test]
fn async_engine_snapshot_unknown_query_returns_none() {
    let (mut e, _tid) = engine_with_values(vec![]);
    assert!(block_on(e.snapshot(99999)).unwrap().is_none());
    assert_eq!(e.connector().call_count(), 0);
}

/// Connector failure aborts the batch with `ReExecError::Connector`.
#[test]
fn async_engine_connector_error_aborts_batch() {
    let (mut e, tid) = engine_with_values(vec![]);
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

    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match block_on(e.resolve_collect()) {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError::Unstaged(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}

#[test]
fn async_connector_error_names_its_subscription() {
    let (mut e, tid) = engine_with_values(vec![]);
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
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match block_on(e.resolve_collect()) {
        Ok(_) => panic!("expected the triggered read to fail"),
        Err(ReExecError::Connector {
            subscription,
            error: MockError::Unstaged(msg),
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert_eq!(msg, "queue empty");
        }
        Err(other) => panic!("expected Connector naming its subscription, got {other:?}"),
    }
}

#[test]
fn async_cursor_error_names_its_subscription() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::WholeRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected WholeRows, got {other:?}"),
    };
    match block_on(e.snapshot(qid)) {
        Ok(_) => panic!("expected the cursorless read to fail"),
        Err(ReExecError::Cursor {
            subscription,
            error,
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert!(matches!(error, crate::reexec::CursorError::Unsupported));
        }
        Err(other) => panic!("expected Cursor naming its subscription, got {other:?}"),
    }
}
