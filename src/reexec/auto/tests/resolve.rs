//! The resolve path: what a triggered read answers, and how it fails.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// Full path: register, bootstrap install, insert that does not displace
/// the extreme (in-process scalar update, no connector call), delete of
/// the current extreme (trigger -> connector -> ScalarUpdate). The
/// returned notifications carry no triggers under AutoResolvingEngine.
#[test]
fn delete_of_extreme_resolves_via_connector() {
    // Connector returns 7.0 when re-run after the extreme is removed.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);

    // Bootstrap: model = {1=>5.0}. Current MIN = 5.0.
    let qid = crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    // Insert price=9.0 (>5.0): in-process Unchanged, no scalar update, no trigger.
    let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
    assert!(n.scalar_updates.is_empty(), "insert above extreme");
    assert_eq!(e.connector().call_count(), 0, "no re-execution yet");

    // Delete id=1, price=5.0 (the current extreme): trigger -> connector -> 7.0.
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(n.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.connector().call_count(), 1);
    let (sql, kind) = e.connector().calls.borrow()[0].clone();
    assert!(sql.contains("MIN"));
    assert_eq!(kind, ScalarFamily::Float);
}

#[test]
fn unrelated_column_update_does_not_call_connector() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MAX(price) FROM orders",
        10.0,
    );

    let n = e.apply(&update_status_only(tid, 1, 10.0)).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(e.connector().call_count(), 0);
}

#[test]
fn connector_error_aborts_batch() {
    // Empty queue: the connector errors on first call.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match e.resolve_collect() {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError::Unstaged(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}

/// `snapshot(subscription_id)` reads through the connector and installs the
/// value so subsequent dispatches see it as the current state.
#[test]
fn snapshot_installs_via_connector() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(12.5)]);
    let qid = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );

    // No bootstrap install: snapshot does it.
    let snap = e.snapshot(qid).unwrap().expect("subscription_id exists");
    match snap {
        SnapshotResult::Scalar(value, checkpoint) => {
            assert_eq!(value, Value::Float(12.5));
            // MockConnector returns checkpoint = None.
            assert!(checkpoint.is_none());
        }
        other => panic!("a scalar capture snapshots as a scalar, got {other:?}"),
    }
    assert_eq!(e.connector().call_count(), 1);

    // After snapshot, the engine treats 12.5 as the current MIN.
    // An insert below it (e.g. 9.0) becomes the new in-process MIN.
    let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
    assert_eq!(e.connector().call_count(), 1);
}

/// `snapshot(subscription_id)` on an unknown id returns `Ok(None)` rather
/// than panicking so callers can race snapshot against unregister.
#[test]
fn snapshot_unknown_query_returns_none() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    assert!(e.snapshot(99999).unwrap().is_none());
    // Connector was never called.
    assert_eq!(e.connector().call_count(), 0);
}

#[test]
fn connector_error_names_its_subscription() {
    // Empty queue: the connector errors on the triggered read.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    let qid = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match e.resolve_collect() {
        Ok(_) => panic!("expected the triggered read to fail"),
        Err(ReExecError::Connector {
            subscription,
            error,
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert_eq!(error, MockError::Unstaged("queue empty"));
        }
        Err(other) => panic!("expected Connector naming its subscription, got {other:?}"),
    }
}

#[test]
fn cursor_error_names_its_subscription() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
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
    // snapshot reads the whole result through a cursor. The mock connector
    // holds none, so the read fails, and the error must name the query.
    match e.snapshot(qid) {
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

/// The two-stage contract: one event applies exactly once however its
/// reads fare. The fused dispatch this replaced could only retry a
/// failed read by redispatching the event, which folded the delete into
/// the count a second time.
#[test]
fn applied_event_survives_failed_resolve() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    let count = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
        .subscription_id;
    crate::Install::install(
        &mut e,
        count,
        crate::AggregateSeedInstall {
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    let minimum = e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
        .subscription_id;
    crate::Install::install(
        &mut e,
        minimum,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();

    // The delete folds the count in memory and queues the MIN re-read.
    let applied = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert_eq!(applied.aggregate_updates.len(), 1);
    assert_eq!(
        applied.aggregate_updates[0].folded_value(),
        Some(crate::AggValue::CountStar(4)),
        "the delete folds exactly once, at apply time"
    );
    assert_eq!(e.pending_read_count(), 1, "the displaced MIN queues a read");

    // The read fails: the fold stands, the read stays queued.
    assert!(matches!(
        e.resolve_collect(),
        Err(ReExecError::Connector { subscription, .. }) if subscription == minimum
    ));
    assert_eq!(e.pending_read_count(), 1, "a failed read stays queued");

    // Retrying resolves the read alone: no second application.
    e.connector().values.borrow_mut().push(Value::Float(7.0));
    let resolved = e.resolve_collect().unwrap();
    assert_eq!(resolved.scalar_updates.len(), 1);
    assert_eq!(resolved.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.pending_read_count(), 0);
    assert_eq!(
        e.connector().call_count(),
        2,
        "one failed try, one successful retry, never a redispatch"
    );
}

/// An in-process row subscription whose predicate read a cell the event
/// did not carry is re-executed against the database, which is the half
/// of the missing-cell work the core cannot do: it holds no connector.
///
/// The core reports; the wrapper owns the connector and turns the report
/// into a read, which is the same ladder every other unresolvable
/// maintenance takes.
#[test]
fn an_unanswered_cell_is_re_executed_by_the_auto_engine() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
        (),
    )
    .expect("the filter is served in process");
    e.connector()
        .cursor_pages
        .borrow_mut()
        .push(crate::reexec::RowPage {
            columns: alloc::vec![String::from("id"), String::from("status")],
            rows: alloc::vec![alloc::vec![Value::Int(1), Value::String("paid".into())]],
            more: false,
        });

    // The row image omits `status`, which the predicate reads, so the
    // event cannot answer it: an unchanged TOASTed column arrives this
    // way.
    let mut cells = row(1, 5.0);
    cells[3] = Value::Missing;
    let event = TestEvent::<Postgres>::update(tid, row(1, 5.0), cells)
        .with_pk_columns([0u16])
        .with_changed_columns([1u16]);

    let applied = e.apply(&event).expect("the event applies");
    assert_eq!(
        applied.engine.unanswered().len(),
        1,
        "the core reports the subscription it could not answer"
    );
    assert_eq!(
        e.pending_read_count(),
        1,
        "and the wrapper queues a read for it"
    );

    let resolved = e.resolve_collect().expect("the read resolves");
    assert_eq!(
        resolved.rows_updates.len(),
        1,
        "the subscriber is given the answer the database holds"
    );
    assert_eq!(
        resolved.rows_updates[0].subscription_id,
        applied.engine.unanswered()[0].subscription_id
    );
}

/// Retaining the query does not make an in-process filter snapshottable.
/// The stream is how that answer is produced, so priming it from the
/// database was never part of its contract and still reads nothing.
#[test]
fn a_stream_served_filter_is_not_snapshotted() {
    let (mut e, _) = engine_with_values(alloc::vec![]);
    let id = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
            (),
        )
        .expect("the filter is served in process")
        .subscription_id;

    assert!(
        e.snapshot(id).unwrap().is_none(),
        "the stream answers this subscription, so there is nothing to prime"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "and no read is issued for it"
    );
}
