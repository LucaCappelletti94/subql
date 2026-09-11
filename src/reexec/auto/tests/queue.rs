//! The queue of pending reads: coalescing, displacement and reclamation.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// `outstanding` is the queue's depth now: not a constant, not one per
/// event, and not the number ever queued.
#[test]
fn a_dispatch_reports_the_reads_it_queued() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .expect("whole read registers");

    // An event no subscription cares about queues nothing.
    let quiet = engine
        .apply(&insert_event(table, 1, 5.0))
        .expect("the event applies");
    let after_first = quiet.outstanding;

    engine
        .connector()
        .cursor_pages
        .borrow_mut()
        .push(crate::reexec::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: false,
        });
    assert_eq!(
        after_first,
        engine.pending_read_count(),
        "the report is the depth of the queue, not a guess about it"
    );
    assert!(
        after_first > 0,
        "a whole-result subscription queues a read for this event"
    );

    // A second event for the same subscription coalesces: the queue is
    // keyed by subscription, so the depth does not grow.
    let again = engine
        .apply(&insert_event(table, 2, 6.0))
        .expect("the event applies");
    assert_eq!(
        again.outstanding, after_first,
        "a burst coalesces, so the report does not count events"
    );

    // Depth two, so a count collapsed to `min(1)` cannot pass.
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT DISTINCT quantity FROM orders"),
            (),
        )
        .expect("a second whole read registers");
    let two = engine
        .apply(&insert_event(table, 4, 8.0))
        .expect("the event applies");
    assert_eq!(
        two.outstanding, 2,
        "two subscriptions each queued one read, so the depth is two"
    );

    // The mock refuses a cursor it has no page for.
    engine
        .connector()
        .cursor_pages
        .borrow_mut()
        .push(crate::reexec::RowPage {
            columns: alloc::vec![String::from("quantity")],
            rows: alloc::vec![alloc::vec![Value::Int(1)]],
            more: false,
        });
    engine.resolve_collect().expect("the reads run");
    let settled = engine
        .apply(&update_status_only(table, 3, 7.0))
        .expect("the event applies");
    assert_eq!(
        engine.pending_read_count(),
        settled.outstanding,
        "and it still agrees with the queue after a drain"
    );
}

/// T4.1 + T4.2: a batch of 3 events that displace the same captured
/// query's extreme produces ONE connector call (dedup), and engine
/// notifications come back in input order.
#[test]
fn applied_burst_coalesces_repeated_triggers() {
    // Connector serves a single value, which is what we expect since
    // the trigger should be deduplicated to one call.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(99.0)]);
    // Bootstrap: extreme is 5.0.
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    // Three DELETEs of the current extreme. Each one in isolation
    // would emit a trigger, the batch should collapse them.
    let events = alloc::vec![
        delete_event(tid, 1, 5.0),
        delete_event(tid, 2, 5.0),
        delete_event(tid, 3, 5.0),
    ];

    let per_event: alloc::vec::Vec<_> = events.iter().map(|ev| e.apply(ev).unwrap()).collect();
    let resolve_outcome = e.resolve_collect().unwrap();
    assert_eq!(
        per_event.len(),
        3,
        "per_event must align positionally with input"
    );
    assert_eq!(
        e.connector().call_count(),
        1,
        "three displacing events must collapse to one connector call"
    );
    assert_eq!(resolve_outcome.scalar_updates.len(), 1);
    assert_eq!(resolve_outcome.scalar_updates[0].value, Value::Float(99.0));
    assert_eq!(e.pending_read_count(), 0, "auto-resolving drains reads");
}

/// T4.3: a connector failure mid-batch aborts the whole batch with
/// `ReExecError::Connector` and the caller is expected to retry.
#[test]
fn applied_burst_error_surfaces_from_resolve() {
    // Empty value queue: connector errors on first call.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
        5.0,
    );

    let events = alloc::vec![delete_event(tid, 1, 5.0)];
    for ev in &events {
        e.apply(ev).unwrap();
    }
    match e.resolve_collect() {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError::Unstaged(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}

/// Coalescing only collapses the **same** `subscription_id`. Distinct captured
/// queries each trigger their own connector call.
#[test]
#[allow(clippy::similar_names)]
fn applied_burst_keeps_distinct_queries_apart() {
    // Two captured queries on the same table. Connector returns 11.0
    // (popped first) for one and 22.0 (popped second) for the other.
    // MockConnector pops from the back, so push values in reverse:
    // first pop = 22.0, second pop = 11.0.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(22.0), Value::Float(11.0)]);
    let qid1 = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    let qid2 = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        2u64,
        "SELECT MAX(price) FROM orders",
    );
    // Bootstrap both at 7.0 so deleting price=7.0 displaces both.
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

    let events = alloc::vec![delete_event(tid, 1, 7.0)];
    for ev in &events {
        e.apply(ev).unwrap();
    }
    let resolve_outcome = e.resolve_collect().unwrap();
    assert_eq!(e.connector().call_count(), 2, "one call per distinct query");
    assert_eq!(resolve_outcome.scalar_updates.len(), 2);
    let qids: alloc::collections::BTreeSet<_> = resolve_outcome
        .scalar_updates
        .iter()
        .map(|u| u.subscription_id)
        .collect();
    assert!(qids.contains(&qid1));
    assert!(qids.contains(&qid2));
}

#[test]
fn grouped_batch_keeps_one_trigger_per_displaced_group() {
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
            rows: vec![
                vec![
                    Value::String("paid".into()),
                    Value::Float(5.0),
                    Value::Int(2),
                ],
                vec![
                    Value::String("void".into()),
                    Value::Float(7.0),
                    Value::Int(2),
                ],
            ],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("group map installs");
    let delete = |id, price, status: &str| {
        TestEvent::<Postgres>::delete(
            table,
            vec![
                Value::Int(id),
                Value::Float(price),
                Value::Int(1),
                Value::String(status.into()),
            ],
        )
        .with_pk_columns([0u16])
    };
    engine
        .apply(&delete(1, 5.0, "paid"))
        .expect("first delete dispatches");
    engine
        .apply(&delete(2, 7.0, "void"))
        .expect("second delete dispatches");
    assert_eq!(
        engine.pending_read_count(),
        2,
        "one queued read per displaced group, never coalesced across groups"
    );
}

/// An install failure is deterministic: the database answer does not
/// match the subscription, and the same read returns the same answer.
/// The failing read is dropped rather than requeued, exactly as the
/// resolve contract documents, so it cannot block every read behind it.
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
    let error = engine.resolve_collect().unwrap_err();
    assert!(
        matches!(error, crate::reexec::ReExecError::AggregateInstall(_)),
        "the row count mismatch reports as an aggregate install failure, got {error:?}"
    );
    assert_eq!(
        engine.pending_read_count(),
        0,
        "a non-retryable read is dropped, never requeued"
    );
    engine
        .resolve_collect()
        .expect("the next resolve is a clean no-op");
}

/// Removing reads by key must not leave storage behind: the async
/// resolver drains exclusively through key removal and never pops, so
/// consumed entries have to be reclaimed or the queue grows for the
/// process lifetime and every snapshot rescans dead history.
#[test]
fn key_removal_reclaims_queue_storage() {
    let mut queue: super::ReadQueue<DefaultIds, crate::NoCheckpoint, Postgres> =
        super::ReadQueue::new();
    for round in 0..64u64 {
        queue.enqueue(crate::reexec::ReExecutionTrigger {
            subscription_id: round,
            consumer_id: 1u64,
            read: crate::reexec::ReExecutionRead::Subscription,
            checkpoint: None,
        });
        queue.remove(round, None);
    }
    assert!(queue.is_empty(), "every queued read was removed");
    assert!(
        queue.entry_slots() <= 1,
        "consumed entries are reclaimed, got {} slots for an empty queue",
        queue.entry_slots()
    );
}

/// A burst of displacements queues one read per subscription: the queue
/// dedup is what the deleted batch entry point implemented separately.
#[test]
fn burst_of_displacements_costs_one_read() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(9.0)]);
    let qid = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
        .subscription_id;
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
    e.apply(&delete_event(tid, 2, 5.0)).unwrap();
    assert_eq!(e.pending_read_count(), 1, "same subscription, one read");

    let resolved = e.resolve_collect().unwrap();
    assert_eq!(e.connector().call_count(), 1, "the burst costs one read");
    assert_eq!(resolved.scalar_updates.len(), 1);
    assert_eq!(resolved.scalar_updates[0].value, Value::Float(9.0));
}
