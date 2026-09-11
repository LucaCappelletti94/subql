//! The row tiers: cursors, replay, pages and generations.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn async_cursor_state_is_reachable_through_the_wrapper() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let session = 1u64;
    let sub = 7u64;
    let cp = crate::OpaqueCheckpoint(vec![1, 2, 3]);
    assert_eq!(e.advance_cursor(session, sub, cp.clone()), Ok(None));
    assert_eq!(e.cursor_for(session, sub), Some(&cp));
    let older = crate::OpaqueCheckpoint(vec![0]);
    assert_eq!(e.force_set_cursor(session, sub, older.clone()), Some(cp));
    assert_eq!(e.cursor_for(session, sub), Some(&older));
    let listed: Vec<_> = e
        .cursors_for_session(session)
        .map(|(s, c)| (s, c.clone()))
        .collect();
    assert_eq!(listed, vec![(sub, older.clone())]);
    assert_eq!(e.drop_cursor(session, sub), Some(older));
    assert_eq!(e.cursor_for(session, sub), None);
}

#[test]
fn async_match_rows_replays_without_reading_or_folding() {
    // match_rows must be a plain sync call here: the inner match does no
    // I/O, so no block_on wraps it. One value, for the single live read.
    let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
    let min_id = crate::reexec::test_fixtures::register_scalar_query(
        &mut e,
        1u64,
        "SELECT MIN(price) FROM orders",
    );
    crate::Install::install(
        &mut e,
        min_id,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    e.register(
        SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE price < 100"),
        (),
    )
    .unwrap();

    // A delete of the current extreme, replayed against the seeded model
    // before any live dispatch has moved it, so it is still displacing.
    let ev = delete_event(tid, 1, 5.0);
    let replay = e.match_rows(&ev).unwrap();
    assert!(
        !replay.deleted().is_empty(),
        "match_rows matched the row subscription"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "match_rows read nothing from the connector even for a displacing delete"
    );

    // The live dispatch of the same delete is still the first read: proof
    // match_rows left the re-execution model untouched.
    e.apply(&ev).unwrap();
    let live = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        1,
        "the re-execution model was untouched, so the live read is the first"
    );
    assert_eq!(live.scalar_updates.len(), 1);
    assert_eq!(live.scalar_updates[0].value, Value::Float(7.0));
}

#[test]
fn async_describe_terms_is_reachable_through_the_wrapper() {
    // Sync method even on the async wrapper: it only reads the engine's
    // compiler, no I/O, so no block_on.
    let (e, _tid) = engine_with_values(vec![]);
    let plain = e
        .describe_terms(&SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE price > 100",
        ))
        .expect("a plain filter is describable");
    assert!(plain.is_empty(), "a plain filter has no membership terms");
    let refused = e.describe_terms(&SubscriptionRequest::new(
        2u64,
        "SELECT * FROM orders WHERE nonexistent_column > 5",
    ));
    assert!(
        refused.is_err(),
        "an unknown-column filter is refused, got {refused:?}"
    );
}

#[test]
fn async_ordered_row_query_folds_in_process() {
    let (mut e, tid) = engine_with_values(vec![]);
    match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY price"),
            (),
        )
        .unwrap()
    {
        Registered {
            tier: Tier::InProcess(_),
            ..
        } => {}
        other => panic!("expected InProcess for an ordered row query, got {other:?}"),
    }
    let n = e.apply(&insert_event(tid, 1, 5.0)).unwrap();
    assert!(
        n.engine.inserted().contains(&1),
        "the ordered row list is notified of the insert"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "an ordered row list reads nothing"
    );
}

/// Each page reaches the sink before the next page is fetched, so
/// retained memory tracks one page rather than the whole answer.
#[test]
fn async_pages_reach_the_sink_before_the_next_fetch() {
    let (mut e, tid) = engine_with_values(vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.lock().extend([
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: true,
        },
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();

    let log = Arc::clone(&e.connector().log);
    block_on(e.resolve(move |delivery| {
        if matches!(delivery, crate::reexec::ReadDelivery::Rows(_)) {
            log.lock().push("deliver");
        }
    }))
    .unwrap();
    assert_eq!(
        *e.connector().log.lock(),
        ["open", "fetch", "deliver", "fetch", "deliver", "close"],
        "a page is delivered before the next one is fetched"
    );
}

/// A resolve dropped between pages leaves the read queued, and the retry
/// streams a complete answer under a higher generation, which is the
/// consumer's signal to discard the partial one.
#[test]
fn dropped_stream_is_superseded_by_a_higher_generation() {
    let (mut e, tid) = engine_with_values(vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.lock().extend([
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: true,
        },
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();

    // The second fetch suspends, and the future is dropped there: one
    // partial page was already delivered.
    *e.connector().pend_fetch_at.lock() = Some(1);
    let partial = Arc::new(Mutex::new(Vec::new()));
    {
        let partial = Arc::clone(&partial);
        let mut ctx = Context::from_waker(core::task::Waker::noop());
        let fut = e.resolve(move |delivery| {
            if let crate::reexec::ReadDelivery::Rows(page) = delivery {
                partial.lock().push(page.generation);
            }
        });
        let mut pinned = pin!(fut);
        assert!(
            pinned.as_mut().poll(&mut ctx).is_pending(),
            "the resolve suspends between pages"
        );
    }
    assert_eq!(partial.lock().len(), 1, "one partial page was delivered");
    assert_eq!(e.pending_read_count(), 1, "the dropped read stayed queued");

    // The retry streams a complete answer under a higher generation.
    e.connector()
        .cursor_pages
        .lock()
        .push(crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: false,
        });
    let retried = block_on(e.resolve_collect()).unwrap();
    assert_eq!(e.pending_read_count(), 0);
    assert!(!retried.rows_updates.is_empty());
    let partial_generation = partial.lock()[0];
    assert!(
        retried
            .rows_updates
            .iter()
            .all(|page| page.generation > partial_generation),
        "the complete answer supersedes the partial generation"
    );
    assert!(
        !retried.rows_updates.last().unwrap().more,
        "the retry ends its generation"
    );
}
