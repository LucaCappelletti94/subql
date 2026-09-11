//! The row tiers: cursors, replay, pages and generations.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn cursor_state_is_reachable_through_the_wrapper() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    let session = 1u64;
    let sub = 7u64;
    let cp = crate::OpaqueCheckpoint(alloc::vec![1, 2, 3]);
    assert_eq!(e.advance_cursor(session, sub, cp.clone()), Ok(None));
    assert_eq!(e.cursor_for(session, sub), Some(&cp));
    // force_set bypasses the monotonic rule and returns the previous value.
    let older = crate::OpaqueCheckpoint(alloc::vec![0]);
    assert_eq!(e.force_set_cursor(session, sub, older.clone()), Some(cp));
    assert_eq!(e.cursor_for(session, sub), Some(&older));
    let listed: Vec<_> = e
        .cursors_for_session(session)
        .map(|(s, c)| (s, c.clone()))
        .collect();
    assert_eq!(listed, alloc::vec![(sub, older.clone())]);
    assert_eq!(e.drop_cursor(session, sub), Some(older));
    assert_eq!(e.cursor_for(session, sub), None);
}

#[test]
fn match_rows_replays_without_reading_or_folding() {
    // One value, for the single live re-execution below. match_rows reads
    // nothing, so it must never consume it.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);
    // Captured MIN: a delete of its extreme is a read on the live path, and
    // the connector call that read needs is the guard that match_rows stays
    // off the resolving path.
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
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    // In-process row subscription: gives match_rows a non-empty verdict.
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

    // The live dispatch of the same delete is still the first read, which
    // proves match_rows left the re-execution model untouched: it resolves
    // MIN once, to 7.0.
    e.apply(&ev).unwrap();
    let live = e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        1,
        "the re-execution model was untouched, so the live read is the first"
    );
    assert_eq!(live.scalar_updates.len(), 1);
    assert_eq!(live.scalar_updates[0].value, Value::Float(7.0));
}

#[test]
fn describe_terms_is_reachable_through_the_wrapper() {
    let (e, _tid) = engine_with_values(alloc::vec![]);
    // A filter naming no membership subquery describes as empty.
    let plain = e
        .describe_terms(&SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE price > 100",
        ))
        .expect("a plain filter is describable");
    assert!(plain.is_empty(), "a plain filter has no membership terms");
    // A filter subql cannot compile is refused, which proves the call
    // reaches the engine's compiler rather than returning a stub.
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
fn ordered_row_query_folds_in_process() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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

#[test]
fn ordered_row_query_with_a_window_stays_a_read_tier() {
    // A window changes membership, so ordering plus LIMIT/OFFSET stays a
    // whole-answer read tier rather than an in-process row list.
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    for sql in [
        "SELECT * FROM orders ORDER BY price LIMIT 3",
        "SELECT * FROM orders ORDER BY price OFFSET 5",
    ] {
        let reg = e.register(SubscriptionRequest::new(1u64, sql), ()).unwrap();
        assert!(
            !matches!(reg.tier, Tier::InProcess(_)),
            "a windowed order stays a read tier: {sql} classified {:?}",
            reg.tier
        );
    }
}

/// Each page reaches the sink before the next page is fetched, so
/// retained memory tracks one page rather than the whole answer.
#[test]
fn pages_reach_the_sink_before_the_next_fetch() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.borrow_mut().extend([
        crate::reexec::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: true,
        },
        crate::reexec::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();
    assert_eq!(e.pending_read_count(), 1);

    let log = alloc::rc::Rc::clone(&e.connector().log);
    e.resolve(|delivery| {
        if matches!(delivery, crate::reexec::ReadDelivery::Rows(_)) {
            log.borrow_mut().push("deliver");
        }
    })
    .unwrap();
    assert_eq!(
        *e.connector().log.borrow(),
        ["open", "fetch", "deliver", "fetch", "deliver", "close"],
        "a page is delivered before the next one is fetched"
    );
}

/// A whole read that fails part way delivers its retry under a higher
/// generation, exactly as the async path does.
///
/// The async side has pinned this since it was written
/// (`dropped_stream_is_superseded_by_a_higher_generation`), and the
/// sync side has carried the same bump since the read tier was added
/// without a test naming it. That is the drift this phase is about:
/// two copies of one rule, one of them unpinned, so a change to the
/// sync copy is caught by nothing. The consumer contract is the same
/// on both: a generation with no final page is partial, and a higher
/// generation is the signal to discard it.
#[test]
fn sync_whole_read_bumps_the_generation_like_the_async_path() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.borrow_mut().extend([
        crate::reexec::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: true,
        },
        crate::reexec::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();

    // The second fetch raises, so one partial page was delivered and
    // the generation it carried has no final page.
    *e.connector().fail_fetch_at.borrow_mut() = Some(1);
    let partial = alloc::rc::Rc::new(RefCell::new(alloc::vec::Vec::new()));
    {
        let partial = alloc::rc::Rc::clone(&partial);
        let outcome = e.resolve(move |delivery| {
            if let crate::reexec::ReadDelivery::Rows(page) = delivery {
                partial.borrow_mut().push(page.generation);
            }
        });
        assert!(outcome.is_err(), "the read failed part way through");
    }
    assert_eq!(partial.borrow().len(), 1, "one partial page was delivered");

    // The retry serves a complete answer under a higher generation.
    *e.connector().fail_fetch_at.borrow_mut() = None;
    e.connector()
        .cursor_pages
        .borrow_mut()
        .push(crate::reexec::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: false,
        });
    e.apply(&insert_event(tid, 2, 6.0)).unwrap();
    let retried = e.resolve_collect().expect("the retry reads");
    assert!(!retried.rows_updates.is_empty(), "the retry delivered rows");
    let partial_generation = partial.borrow()[0];
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
