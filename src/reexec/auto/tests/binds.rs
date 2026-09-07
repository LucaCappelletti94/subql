//! Registration binds reaching each tier's read.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn sync_scalar_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(alloc::vec![Value::Float(5.0)]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(alloc::vec![Value::Int(2)]),
            (),
        )
        .expect("scalar registers")
        .subscription_id;

    engine
        .snapshot(subscription)
        .expect("snapshot succeeds")
        .expect("snapshot exists");
    let queries = engine.connector().scalar_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(2)]);
}

#[test]
fn sync_scalar_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![Value::Float(9.0)]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(alloc::vec![Value::Int(0)]),
            (),
        )
        .expect("scalar registers")
        .subscription_id;
    crate::Install::install(
        &mut engine,
        subscription,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .expect("scalar installs");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    engine.resolve_collect().expect("delete resolves");
    let queries = engine.connector().scalar_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
}

#[test]
fn sync_keyed_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(alloc::vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(alloc::vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");
    let Tier::KeyedRows { ref query, .. } = registered.tier else {
        panic!("expected keyed rows")
    };

    let _ = engine.snapshot(registered.subscription_id);
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), query.sql());
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

#[test]
fn sync_keyed_event_scopes_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(alloc::vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = engine.resolve_collect();
    let queries = engine.connector().page_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE (lower(status) = $1) AND \"id\" IN (1)"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

/// A keyed read asks about the row the event says is its own.
///
/// The key is built from the columns the event declares as its
/// primary key, in `KeyedQuery::on_event`, so a declaration naming
/// the wrong column asks the database about the wrong row. Two
/// inserts are used rather than one, and their ids differ, because
/// one insert cannot tell a correct declaration from one naming a
/// column that happens to hold the same value in every fixture row:
/// `quantity` is always 1 here, so pointing the key at it collapses
/// both rows onto a single key and the read asks about half of what
/// changed.
///
/// The delete path was already covered by
/// `sync_keyed_event_scopes_registration_binds`. This is the insert
/// path, which nothing depended on: pointing the shared fixture's
/// insert at another column reddened no test in the suite.
#[test]
fn sync_keyed_insert_asks_about_the_declared_key() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = 'paid'"),
            (),
        )
        .expect("keyed read registers");

    engine.apply(&insert_event(table, 1, 5.0)).unwrap();
    engine.apply(&insert_event(table, 2, 6.0)).unwrap();
    let _ = engine.resolve_collect();

    let queries = engine.connector().page_queries.borrow();
    assert_eq!(queries.len(), 1, "both keys are asked in one read");
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE (lower(status) = 'paid') AND \"id\" IN (1, 2)",
        "the read names the id column and both ids"
    );
}

#[test]
fn sync_grouped_bootstrap_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(alloc::vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(alloc::vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers");
    let Tier::GroupedScalar { ref bootstrap } = registered.tier else {
        panic!("expected grouped scalar")
    };

    let _ = engine.snapshot(registered.subscription_id);
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), bootstrap.query.sql());
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
}

#[test]
fn sync_grouped_scoped_read_orders_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(alloc::vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers")
        .subscription_id;
    crate::Install::install(
        &mut engine.inner,
        subscription,
        crate::GroupedScalarSeedInstall {
            rows: alloc::vec![alloc::vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(2),
            ]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("grouped seed installs");
    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = engine.resolve_collect();
    let queries = engine.connector().page_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT MIN(\"price\") AS v, COUNT(*) AS c1 FROM orders WHERE (quantity > $1) AND \"status\" = $2"
    );
    assert_eq!(
        queries[0].binds(),
        &[Value::Int(0), Value::String("paid".into())]
    );
}

#[test]
fn whole_snapshot_forwards_registration_binds() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    let qid = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(alloc::vec![Value::Int(3)]),
            (),
        )
        .unwrap()
        .subscription_id;

    let _ = e.snapshot(qid);

    let queries = e.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
}

#[test]
fn sync_whole_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(alloc::vec![Value::Int(3)]),
            (),
        )
        .expect("whole read registers");

    engine.apply(&insert_event(table, 2, 9.0)).unwrap();
    let _ = engine.resolve_collect();
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders ORDER BY id DESC LIMIT $1"
    );
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
}

#[test]
fn registration_request_metadata_comes_from_consumed_request() {
    let session = 91u64;
    let request = DisagreeingRegistrationRequest(
        SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
            .binds(alloc::vec![Value::Int(3)])
            .scope(SubscriptionScope::Session(session)),
    );
    let (mut engine, _) = engine_with_values(alloc::vec![]);
    let subscription = engine
        .register(request, ())
        .expect("request registers")
        .subscription_id;

    let _ = engine.snapshot(subscription);
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
    drop(queries);

    let _ = engine.unregister_session(session);
    assert!(engine.contexts.is_empty());
}
