//! Registration binds reaching each tier's read.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn async_scalar_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(vec![Value::Float(5.0)]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(vec![Value::Int(2)]),
            (),
        )
        .expect("scalar registers")
        .subscription_id;

    block_on(engine.snapshot(subscription))
        .expect("snapshot succeeds")
        .expect("snapshot exists");
    let queries = engine.connector().scalar_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(2)]);
    drop(queries);
}

#[test]
fn async_scalar_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![Value::Float(9.0)]);

    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(vec![Value::Int(0)]),
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

    engine
        .apply(&delete_event(table, 1, 5.0))
        .expect("apply succeeds");
    let _ = block_on(engine.resolve_collect()).expect("resolve succeeds");
    let queries = engine.connector().scalar_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
    drop(queries);
}

#[test]
fn async_keyed_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");
    let Tier::KeyedRows { ref query, .. } = registered.tier else {
        panic!("expected keyed rows")
    };

    let _ = block_on(engine.snapshot(registered.subscription_id));
    let queries = engine.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), query.sql());
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
    drop(queries);
}

#[test]
fn async_keyed_event_scopes_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = block_on(engine.resolve_collect());
    let queries = engine.connector().page_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE (lower(status) = $1) AND \"id\" IN (1)"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
    drop(queries);
}

#[test]
fn async_grouped_bootstrap_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers");
    let Tier::GroupedScalar { ref bootstrap } = registered.tier else {
        panic!("expected grouped scalar")
    };

    let _ = block_on(engine.snapshot(registered.subscription_id));
    let queries = engine.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), bootstrap.query.sql());
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
    drop(queries);
}

#[test]
fn async_grouped_scoped_read_orders_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers")
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
    .expect("grouped seed installs");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = block_on(engine.resolve_collect());
    let queries = engine.connector().page_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT MIN(\"price\") AS v, COUNT(*) AS c1 FROM orders WHERE (quantity > $1) AND \"status\" = $2"
    );
    assert_eq!(
        queries[0].binds(),
        &[Value::Int(0), Value::String("paid".into())]
    );
    drop(queries);
}

#[test]
fn async_whole_snapshot_forwards_registration_binds() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let qid = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(vec![Value::Int(3)]),
            (),
        )
        .unwrap()
        .subscription_id;

    let _ = block_on(e.snapshot(qid));

    let queries = e.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
    drop(queries);
}

#[test]
fn async_whole_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(vec![Value::Int(3)]),
            (),
        )
        .expect("whole read registers");

    engine.apply(&insert_event(table, 2, 9.0)).unwrap();
    let _ = block_on(engine.resolve_collect());
    let queries = engine.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders ORDER BY id DESC LIMIT $1"
    );
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
    drop(queries);
}
