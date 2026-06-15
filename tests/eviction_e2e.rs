//! Docker-backed end-to-end tests for registry-cap eviction policies.
//!
//! Spins up a real Postgres with `wal2json`, registers subscriptions on a
//! capped engine, drives real DML through the wal2json slot, parses the
//! events with [`subql::Wal2JsonV2Parser`], and feeds them through
//! [`subql::SubscriptionEngine::consumers`]. Asserts that:
//!
//! - `EvictOldest` evicts the right victim under live WAL flow and the
//!   evicted subscription stops receiving notifications.
//! - `EvictLeastActive` stamps activity from real dispatch (not synthetic
//!   events) and picks the least recently stamped subscription.
//! - `register_batch` enforces the cap end-to-end: surviving subscriptions
//!   match subsequent real WAL events; evicted ones do not.
//!
//! `#[ignore]`d; run with:
//!
//! ```sh
//! cargo test --test eviction_e2e -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    catalog_helpers, ClockHandle, DefaultIds, EvictionPolicy, ManualClock, PgLsn, RegisterError,
    SubscriptionEngine, SubscriptionRequest, Wal2JsonV2Parser, WalEvent, WalParser,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

fn drain_typed(
    parser: &Wal2JsonV2Parser,
    catalog: &ParserDB,
    msgs: &[String],
) -> Vec<WalEvent<PgLsn>> {
    msgs.iter()
        .flat_map(|m| {
            parser
                .parse_wal_message(m.as_bytes(), catalog)
                .expect("wal2json parse")
        })
        .collect()
}

/// `EvictOldest` end-to-end: after the oldest subscription is evicted to
/// make room for a third one, a real WAL event matching the evicted sub's
/// predicate is delivered only to the surviving subscriptions.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn evict_oldest_drops_subscription_from_dispatch_path() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_evict_oldest_slot";
    common::create_slot(&mut setup, slot);

    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let _orders_id = catalog_helpers::table_id(&*catalog, "orders").unwrap();
    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(Arc::clone(&catalog), PostgreSqlDialect {})
            .with_max_subscriptions(2, EvictionPolicy::EvictOldest);

    let s_oldest = engine
        .register(SubscriptionRequest::new(
            10u64,
            "SELECT * FROM orders WHERE id = 1",
        ))
        .expect("oldest");
    let s_keep = engine
        .register(SubscriptionRequest::new(
            20u64,
            "SELECT * FROM orders WHERE id = 2",
        ))
        .expect("keep");
    let s_new = engine
        .register(SubscriptionRequest::new(
            30u64,
            "SELECT * FROM orders WHERE id = 3",
        ))
        .expect("third causes eviction");
    assert_eq!(
        s_new.evicted,
        vec![s_oldest.subscription_id],
        "EvictOldest must evict the lowest sub id"
    );
    assert_eq!(engine.subscription_count(), 2);

    // Real WAL: insert id=1 (matches the evicted predicate), id=2 (matches
    // the survivor), id=3 (matches the newcomer).
    sql_query("INSERT INTO orders VALUES (1, 5.0)")
        .execute(&mut dml)
        .expect("insert id=1");
    sql_query("INSERT INTO orders VALUES (2, 5.0)")
        .execute(&mut dml)
        .expect("insert id=2");
    sql_query("INSERT INTO orders VALUES (3, 5.0)")
        .execute(&mut dml)
        .expect("insert id=3");

    let msgs = common::drain_slot(&mut setup, slot);
    let parser = Wal2JsonV2Parser;
    let events = drain_typed(&parser, &catalog, &msgs);

    let mut matched_consumers: Vec<u64> = Vec::new();
    for ev in &events {
        let notifs = engine.consumers(ev).expect("dispatch");
        matched_consumers.extend(notifs.inserted().iter().copied());
    }

    assert!(
        !matched_consumers.contains(&10),
        "evicted consumer (10) must NOT appear in dispatch results, got {matched_consumers:?}"
    );
    assert!(
        matched_consumers.contains(&20),
        "kept consumer (20) must match id=2 event"
    );
    assert!(
        matched_consumers.contains(&30),
        "new consumer (30) must match id=3 event"
    );

    let _ = s_keep;
    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// `EvictLeastActive` end-to-end: real WAL events stamp activity through
/// `consumers()`. The subscription whose most-recent stamp is older gets
/// evicted when the cap fills.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn evict_least_active_uses_real_wal_dispatch_timestamps() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_evict_least_active_slot";
    common::create_slot(&mut setup, slot);

    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let clock = Arc::new(ManualClock::new(0));
    #[allow(clippy::clone_on_ref_ptr)] // explicit dyn-trait unsize coercion
    let handle: ClockHandle = clock.clone();
    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(Arc::clone(&catalog), PostgreSqlDialect {})
            .with_max_subscriptions(2, EvictionPolicy::EvictLeastActive)
            .with_activity_clock(handle);

    let s_a = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE id = 1",
        ))
        .expect("s_a");
    let s_b = engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE id = 2",
        ))
        .expect("s_b");

    // Stamp s_a first via a real WAL event.
    clock.advance(Duration::from_micros(100));
    sql_query("INSERT INTO orders VALUES (1, 5.0)")
        .execute(&mut dml)
        .expect("insert id=1");
    let parser = Wal2JsonV2Parser;
    let events = drain_typed(&parser, &catalog, &common::drain_slot(&mut setup, slot));
    for ev in &events {
        engine.consumers(ev).expect("dispatch id=1");
    }

    // Stamp s_b later.
    clock.advance(Duration::from_millis(1));
    sql_query("INSERT INTO orders VALUES (2, 9.0)")
        .execute(&mut dml)
        .expect("insert id=2");
    let events = drain_typed(&parser, &catalog, &common::drain_slot(&mut setup, slot));
    for ev in &events {
        engine.consumers(ev).expect("dispatch id=2");
    }

    // Register s_c -> evicts s_a (lowest last_dispatch_at).
    let s_c = engine
        .register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM orders WHERE id = 3",
        ))
        .expect("s_c");
    assert_eq!(
        s_c.evicted,
        vec![s_a.subscription_id],
        "least active (oldest stamp) is evicted; got {:?}",
        s_c.evicted
    );

    // Real WAL: insert id=1 — s_a was the only subscriber, and it has
    // been evicted, so nobody matches.
    sql_query("INSERT INTO orders VALUES (10, 5.0)")
        .execute(&mut dml)
        .expect("insert id=10");
    sql_query("UPDATE orders SET price = 6.0 WHERE id = 1")
        .execute(&mut dml)
        .expect("update id=1");
    let events = drain_typed(&parser, &catalog, &common::drain_slot(&mut setup, slot));
    let mut hits: Vec<u64> = Vec::new();
    for ev in &events {
        let notifs = engine.consumers(ev).expect("dispatch");
        hits.extend(notifs.inserted().iter().copied());
        hits.extend(notifs.updated().iter().copied());
    }
    assert!(
        !hits.contains(&1),
        "consumer 1 was evicted; should not see id=1 events, got {hits:?}"
    );

    let _ = s_b;
    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// `register_batch` with `EvictOldest` end-to-end: a batch that exceeds the
/// cap surfaces per-spec evictions, and the surviving subscriptions match
/// real WAL events while the evicted ones do not.
///
/// Note: `pick_eviction_victim` only inspects already-committed bindings
/// in `subscription_to_table`, not the in-flight batch entries that are
/// deferred to phase 3. So a batch can only evict subscriptions that
/// existed before the batch started. The test exercises that contract by
/// pre-registering enough subscriptions to absorb every over-cap eviction.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn register_batch_cap_eviction_round_trips_through_wal() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_evict_batch_slot";
    common::create_slot(&mut setup, slot);

    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(Arc::clone(&catalog), PostgreSqlDialect {})
            .with_max_subscriptions(2, EvictionPolicy::EvictOldest);

    // Pre-fill the cap with two subscriptions. The batch below will evict
    // both of them, one per over-cap entry, leaving only the batch entries
    // live afterwards.
    let pre1 = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE id = 1",
        ))
        .expect("pre1");
    let pre2 = engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE id = 2",
        ))
        .expect("pre2");
    assert_eq!(engine.subscription_count(), 2);

    let results = engine.register_batch(vec![
        SubscriptionRequest::new(3u64, "SELECT * FROM orders WHERE id = 3"),
        SubscriptionRequest::new(4u64, "SELECT * FROM orders WHERE id = 4"),
    ]);
    let r0 = results[0].as_ref().expect("entry 0");
    let r1 = results[1].as_ref().expect("entry 1");
    assert_eq!(
        r0.evicted,
        vec![pre1.subscription_id],
        "first over-cap entry evicts pre1 (oldest committed sub)"
    );
    assert_eq!(
        r1.evicted,
        vec![pre2.subscription_id],
        "second over-cap entry evicts pre2 (next oldest committed sub)"
    );
    assert_eq!(engine.subscription_count(), 2);

    // Real WAL: insert id=1..=4. Consumers 1 and 2 must NOT see anything
    // (they were evicted). Consumers 3 and 4 must see their own id.
    for id in 1..=4 {
        sql_query(format!("INSERT INTO orders VALUES ({id}, 5.0)"))
            .execute(&mut dml)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
    }
    let parser = Wal2JsonV2Parser;
    let events = drain_typed(&parser, &catalog, &common::drain_slot(&mut setup, slot));
    let mut hits: Vec<u64> = Vec::new();
    for ev in &events {
        let notifs = engine.consumers(ev).expect("dispatch");
        hits.extend(notifs.inserted().iter().copied());
    }

    assert!(
        !hits.contains(&1),
        "consumer 1 was evicted, got hits {hits:?}"
    );
    assert!(
        !hits.contains(&2),
        "consumer 2 was evicted, got hits {hits:?}"
    );
    assert!(
        hits.contains(&3),
        "consumer 3 survived and should match id=3, got hits {hits:?}"
    );
    assert!(
        hits.contains(&4),
        "consumer 4 survived and should match id=4, got hits {hits:?}"
    );

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// `register_batch` documents the within-batch eviction limit: a batch
/// that would need to evict from its own pending entries surfaces
/// `RegistryFull` for those entries. Caller can recover by re-issuing
/// them in a follow-up call after the previous ones have committed.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn register_batch_cannot_evict_in_flight_entries() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_evict_batch_limit_slot";
    common::create_slot(&mut setup, slot);

    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(Arc::clone(&catalog), PostgreSqlDialect {})
            .with_max_subscriptions(1, EvictionPolicy::EvictOldest);

    let _pre = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE id = 1",
        ))
        .expect("pre");

    // The batch wants three more subs against a cap of 1 with only one
    // committed sub available to evict. Entry 0 evicts `_pre`; entries
    // 1/2 would need to evict from the batch's own pending state, which
    // is not supported, so they return RegistryFull.
    let results = engine.register_batch(vec![
        SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE id = 2"),
        SubscriptionRequest::new(3u64, "SELECT * FROM orders WHERE id = 3"),
        SubscriptionRequest::new(4u64, "SELECT * FROM orders WHERE id = 4"),
    ]);
    assert!(results[0].is_ok(), "entry 0 evicts the committed pre");
    assert!(matches!(
        results[1],
        Err(RegisterError::RegistryFull { cap: 1 })
    ));
    assert!(matches!(
        results[2],
        Err(RegisterError::RegistryFull { cap: 1 })
    ));

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// `EvictionPolicy::Reject` end-to-end: the over-cap registration fails
/// with `RegistryFull`, and a real WAL event still reaches the
/// already-registered subscribers.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn reject_keeps_existing_subscriptions_intact() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_evict_reject_slot";
    common::create_slot(&mut setup, slot);

    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(Arc::clone(&catalog), PostgreSqlDialect {})
            .with_max_subscriptions(1, EvictionPolicy::Reject);

    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE id = 1",
        ))
        .expect("first fits");
    match engine.register(SubscriptionRequest::new(
        2u64,
        "SELECT * FROM orders WHERE id = 2",
    )) {
        Err(RegisterError::RegistryFull { cap: 1 }) => {}
        other => panic!("expected RegistryFull, got {other:?}"),
    }

    sql_query("INSERT INTO orders VALUES (1, 5.0)")
        .execute(&mut dml)
        .expect("insert id=1");
    let parser = Wal2JsonV2Parser;
    let events = drain_typed(&parser, &catalog, &common::drain_slot(&mut setup, slot));
    let mut hits: Vec<u64> = Vec::new();
    for ev in &events {
        let notifs = engine.consumers(ev).expect("dispatch");
        hits.extend(notifs.inserted().iter().copied());
    }
    assert_eq!(
        hits,
        vec![1],
        "the first subscriber must still receive matches"
    );

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}
