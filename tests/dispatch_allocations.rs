//! Allocation counts on the row-dispatch path, measured under `dhat`.
//!
//! Its own test binary because the global allocator is process-wide.
#![cfg(feature = "dhat-heap")]
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::sync::Mutex;
use subql::backend::{CdcEvent, Postgres};
use subql::testing::dispatch_fixtures::{bench_catalog_folded, make_test_event_folded, mix_seed};
use subql::wal::{Wal2JsonV2Event, Wal2JsonV2Reader};
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// `dhat` runs one profiler per process, and `cargo test` shares a process.
static PROFILER: Mutex<()> = Mutex::new(());

const EVENTS: u64 = 64;

/// Heap blocks one dispatch allocates, averaged over a warmed event corpus,
/// with `subscriptions` registrations of `sql` on the `orders` table.
fn blocks_per_event<E: CdcEvent<Backend = Postgres>>(
    subscriptions: u64,
    sql: &impl Fn(u64) -> String,
    event: &impl Fn(u64) -> E,
) -> u64 {
    let mut engine: SubscriptionEngine<E, DefaultIds, ParserDB> =
        SubscriptionEngine::new(bench_catalog_folded(), PostgreSqlDialect {});
    for i in 0..subscriptions {
        engine
            .register(SubscriptionRequest::new(i % 100, sql(i)))
            .unwrap();
    }
    let events: Vec<_> = (0..EVENTS).map(|i| event(mix_seed(i))).collect();
    for event in &events {
        engine.consumers(event).unwrap();
    }
    let before = dhat::HeapStats::get().total_blocks;
    for event in &events {
        engine.consumers(event).unwrap();
    }
    (dhat::HeapStats::get().total_blocks - before) / EVENTS
}

/// Asserts that ten times the fallback subscriptions, nearly all of them
/// evaluated, does not multiply the allocations of one dispatch.
fn assert_flat_in_subscriptions<E: CdcEvent<Backend = Postgres>>(
    sql: impl Fn(u64) -> String,
    event: impl Fn(u64) -> E,
) {
    let _serial = PROFILER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _profiler = dhat::Profiler::builder().testing().build();
    let few = blocks_per_event(100, &sql, &event);
    let many = blocks_per_event(1_000, &sql, &event);
    // A growing output `Vec` doubles a few more times, which is all the slack allowed.
    assert!(
        many <= few + 16,
        "{few} blocks per event at 100 subscriptions, {many} at 1000"
    );
}

/// A wal2json insert carrying only the key and the `folded` text cell,
/// which the wire format decodes into a fresh `String` on every read.
fn wal2json_insert(seed: u64) -> Wal2JsonV2Event {
    let json = format!(
        r#"{{"action":"I","schema":"public","table":"orders","columns":[{{"name":"id","type":"integer","value":{seed}}},{{"name":"folded","type":"text","value":"ship{seed}"}}]}}"#,
        seed = seed % 1_000
    );
    let mut reader = Wal2JsonV2Reader::new();
    reader.parse(br#"{"action":"B"}"#).unwrap();
    reader.parse(json.as_bytes()).unwrap().unwrap()
}

#[test]
fn an_integer_comparison_allocates_nothing_per_matched_predicate() {
    assert_flat_in_subscriptions(
        |i| format!("SELECT * FROM orders WHERE id <> {i}"),
        make_test_event_folded,
    );
}

#[test]
fn a_like_allocates_nothing_per_evaluated_predicate() {
    assert_flat_in_subscriptions(
        |i| format!("SELECT * FROM orders WHERE folded LIKE 'ship%{i}'"),
        make_test_event_folded,
    );
}

#[test]
fn a_text_comparison_allocates_nothing_per_evaluated_predicate() {
    assert_flat_in_subscriptions(
        |i| format!("SELECT * FROM orders WHERE folded <> 'ship{i}'"),
        make_test_event_folded,
    );
}

#[test]
fn a_wire_cell_is_decoded_once_per_event() {
    assert_flat_in_subscriptions(
        |i| format!("SELECT * FROM orders WHERE folded <> 'ship{i}'"),
        wal2json_insert,
    );
}
