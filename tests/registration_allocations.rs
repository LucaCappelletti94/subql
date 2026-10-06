//! Allocation scaling for distinct equality subscriptions.
#![cfg(feature = "dhat-heap")]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{DefaultIds, PgChangeEvent, SubscriptionEngine, SubscriptionRequest};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn blocks_for_registration(subscriptions: u64) -> u64 {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE items (id INT PRIMARY KEY, owner TEXT NOT NULL);",
    )
    .expect("catalog");
    let mut engine: SubscriptionEngine<PgChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let specs = (0..subscriptions)
        .map(|i| {
            SubscriptionRequest::new(i, format!("SELECT * FROM items WHERE owner = 'user-{i}'"))
        })
        .collect();
    for result in engine.register_batch(specs) {
        assert!(result
            .expect("preloaded equality filter")
            .served()
            .is_some());
    }
    let request = SubscriptionRequest::new(
        subscriptions,
        format!("SELECT * FROM items WHERE owner = 'user-{subscriptions}'"),
    );
    let before = dhat::HeapStats::get().total_blocks;
    let registered = engine.register(request).expect("distinct equality filter");
    assert!(registered.served().is_some());
    dhat::HeapStats::get().total_blocks - before
}

#[test]
fn a_distinct_equality_registration_does_not_copy_every_existing_value() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let few = blocks_for_registration(128);
    let many = blocks_for_registration(2_048);
    assert!(
        many <= few + 128,
        "{few} blocks at 128 subscriptions, {many} at 2048"
    );
}
