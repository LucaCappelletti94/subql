//! The registry cap counts answers a batch has not committed yet.
//!
//! `register_batch` defers its writes to a later phase, so the cap
//! cannot be checked against the committed registry alone. It projects
//! the post-batch size as the committed size plus the answers already
//! accepted earlier in the same batch. Two specs that compile to the
//! same predicate share one predicate but remain two answers, and that
//! is the case where the projection is easiest to get wrong.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{DefaultIds, EvictionPolicy, RegisterError, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine_capped_at_two() -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    SubscriptionEngine::new(catalog, PostgreSqlDialect {})
        .with_max_subscriptions(2, EvictionPolicy::Reject)
}

/// A spec that shares its predicate with an earlier one still fills a seat.
///
/// The two matching filters compile to one predicate, so an accounting
/// that counted predicates rather than answers would leave room for the
/// third spec and overfill a registry that was capped at two.
#[test]
fn a_deduplicated_spec_in_a_batch_still_counts_against_the_cap() {
    let mut engine = engine_capped_at_two();

    let results = engine.register_batch(vec![
        SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
        SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE status = 'paid'"),
        SubscriptionRequest::new(3u64, "SELECT * FROM orders WHERE status = 'shipped'"),
    ]);

    assert!(
        results[0].is_ok() && results[1].is_ok(),
        "the first two fit, got {:?} and {:?}",
        results[0],
        results[1]
    );
    assert!(
        matches!(results[2], Err(RegisterError::RegistryFull { cap: 2 })),
        "the third finds the registry full, got {:?}",
        results[2]
    );
    assert_eq!(
        engine.subscription_count(),
        2,
        "and the cap holds across the batch"
    );
}

/// `EvictByConsumer` takes the seat from the consumer holding the most.
///
/// The policy exists to take a seat back from whoever holds more than
/// their share. Tallying holdings wrongly leaves every consumer looking
/// equal, at which point the deterministic tie breaker takes over and the
/// consumer holding one seat pays for the one holding two.
#[test]
fn evicting_by_consumer_takes_a_seat_from_the_largest_holder() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let mut engine: Engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {})
        .with_max_subscriptions(3, EvictionPolicy::EvictByConsumer);

    // Consumer 1 holds one seat and carries the lowest id, which is what
    // the tie breaker reaches for when no holder stands out.
    let lone = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("the lone answer registers")
        .subscription_id;
    let larger: Vec<_> = [
        "SELECT * FROM orders WHERE status = 'shipped'",
        "SELECT * FROM orders WHERE status = 'held'",
    ]
    .into_iter()
    .map(|filter| {
        engine
            .register(SubscriptionRequest::new(2u64, filter))
            .expect("the larger holder registers")
            .subscription_id
    })
    .collect();

    // The registry is full, so this one costs somebody a seat.
    engine
        .register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM orders WHERE status = 'void'",
        ))
        .expect("the newcomer registers by evicting");

    assert!(
        engine.unregister_subscription(lone),
        "the consumer holding one seat keeps it"
    );
    let surviving = larger
        .into_iter()
        .filter(|id| engine.unregister_subscription(*id))
        .count();
    assert_eq!(
        surviving, 1,
        "and exactly one of the two seats of the largest holder was taken"
    );
}
