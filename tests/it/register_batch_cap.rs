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
