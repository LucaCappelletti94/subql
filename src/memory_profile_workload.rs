//! Memory profiling with dhat
//!
//! Run with: cargo run --release --features dhat-heap --bin memory_profile
#![allow(clippy::unwrap_used, clippy::unreadable_literal)]
#![allow(clippy::print_stdout, clippy::unnecessary_cast)]

use alloc::vec::Vec;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::{Postgres, Value};
use crate::testing::workload::{
    bounded_i64, mix_seed, realistic_tree_sql, realistic_workload_seed, status_for,
};
use crate::testing::TestEvent;
use crate::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// Build the bench fixture catalog as a [`ParserDB`]. A placeholder table
/// before `orders` keeps the orders table id stable at 1 (matching the
/// hardcoded `TestEvent::<Postgres>::insert(1, ...)` in `make_test_event`).
fn bench_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE _bench_pad (id INT);\n\
         CREATE TABLE orders (\
             id INT PRIMARY KEY, user_id INT, amount INT, status TEXT, \
             priority INT, quantity INT, discount INT, tax INT, shipping INT, \
             created_at INT\
         );",
    )
    .expect("bench fixture DDL parses")
}

fn make_test_event(seed: u64) -> TestEvent<Postgres> {
    let id = 1 + bounded_i64(seed ^ 0x1A2A, 500_000);
    let user_id = bounded_i64(seed ^ 0x2B3B, 20_000);
    let amount = 30 + bounded_i64(seed ^ 0x3C4C, 3_500);
    let priority = 1 + bounded_i64(seed ^ 0x4D5D, 9);
    let quantity = 1 + bounded_i64(seed ^ 0x5E6E, 40);
    let discount = if mix_seed(seed ^ 0x6F7F).is_multiple_of(5) {
        Value::<Postgres>::Null
    } else {
        Value::<Postgres>::Int(bounded_i64(seed ^ 0x7A8A, 18))
    };
    let tax = 2 + bounded_i64(seed ^ 0x8B9B, 40);
    let shipping = 4 + bounded_i64(seed ^ 0x9CAC, 30);
    let created_at = 1_699_500_000 + bounded_i64(seed ^ 0xADBD, 240 * 24 * 3600);
    let status = status_for(seed ^ 0xBECF);

    TestEvent::<Postgres>::insert(
        1,
        vec![
            Value::Int(id),
            Value::Int(user_id),
            Value::Int(amount),
            Value::String(status.into()),
            Value::Int(priority),
            Value::Int(quantity),
            discount,
            Value::Int(tax),
            Value::Int(shipping),
            Value::Int(created_at),
        ],
    )
    .with_pk_columns([0u16])
}

pub fn run_memory_profile(show_progress: bool) {
    println!("SubQL Memory Profiling");
    println!("======================");
    println!();

    let mut engine = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        bench_catalog(),
        PostgreSqlDialect {},
    );

    println!("Registering 100,000 predicates with realistic tree shapes...");
    if !show_progress {
        println!("Progress logs disabled (set SUBQL_MEMORY_BENCH_PROGRESS=1 to enable)");
    }

    for i in 0_u64..100_000 {
        let spec =
            SubscriptionRequest::new(i % 10_000, realistic_tree_sql(realistic_workload_seed(i)));
        engine.register(spec).unwrap();

        if show_progress && (i + 1) % 10_000 == 0 {
            println!("  {} predicates registered", i + 1);
        }
    }

    println!();
    println!("Dispatching 1,000 events from a rotating event corpus...");

    let event_corpus: Vec<TestEvent<Postgres>> = (0_u64..32)
        .map(|seed| make_test_event(seed ^ 0x1234_5678_9ABC_DEF0))
        .collect();
    let event_corpus_len_u64 = u64::try_from(event_corpus.len()).unwrap_or(1);

    for i in 0_u64..1_000 {
        let event_idx_u64 = i % event_corpus_len_u64;
        let event_idx = usize::try_from(event_idx_u64).unwrap_or(0);
        let user_count = engine
            .consumers(&event_corpus[event_idx])
            .unwrap()
            .into_iter()
            .count();

        if show_progress && (i + 1) % 100 == 0 {
            println!(
                "  {} events dispatched (matched {} users)",
                i + 1,
                user_count
            );
        }
    }

    println!();
    println!("Memory profiling complete!");
    println!();
    println!("Results:");
    println!("  Total subscriptions: {}", engine.subscription_count());
    println!();

    #[cfg(feature = "dhat-heap")]
    println!("Check dhat-heap.json for detailed memory profile");

    #[cfg(not(feature = "dhat-heap"))]
    println!("Run with --features dhat-heap to enable memory profiling");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventKind;

    #[test]
    fn realistic_tree_sql_is_deterministic_for_seed() {
        let first = realistic_tree_sql(42);
        let second = realistic_tree_sql(42);
        assert_eq!(first, second);
        assert!(first.starts_with("SELECT * FROM orders WHERE"));
    }

    #[test]
    fn make_test_event_is_deterministic_for_seed() {
        let event_a = make_test_event(1234);
        let event_b = make_test_event(1234);
        assert_eq!(event_a.kind, EventKind::Insert);
        assert_eq!(event_a.table_id, 1);
        assert_eq!(event_a.pk_columns, event_b.pk_columns);
        assert_eq!(event_a.new_row, event_b.new_row);
    }
}
