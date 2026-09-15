//! Memory profiling with dhat
//!
//! Run with: cargo run --release --features dhat-heap --bin memory_profile
#![allow(clippy::unwrap_used, clippy::unreadable_literal)]
#![allow(clippy::print_stdout, clippy::unnecessary_cast)]

use alloc::vec::Vec;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::Postgres;
use crate::testing::dispatch_fixtures::{
    bench_catalog, make_test_event, realistic_tree_sql, realistic_workload_seed,
};
use crate::testing::TestEvent;
use crate::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

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
