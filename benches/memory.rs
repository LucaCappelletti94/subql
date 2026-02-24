//! Memory profiling with dhat
//!
//! Run with: cargo run --release --features dhat-heap --bin memory_profile
#![allow(clippy::unwrap_used, clippy::unreadable_literal)]
#![allow(clippy::print_stdout, clippy::unnecessary_cast)]

use subql::memory_profile_workload::run_memory_profile;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let show_progress = std::env::var_os("SUBQL_MEMORY_BENCH_PROGRESS").is_some();
    run_memory_profile(show_progress);
}
