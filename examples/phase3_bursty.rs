//! Phase 3 — Bursty + transactional workloads.
//!
//! Looks for regimes where the transports diverge from the
//! "wire + P/2" latency model. Large transactions are particularly
//! interesting: pgoutput delivers events at COMMIT time (not per
//! row), so the FIRST event of a large transaction inherits any
//! commit-time WAL-flush latency on top of the transport's own cost.
//! Both transports share that upstream bottleneck.
//!
//! **SCAFFOLD ONLY.** Workload bodies are `todo!()` placeholders for
//! follow-up work. The shared infrastructure (PG container helpers,
//! 3-table schema, `LatencyStats`, generic event receiver, Markdown
//! output) lives in `examples/cdc_bench_common/mod.rs`. See
//! `docs/cdc-workload-benchmark-plan.md` § Phase 3 for the planned
//! workload set and expected outcomes.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase3_bursty --features pg-streaming
//! ```

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::todo,
    clippy::unused_async,
    dead_code
)]

#[path = "cdc_bench_common/mod.rs"]
mod common;

#[derive(Debug, Clone, Copy)]
struct WorkloadDesc {
    name: &'static str,
    description: &'static str,
}

const WORKLOADS: &[WorkloadDesc] = &[
    WorkloadDesc {
        name: "W3.1",
        description: "Bursty traffic (100 events in 100 ms, then 2 s quiet, repeat 5x)",
    },
    WorkloadDesc {
        name: "W3.2",
        description: "Large transactions (1 commit with 1000 rows)",
    },
    WorkloadDesc {
        name: "W3.3",
        description: "Many small transactions (1000 commits of 1 row each over 10 s)",
    },
];

/// Stub workload dispatcher. Each arm panics with `todo!()` until the
/// follow-up step implements the corresponding workload.
async fn drive_for_workload(workload: &WorkloadDesc) {
    match workload.name {
        "W3.1" => todo!("W3.1 — Bursty traffic"),
        "W3.2" => todo!("W3.2 — Large transactions"),
        "W3.3" => todo!("W3.3 — Many small transactions"),
        other => panic!("unknown workload {other}"),
    }
}

fn main() {
    let _ddl_len = common::PARSER_DDL.len();

    println!("Phase 3 — Bursty + transactional workloads");
    println!();
    println!("Scaffold — workload implementations land in follow-up work.");
    println!("See docs/cdc-workload-benchmark-plan.md § Phase 3 for the spec.");
    println!();
    println!("Planned workloads:");
    for w in WORKLOADS {
        println!("  - {} — {}", w.name, w.description);
    }
}
