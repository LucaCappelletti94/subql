//! Phase 2 — Multi-table schema with I/U/D mix.
//!
//! Validates that the polling-vs-push verdict holds when the
//! operation mix isn't pure INSERTs. UPDATE / DELETE produce more
//! WAL bytes per event; if polling amortizes drain RTT better on
//! larger WAL batches, this is where it shows up.
//!
//! **SCAFFOLD ONLY.** Workload bodies are `todo!()` placeholders for
//! follow-up work. The shared infrastructure (PG container helpers,
//! 3-table schema, `LatencyStats`, generic event receiver, Markdown
//! output) lives in `examples/cdc_bench_common/mod.rs`. See
//! `docs/cdc-workload-benchmark-plan.md` § Phase 2 for the planned
//! workload set and expected outcomes.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase2_mixed_iud --features pg-streaming
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
        name: "W2.1",
        description:
            "Mixed I/U/D, steady (1 event/s rotating insert/update/delete on the same row set)",
    },
    WorkloadDesc {
        name: "W2.2",
        description: "Order-and-items transaction (1 orders row + 3 order_items rows per commit)",
    },
    WorkloadDesc {
        name: "W2.3",
        description: "UPDATE-heavy (100 UPDATEs/s against users)",
    },
];

/// Stub workload dispatcher. Each arm panics with `todo!()` until the
/// follow-up step implements the corresponding workload. The
/// signature matches what `examples/phase1_baseline.rs` uses for
/// `drive_for_workload`, so this scaffold maps cleanly onto the
/// existing measurement harness.
async fn drive_for_workload(workload: &WorkloadDesc) {
    match workload.name {
        "W2.1" => todo!("W2.1 — Mixed I/U/D, steady"),
        "W2.2" => todo!("W2.2 — Order-and-items transaction"),
        "W2.3" => todo!("W2.3 — UPDATE-heavy"),
        other => panic!("unknown workload {other}"),
    }
}

fn main() {
    // Keep the import of the shared module non-dead so the scaffold
    // exercises the same compile path the eventual implementation
    // will use.
    let _ddl_len = common::PARSER_DDL.len();

    println!("Phase 2 — Multi-table schema with I/U/D mix");
    println!();
    println!("Scaffold — workload implementations land in follow-up work.");
    println!("See docs/cdc-workload-benchmark-plan.md § Phase 2 for the spec.");
    println!();
    println!("Planned workloads:");
    for w in WORKLOADS {
        println!("  - {} — {}", w.name, w.description);
    }
}
