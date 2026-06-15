//! Phase 4 — Adversarial / operational regimes.
//!
//! Push holds a long-lived TCP connection; polling reconnects each
//! query (or holds a pool). These have different failure modes that
//! don't show up in raw latency tests. Phase 4 measures the
//! operational dimensions: long idle, slow consumer, WAL retention
//! under low ack frequency, and connection-flap resilience. The most
//! interesting phase from an "honest about trade-offs" standpoint.
//!
//! **SCAFFOLD ONLY.** Workload bodies are `todo!()` placeholders for
//! follow-up work. The shared infrastructure (PG container helpers,
//! 3-table schema, `LatencyStats`, generic event receiver, Markdown
//! output) lives in `examples/cdc_bench_common/mod.rs`. See
//! `docs/cdc-workload-benchmark-plan.md` § Phase 4 for the planned
//! workload set and expected outcomes.
//!
//! `W4.1` (5 minute idle) is the slowest workload across all phases.
//! Gate it behind a sub-command argument or env var so the default
//! `cargo run --example phase4_adversarial` finishes in a reasonable
//! time. `W4.4` (network flap) requires `tc netem` inside the
//! container or a packet-drop sidecar; the default test container may
//! not have the right capabilities. Document the limitation in the
//! follow-up implementation.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase4_adversarial --features pg-streaming
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
        name: "W4.1",
        description: "Long idle (5 min idle, then 1 event) — gate behind a flag; runs long",
    },
    WorkloadDesc {
        name: "W4.2",
        description: "Slow consumer (consumer takes 100 ms per event)",
    },
    WorkloadDesc {
        name: "W4.3",
        description: "Server WAL retention (5-min workload, low ack frequency)",
    },
    WorkloadDesc {
        name: "W4.4",
        description: "Connection-flap resilience (kill TCP mid-workload, measure recovery)",
    },
];

/// Stub workload dispatcher. Each arm panics with `todo!()` until the
/// follow-up step implements the corresponding workload.
async fn drive_for_workload(workload: &WorkloadDesc) {
    match workload.name {
        "W4.1" => todo!("W4.1 — Long idle"),
        "W4.2" => todo!("W4.2 — Slow consumer"),
        "W4.3" => todo!("W4.3 — Server WAL retention"),
        "W4.4" => todo!("W4.4 — Connection-flap resilience"),
        other => panic!("unknown workload {other}"),
    }
}

fn main() {
    let _ddl_len = common::PARSER_DDL.len();

    println!("Phase 4 — Adversarial / operational regimes");
    println!();
    println!("Scaffold — workload implementations land in follow-up work.");
    println!("See docs/cdc-workload-benchmark-plan.md § Phase 4 for the spec.");
    println!();
    println!("Planned workloads:");
    for w in WORKLOADS {
        println!("  - {} — {}", w.name, w.description);
    }
}
