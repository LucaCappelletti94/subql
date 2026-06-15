//! Phase 5 — Schema-size sweep.
//!
//! Tests whether schema complexity changes the verdict. The pgoutput
//! protocol emits a `Relation` message on first contact with each
//! table — does that one-time cost dominate any short workload? Wide
//! rows exercise parse-cost-per-event; many tables exercise the
//! publication / relation-cache machinery; mixed audit + lookup
//! exercises both patterns at once.
//!
//! **SCAFFOLD ONLY.** Workload bodies are `todo!()` placeholders for
//! follow-up work. The shared infrastructure (PG container helpers,
//! 3-table schema, `LatencyStats`, generic event receiver, Markdown
//! output) lives in `examples/cdc_bench_common/mod.rs`. Phase 5
//! introduces NEW schemas beyond the e-commerce 3-table set: a wide
//! 50-column table (`S5.1`), the Pagila DVD-rental schema (`S5.2`),
//! and an append-only audit + lookup pairing (`S5.3`). The follow-up
//! implementation will add schema setup helpers either alongside
//! `cdc_bench_common::PG_DDL` or as separate per-phase modules.
//!
//! See `docs/cdc-workload-benchmark-plan.md` § Phase 5 for the spec.
//!
//! `S5.2` (Pagila) requires downloading the Pagila SQL dump on first
//! run; cache the Docker image with Pagila preloaded if this phase
//! becomes part of the regular sweep. Document the dependency in the
//! follow-up implementation.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase5_schema_sweep --features pg-streaming
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
        name: "S5.1",
        description: "Wide rows (50-column table, mixed text/numeric/timestamp/JSONB)",
    },
    WorkloadDesc {
        name: "S5.2",
        description: "Many tables (Pagila — 15 tables, DVD rental schema)",
    },
    WorkloadDesc {
        name: "S5.3",
        description: "Append-only audit log + occasional UPDATE-heavy lookup table",
    },
];

/// Stub workload dispatcher. Each arm panics with `todo!()` until the
/// follow-up step implements the corresponding workload.
async fn drive_for_workload(workload: &WorkloadDesc) {
    match workload.name {
        "S5.1" => todo!("S5.1 — Wide rows"),
        "S5.2" => todo!("S5.2 — Many tables (Pagila)"),
        "S5.3" => todo!("S5.3 — Append-only audit log + lookup"),
        other => panic!("unknown workload {other}"),
    }
}

fn main() {
    let _ddl_len = common::PARSER_DDL.len();

    println!("Phase 5 — Schema-size sweep");
    println!();
    println!("Scaffold — workload implementations land in follow-up work.");
    println!("See docs/cdc-workload-benchmark-plan.md § Phase 5 for the spec.");
    println!();
    println!("Planned workloads:");
    for w in WORKLOADS {
        println!("  - {} — {}", w.name, w.description);
    }
}
