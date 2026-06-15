//! Phase 1 — Append-only baseline (single table).
//!
//! Reproduces the prior one-shot benchmark
//! (`docs/benchmarks/pg-streaming-latency-2026-06-15.txt`) using the
//! new examples harness in `examples/cdc_bench_common/`. If the
//! numbers diverge significantly from the prior table, the harness
//! has a bug — debug before proceeding to subsequent phases.
//!
//! Three workloads, four transports:
//!
//! - W1.1 — single-row inserts spaced at `max(50ms, 1.3 x poll interval)`
//!   so commits land at varied offsets within the polling cycle. This
//!   is the load-bearing reproducer of the prior one-shot numbers.
//! - W1.2 — sustained high-rate inserts (500/s)
//! - W1.3 — idle-then-burst (3 s idle then a single insert, repeated)
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase1_baseline --features pg-streaming
//! ```
//!
//! Pipe to `docs/benchmarks/phase1-<date>.md` to capture the run. Each
//! cell prints a Markdown table; the trailing summary block is the
//! one most worth keeping.

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::duration_suboptimal_units,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]

#[path = "cdc_bench_common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::PgConnection;
use diesel::{sql_query, RunQueryDsl};
use subql::{PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource};

use common::{
    assert_docker_available, collect_latencies, create_pgoutput_slot, create_publication_all,
    drive_idle_then_burst, drive_inserts, markdown_table_header, parser_db, pg_connect, pg_port,
    pg_replication_url, pg_url, pg_with_wal2json, setup_schema, spawn_event_receiver, LatencyStats,
};

/// How long to give a transport to drain a workload after the last
/// commit. Sized for the slowest poll cadence we test (1 s).
const DRAIN_GRACE: Duration = Duration::from_secs(15);

/// Polling cadences we sweep. The push transport is always run first.
const POLL_INTERVALS_MS: &[u64] = &[10, 100, 1_000];

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

/// Identifier for a (workload, transport) measurement. Used to
/// construct slot names so each measurement has its own slot.
#[derive(Debug, Clone, Copy)]
enum Transport {
    Push,
    Poll { interval_ms: u64 },
}

impl Transport {
    fn slot_suffix(self) -> String {
        match self {
            Self::Push => "push".to_string(),
            Self::Poll { interval_ms } => format!("poll_{interval_ms}"),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Push => "push (PgStreamingCdcSource)".to_string(),
            Self::Poll { interval_ms } => format!("poll @ {interval_ms}ms interval"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkloadDesc {
    /// Short name for the measurement section.
    name: &'static str,
    /// Human-readable description for the section header.
    description: &'static str,
    /// Base id used by the driver; must be unique per workload so
    /// successive runs against different slots can use the same
    /// schema without colliding on the PK.
    id_base: i64,
}

const W1_1: WorkloadDesc = WorkloadDesc {
    name: "W1.1",
    description: "single-row inserts, gap = max(50ms, 1.3 x poll interval)",
    id_base: 1_000,
};
const W1_2: WorkloadDesc = WorkloadDesc {
    name: "W1.2",
    description: "sustained high-rate inserts at 500/s",
    id_base: 10_000,
};
const W1_3: WorkloadDesc = WorkloadDesc {
    name: "W1.3",
    description: "idle-then-burst (3 s idle, 1 insert, repeated)",
    id_base: 100_000,
};

fn slot_name(workload: &WorkloadDesc, transport: Transport) -> String {
    // PG slot names must match `[a-z0-9_]+`; lowercase the workload
    // name (e.g. "W1.1" → "w1_1") and the transport suffix.
    format!(
        "phase1_{}_{}",
        workload.name.replace('.', "_").to_lowercase(),
        transport.slot_suffix()
    )
}

/// Insert a sentinel user (id=1) referenced by every inserted order's
/// `user_id`. We do not enforce a foreign key (see `cdc_bench_common`
/// docs) but downstream readers may expect a real user to exist.
fn insert_sentinel_user(conn: &mut PgConnection) {
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW())",
    )
    .execute(conn)
    .expect("insert sentinel user");
}

/// Drive workload `W1.1`: 15 single-row inserts spaced `gap` apart.
/// The caller picks `gap` based on the transport being measured so
/// commits land at varied offsets within the polling cycle (per the
/// prior one-shot methodology in `tests/polling_vs_push_benchmark.rs`).
/// Pinning the gap to the polling interval would phase-lock every
/// commit to the same poll-cycle offset and bias the median; pinning
/// it independent of cadence (e.g. always 1 s) phase-locks the
/// 1 s-cadence variant.
async fn drive_w1_1(conn: &mut PgConnection, id_base: i64, gap: Duration) -> HashMap<i64, Instant> {
    const COUNT: i64 = 15;
    drive_inserts(conn, id_base, COUNT, gap).await
}

/// Per-transport insert gap for [`drive_w1_1`]. Mirrors the prior
/// one-shot benchmark's selector so the W1.1 cell of the output table
/// can be compared directly with
/// `docs/benchmarks/pg-streaming-latency-2026-06-15.txt`.
fn w1_1_gap_for(transport: Transport) -> Duration {
    match transport {
        Transport::Push => Duration::from_millis(50),
        Transport::Poll { interval_ms } => {
            let scaled = Duration::from_millis(interval_ms) * 13 / 10;
            std::cmp::max(Duration::from_millis(50), scaled)
        }
    }
}

/// Drive workload `W1.2`: 500 inserts at 2 ms gap (~500/s for ~1 s).
/// Reduced from the plan's 500/s × 5 s = 2500 events to keep the
/// sweep brisk; 500 events still gives a tight median.
async fn drive_w1_2(conn: &mut PgConnection, id_base: i64) -> HashMap<i64, Instant> {
    const COUNT: i64 = 500;
    drive_inserts(conn, id_base, COUNT, Duration::from_millis(2)).await
}

/// Drive workload `W1.3`: 5 bursts of (3 s idle + 1 INSERT). 5 samples
/// per row; minimum useful sample count, accepted because the workload
/// is intentionally adversarial for polling and we want a few stable
/// observations per cell.
async fn drive_w1_3(conn: &mut PgConnection, id_base: i64) -> HashMap<i64, Instant> {
    const BURSTS: i64 = 5;
    const IDLE: Duration = Duration::from_secs(3);
    let mut all: HashMap<i64, Instant> = HashMap::new();
    for i in 0..BURSTS {
        let chunk = drive_idle_then_burst(conn, id_base + i, IDLE, 1).await;
        all.extend(chunk);
    }
    all
}

async fn measure_push(
    pg_replication_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    label: String,
    transport: Transport,
    workload: WorkloadDesc,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PgStreamingConfig::new(pg_replication_url, slot, publication);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = spawn_event_receiver(source);
    // Give the source's START_REPLICATION handshake time to complete
    // before driving inserts; otherwise the first few commits may slip
    // through before the inner task is actively reading.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let commit_times = drive_for_workload(&workload, transport, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE;
    let samples = collect_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

async fn measure_poll(
    pg_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    interval: Duration,
    label: String,
    transport: Transport,
    workload: WorkloadDesc,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PollingPgCdcConfig::new(pg_url, slot, publication).poll_interval(interval);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    let (rx, task) = spawn_event_receiver(source);
    // Short warmup; the polling task starts on the first tick. We
    // burn the first tick inside `polling_task` already, so a 200 ms
    // wait here just gives Tokio a moment to schedule it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let commit_times = drive_for_workload(&workload, transport, dml).await;
    // Drain budget: workload tail plus several polling intervals to
    // make sure the last commit's events are observed.
    let deadline = Instant::now() + DRAIN_GRACE + interval * 4;
    let samples = collect_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

fn main() {
    assert_docker_available();
    println!("Phase 1 — Append-only baseline (single table)");
    println!("Reproducing prior one-shot push-vs-poll numbers across three workloads.");
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "phase1_pub";
    create_publication_all(&mut setup, publication);

    let workloads = [W1_1, W1_2, W1_3];
    let transports = {
        let mut v = Vec::with_capacity(1 + POLL_INTERVALS_MS.len());
        v.push(Transport::Push);
        for &ms in POLL_INTERVALS_MS {
            v.push(Transport::Poll { interval_ms: ms });
        }
        v
    };

    let catalog = parser_db();
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let rt = current_thread_rt();
    let mut all_stats: Vec<(WorkloadDesc, Vec<LatencyStats>)> = Vec::new();

    for w in workloads {
        println!("=== {} — {} ===", w.name, w.description);
        let mut row: Vec<LatencyStats> = Vec::with_capacity(transports.len());
        for (ti, &t) in transports.iter().enumerate() {
            let slot = slot_name(&w, t);
            // Create the slot JUST BEFORE the measurement so it
            // starts at the current WAL position and does not
            // accumulate events from prior measurements that share
            // the publication.
            create_pgoutput_slot(&mut setup, &slot);
            let label = t.label();
            // Each transport for a workload uses a distinct id
            // range so successive runs don't collide on the PK.
            let workload_for_run = WorkloadDesc {
                id_base: w.id_base + (ti as i64) * 10_000,
                ..w
            };
            let stats = rt.block_on(async {
                match t {
                    Transport::Push => {
                        measure_push(
                            pg_repl_url.clone(),
                            slot,
                            publication.to_string(),
                            Arc::clone(&catalog),
                            label,
                            t,
                            workload_for_run,
                            &mut dml,
                        )
                        .await
                    }
                    Transport::Poll { interval_ms } => {
                        measure_poll(
                            pg_sql_url.clone(),
                            slot,
                            publication.to_string(),
                            Arc::clone(&catalog),
                            Duration::from_millis(interval_ms),
                            label,
                            t,
                            workload_for_run,
                            &mut dml,
                        )
                        .await
                    }
                }
            });
            println!("{}", stats.text_summary());
            row.push(stats);
        }
        println!();
        all_stats.push((w, row));
    }

    // Markdown summary for paste-into-doc.
    println!("---");
    println!();
    for (w, row) in &all_stats {
        println!("### {} — {}", w.name, w.description);
        println!();
        println!("{}", markdown_table_header());
        for stats in row {
            println!("{}", stats.markdown_row());
        }
        println!();
    }

    // Load-bearing assertion: W1.1 uses the prior one-shot
    // benchmark's methodology (gap = max(50ms, 1.3 x poll interval),
    // 15 events). Every cell must reproduce the prior one-shot's
    // median within a tolerance band; if a cell diverges by more than
    // 2x, the harness or one of the transports likely has a bug —
    // debug before proceeding.
    let (w1_1, w1_1_row) = &all_stats[0];
    assert_eq!(w1_1.name, "W1.1");
    // (transport_index, label, prior median in ms). Pulled from
    // `docs/benchmarks/pg-streaming-latency-2026-06-15.txt`.
    let prior_w1_1: &[(usize, &str, f64)] = &[
        (0, "push", 4.6),
        (1, "poll@10ms", 5.5),
        (2, "poll@100ms", 57.9),
        (3, "poll@1000ms", 554.7),
    ];
    println!("Phase 1 harness validation against prior one-shot W1.1:");
    for (idx, label, prior_ms) in prior_w1_1 {
        let observed = w1_1_row[*idx].median();
        let observed_ms = common::ms_of(observed);
        let lower = prior_ms * 0.5;
        let upper = prior_ms * 2.0;
        assert!(
            observed_ms >= lower && observed_ms <= upper,
            "W1.1 {label} median {observed_ms:.1}ms outside [{lower:.1}, {upper:.1}] \
             (prior one-shot: {prior_ms}ms). Harness likely has a bug — debug \
             before proceeding to Phase 2+.",
        );
        let delta_pct = ((observed_ms - prior_ms) / prior_ms) * 100.0;
        println!(
            "  W1.1 {label}: observed {observed_ms:>6.1} ms vs prior {prior_ms:>6.1} ms ({delta_pct:+.0}%)",
        );
    }
    println!("Phase 1 harness validation: PASS");
}

async fn drive_for_workload(
    w: &WorkloadDesc,
    transport: Transport,
    dml: &mut PgConnection,
) -> HashMap<i64, Instant> {
    match w.name {
        "W1.1" => drive_w1_1(dml, w.id_base, w1_1_gap_for(transport)).await,
        "W1.2" => drive_w1_2(dml, w.id_base).await,
        "W1.3" => drive_w1_3(dml, w.id_base).await,
        other => panic!("unknown workload {other}"),
    }
}
