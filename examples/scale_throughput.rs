//! Scale benchmark: latency and throughput ceiling vs event rate.
//!
//! Sweeps producer event rate from 100/s to 50_000/s and measures
//! per-event latency PLUS sustained drain rate for push and three
//! polling cadences. The drain-rate-vs-producer-rate curve reveals
//! the throughput ceiling: the rate at which each transport stops
//! keeping up and backlog grows.
//!
//! Methodology (fresh source per trial, smoothed-Phase-1 pattern):
//! for each (rate, transport) cell, run 3 trials, pool per-event
//! latencies, compute median/p99/drain-rate. Server-side load is
//! snapshotted before/after each trial to attribute throughput cost.
//!
//! Outputs `docs/benchmarks/scale-throughput-<date>.md` (stdout)
//! AND `docs/benchmarks/scale-throughput-<date>.tsv` (long-form
//! rows for external plotting).
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example scale_throughput --features pg-streaming
//! ```

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::format_push_string,
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

use diesel::connection::SimpleConnection;
use diesel::{sql_query, PgConnection, RunQueryDsl};
use subql::{
    EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource,
};

use common::{
    assert_docker_available, create_pgoutput_slot, create_publication_all, force_drop_slot, ms_of,
    parser_db, pg_connect, pg_port, pg_replication_url, pg_url, pg_with_wal2json,
    resolve_table_ids, setup_schema, snapshot_delta, snapshot_server_load,
    spawn_full_event_receiver, EventKey, EventObservation, LatencyStats, TsvWriter,
};

const TRIALS_PER_CELL: usize = 3;
/// Per-trial wall-clock target. The producer drives at the target
/// rate for this long; the collector waits this + a drain grace.
const TRIAL_DURATION: Duration = Duration::from_secs(10);

/// Producer rates we sweep, in events/sec.
const PRODUCER_RATES: &[u64] = &[100, 500, 1_000, 5_000, 10_000, 25_000];

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

fn insert_sentinel_user(conn: &mut PgConnection) {
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(conn)
    .expect("insert sentinel user");
}

#[derive(Debug, Clone, Copy)]
enum Transport {
    Push,
    Poll { interval_ms: u64 },
}

impl Transport {
    fn label(self) -> String {
        match self {
            Self::Push => "push".to_string(),
            Self::Poll { interval_ms } => format!("poll@{interval_ms}ms"),
        }
    }
}

/// Drive INSERTs at `target_rate` events/sec for `duration` on a
/// DEDICATED OS thread so the tokio runtime's receiver task isn't
/// starved during diesel's blocking SQL calls. Returns
/// `(commits_map, actual_producer_rate)` once the thread joins.
async fn drive_at_rate(
    pg_url: String,
    id_base: i64,
    target_rate: u64,
    duration: Duration,
    orders_table_id: u32,
) -> (HashMap<EventKey, Instant>, f64) {
    let batch_size: u64 = if target_rate >= 10_000 {
        50
    } else if target_rate >= 5_000 {
        20
    } else if target_rate >= 1_000 {
        5
    } else {
        1
    };
    let batches_per_sec = target_rate.div_ceil(batch_size);
    let gap = Duration::from_secs_f64(1.0 / batches_per_sec as f64);
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        use diesel::Connection;
        let mut conn = diesel::PgConnection::establish(&pg_url).expect("PG conn");
        let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(
            ((target_rate as usize) * duration.as_secs() as usize).max(1024),
        );
        let mut id = id_base;
        let producer_start = Instant::now();
        let end = producer_start + duration;
        let mut produced: u64 = 0;
        while Instant::now() < end {
            let batch_start = Instant::now();
            if batch_size == 1 {
                let commit_at = Instant::now();
                sql_query(format!(
                    "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                     VALUES ({id}, 1, 'paid', {p}, NOW())",
                    p = id * 100
                ))
                .execute(&mut conn)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
                commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
                id += 1;
                produced += 1;
            } else {
                let mut stmts = String::from("BEGIN;");
                let commit_at = Instant::now();
                for _ in 0..batch_size {
                    stmts.push_str(&format!(
                        " INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                          VALUES ({id}, 1, 'paid', {p}, NOW());",
                        p = id * 100
                    ));
                    commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
                    id += 1;
                    produced += 1;
                }
                stmts.push_str(" COMMIT;");
                conn.batch_execute(&stmts)
                    .unwrap_or_else(|e| panic!("batch insert: {e}"));
            }
            let elapsed = batch_start.elapsed();
            if let Some(remaining) = gap.checked_sub(elapsed) {
                std::thread::sleep(remaining);
            }
        }
        let actual_rate = produced as f64 / producer_start.elapsed().as_secs_f64();
        let _ = tx.send((commits, actual_rate));
    });
    rx.await.expect("producer thread")
}

/// Drain events from a receiver until `deadline`, returning all
/// observations. Caller correlates with `commit_times`.
async fn drain_observations(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    deadline: Instant,
) -> Vec<EventObservation> {
    let mut out = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ob)) => out.push(ob),
            Ok(None) | Err(_) => break,
        }
    }
    out
}

/// Compute per-event latencies by correlating observations with
/// commit times. Returns the latency vec PLUS the number of events
/// observed (whether or not they matched commit times).
fn correlate(
    commit_times: &HashMap<EventKey, Instant>,
    observations: &[EventObservation],
) -> (Vec<Duration>, usize) {
    let observed_count = observations.len();
    let mut samples = Vec::with_capacity(observed_count);
    let mut seen = std::collections::HashSet::new();
    for ob in observations {
        let key = (ob.table_id, ob.pk_int, ob.kind);
        if !seen.insert(key) {
            continue;
        }
        if let Some(commit_at) = commit_times.get(&key) {
            samples.push(ob.observed_at.saturating_duration_since(*commit_at));
        }
    }
    (samples, observed_count)
}

#[derive(Debug, Clone)]
struct CellResult {
    transport_label: String,
    target_rate: u64,
    pooled_latencies: Vec<Duration>,
    actual_producer_rate: f64,
    drain_rate: f64,
    events_received: u64,
    events_expected: u64,
    /// `xact_commit/s` during the producer window. Useful as a
    /// cross-check on the actual producer rate.
    xact_commit_per_sec: f64,
    wal_bytes_written: i64,
}

struct TrialResult {
    samples: Vec<Duration>,
    producer_rate: f64,
    drain_rate: f64,
    events_received: u64,
    events_expected: u64,
    xact_commit_per_sec: f64,
    wal_bytes_written: i64,
}

async fn run_trial(
    pg_repl_url: &str,
    pg_sql_url: &str,
    slot: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    orders_table_id: u32,
    transport: Transport,
    target_rate: u64,
    id_base: i64,
    producer_pg_url: &str,
    setup: &mut PgConnection,
) -> TrialResult {
    let (rx, task, drain_grace) = match transport {
        Transport::Push => {
            let config = PgStreamingConfig::new(pg_repl_url.to_string(), slot, publication);
            let source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("connect push source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(500)).await;
            (rx, task, Duration::from_secs(5))
        }
        Transport::Poll { interval_ms } => {
            let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), slot, publication)
                .poll_interval(Duration::from_millis(interval_ms));
            let source = PollingPgCdcSource::connect(config, catalog)
                .await
                .expect("connect polling source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(200)).await;
            (
                rx,
                task,
                Duration::from_secs(5) + Duration::from_millis(interval_ms) * 4,
            )
        }
    };
    let before = snapshot_server_load(setup);
    let (commit_times, producer_rate) = drive_at_rate(
        producer_pg_url.to_string(),
        id_base,
        target_rate,
        TRIAL_DURATION,
        orders_table_id,
    )
    .await;
    let after_producer = Instant::now();
    let deadline = after_producer + drain_grace;
    let observations = drain_observations(rx, deadline).await;
    let after = snapshot_server_load(setup);
    let delta = snapshot_delta(&before, &after);
    let (samples, observed_count) = correlate(&commit_times, &observations);
    let window_secs = after
        .taken_at
        .saturating_duration_since(before.taken_at)
        .as_secs_f64()
        .max(1e-6);
    let drain_rate = observed_count as f64 / window_secs;
    let events_received = observed_count as u64;
    let events_expected = commit_times.len() as u64;
    task.abort();
    TrialResult {
        samples,
        producer_rate,
        drain_rate,
        events_received,
        events_expected,
        xact_commit_per_sec: delta.xact_commit_per_sec,
        wal_bytes_written: delta.wal_bytes_written,
    }
}

fn main() {
    assert_docker_available();
    println!("Scale benchmark: throughput ceiling vs event rate");
    println!(
        "{} trials per cell, {} s producer window, push + poll@{{10,100,1000}}ms.",
        TRIALS_PER_CELL,
        TRIAL_DURATION.as_secs(),
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "scale_throughput_pub";
    create_publication_all(&mut setup, publication);

    // Warmup
    for warmup_id in 999_000_i64..999_010 {
        sql_query(format!(
            "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
             VALUES ({warmup_id}, 1, 'warmup', 0, NOW())"
        ))
        .execute(&mut dml)
        .ok();
    }
    std::thread::sleep(Duration::from_millis(500));

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let transports: Vec<Transport> = vec![
        Transport::Push,
        Transport::Poll { interval_ms: 10 },
        Transport::Poll { interval_ms: 100 },
        Transport::Poll { interval_ms: 1_000 },
    ];

    let tsv_path = "docs/benchmarks/scale-throughput-2026-06-15.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV writer");

    let rt = current_thread_rt();
    let mut id_counter: i64 = 1;
    let mut cells: Vec<CellResult> = Vec::new();

    for &target_rate in PRODUCER_RATES {
        for &transport in &transports {
            let mut pooled: Vec<Duration> = Vec::new();
            let mut producer_rates: Vec<f64> = Vec::new();
            let mut drain_rates: Vec<f64> = Vec::new();
            let mut per_trial_medians: Vec<f64> = Vec::new();
            let mut per_trial_p99s: Vec<f64> = Vec::new();
            let mut total_received: u64 = 0;
            let mut total_expected: u64 = 0;
            let mut xact_rates: Vec<f64> = Vec::new();
            let mut wal_totals: i64 = 0;
            for trial in 0..TRIALS_PER_CELL {
                // Reserve plenty of id space per trial.
                id_counter += 500_000;
                let id_base = id_counter;
                let slot = format!("scale_t_{}_r{target_rate}_t{trial}", transport.label())
                    .replace('@', "")
                    .replace("ms", "");
                create_pgoutput_slot(&mut setup, &slot);
                let result = rt.block_on(run_trial(
                    &pg_repl_url,
                    &pg_sql_url,
                    &slot,
                    publication,
                    Arc::clone(&catalog),
                    table_ids.orders,
                    transport,
                    target_rate,
                    id_base,
                    &pg_sql_url,
                    &mut setup,
                ));
                // Compute per-trial median + p99 BEFORE pooling, so
                // we can report across-trial variance separately from
                // the within-trial latency distribution.
                if !result.samples.is_empty() {
                    let mut s = result.samples.clone();
                    s.sort();
                    let n = s.len();
                    per_trial_medians.push(ms_of(s[n / 2]));
                    per_trial_p99s.push(ms_of(s[(n.saturating_sub(1)).min((n * 99) / 100)]));
                    tsv.row(
                        "throughput",
                        &format!("rate={target_rate}"),
                        &transport.label(),
                        &format!("trial{trial}_median_ms"),
                        ms_of(s[n / 2]),
                    );
                    tsv.row(
                        "throughput",
                        &format!("rate={target_rate}"),
                        &transport.label(),
                        &format!("trial{trial}_drain_rate"),
                        result.drain_rate,
                    );
                }
                pooled.extend(result.samples);
                producer_rates.push(result.producer_rate);
                drain_rates.push(result.drain_rate);
                total_received += result.events_received;
                total_expected += result.events_expected;
                xact_rates.push(result.xact_commit_per_sec);
                wal_totals += result.wal_bytes_written;
                force_drop_slot(&mut setup, &slot);
            }
            // Suppress unused warnings: the per-trial vectors are
            // emitted to TSV as separate rows, used only for the
            // post-loop summary.
            let _ = &per_trial_medians;
            let _ = &per_trial_p99s;
            let avg = |v: &[f64]| v.iter().copied().sum::<f64>() / v.len() as f64;
            let cell = CellResult {
                transport_label: transport.label(),
                target_rate,
                pooled_latencies: pooled,
                actual_producer_rate: avg(&producer_rates),
                drain_rate: avg(&drain_rates),
                events_received: total_received,
                events_expected: total_expected,
                xact_commit_per_sec: avg(&xact_rates),
                wal_bytes_written: wal_totals,
            };
            let scale_key = format!("rate={target_rate}");
            let cell_key = cell.transport_label.clone();
            if cell.pooled_latencies.is_empty() {
                println!(
                    "  rate={target_rate:>5}/s  {:<14}  NO SAMPLES (events_expected={} events_received={})",
                    cell.transport_label, cell.events_expected, cell.events_received,
                );
            } else {
                let stats =
                    LatencyStats::new(cell.transport_label.clone(), cell.pooled_latencies.clone());
                tsv.row(
                    "throughput",
                    &scale_key,
                    &cell_key,
                    "median_ms",
                    ms_of(stats.median()),
                );
                tsv.row(
                    "throughput",
                    &scale_key,
                    &cell_key,
                    "p99_ms",
                    ms_of(stats.p99()),
                );
                tsv.row(
                    "throughput",
                    &scale_key,
                    &cell_key,
                    "samples",
                    stats.samples.len() as f64,
                );
                println!(
                    "  rate={target_rate:>5}/s  {:<14}  median={:>6.1}ms  p99={:>6.1}ms  drain={:>6.0}/s  producer={:>6.0}/s",
                    cell.transport_label,
                    ms_of(stats.median()),
                    ms_of(stats.p99()),
                    cell.drain_rate,
                    cell.actual_producer_rate,
                );
            }
            tsv.row(
                "throughput",
                &scale_key,
                &cell_key,
                "actual_producer_rate",
                cell.actual_producer_rate,
            );
            tsv.row(
                "throughput",
                &scale_key,
                &cell_key,
                "drain_rate",
                cell.drain_rate,
            );
            tsv.row_int(
                "throughput",
                &scale_key,
                &cell_key,
                "events_received",
                cell.events_received as i64,
            );
            tsv.row_int(
                "throughput",
                &scale_key,
                &cell_key,
                "events_expected",
                cell.events_expected as i64,
            );
            tsv.row(
                "throughput",
                &scale_key,
                &cell_key,
                "xact_commit_per_sec",
                cell.xact_commit_per_sec,
            );
            tsv.row_int(
                "throughput",
                &scale_key,
                &cell_key,
                "wal_bytes_written",
                cell.wal_bytes_written,
            );
            cells.push(cell);
        }
        println!();
    }

    // Markdown summary.
    println!("---");
    println!();
    println!("### Throughput ceiling: median latency by (rate, transport)");
    println!();
    print!("| rate (/s) |");
    for t in &transports {
        print!(" {} median (ms) | {} drain (/s) |", t.label(), t.label());
    }
    println!();
    print!("| ---:|");
    for _ in &transports {
        print!(" ---:| ---:|");
    }
    println!();
    for &rate in PRODUCER_RATES {
        print!("| {rate} |");
        for t in &transports {
            let cell = cells
                .iter()
                .find(|c| c.target_rate == rate && c.transport_label == t.label())
                .expect("cell exists");
            if cell.pooled_latencies.is_empty() {
                print!(" - | {:.0} |", cell.drain_rate);
            } else {
                let mut s = cell.pooled_latencies.clone();
                s.sort();
                let median = s[s.len() / 2];
                print!(" {:.1} | {:.0} |", ms_of(median), cell.drain_rate);
            }
        }
        println!();
    }
    println!();
    println!("Captured TSV: {tsv_path}");
}
