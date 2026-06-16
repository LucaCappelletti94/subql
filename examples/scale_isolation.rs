//! Scale benchmark: is the N=100 cliff a CDC problem or a writer
//! problem?
//!
//! Experiment A of `scale_consumers.rs` showed a 3-order-of-magnitude
//! cliff between N=30 (6 000 ev/s, ~8 ms median) and N=100 (20 000
//! ev/s, ~7.9 s median). Both transports collapsed together, which
//! points the bottleneck somewhere upstream of the transport choice
//! — but it does not tell us whether the upstream resource is on the
//! WRITE side (100 producer backends inserting into one table) or on
//! the CDC side (100 wal-sender backends streaming WAL out).
//!
//! This example decouples them:
//!
//! - **Experiment W (writers only)**: N producer threads doing
//!   single-row INSERTs at 200 ev/s each. NO CDC consumers attached
//!   to the publication. Measures actual_producer_rate and
//!   xact_commit_per_sec at the PG server. If 100 writers alone
//!   collapse, the cliff is write-side.
//! - **Experiment C (CDC only, fixed rate)**: a SINGLE batched
//!   producer at a fixed total rate, N CDC consumers reading the
//!   same publication. Measures per-consumer median latency. If 100
//!   CDC consumers collapse at a rate that 1 producer can sustain,
//!   the cliff is CDC-side.
//!
//! Both transports tested in (C); (W) has no CDC and so no transport
//! to vary.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example scale_isolation --features pg-streaming
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
use diesel::{sql_query, Connection, PgConnection, RunQueryDsl};

use common::{
    assert_docker_available, create_publication_all, force_drop_slot, ms_of, parser_db, pg_connect,
    pg_port, pg_replication_url, pg_url, pg_with_wal2json, precreate_poll_slots,
    precreate_push_slots, resolve_table_ids, setup_schema, snapshot_delta, snapshot_server_load,
    spawn_n_poll_consumers, spawn_n_push_consumers, ConsumerHandle, EventKey, EventObservation,
    LatencyStats, TsvWriter,
};
use subql::EventKind;

const WINDOW: Duration = Duration::from_secs(20);
const TRIALS_PER_CELL: usize = 3;
const PER_WRITER_RATE: u64 = 200;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const BUFFER_CAPACITY: usize = 4_096;

/// Writer counts for Experiment W (writers only).
const WRITER_COUNTS: &[usize] = &[1, 30, 100];

/// Consumer counts for Experiment C (CDC only, fixed rate).
const CONSUMER_COUNTS: &[usize] = &[1, 30, 100];

/// Total event rate options for Experiment C. The first matches
/// Experiment A N=30's rate (6 000 ev/s), the second matches A N=100
/// (20 000 ev/s). If C collapses at 20 000 ev/s with 100 consumers
/// but is fine at 6 000 ev/s, that's a CDC fanout cost. If both are
/// fine, the cliff was writer-side.
const C_TOTAL_RATES: &[u64] = &[6_000, 20_000];

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
    Poll,
}

impl Transport {
    const fn label(self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::Poll => "poll@100ms",
        }
    }
}

// ----------------------------------------------------------------------
// Experiment W: writers only
// ----------------------------------------------------------------------

/// Spawn N producer threads, each doing single-row INSERTs at
/// `per_rate` ev/s for `duration`. NO CDC consumers attached. Returns
/// per-producer (actual rate, count).
fn run_writers_only(
    n: usize,
    per_rate: u64,
    duration: Duration,
    pg_url: &str,
    id_base: i64,
) -> (Vec<f64>, u64) {
    let mut handles: Vec<std::thread::JoinHandle<(f64, u64)>> = Vec::with_capacity(n);
    for i in 0..n {
        let producer_id_base = id_base + (i as i64) * 1_000_000;
        let url = pg_url.to_string();
        let handle = std::thread::spawn(move || {
            let mut conn = PgConnection::establish(&url).expect("PG conn");
            let gap = Duration::from_secs_f64(1.0 / per_rate as f64);
            let end = Instant::now() + duration;
            let start = Instant::now();
            let mut id = producer_id_base;
            let mut produced: u64 = 0;
            while Instant::now() < end {
                let batch_start = Instant::now();
                sql_query(format!(
                    "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                     VALUES ({id}, 1, 'paid', {p}, NOW())",
                    p = id * 100
                ))
                .execute(&mut conn)
                .unwrap_or_else(|e| panic!("writer {i} insert id={id}: {e}"));
                id += 1;
                produced += 1;
                if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                    std::thread::sleep(remaining);
                }
            }
            let actual_rate = produced as f64 / start.elapsed().as_secs_f64();
            (actual_rate, produced)
        });
        handles.push(handle);
    }
    let mut rates: Vec<f64> = Vec::with_capacity(n);
    let mut total: u64 = 0;
    for h in handles {
        let (r, c) = h.join().expect("writer join");
        rates.push(r);
        total += c;
    }
    (rates, total)
}

// ----------------------------------------------------------------------
// Experiment C: CDC only, single batched producer at fixed total rate
// ----------------------------------------------------------------------

/// Drive a single batched-INSERT producer at `total_rate` ev/s for
/// `duration`. Returns the commit-time map keyed by (table, pk, kind).
/// Uses an async oneshot so the current-thread tokio runtime keeps
/// driving consumer tasks during the producer window.
async fn run_batched_producer(
    pg_url: String,
    id_base: i64,
    total_rate: u64,
    duration: Duration,
    orders_table_id: u32,
) -> (HashMap<EventKey, Instant>, f64) {
    let url = pg_url;
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut conn = PgConnection::establish(&url).expect("PG conn");
        let batch_size: u64 = if total_rate >= 10_000 {
            50
        } else if total_rate >= 5_000 {
            20
        } else if total_rate >= 1_000 {
            5
        } else {
            1
        };
        let batches_per_sec = total_rate.div_ceil(batch_size);
        let gap = Duration::from_secs_f64(1.0 / batches_per_sec as f64);
        let end = Instant::now() + duration;
        let start = Instant::now();
        let mut commits: HashMap<EventKey, Instant> =
            HashMap::with_capacity(((total_rate as usize) * duration.as_secs() as usize).max(1024));
        let mut id = id_base;
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
                .unwrap_or_else(|e| panic!("producer insert id={id}: {e}"));
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
            if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                std::thread::sleep(remaining);
            }
        }
        let actual = produced as f64 / start.elapsed().as_secs_f64();
        let _ = tx.send((commits, actual));
    });
    rx.await.expect("producer result")
}

/// Drain N consumers concurrently, abort on the way out, return per-
/// consumer observations.
async fn drain_n(handles: Vec<ConsumerHandle>, deadline: Instant) -> Vec<Vec<EventObservation>> {
    let mut drainers: Vec<tokio::task::JoinHandle<Vec<EventObservation>>> =
        Vec::with_capacity(handles.len());
    for handle in handles {
        let drainer = tokio::spawn(async move {
            let mut rx = handle.rx;
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
            handle.task.abort();
            out
        });
        drainers.push(drainer);
    }
    let mut all = Vec::with_capacity(drainers.len());
    for d in drainers {
        all.push(d.await.unwrap_or_default());
    }
    all
}

fn pool_latencies(
    commits: &HashMap<EventKey, Instant>,
    observations: &[Vec<EventObservation>],
) -> Vec<Duration> {
    let mut samples = Vec::new();
    for consumer_obs in observations {
        let mut seen = std::collections::HashSet::new();
        for ob in consumer_obs {
            let key = (ob.table_id, ob.pk_int, ob.kind);
            if !seen.insert(key) {
                continue;
            }
            if let Some(commit_at) = commits.get(&key) {
                samples.push(ob.observed_at.saturating_duration_since(*commit_at));
            }
        }
    }
    samples
}

fn main() {
    assert_docker_available();
    println!("Scale benchmark: isolation (writers only vs CDC only)");
    println!(
        "Window: {} s per cell, {TRIALS_PER_CELL} trials per cell.",
        WINDOW.as_secs()
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);
    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "scale_isolation_pub";
    create_publication_all(&mut setup, publication);

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

    let tsv_path = "docs/benchmarks/scale-isolation-2026-06-16.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV writer");

    let rt = current_thread_rt();
    let mut id_counter: i64 = 1_000_000;

    // -------------------------------------------------------------
    // Experiment W: writers only (no CDC consumers)
    // -------------------------------------------------------------
    println!("=== Experiment W: writers only (no CDC) ===");
    for &n in WRITER_COUNTS {
        let mut per_trial_rates: Vec<f64> = Vec::new();
        let mut per_trial_xact: Vec<f64> = Vec::new();
        let scale_key = format!("exp_w_writers_n{n}");
        for trial in 0..TRIALS_PER_CELL {
            id_counter += 500_000_000;
            let id_base = id_counter;
            let before = snapshot_server_load(&mut setup);
            let (per_writer_rates, total_count) =
                run_writers_only(n, PER_WRITER_RATE, WINDOW, &pg_sql_url, id_base);
            let after = snapshot_server_load(&mut setup);
            let delta = snapshot_delta(&before, &after);
            let total_rate: f64 = per_writer_rates.iter().sum();
            per_trial_rates.push(total_rate);
            per_trial_xact.push(delta.xact_commit_per_sec);
            tsv.row(
                "isolation",
                &scale_key,
                "writers",
                &format!("trial{trial}_total_actual_rate"),
                total_rate,
            );
            tsv.row(
                "isolation",
                &scale_key,
                "writers",
                &format!("trial{trial}_xact_commit_per_sec"),
                delta.xact_commit_per_sec,
            );
            tsv.row_int(
                "isolation",
                &scale_key,
                "writers",
                &format!("trial{trial}_events_committed"),
                total_count as i64,
            );
        }
        let avg = |v: &[f64]| -> f64 { v.iter().sum::<f64>() / v.len() as f64 };
        let stddev = |v: &[f64]| -> f64 {
            if v.len() < 2 {
                return 0.0;
            }
            let m = avg(v);
            (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
        };
        let rate_mean = avg(&per_trial_rates);
        let rate_std = stddev(&per_trial_rates);
        let xact_mean = avg(&per_trial_xact);
        let xact_std = stddev(&per_trial_xact);
        println!(
            "  N={n:>3}  target_total={:>6}/s  measured_rate={rate_mean:>6.0}±{rate_std:.0}/s  xact/s={xact_mean:>6.0}±{xact_std:.0}",
            (n as u64) * PER_WRITER_RATE
        );
        tsv.row(
            "isolation",
            &scale_key,
            "writers",
            "across_trial_rate_mean",
            rate_mean,
        );
        tsv.row(
            "isolation",
            &scale_key,
            "writers",
            "across_trial_rate_std",
            rate_std,
        );
        tsv.row(
            "isolation",
            &scale_key,
            "writers",
            "across_trial_xact_mean",
            xact_mean,
        );
        tsv.row(
            "isolation",
            &scale_key,
            "writers",
            "across_trial_xact_std",
            xact_std,
        );
        tsv.row_int("isolation", &scale_key, "writers", "writer_count", n as i64);
        tsv.row_int(
            "isolation",
            &scale_key,
            "writers",
            "target_total_rate",
            ((n as u64) * PER_WRITER_RATE) as i64,
        );
    }
    println!();

    // -------------------------------------------------------------
    // Experiment C: CDC only, single batched producer, N consumers
    // -------------------------------------------------------------
    println!("=== Experiment C: CDC only (1 batched producer at fixed total rate) ===");
    for &total_rate in C_TOTAL_RATES {
        for &n in CONSUMER_COUNTS {
            for transport in [Transport::Push, Transport::Poll] {
                let mut per_trial_medians: Vec<f64> = Vec::new();
                let mut per_trial_xact: Vec<f64> = Vec::new();
                let scale_key = format!("exp_c_cdc_rate{total_rate}_n{n}");
                let cell_key = transport.label().to_string();
                for trial in 0..TRIALS_PER_CELL {
                    let label_prefix = format!("scale_iso_c_r{total_rate}_t{trial}");
                    let slots = match transport {
                        Transport::Push => {
                            common::precreate_push_slots(&mut setup, &label_prefix, n)
                        }
                        Transport::Poll => {
                            precreate_poll_slots(&mut setup, &label_prefix, POLL_INTERVAL, n)
                        }
                    };
                    let _ = precreate_push_slots; // keep linker happy if unused below
                    id_counter += 500_000_000;
                    let id_base = id_counter;

                    let before = snapshot_server_load(&mut setup);
                    let pg_sql_url_cloned = pg_sql_url.clone();
                    let pg_repl_url_cloned = pg_repl_url.clone();
                    let catalog_cloned = Arc::clone(&catalog);
                    let table_id = table_ids.orders;

                    let trial_latencies = rt.block_on(async move {
                        let handles = match transport {
                            Transport::Push => {
                                spawn_n_push_consumers(
                                    n,
                                    &label_prefix,
                                    &pg_repl_url_cloned,
                                    publication,
                                    Arc::clone(&catalog_cloned),
                                    STATUS_INTERVAL,
                                    BUFFER_CAPACITY,
                                )
                                .await
                            }
                            Transport::Poll => {
                                spawn_n_poll_consumers(
                                    n,
                                    &label_prefix,
                                    &pg_sql_url_cloned,
                                    publication,
                                    Arc::clone(&catalog_cloned),
                                    POLL_INTERVAL,
                                    BUFFER_CAPACITY,
                                )
                                .await
                            }
                        };
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        let (commits, _producer_rate) = run_batched_producer(
                            pg_sql_url_cloned.clone(),
                            id_base,
                            total_rate,
                            WINDOW,
                            table_id,
                        )
                        .await;
                        let deadline = Instant::now() + Duration::from_secs(5) + POLL_INTERVAL * 4;
                        let observations = drain_n(handles, deadline).await;
                        pool_latencies(&commits, &observations)
                    });
                    let after = snapshot_server_load(&mut setup);
                    let delta = snapshot_delta(&before, &after);

                    if !trial_latencies.is_empty() {
                        let stats = LatencyStats::new(cell_key.clone(), trial_latencies.clone());
                        let median = ms_of(stats.median());
                        per_trial_medians.push(median);
                        per_trial_xact.push(delta.xact_commit_per_sec);
                        tsv.row(
                            "isolation",
                            &scale_key,
                            &cell_key,
                            &format!("trial{trial}_median_ms"),
                            median,
                        );
                        tsv.row(
                            "isolation",
                            &scale_key,
                            &cell_key,
                            &format!("trial{trial}_xact_commit_per_sec"),
                            delta.xact_commit_per_sec,
                        );
                    }
                    drop(trial_latencies);

                    for slot in &slots {
                        force_drop_slot(&mut setup, slot);
                    }
                }

                let avg = |v: &[f64]| -> f64 {
                    if v.is_empty() {
                        0.0
                    } else {
                        v.iter().sum::<f64>() / v.len() as f64
                    }
                };
                let stddev = |v: &[f64]| -> f64 {
                    if v.len() < 2 {
                        return 0.0;
                    }
                    let m = avg(v);
                    (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
                };
                let med_mean = avg(&per_trial_medians);
                let med_std = stddev(&per_trial_medians);
                println!(
                    "  rate={total_rate:>5}/s  N={n:>3}  {cell_key:<10}  median(mean±std)={med_mean:>7.1}±{med_std:.1}ms"
                );
                tsv.row(
                    "isolation",
                    &scale_key,
                    &cell_key,
                    "across_trial_median_mean_ms",
                    med_mean,
                );
                tsv.row(
                    "isolation",
                    &scale_key,
                    &cell_key,
                    "across_trial_median_std_ms",
                    med_std,
                );
                tsv.row(
                    "isolation",
                    &scale_key,
                    &cell_key,
                    "across_trial_xact_mean",
                    avg(&per_trial_xact),
                );
                tsv.row_int(
                    "isolation",
                    &scale_key,
                    &cell_key,
                    "consumer_count",
                    n as i64,
                );
                tsv.row_int(
                    "isolation",
                    &scale_key,
                    &cell_key,
                    "total_rate",
                    total_rate as i64,
                );
            }
        }
        println!();
    }

    println!("Captured TSV: {tsv_path}");
}
