//! Scale benchmark: 2D mesh of (N consumers x producer rate) for
//! both transports.
//!
//! Previous experiments swept N at fixed rate (Experiment B) or
//! varied rate at N=1 (`scale_throughput.rs`) or coupled the two
//! (`scale_consumers.rs` Experiment A). This example treats N and
//! event rate as independent axes and emits a long-form (N, rate,
//! transport, median_ms) table suitable for a heatmap.
//!
//! Each (N, rate) cell uses a SINGLE batched producer + N CDC
//! consumers reading the same publication, so the writer side does
//! not scale with N. This isolates per-consumer latency from
//! producer-side contention.
//!
//! Grid: N ∈ {5, 30, 100} × rate ∈ {1000, 5000, 10000, 20000} ev/s
//! Push + poll@100ms per cell, 2 trials per cell, 15 s window.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example scale_mesh --features pg-streaming
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

const WINDOW: Duration = Duration::from_secs(15);
const TRIALS_PER_CELL: usize = 2;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const BUFFER_CAPACITY: usize = 4_096;

const CONSUMER_COUNTS: &[usize] = &[5, 30, 100];
const PRODUCER_RATES: &[u64] = &[1_000, 5_000, 10_000, 20_000];

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

async fn run_batched_producer(
    pg_url: String,
    id_base: i64,
    total_rate: u64,
    duration: Duration,
    orders_table_id: u32,
) -> HashMap<EventKey, Instant> {
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
        let mut commits: HashMap<EventKey, Instant> =
            HashMap::with_capacity(((total_rate as usize) * duration.as_secs() as usize).max(1024));
        let mut id = id_base;
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
                }
                stmts.push_str(" COMMIT;");
                conn.batch_execute(&stmts)
                    .unwrap_or_else(|e| panic!("batch insert: {e}"));
            }
            if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                std::thread::sleep(remaining);
            }
        }
        let _ = tx.send(commits);
    });
    rx.await.expect("producer result")
}

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
    println!("Scale benchmark: 2D mesh of (N consumers x producer rate)");
    println!(
        "{TRIALS_PER_CELL} trials per cell, {} s window. N in {CONSUMER_COUNTS:?}, rate in {PRODUCER_RATES:?} ev/s.",
        WINDOW.as_secs()
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);
    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "scale_mesh_pub";
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

    let tsv_path = "docs/benchmarks/scale-mesh-2026-06-16.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV writer");

    let rt = current_thread_rt();
    let mut id_counter: i64 = 1_000_000;

    for &n in CONSUMER_COUNTS {
        for &rate in PRODUCER_RATES {
            for transport in [Transport::Push, Transport::Poll] {
                let mut per_trial_medians: Vec<f64> = Vec::new();
                let mut per_trial_xact: Vec<f64> = Vec::new();
                let scale_key = format!("n{n}_rate{rate}");
                let cell_key = transport.label().to_string();
                for trial in 0..TRIALS_PER_CELL {
                    let label_prefix = format!("scale_mesh_n{n}_r{rate}_t{trial}");
                    let slots = match transport {
                        Transport::Push => precreate_push_slots(&mut setup, &label_prefix, n),
                        Transport::Poll => {
                            precreate_poll_slots(&mut setup, &label_prefix, POLL_INTERVAL, n)
                        }
                    };
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
                        let commits = run_batched_producer(
                            pg_sql_url_cloned.clone(),
                            id_base,
                            rate,
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
                            "mesh",
                            &scale_key,
                            &cell_key,
                            &format!("trial{trial}_median_ms"),
                            median,
                        );
                        tsv.row(
                            "mesh",
                            &scale_key,
                            &cell_key,
                            &format!("trial{trial}_xact_commit_per_sec"),
                            delta.xact_commit_per_sec,
                        );
                    }

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
                    "  N={n:>3}  rate={rate:>5}/s  {cell_key:<10}  median(mean±std)={med_mean:>7.1}±{med_std:.1}ms"
                );
                tsv.row(
                    "mesh",
                    &scale_key,
                    &cell_key,
                    "across_trial_median_mean_ms",
                    med_mean,
                );
                tsv.row(
                    "mesh",
                    &scale_key,
                    &cell_key,
                    "across_trial_median_std_ms",
                    med_std,
                );
                tsv.row(
                    "mesh",
                    &scale_key,
                    &cell_key,
                    "across_trial_xact_mean",
                    avg(&per_trial_xact),
                );
                tsv.row_int("mesh", &scale_key, &cell_key, "consumer_count", n as i64);
                tsv.row_int("mesh", &scale_key, &cell_key, "producer_rate", rate as i64);
            }
        }
        println!();
    }

    println!("Captured TSV: {tsv_path}");
}
