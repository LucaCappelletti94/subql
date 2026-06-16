//! Scale benchmark: per-consumer latency and PG server load vs N
//! concurrent consumers, under two producer shapes.
//!
//! Two experiments:
//!
//! - **Experiment A — per-consumer dedicated producers.** N consumers
//!   plus N dedicated DML producers, each producer driving ~200 ev/s
//!   into its own id range. Total event rate = `N x 200/s`. Matches
//!   real SaaS multi-tenant fan-out where each tenant has its own
//!   writes.
//! - **Experiment B — single shared producer at fixed rate.** One DML
//!   producer at 1000 ev/s; N consumers each drain the same
//!   publication. Cleaner control variable; isolates per-consumer
//!   cost from rate growth.
//!
//! For each (experiment, N, transport): 30 s wall-clock window,
//! single trial, push + poll@100ms. Outputs both pooled latency stats
//! and a `ServerLoadDelta` snapshot.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example scale_consumers --features pg-streaming
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

use diesel::{sql_query, PgConnection, RunQueryDsl};

use common::{
    assert_docker_available, create_publication_all, force_drop_slot, ms_of, parser_db, pg_connect,
    pg_port, pg_replication_url, pg_url, pg_with_wal2json, precreate_poll_slots,
    precreate_push_slots, resolve_table_ids, setup_schema, snapshot_delta, snapshot_server_load,
    spawn_n_poll_consumers, spawn_n_push_consumers, ConsumerHandle, EventKey, EventObservation,
    LatencyStats, TsvWriter,
};
use subql::EventKind;

const WINDOW: Duration = Duration::from_secs(30);
const CONSUMER_COUNTS: &[usize] = &[1, 5, 10, 30, 100];
const PER_CONSUMER_RATE_A: u64 = 200;
const SHARED_RATE_B: u64 = 1_000;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const BUFFER_CAPACITY: usize = 4_096;

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

/// Spawn N producers, each driving `per_rate` events/sec into its own
/// id range, for `duration`. Returns N commit-time maps (one per
/// producer) and a vector of join handles for the producer tasks.
async fn spawn_n_producers(
    n: usize,
    pg_url: &str,
    id_base: i64,
    per_rate: u64,
    duration: Duration,
    orders_table_id: u32,
) -> (
    Vec<HashMap<EventKey, Instant>>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    use diesel::Connection;
    let mut rxs: Vec<tokio::sync::oneshot::Receiver<HashMap<EventKey, Instant>>> =
        Vec::with_capacity(n);
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(n);
    for i in 0..n {
        let (tx, rx) = tokio::sync::oneshot::channel();
        rxs.push(rx);
        let producer_id_base = id_base + (i as i64) * 1_000_000;
        let url = pg_url.to_string();
        let task = tokio::task::spawn_blocking(move || {
            let mut conn = diesel::PgConnection::establish(&url).expect("PG conn");
            let gap = Duration::from_secs_f64(1.0 / per_rate as f64);
            let end = Instant::now() + duration;
            let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(
                ((per_rate as usize) * duration.as_secs() as usize).max(1024),
            );
            let mut id = producer_id_base;
            while Instant::now() < end {
                let batch_start = Instant::now();
                let commit_at = Instant::now();
                sql_query(format!(
                    "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                     VALUES ({id}, 1, 'paid', {p}, NOW())",
                    p = id * 100
                ))
                .execute(&mut conn)
                .unwrap_or_else(|e| panic!("producer {i} insert id={id}: {e}"));
                commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
                id += 1;
                if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                    std::thread::sleep(remaining);
                }
            }
            let _ = tx.send(commits);
        });
        tasks.push(task);
    }
    let mut commits_per_producer: Vec<HashMap<EventKey, Instant>> = Vec::with_capacity(n);
    for rx in rxs {
        commits_per_producer.push(rx.await.expect("producer commit map"));
    }
    (commits_per_producer, tasks)
}

/// Drain observations from N receiver channels until each is empty or
/// `deadline` elapses. Aborts each consumer's task on the way out.
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
    for drainer in drainers {
        all.push(drainer.await.unwrap_or_default());
    }
    all
}

/// Pool observations from N consumers into a single sample vector
/// against a merged commit-times map.
fn pool_latencies(
    commits_per_producer: &[HashMap<EventKey, Instant>],
    observations: &[Vec<EventObservation>],
) -> Vec<Duration> {
    let mut merged: HashMap<EventKey, Instant> = HashMap::new();
    for c in commits_per_producer {
        merged.extend(c.iter().map(|(k, v)| (*k, *v)));
    }
    let mut samples = Vec::new();
    for consumer_obs in observations {
        let mut seen = std::collections::HashSet::new();
        for ob in consumer_obs {
            let key = (ob.table_id, ob.pk_int, ob.kind);
            if !seen.insert(key) {
                continue;
            }
            if let Some(commit_at) = merged.get(&key) {
                samples.push(ob.observed_at.saturating_duration_since(*commit_at));
            }
        }
    }
    samples
}

#[derive(Debug, Clone, Copy)]
enum Experiment {
    PerConsumerProducers, // Experiment A
    SharedProducer,       // Experiment B
}

impl Experiment {
    const fn label(self) -> &'static str {
        match self {
            Self::PerConsumerProducers => "exp_a_per_consumer_producers",
            Self::SharedProducer => "exp_b_shared_producer",
        }
    }
}

fn main() {
    assert_docker_available();
    println!("Scale benchmark: latency + server load vs consumer count");
    println!(
        "Experiment A: N producers x {PER_CONSUMER_RATE_A} ev/s.  Experiment B: 1 producer x {SHARED_RATE_B} ev/s."
    );
    println!(
        "Window: {} s per cell, push + poll@{}ms.",
        WINDOW.as_secs(),
        POLL_INTERVAL.as_millis()
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);
    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "scale_consumers_pub";
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

    let tsv_path = "docs/benchmarks/scale-consumers-2026-06-15.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV writer");

    let rt = current_thread_rt();
    let mut id_counter: i64 = 1_000_000;

    for &experiment in &[Experiment::PerConsumerProducers, Experiment::SharedProducer] {
        for &n in CONSUMER_COUNTS {
            for transport in [Transport::Push, Transport::Poll] {
                let label_prefix = format!("scale_c_{}", experiment.label());
                let slots = match transport {
                    Transport::Push => precreate_push_slots(&mut setup, &label_prefix, n),
                    Transport::Poll => {
                        precreate_poll_slots(&mut setup, &label_prefix, POLL_INTERVAL, n)
                    }
                };
                // Each cell needs MAX_N * per_producer_width = 100 * 1M = 100M
                // distinct ids. Use a 500M increment to leave plenty of headroom.
                id_counter += 500_000_000;
                let id_base = id_counter;

                let baseline = snapshot_server_load(&mut setup);
                let pg_sql_url_cloned = pg_sql_url.clone();
                let pg_repl_url_cloned = pg_repl_url.clone();
                let catalog_cloned = Arc::clone(&catalog);
                let table_id = table_ids.orders;

                let pooled_latencies = rt.block_on(async move {
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

                    let (commits_per_producer, producer_tasks) = match experiment {
                        Experiment::PerConsumerProducers => {
                            spawn_n_producers(
                                n,
                                &pg_sql_url_cloned,
                                id_base,
                                PER_CONSUMER_RATE_A,
                                WINDOW,
                                table_id,
                            )
                            .await
                        }
                        Experiment::SharedProducer => {
                            spawn_n_producers(
                                1,
                                &pg_sql_url_cloned,
                                id_base,
                                SHARED_RATE_B,
                                WINDOW,
                                table_id,
                            )
                            .await
                        }
                    };
                    for t in producer_tasks {
                        let _ = t.await;
                    }

                    let deadline = Instant::now() + Duration::from_secs(5) + POLL_INTERVAL * 4;
                    let observations = drain_n(handles, deadline).await;
                    pool_latencies(&commits_per_producer, &observations)
                });
                let after = snapshot_server_load(&mut setup);
                let delta = snapshot_delta(&baseline, &after);

                let scale_key = format!("{}_n{n}", experiment.label());
                let cell_key = transport.label().to_string();
                if pooled_latencies.is_empty() {
                    println!(
                        "  {:<35}  N={:>3}  {:<10}  NO SAMPLES  active_pid_end={}",
                        experiment.label(),
                        n,
                        cell_key,
                        delta.active_pid_count_end,
                    );
                } else {
                    let stats = LatencyStats::new(cell_key.clone(), pooled_latencies.clone());
                    tsv.row(
                        "consumers",
                        &scale_key,
                        &cell_key,
                        "median_ms",
                        ms_of(stats.median()),
                    );
                    tsv.row(
                        "consumers",
                        &scale_key,
                        &cell_key,
                        "p99_ms",
                        ms_of(stats.p99()),
                    );
                    tsv.row(
                        "consumers",
                        &scale_key,
                        &cell_key,
                        "samples",
                        stats.samples.len() as f64,
                    );
                    println!(
                        "  {:<35}  N={:>3}  {:<10}  median={:>6.1}ms  p99={:>6.1}ms  active_pid_end={}  xact/s={:.0}  tup/s={:.0}",
                        experiment.label(),
                        n,
                        cell_key,
                        ms_of(stats.median()),
                        ms_of(stats.p99()),
                        delta.active_pid_count_end,
                        delta.xact_commit_per_sec,
                        delta.tup_inserted_per_sec,
                    );
                }
                tsv.row(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "xact_commit_per_sec",
                    delta.xact_commit_per_sec,
                );
                tsv.row(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "tup_inserted_per_sec",
                    delta.tup_inserted_per_sec,
                );
                tsv.row_int(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "active_pid_count_end",
                    delta.active_pid_count_end,
                );
                tsv.row_int(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "active_query_count_end",
                    delta.active_query_count_end,
                );
                tsv.row_int(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "slot_count_end",
                    delta.slot_count_end,
                );
                tsv.row_int(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "wal_bytes_written",
                    delta.wal_bytes_written,
                );
                tsv.row_int(
                    "consumers",
                    &scale_key,
                    &cell_key,
                    "consumer_count",
                    n as i64,
                );

                for slot in &slots {
                    force_drop_slot(&mut setup, slot);
                }
            }
            println!();
        }
    }

    println!("Captured TSV: {tsv_path}");
}
