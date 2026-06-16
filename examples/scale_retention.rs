//! Scale benchmark: WAL retention dynamics under realistic
//! consumer behavior.
//!
//! Three scenarios x {push, poll} x N in {5, 30}, 90 s window each:
//!
//! - **A: All healthy.** N consumers draining at line rate. Baseline.
//! - **B: One slow consumer.** N-1 healthy + 1 with 50 ms per-event
//!   sleep. Measures whether the slow consumer's slot lag drags
//!   healthy slots' lag.
//! - **C: One crashed consumer.** N consumers; at t=30 s, abort one
//!   consumer's task. Measures slot lag for the dead slot vs healthy
//!   slots over the subsequent 60 s.
//!
//! Slot lag is sampled every 5 s via `pg_replication_slots`. Output
//! is time-series TSV plus a summary Markdown table.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example scale_retention --features pg-streaming
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

use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::{sql_query, PgConnection, RunQueryDsl};

use common::{
    assert_docker_available, create_pgoutput_slot, create_publication_all, force_drop_slot,
    parser_db, pg_connect, pg_port, pg_replication_url, pg_url, pg_with_wal2json,
    resolve_table_ids, setup_schema, snapshot_server_load, spawn_full_event_receiver,
    spawn_slow_event_receiver, EventObservation, TsvWriter,
};
use subql::{PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource};

const WINDOW: Duration = Duration::from_secs(90);
const SAMPLING_INTERVAL: Duration = Duration::from_secs(5);
const PRODUCER_RATE: u64 = 500;
const SLOW_CONSUMER_DELAY: Duration = Duration::from_millis(50);
const CRASH_AT: Duration = Duration::from_secs(30);
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

#[derive(Debug, Clone, Copy)]
enum Scenario {
    AllHealthy,
    OneSlow,
    OneCrashed,
}

impl Scenario {
    const fn label(self) -> &'static str {
        match self {
            Self::AllHealthy => "all_healthy",
            Self::OneSlow => "one_slow",
            Self::OneCrashed => "one_crashed",
        }
    }
}

/// Spawn one push consumer; consumer index 0 may use the slow
/// receiver path when `slow == true`. Returns `(slot_name, receiver,
/// task_handle)`.
async fn spawn_push_consumer(
    slot_name: String,
    pg_repl_url: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    slow: bool,
) -> (
    String,
    tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    tokio::task::JoinHandle<()>,
) {
    let config = PgStreamingConfig::new(pg_repl_url.to_string(), &slot_name, publication)
        .status_interval(STATUS_INTERVAL)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = if slow {
        spawn_slow_event_receiver(source, SLOW_CONSUMER_DELAY)
    } else {
        spawn_full_event_receiver(source)
    };
    (slot_name, rx, task)
}

async fn spawn_poll_consumer(
    slot_name: String,
    pg_sql_url: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    slow: bool,
) -> (
    String,
    tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    tokio::task::JoinHandle<()>,
) {
    let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), &slot_name, publication)
        .poll_interval(POLL_INTERVAL)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    let (rx, task) = if slow {
        spawn_slow_event_receiver(source, SLOW_CONSUMER_DELAY)
    } else {
        spawn_full_event_receiver(source)
    };
    (slot_name, rx, task)
}

/// Continuously drain `rx` until it's closed.
fn spawn_drainer(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { while rx.recv().await.is_some() {} })
}

/// Run a single scenario cell and return per-slot time-series of lag
/// bytes (sampled at `SAMPLING_INTERVAL`).
async fn run_scenario(
    pg_repl_url: &str,
    pg_sql_url: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    setup: &mut PgConnection,
    transport: Transport,
    scenario: Scenario,
    n: usize,
    slot_prefix: &str,
    table_ids_orders: u32,
    id_base: i64,
) -> (Vec<String>, Vec<Vec<(f64, i64)>>) {
    // Pre-create slots for all N consumers.
    let mut slot_names: Vec<String> = (0..n).map(|i| format!("{slot_prefix}_c{i}")).collect();
    for s in &slot_names {
        create_pgoutput_slot(setup, s);
    }

    // Spawn consumers. Consumer 0 may be the slow one (scenario B).
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(n);
    let mut crashed_task_index: Option<usize> = None;
    for (i, slot_name) in slot_names.iter().enumerate() {
        let is_slow = matches!(scenario, Scenario::OneSlow) && i == 0;
        match transport {
            Transport::Push => {
                let (_, rx, task) = spawn_push_consumer(
                    slot_name.clone(),
                    pg_repl_url,
                    publication,
                    Arc::clone(&catalog),
                    is_slow,
                )
                .await;
                let drainer = spawn_drainer(rx);
                // Keep both handles alive; we'll abort the receiver
                // task at the end via tasks vec.
                tasks.push(task);
                if matches!(scenario, Scenario::OneCrashed) && i == 0 {
                    crashed_task_index = Some(tasks.len() - 1);
                }
                // The drainer simply consumes events; abort at end.
                tasks.push(drainer);
            }
            Transport::Poll => {
                let (_, rx, task) = spawn_poll_consumer(
                    slot_name.clone(),
                    pg_sql_url,
                    publication,
                    Arc::clone(&catalog),
                    is_slow,
                )
                .await;
                let drainer = spawn_drainer(rx);
                tasks.push(task);
                if matches!(scenario, Scenario::OneCrashed) && i == 0 {
                    crashed_task_index = Some(tasks.len() - 1);
                }
                tasks.push(drainer);
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start producer.
    let producer_url = pg_sql_url.to_string();
    let producer_id_base = id_base;
    let producer_rate = PRODUCER_RATE;
    let producer_table = table_ids_orders;
    let producer_window = WINDOW;
    let producer = tokio::task::spawn_blocking(move || {
        use diesel::Connection;
        let mut conn = diesel::PgConnection::establish(&producer_url).expect("PG conn");
        let gap = Duration::from_secs_f64(1.0 / producer_rate as f64);
        let end = Instant::now() + producer_window;
        let mut id = producer_id_base;
        while Instant::now() < end {
            let batch_start = Instant::now();
            sql_query(format!(
                "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                 VALUES ({id}, 1, 'paid', {p}, NOW())",
                p = id * 100
            ))
            .execute(&mut conn)
            .unwrap_or_else(|e| panic!("retention insert id={id}: {e}"));
            id += 1;
            if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                std::thread::sleep(remaining);
            }
        }
        // table_id unused in producer but suppress unused warning
        let _ = producer_table;
    });

    // Sample slot lag every SAMPLING_INTERVAL seconds.
    let start = Instant::now();
    let mut samples: Vec<Vec<(f64, i64)>> = vec![Vec::new(); n];
    let mut crash_triggered = false;
    while start.elapsed() < WINDOW {
        let elapsed = start.elapsed().as_secs_f64();
        // Crash trigger for scenario C.
        if matches!(scenario, Scenario::OneCrashed)
            && !crash_triggered
            && start.elapsed() >= CRASH_AT
        {
            if let Some(idx) = crashed_task_index {
                tasks[idx].abort();
                crash_triggered = true;
            }
        }
        let snapshot = snapshot_server_load(setup);
        // Match slot_names to lag entries.
        for (i, name) in slot_names.iter().enumerate() {
            if let Some((_, lag)) = snapshot
                .per_slot_lag_bytes
                .iter()
                .find(|(n_, _)| n_ == name)
            {
                samples[i].push((elapsed, *lag));
            }
        }
        tokio::time::sleep(SAMPLING_INTERVAL).await;
    }
    // Final sample.
    let elapsed = start.elapsed().as_secs_f64();
    let snapshot = snapshot_server_load(setup);
    for (i, name) in slot_names.iter().enumerate() {
        if let Some((_, lag)) = snapshot
            .per_slot_lag_bytes
            .iter()
            .find(|(n_, _)| n_ == name)
        {
            samples[i].push((elapsed, *lag));
        }
    }
    let _ = producer.await;

    // Stop all consumer tasks.
    for task in tasks {
        task.abort();
    }
    // Force-drop slots so the next cell starts clean.
    let names = std::mem::take(&mut slot_names);
    for slot in &names {
        force_drop_slot(setup, slot);
    }
    (names, samples)
}

fn main() {
    assert_docker_available();
    println!("Scale benchmark: WAL retention dynamics");
    println!(
        "{} s window, slot lag sampled every {} s, producer @ {} ev/s.",
        WINDOW.as_secs(),
        SAMPLING_INTERVAL.as_secs(),
        PRODUCER_RATE,
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);
    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "scale_retention_pub";
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

    let tsv_path = "docs/benchmarks/scale-retention-2026-06-15.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV writer");

    let rt = current_thread_rt();
    let mut id_counter: i64 = 5_000_000;

    let scenarios = [
        Scenario::AllHealthy,
        Scenario::OneSlow,
        Scenario::OneCrashed,
    ];
    let consumer_counts = [5usize, 30];
    let transports = [Transport::Push, Transport::Poll];

    for &scenario in &scenarios {
        for &n in &consumer_counts {
            for &transport in &transports {
                id_counter += 1_000_000;
                let id_base = id_counter;
                let slot_prefix =
                    format!("scale_r_{}_{}_n{n}", scenario.label(), transport.label())
                        .replace('@', "")
                        .replace("ms", "");
                println!(
                    "  scenario={:<12}  N={:>3}  transport={:<10}",
                    scenario.label(),
                    n,
                    transport.label(),
                );

                let (slot_names, samples) = rt.block_on(run_scenario(
                    &pg_repl_url,
                    &pg_sql_url,
                    publication,
                    Arc::clone(&catalog),
                    &mut setup,
                    transport,
                    scenario,
                    n,
                    &slot_prefix,
                    table_ids.orders,
                    id_base,
                ));

                // Emit time-series TSV. cell_key encodes scenario + N +
                // transport + slot index.
                for (i, slot_samples) in samples.iter().enumerate() {
                    let slot_label = if matches!(scenario, Scenario::OneSlow) && i == 0 {
                        "slow"
                    } else if matches!(scenario, Scenario::OneCrashed) && i == 0 {
                        "crashed"
                    } else {
                        "healthy"
                    };
                    let cell_key = format!("{}_c{i}_{slot_label}", transport.label());
                    let scale_key = format!("{}_n{n}", scenario.label());
                    for (t, lag) in slot_samples {
                        tsv.row(
                            "retention",
                            &scale_key,
                            &cell_key,
                            &format!("lag_t{t:.0}_bytes"),
                            *lag as f64,
                        );
                    }
                    // Headline: lag at the END of the window.
                    if let Some((_, end_lag)) = slot_samples.last() {
                        tsv.row(
                            "retention",
                            &scale_key,
                            &cell_key,
                            "lag_end_bytes",
                            *end_lag as f64,
                        );
                    }
                    if let Some((_, max_lag)) = slot_samples.iter().max_by_key(|(_, lag)| *lag) {
                        tsv.row(
                            "retention",
                            &scale_key,
                            &cell_key,
                            "lag_max_bytes",
                            *max_lag as f64,
                        );
                    }
                }

                // Print a concise summary: classify slots into
                // healthy/slow/crashed and report mean+max+end lag.
                let (mut healthy_end, mut healthy_max, mut healthy_count) = (0i64, 0i64, 0i64);
                let (mut special_end, mut special_max) = (0i64, 0i64);
                let mut special_label: Option<&str> = None;
                for (i, slot_samples) in samples.iter().enumerate() {
                    let is_special = (matches!(scenario, Scenario::OneSlow)
                        || matches!(scenario, Scenario::OneCrashed))
                        && i == 0;
                    let end_lag = slot_samples.last().map_or(0, |(_, l)| *l);
                    let max_lag = slot_samples.iter().map(|(_, l)| *l).max().unwrap_or(0);
                    if is_special {
                        special_end = end_lag;
                        special_max = max_lag;
                        special_label = if matches!(scenario, Scenario::OneSlow) {
                            Some("slow")
                        } else {
                            Some("crashed")
                        };
                    } else {
                        healthy_end += end_lag;
                        healthy_max = healthy_max.max(max_lag);
                        healthy_count += 1;
                    }
                }
                let healthy_mean_end = if healthy_count > 0 {
                    healthy_end / healthy_count
                } else {
                    0
                };
                println!(
                    "    healthy: mean_end_lag={healthy_mean_end:>10}B  max_lag={healthy_max:>10}B  (n={healthy_count})"
                );
                if let Some(lbl) = special_label {
                    println!(
                        "    {lbl:<8}: end_lag={special_end:>10}B  max_lag={special_max:>10}B"
                    );
                }
                println!();
                let _ = slot_names;
            }
        }
    }

    println!("Captured TSV: {tsv_path}");
}
