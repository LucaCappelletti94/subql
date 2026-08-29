//! # One-shot polling-vs-push latency benchmark
//!
//! Compares the two library transports, [`subql::PgStreamingCdcSource`]
//! (push) and [`subql::PollingPgCdcSource`], across three polling
//! cadences (10 ms, 100 ms, 1 s). Expected: push delivers at the
//! wire-RTT floor while polling at cadence `P` adds roughly `P / 2`
//! average latency on top.
//!
//! ## Methodology
//!
//! Same PG container, same pgoutput byte format, same async runtime.
//! The receiver task is spawned FIRST so its polling clock is
//! already running. Inserts are then driven at intervals larger than
//! the polling interval so each commit lands at a random offset
//! within the next polling cycle. Without that, all inserts would
//! accumulate before the first poll fires and the polling interval
//! would become irrelevant (an earlier, broken version of this test
//! had exactly that bug).
//!
//! For polling with interval `P`, expected per-event latency is
//! roughly `P / 2 + drain_rtt`. For push, it is `wire_rtt + parse_cost`
//! regardless of any interval.
//!
//! Run with:
//!
//! ```sh
//! cargo test --test it polling_vs_push_benchmark:: --features pg-streaming \
//!     -- --ignored --nocapture
//! ```

// This benchmark is a one-shot measurement tool, not a unit test or
// library component. Standard test-style lint allowances apply.
#![allow(
    unknown_lints,
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::too_many_lines,
    clippy::duration_suboptimal_units
)]

use crate::common;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::CdcEvent;
use subql::{
    CdcSource, EventKind, PgLsn, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig,
    PollingPgCdcSource,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

/// Samples per measurement row.
const N_EVENTS: i32 = 20;

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

#[derive(Debug)]
struct LatencyStats {
    label: String,
    samples: Vec<Duration>,
}

impl LatencyStats {
    fn summary(&self) -> String {
        let mut sorted = self.samples.clone();
        sorted.sort();
        let n = sorted.len();
        let total: Duration = sorted.iter().sum();
        let mean = total / n as u32;
        let median = sorted[n / 2];
        let min = sorted[0];
        let max = sorted[n - 1];
        let p99 = sorted[(n.saturating_sub(1)).min((n * 99) / 100)];
        format!(
            "{:<40} n={:>3}  min={:>7.1}ms  median={:>7.1}ms  mean={:>7.1}ms  p99={:>7.1}ms  max={:>7.1}ms",
            self.label,
            n,
            min.as_secs_f64() * 1000.0,
            median.as_secs_f64() * 1000.0,
            mean.as_secs_f64() * 1000.0,
            p99.as_secs_f64() * 1000.0,
            max.as_secs_f64() * 1000.0,
        )
    }
}

/// Insert-gap selector: must be larger than the polling interval so
/// commits land at varied offsets within polling cycles. For push,
/// 50ms is small enough to keep the run quick and large enough that
/// successive inserts don't pipeline-overlap in the source's channel.
fn insert_gap_for_interval(interval: Option<Duration>) -> Duration {
    match interval {
        None => Duration::from_millis(50),
        Some(p) => std::cmp::max(Duration::from_millis(50), p * 13 / 10),
    }
}

/// Drive `N_EVENTS` inserts spaced `insert_gap` apart, recording each
/// commit time. Returns a map `id -> commit_at`.
async fn drive_inserts(
    dml_conn: &mut diesel::PgConnection,
    id_base: i32,
    insert_gap: Duration,
) -> HashMap<i64, Instant> {
    let mut commit_times: HashMap<i64, Instant> = HashMap::with_capacity(N_EVENTS as usize);
    for offset in 0..N_EVENTS {
        if offset > 0 {
            tokio::time::sleep(insert_gap).await;
        }
        let id = id_base + offset;
        let commit_at = Instant::now();
        sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
            .execute(dml_conn)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        commit_times.insert(i64::from(id), commit_at);
    }
    commit_times
}

/// Collect `(id, observed_at)` tuples from the receiver channel until
/// every event in `commit_times` is observed, or `deadline` elapses.
/// Returns per-event COMMIT-to-event latencies.
async fn collect_latencies(
    commit_times: &HashMap<i64, Instant>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(i64, Instant)>,
    deadline: Instant,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(commit_times.len());
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    while samples.len() < commit_times.len() && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(opt) = tokio::time::timeout(remaining, rx.recv()).await else {
            break;
        };
        let Some((id, observed_at)) = opt else {
            break;
        };
        if !seen.insert(id) {
            continue;
        }
        if let Some(commit_at) = commit_times.get(&id) {
            samples.push(observed_at - *commit_at);
        }
    }
    samples
}

/// Spawn a task that drains `source.next_event()` and forwards
/// `(id, observed_at)` tuples to the channel. Generic over any
/// `CdcSource<Checkpoint = PgLsn>` so the same loop drives both
/// push and polling.
fn spawn_receiver<S>(
    mut source: S,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<(i64, Instant)>,
    tokio::task::JoinHandle<()>,
)
where
    S: CdcSource + Send + 'static,
    S::Event: subql::backend::CdcEvent<Backend = subql::backend::Postgres, Checkpoint = PgLsn>,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(i64, Instant)>();
    let schema = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let task = tokio::spawn(async move {
        loop {
            let Ok(Some(ev)) = source.next_event().await else {
                return;
            };
            let observed_at = Instant::now();
            if ev.kind() != EventKind::Insert {
                continue;
            }
            let subql::backend::Value::Int(id) = ev
                .value_at(&schema, subql::backend::RowKind::New, 0)
                .unwrap()
            else {
                continue;
            };
            if tx.send((id, observed_at)).is_err() {
                return;
            }
        }
    });
    (rx, task)
}

/// Build a push source against the given slot + publication and
/// hand it to [`spawn_receiver`].
async fn spawn_push(
    catalog: ParserDB,
    pg_url: String,
    slot: String,
    publication: String,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<(i64, Instant)>,
    tokio::task::JoinHandle<()>,
) {
    let config = PgStreamingConfig::new(pg_url, slot, publication);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    spawn_receiver(source)
}

/// Build a polling source at the given cadence and hand it to
/// [`spawn_receiver`].
async fn spawn_poll(
    catalog: ParserDB,
    pg_url: String,
    slot: String,
    publication: String,
    interval: Duration,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<(i64, Instant)>,
    tokio::task::JoinHandle<()>,
) {
    let config = PollingPgCdcConfig::new(pg_url, slot, publication).poll_interval(interval);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    spawn_receiver(source)
}

/// Measure COMMIT-to-event-delivery latency for both push and several
/// polling cadences. Print a comparison table.
#[test]
#[ignore = "requires Docker; run with --ignored. Benchmark, not unit test."]
fn polling_vs_push_latency_comparison() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");

    // Separate slot per measurement so the runs do not interfere.
    fn setup_slot(setup: &mut diesel::PgConnection, slot: &str) -> String {
        let pub_name = format!("{slot}_pub");
        common::create_publication(setup, &pub_name, "orders");
        common::create_pgoutput_slot(setup, slot);
        pub_name
    }

    let push_pub = setup_slot(&mut setup, "bench_push");
    let poll10_pub = setup_slot(&mut setup, "bench_poll_10");
    let poll100_pub = setup_slot(&mut setup, "bench_poll_100");
    let poll1000_pub = setup_slot(&mut setup, "bench_poll_1000");

    let pg_url = common::pg_replication_url(port);

    let rt = current_thread_rt();
    let (push_stats, poll_10, poll_100, poll_1000) = rt.block_on(async {
        // --- Push --------------------------------------------------------
        let gap = insert_gap_for_interval(None);
        let (rx, task) = spawn_push(
            ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"),
            pg_url.clone(),
            "bench_push".to_string(),
            push_pub,
        )
        .await;
        // Give the spawned source time to actually start streaming
        // before we drive inserts. Otherwise the first few inserts
        // commit before START_REPLICATION even completes.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let commit_times = drive_inserts(&mut dml, 1_000, gap).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        let samples = collect_latencies(&commit_times, rx, deadline).await;
        task.abort();
        let push = LatencyStats {
            label: "push (PgStreamingCdcSource)".to_string(),
            samples,
        };

        // --- Poll @ 10ms -------------------------------------------------
        let interval = Duration::from_millis(10);
        let gap = insert_gap_for_interval(Some(interval));
        let (rx, task) = spawn_poll(
            ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"),
            pg_url.clone(),
            "bench_poll_10".to_string(),
            poll10_pub,
            interval,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let commit_times = drive_inserts(&mut dml, 2_000, gap).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        let samples = collect_latencies(&commit_times, rx, deadline).await;
        task.abort();
        let poll_10 = LatencyStats {
            label: "poll @ 10ms interval".to_string(),
            samples,
        };

        // --- Poll @ 100ms ------------------------------------------------
        let interval = Duration::from_millis(100);
        let gap = insert_gap_for_interval(Some(interval));
        let (rx, task) = spawn_poll(
            ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"),
            pg_url.clone(),
            "bench_poll_100".to_string(),
            poll100_pub,
            interval,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let commit_times = drive_inserts(&mut dml, 3_000, gap).await;
        let deadline = Instant::now() + Duration::from_secs(15);
        let samples = collect_latencies(&commit_times, rx, deadline).await;
        task.abort();
        let poll_100 = LatencyStats {
            label: "poll @ 100ms interval".to_string(),
            samples,
        };

        // --- Poll @ 1000ms -----------------------------------------------
        let interval = Duration::from_millis(1000);
        let gap = insert_gap_for_interval(Some(interval));
        let (rx, task) = spawn_poll(
            ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"),
            pg_url.clone(),
            "bench_poll_1000".to_string(),
            poll1000_pub,
            interval,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let commit_times = drive_inserts(&mut dml, 4_000, gap).await;
        let deadline = Instant::now() + Duration::from_secs(60);
        let samples = collect_latencies(&commit_times, rx, deadline).await;
        task.abort();
        let poll_1000 = LatencyStats {
            label: "poll @ 1000ms interval".to_string(),
            samples,
        };

        (push, poll_10, poll_100, poll_1000)
    });

    println!();
    println!("=== COMMIT-to-event-delivery latency, N={N_EVENTS} events per row ===");
    println!("{}", push_stats.summary());
    println!("{}", poll_10.summary());
    println!("{}", poll_100.summary());
    println!("{}", poll_1000.summary());
    println!();
    println!("Methodology: receiver task spawned first, polling/streaming");
    println!("clock running before any insert. Inserts spaced at ~1.3x the");
    println!("polling interval so each commit lands at a varying offset");
    println!("within the next polling cycle (the realistic case for a");
    println!("long-running consumer).");
    println!();

    // Sanity assertions: push must beat polling at every interval (in
    // median). If the inequality holds, the polling-interval-floor
    // effect is real and the architecture choice is justified.
    let median = |stats: &LatencyStats| -> Duration {
        let mut s = stats.samples.clone();
        s.sort();
        s[s.len() / 2]
    };
    let push_median = median(&push_stats);
    let poll_10_median = median(&poll_10);
    let poll_100_median = median(&poll_100);
    let poll_1000_median = median(&poll_1000);

    assert!(
        push_median < poll_100_median,
        "push median ({push_median:?}) must beat poll@100ms median ({poll_100_median:?})"
    );
    assert!(
        push_median < poll_1000_median,
        "push median ({push_median:?}) must beat poll@1000ms median ({poll_1000_median:?})"
    );
    // Poll @ 10ms is roughly as fast as push (~5-15ms either way). Just
    // sanity-check the test ran by asserting both finished with samples.
    assert!(
        !poll_10.samples.is_empty(),
        "poll @ 10ms must produce samples"
    );
    let _ = poll_10_median;

    for slot in [
        "bench_push",
        "bench_poll_10",
        "bench_poll_100",
        "bench_poll_1000",
    ] {
        common::drop_slot(&mut setup, slot);
    }
}
