//! Phase 4 — Adversarial / operational regimes.
//!
//! Tests operational dimensions that don't show up in raw latency:
//! long idle, slow consumer (backpressure), server-side WAL retention
//! under low ack frequency, and connection-flap resilience.
//!
//! Four workloads, four transports:
//!
//! - W4.1 — Long idle. Idle for `PHASE4_IDLE_SECS` seconds (default 30,
//!   plan calls for 300), then issue one INSERT. Tests connection
//!   survival across an idle window. The push source's periodic-pump
//!   sends keepalives every 10 s by default; with a server
//!   `wal_sender_timeout` of 60 s the push source should survive
//!   indefinitely. Polling reconnects each query, so it's expected to
//!   tolerate idle periods up to the server's TCP keepalive.
//! - W4.2 — Slow consumer. The receiver task sleeps 50 ms per event,
//!   while the workload INSERTs 100 rows at 1 ms gap. Producer rate
//!   ~1000/s, consumer rate ~20/s. Backlog accumulates. Push
//!   backpressures via TCP (the inner task stops reading
//!   `CopyBothDuplex`); polling backpressures by skipping polls. Both
//!   must deliver every event in order.
//! - W4.3 — Server WAL retention. Drive 1000 INSERTs at 10 ms gap
//!   (10 s workload). No `ack` is called on either transport. After
//!   the workload, query `pg_replication_slots.confirmed_flush_lsn`
//!   per slot and compare against `pg_current_wal_lsn()`. Polling's
//!   slot auto-advances on each drain; the push slot does not advance
//!   without an explicit ack. The reported lag quantifies this
//!   asymmetry.
//! - W4.4 — Connection-flap resilience. STUB. Requires Docker
//!   network manipulation (`tc netem`, `iptables`, or a sidecar) to
//!   drop the TCP connection mid-workload; the default test
//!   container does not have those capabilities. Planned
//!   methodology is documented inline.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase4_adversarial --features pg-streaming
//! ```
//!
//! Override the W4.1 idle window with:
//!
//! ```sh
//! PHASE4_IDLE_SECS=300 cargo run --release --example phase4_adversarial \
//!     --features pg-streaming
//! ```
//!
//! Pipe to `docs/benchmarks/phase4-<date>.md` to capture the run.

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
use subql::{
    EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource,
};

use common::{
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, create_publication_all,
    markdown_table_header, ms_of, parser_db, pg_connect, pg_port, pg_replication_url, pg_url,
    pg_with_wal2json, resolve_table_ids, setup_schema, spawn_full_event_receiver,
    spawn_slow_event_receiver, EventKey, LatencyStats, TableIds,
};

const DRAIN_GRACE: Duration = Duration::from_secs(30);

const POLL_INTERVALS_MS: &[u64] = &[10, 100, 1_000];

const BUFFER_CAPACITY: usize = 4_096;

/// W4.2 consumer-side delay per event. With 100 events at 1 ms gap,
/// producer rate is ~1000/s and consumer rate is 1 / 50 ms = ~20/s.
/// Backlog grows linearly; per-event end-to-end latency for the kth
/// event is roughly `k * (consumer_delay - producer_gap)` ~= 49 ms x k.
const SLOW_CONSUMER_DELAY: Duration = Duration::from_millis(50);

/// W4.3 commit count. 1000 commits at 10 ms gap = 10 s workload.
const W4_3_COMMITS: i64 = 1_000;
const W4_3_COMMIT_GAP: Duration = Duration::from_millis(10);

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

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

fn insert_sentinel_user(conn: &mut PgConnection) {
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(conn)
    .expect("insert sentinel user");
}

fn insert_order_sql(id: i64) -> String {
    format!(
        "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
         VALUES ({id}, 1, 'paid', {p}, NOW())",
        p = id * 100
    )
}

fn idle_secs_from_env() -> u64 {
    std::env::var("PHASE4_IDLE_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(30)
}

fn slot_name(workload_name: &str, transport: Transport) -> String {
    format!(
        "phase4_{}_{}",
        workload_name.replace('.', "_").to_lowercase(),
        transport.slot_suffix()
    )
}

// ----------------------------------------------------------------------
// W4.1 — Long idle
// ----------------------------------------------------------------------

/// Spawn the source, sleep `idle`, INSERT one row, observe the
/// resulting event's latency. Returns `None` if the source's task
/// exited during the idle window (a connection-survival failure on
/// the transport).
async fn w4_1_for_transport(
    pg_repl_url: &str,
    pg_sql_url: &str,
    slot: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    table_ids: TableIds,
    transport: Transport,
    idle: Duration,
    id: i64,
    dml: &mut PgConnection,
) -> Option<Duration> {
    match transport {
        Transport::Push => {
            let config = PgStreamingConfig::new(pg_repl_url.to_string(), slot, publication)
                .buffer_capacity(BUFFER_CAPACITY);
            let source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("connect push source");
            let task_exited_handle = source.task_exited_handle();
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(500)).await;
            tokio::time::sleep(idle).await;
            if task_exited_handle.load(std::sync::atomic::Ordering::Relaxed) {
                task.abort();
                return None;
            }
            let commit_at = Instant::now();
            sql_query(insert_order_sql(id))
                .execute(dml)
                .unwrap_or_else(|e| panic!("post-idle insert id={id}: {e}"));
            let mut commits: HashMap<EventKey, Instant> = HashMap::new();
            commits.insert((table_ids.orders, id, EventKind::Insert), commit_at);
            let deadline = Instant::now() + Duration::from_secs(10);
            let samples = collect_full_latencies(&commits, rx, deadline).await;
            task.abort();
            samples.into_iter().next()
        }
        Transport::Poll { interval_ms } => {
            let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), slot, publication)
                .poll_interval(Duration::from_millis(interval_ms))
                .buffer_capacity(BUFFER_CAPACITY);
            let source = PollingPgCdcSource::connect(config, catalog)
                .await
                .expect("connect polling source");
            let task_exited_handle = source.task_exited_handle();
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(200)).await;
            tokio::time::sleep(idle).await;
            if task_exited_handle.load(std::sync::atomic::Ordering::Relaxed) {
                task.abort();
                return None;
            }
            let commit_at = Instant::now();
            sql_query(insert_order_sql(id))
                .execute(dml)
                .unwrap_or_else(|e| panic!("post-idle insert id={id}: {e}"));
            let mut commits: HashMap<EventKey, Instant> = HashMap::new();
            commits.insert((table_ids.orders, id, EventKind::Insert), commit_at);
            let deadline =
                Instant::now() + Duration::from_secs(10) + Duration::from_millis(interval_ms) * 4;
            let samples = collect_full_latencies(&commits, rx, deadline).await;
            task.abort();
            samples.into_iter().next()
        }
    }
}

// ----------------------------------------------------------------------
// W4.2 — Slow consumer
// ----------------------------------------------------------------------

async fn drive_w4_2(
    conn: &mut PgConnection,
    id_base: i64,
    table_ids: TableIds,
) -> HashMap<EventKey, Instant> {
    const COUNT: i64 = 100;
    const GAP: Duration = Duration::from_millis(1);
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    for i in 0..COUNT {
        if i > 0 {
            tokio::time::sleep(GAP).await;
        }
        let id = id_base + i;
        let commit_at = Instant::now();
        sql_query(insert_order_sql(id))
            .execute(conn)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        commits.insert((table_ids.orders, id, EventKind::Insert), commit_at);
    }
    commits
}

async fn measure_w4_2_push(
    pg_repl_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    label: String,
    id_base: i64,
    table_ids: TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config =
        PgStreamingConfig::new(pg_repl_url, slot, publication).buffer_capacity(BUFFER_CAPACITY);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = spawn_slow_event_receiver(source, SLOW_CONSUMER_DELAY);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let commit_times = drive_w4_2(dml, id_base, table_ids).await;
    let deadline = Instant::now() + DRAIN_GRACE;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

async fn measure_w4_2_poll(
    pg_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    interval: Duration,
    label: String,
    id_base: i64,
    table_ids: TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PollingPgCdcConfig::new(pg_url, slot, publication)
        .poll_interval(interval)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    let (rx, task) = spawn_slow_event_receiver(source, SLOW_CONSUMER_DELAY);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let commit_times = drive_w4_2(dml, id_base, table_ids).await;
    let deadline = Instant::now() + DRAIN_GRACE + interval * 4;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

// ----------------------------------------------------------------------
// W4.3 — Server WAL retention
// ----------------------------------------------------------------------

/// Query lag between `pg_current_wal_lsn()` and the slot's
/// `confirmed_flush_lsn`, in bytes. PG's `pg_lsn` type supports
/// subtraction returning the byte difference as `numeric`.
fn slot_lag_bytes(conn: &mut PgConnection, slot: &str) -> Option<i64> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        lag_bytes: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(format!(
        "SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::BIGINT AS lag_bytes \
         FROM pg_replication_slots WHERE slot_name = '{slot}'"
    ))
    .load(conn)
    .expect("query slot lag");
    rows.into_iter().next().map(|r| r.lag_bytes)
}

/// Drive 1000 single-row INSERTs at 10 ms gap. Returns the commit map
/// for latency tracking AND the wall-clock duration of the workload
/// for context.
async fn drive_w4_3(
    conn: &mut PgConnection,
    id_base: i64,
    table_ids: TableIds,
) -> HashMap<EventKey, Instant> {
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(W4_3_COMMITS as usize);
    for c in 0..W4_3_COMMITS {
        if c > 0 {
            tokio::time::sleep(W4_3_COMMIT_GAP).await;
        }
        let id = id_base + c;
        let commit_at = Instant::now();
        sql_query(insert_order_sql(id))
            .execute(conn)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        commits.insert((table_ids.orders, id, EventKind::Insert), commit_at);
    }
    commits
}

async fn run_w4_3_for_transport(
    pg_repl_url: &str,
    pg_sql_url: &str,
    slot: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    table_ids: TableIds,
    transport: Transport,
    id_base: i64,
    dml: &mut PgConnection,
    setup: &mut PgConnection,
) -> (LatencyStats, Option<i64>) {
    let label = transport.label();
    match transport {
        Transport::Push => {
            let config = PgStreamingConfig::new(pg_repl_url.to_string(), slot, publication)
                .buffer_capacity(BUFFER_CAPACITY);
            let source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("connect push source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(500)).await;
            let commit_times = drive_w4_3(dml, id_base, table_ids).await;
            let deadline = Instant::now() + DRAIN_GRACE;
            let samples = collect_full_latencies(&commit_times, rx, deadline).await;
            // Read slot lag IMMEDIATELY after drain completes; the
            // push slot's confirmed_flush_lsn only advances on ack
            // (which we deliberately never called), so the lag should
            // be substantial.
            let lag = slot_lag_bytes(setup, slot);
            task.abort();
            (LatencyStats::new(label, samples), lag)
        }
        Transport::Poll { interval_ms } => {
            let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), slot, publication)
                .poll_interval(Duration::from_millis(interval_ms))
                .buffer_capacity(BUFFER_CAPACITY);
            let source = PollingPgCdcSource::connect(config, catalog)
                .await
                .expect("connect polling source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let commit_times = drive_w4_3(dml, id_base, table_ids).await;
            let deadline = Instant::now() + DRAIN_GRACE + Duration::from_millis(interval_ms) * 4;
            let samples = collect_full_latencies(&commit_times, rx, deadline).await;
            // Polling auto-advances confirmed_flush_lsn on each
            // drain, so the lag here should be near zero.
            let lag = slot_lag_bytes(setup, slot);
            task.abort();
            (LatencyStats::new(label, samples), lag)
        }
    }
}

// ----------------------------------------------------------------------
// W4.4 — Connection-flap resilience (STUB)
// ----------------------------------------------------------------------

/// W4.4 stub. The full test requires dropping the TCP connection
/// mid-workload via Docker network manipulation (`tc netem`,
/// `iptables`, or a sidecar). Neither transport implements automatic
/// reconnection — both return `Err` / `None` from `next_event` when
/// the connection dies — so the test would verify failure modes:
///
/// 1. Spawn the source.
/// 2. Drive 100 INSERTs at 10 ms gap.
/// 3. At t = 500 ms (mid-workload), drop the TCP connection
///    (`iptables -A INPUT -p tcp --dport 5432 -j DROP` inside the
///    container, or similar).
/// 4. Observe `source.next_event()` returning `Err` for push and
///    `Err` for polling (both go through `tokio_postgres::Client`).
/// 5. Restore the connection. Verify both sources are dead (no
///    automatic reconnect).
/// 6. Reconstruct each source against the existing slot and
///    measure recovery latency.
///
/// The default `testcontainers` setup does not provide `iptables` or
/// `tc` inside the PG container. Implementing this would require
/// either a custom Dockerfile with `NET_ADMIN` capability or a
/// sidecar container. Out of scope for this Phase 4 implementation.
fn run_w4_4_stub() {
    println!("=== W4.4 — Connection-flap resilience ===");
    println!("SKIPPED: requires Docker network manipulation (NET_ADMIN capability + iptables/tc).");
    println!("See `examples/phase4_adversarial.rs` source for the planned methodology.");
    println!();
}

fn main() {
    assert_docker_available();
    let idle_secs = idle_secs_from_env();
    println!("Phase 4 — Adversarial / operational regimes");
    println!(
        "W4.1 idle window: {idle_secs} s (override with PHASE4_IDLE_SECS=NNN; plan calls for 300)."
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "phase4_pub";
    create_publication_all(&mut setup, publication);

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let transports = {
        let mut v = Vec::with_capacity(1 + POLL_INTERVALS_MS.len());
        v.push(Transport::Push);
        for &ms in POLL_INTERVALS_MS {
            v.push(Transport::Poll { interval_ms: ms });
        }
        v
    };

    let rt = current_thread_rt();
    let idle = Duration::from_secs(idle_secs);

    // ---- W4.1 ----------------------------------------------------
    println!("=== W4.1 — Long idle ({idle_secs}s), then 1 INSERT ===");
    let w4_1_id_base: i64 = 800_000;
    let mut w4_1_samples: Vec<(String, Option<Duration>)> = Vec::with_capacity(transports.len());
    for (ti, &t) in transports.iter().enumerate() {
        let slot = slot_name("W4.1", t);
        create_pgoutput_slot(&mut setup, &slot);
        let label = t.label();
        let id = w4_1_id_base + (ti as i64) * 100;
        let sample = rt.block_on(w4_1_for_transport(
            &pg_repl_url,
            &pg_sql_url,
            &slot,
            publication,
            Arc::clone(&catalog),
            table_ids,
            t,
            idle,
            id,
            &mut dml,
        ));
        match sample {
            Some(d) => println!("{label:<40} survived idle; latency = {:.1} ms", ms_of(d)),
            None => println!("{label:<40} DIED during idle window"),
        }
        w4_1_samples.push((label, sample));
    }
    println!();

    // ---- W4.2 ----------------------------------------------------
    println!(
        "=== W4.2 — Slow consumer (100 events at 1ms gap, consumer sleeps {} ms / event) ===",
        SLOW_CONSUMER_DELAY.as_millis()
    );
    let w4_2_id_base: i64 = 810_000;
    let mut w4_2_stats: Vec<LatencyStats> = Vec::with_capacity(transports.len());
    for (ti, &t) in transports.iter().enumerate() {
        let slot = slot_name("W4.2", t);
        create_pgoutput_slot(&mut setup, &slot);
        let label = t.label();
        let id_base = w4_2_id_base + (ti as i64) * 1_000;
        let stats = rt.block_on(async {
            match t {
                Transport::Push => {
                    measure_w4_2_push(
                        pg_repl_url.clone(),
                        slot,
                        publication.to_string(),
                        Arc::clone(&catalog),
                        label,
                        id_base,
                        table_ids,
                        &mut dml,
                    )
                    .await
                }
                Transport::Poll { interval_ms } => {
                    measure_w4_2_poll(
                        pg_sql_url.clone(),
                        slot,
                        publication.to_string(),
                        Arc::clone(&catalog),
                        Duration::from_millis(interval_ms),
                        label,
                        id_base,
                        table_ids,
                        &mut dml,
                    )
                    .await
                }
            }
        });
        println!("{}", stats.text_summary());
        w4_2_stats.push(stats);
    }
    println!();

    // ---- W4.3 ----------------------------------------------------
    println!("=== W4.3 — Server WAL retention (1000 INSERTs at 10ms gap, no acks called) ===");
    let w4_3_id_base: i64 = 820_000;
    let mut w4_3_rows: Vec<(String, LatencyStats, Option<i64>)> =
        Vec::with_capacity(transports.len());
    for (ti, &t) in transports.iter().enumerate() {
        let slot = slot_name("W4.3", t);
        create_pgoutput_slot(&mut setup, &slot);
        let id_base = w4_3_id_base + (ti as i64) * 10_000;
        let label = t.label();
        let (stats, lag) = rt.block_on(run_w4_3_for_transport(
            &pg_repl_url,
            &pg_sql_url,
            &slot,
            publication,
            Arc::clone(&catalog),
            table_ids,
            t,
            id_base,
            &mut dml,
            &mut setup,
        ));
        let lag_str = match lag {
            Some(n) => format!("{n} bytes"),
            None => "n/a".to_string(),
        };
        println!(
            "{:<40} median latency = {:>7.1} ms;  slot lag at end = {lag_str}",
            stats.label,
            ms_of(stats.median()),
        );
        w4_3_rows.push((label, stats, lag));
    }
    println!();

    // ---- W4.4 (stub) ---------------------------------------------
    run_w4_4_stub();

    // ---- Markdown summary ----------------------------------------
    println!("---");
    println!();

    println!("### W4.1 — Long idle ({idle_secs}s)");
    println!();
    println!("| transport | survived | post-idle latency (ms) |");
    println!("| --- | :---: | ---:|");
    for (label, sample) in &w4_1_samples {
        let (survived, latency) = match sample {
            Some(d) => ("Y", format!("{:.1}", ms_of(*d))),
            None => ("N", "n/a".to_string()),
        };
        println!("| {label} | {survived} | {latency} |");
    }
    println!();

    println!("### W4.2 — Slow consumer (consumer sleeps 50 ms / event, producer at 1 ms gap)");
    println!();
    println!("{}", markdown_table_header());
    for stats in &w4_2_stats {
        println!("{}", stats.markdown_row());
    }
    println!();

    println!("### W4.3 — Server WAL retention (no acks called)");
    println!();
    println!("| transport | median latency (ms) | slot lag at end (bytes) |");
    println!("| --- | ---:| ---:|");
    for (label, stats, lag) in &w4_3_rows {
        let lag_str = match lag {
            Some(n) => format!("{n}"),
            None => "n/a".to_string(),
        };
        println!("| {label} | {:.1} | {lag_str} |", ms_of(stats.median()));
    }
    println!();

    // ---- Architectural-claim assertions --------------------------
    //
    // W4.1: both transports MUST survive the idle window at the
    // default 30 s (push's 10 s status pump is well below the 60 s
    // server `wal_sender_timeout`; polling reconnects per-query so
    // survival is trivial). Either dying is a regression.
    for (label, sample) in &w4_1_samples {
        assert!(
            sample.is_some(),
            "W4.1 {label}: transport died during the {idle_secs}s idle window — regression",
        );
    }
    // W4.2: every transport must observe ALL 100 events (consumer
    // backpressure stalls but does not drop). Stats sample count is
    // the load-bearing assertion.
    for stats in &w4_2_stats {
        assert_eq!(
            stats.samples.len(),
            100,
            "W4.2 {}: dropped events under slow consumer (observed {})",
            stats.label,
            stats.samples.len(),
        );
    }
    // W4.3: polling slot lag MUST be small (auto-advances); push
    // slot lag MUST be large (no ack called). The asymmetry is the
    // empirical finding.
    let push_lag = w4_3_rows[0].2.expect("push lag known");
    let poll_100_lag = w4_3_rows[2].2.expect("poll@100ms lag known");
    assert!(
        push_lag > poll_100_lag,
        "W4.3: push slot lag {push_lag} must exceed poll@100ms slot lag {poll_100_lag} \
         (push requires explicit ack; polling auto-advances)",
    );

    println!("Phase 4 architectural-claim check: PASS");
    println!("  W4.1: both transports survived the {idle_secs}s idle window.");
    println!("  W4.2: every transport delivered all 100 events under slow consumer.");
    println!(
        "  W4.3: push slot lag ({push_lag} B) >> polling slot lag ({poll_100_lag} B @ 100ms cadence)."
    );
}
