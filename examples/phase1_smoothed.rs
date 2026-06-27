//! Smoothed Phase 1 benchmark.
//!
//! Phase 1's original sequential structure (W1.1 → W1.2 → W1.3 in one
//! Tokio runtime with sources reconstructed per measurement) showed
//! ~3-5x run-to-run variance on whichever workload happened to run
//! first. The W1.2 and W1.3 investigations established that a
//! fresh-source-per-trial rig produces tight numbers at the wire-RTT
//! floor.
//!
//! This example combines both lessons: each cell runs N TRIALS, each
//! trial gets a fresh slot and a fresh source, and the per-cell
//! samples are pooled across trials before computing stats. Events
//! per trial are scaled by polling cadence so wall-clock stays
//! roughly constant across cells (~30-50 s each).
//!
//! Single workload: steady-rate single-row INSERTs at
//! `gap = max(50ms, 1.3 x poll_interval)`, mirroring the prior
//! one-shot benchmark methodology. Commits land at varied offsets
//! within polling cycles so phase-locking doesn't bias the median.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase1_smoothed --features pg-streaming
//! ```

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

use diesel::{sql_query, PgConnection, RunQueryDsl};
use subql::{
    EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource,
};

use common::{
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, create_publication_all,
    markdown_table_header, parser_db, pg_connect, pg_port, pg_replication_url, pg_url,
    pg_with_wal2json, resolve_table_ids, setup_schema, spawn_full_event_receiver, EventKey,
    LatencyStats,
};

const TRIALS_PER_CELL: usize = 3;

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

fn insert_order_sql(id: i64) -> String {
    format!(
        "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
         VALUES ({id}, 1, 'paid', {p}, NOW())",
        p = id * 100
    )
}

fn force_drop_slot(conn: &mut PgConnection, slot: &str) {
    let _ = sql_query(format!(
        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
         WHERE slot_name = '{slot}' AND active_pid IS NOT NULL"
    ))
    .execute(conn);
    std::thread::sleep(Duration::from_millis(200));
    let _ = sql_query(format!(
        "SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots \
         WHERE slot_name = '{slot}'"
    ))
    .execute(conn);
}

#[derive(Debug, Clone, Copy)]
enum Transport {
    Push,
    Poll { interval_ms: u64 },
}

impl Transport {
    fn label(self) -> String {
        match self {
            Self::Push => "push (PgStreamingCdcSource)".to_string(),
            Self::Poll { interval_ms } => format!("poll @ {interval_ms}ms interval"),
        }
    }

    fn slot_tag(self) -> String {
        match self {
            Self::Push => "push".to_string(),
            Self::Poll { interval_ms } => format!("poll_{interval_ms}"),
        }
    }

    /// Insert gap: max(50 ms, 1.3 x polling interval). Matches the
    /// prior one-shot benchmark methodology so commits land at varied
    /// offsets within polling cycles.
    fn insert_gap(self) -> Duration {
        match self {
            Self::Push => Duration::from_millis(50),
            Self::Poll { interval_ms } => {
                let scaled = Duration::from_millis(interval_ms) * 13 / 10;
                std::cmp::max(Duration::from_millis(50), scaled)
            }
        }
    }

    /// Per-trial event count, scaled so wall-clock per trial is
    /// roughly constant across cadences (target ~10 s per trial).
    const fn events_per_trial(self) -> i64 {
        match self {
            Self::Push => 200,
            Self::Poll { interval_ms } => {
                if interval_ms >= 1_000 {
                    30
                } else if interval_ms >= 100 {
                    100
                } else {
                    200
                }
            }
        }
    }
}

/// Drive `count` single-row INSERTs at `gap`. Returns commit_times
/// map keyed on `(table_id, pk, Insert)`.
async fn drive_inserts(
    conn: &mut PgConnection,
    id_base: i64,
    count: i64,
    gap: Duration,
    orders_table_id: u32,
) -> HashMap<EventKey, Instant> {
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(count as usize);
    for offset in 0..count {
        if offset > 0 {
            tokio::time::sleep(gap).await;
        }
        let id = id_base + offset;
        let commit_at = Instant::now();
        sql_query(insert_order_sql(id))
            .execute(conn)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
    }
    commits
}

/// Run one trial: fresh slot, fresh source, drive `count` inserts,
/// collect per-event latencies. Returns the per-event samples for
/// this trial; the caller pools them across trials.
async fn run_trial(
    pg_repl_url: &str,
    pg_sql_url: &str,
    slot: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    orders_table_id: u32,
    transport: Transport,
    id_base: i64,
    dml: &mut PgConnection,
) -> Vec<Duration> {
    let count = transport.events_per_trial();
    let gap = transport.insert_gap();
    match transport {
        Transport::Push => {
            let config = PgStreamingConfig::new(pg_repl_url.to_string(), slot, publication);
            let source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("connect push source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(500)).await;
            let commit_times = drive_inserts(dml, id_base, count, gap, orders_table_id).await;
            let deadline = Instant::now() + Duration::from_secs(20);
            let samples = collect_full_latencies(&commit_times, rx, deadline).await;
            task.abort();
            samples
        }
        Transport::Poll { interval_ms } => {
            let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), slot, publication)
                .poll_interval(Duration::from_millis(interval_ms));
            let source = PollingPgCdcSource::connect(config, catalog)
                .await
                .expect("connect polling source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let commit_times = drive_inserts(dml, id_base, count, gap, orders_table_id).await;
            let deadline =
                Instant::now() + Duration::from_secs(20) + Duration::from_millis(interval_ms) * 4;
            let samples = collect_full_latencies(&commit_times, rx, deadline).await;
            task.abort();
            samples
        }
    }
}

fn main() {
    assert_docker_available();
    println!("Smoothed Phase 1");
    println!(
        "{TRIALS_PER_CELL} trials per cell with fresh source per trial; samples pooled for stats."
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "phase1_smoothed_pub";
    create_publication_all(&mut setup, publication);

    // Warmup pass so PG / Docker / filesystem cache is hot before
    // the first measured trial.
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

    let rt = current_thread_rt();
    // Each trial gets a 10k-id block so no transport's range can
    // ever overlap another's, even if the per-trial count varies.
    const ID_STEP: i64 = 10_000;
    let mut id_counter: i64 = 0;
    let mut pooled_stats: Vec<LatencyStats> = Vec::with_capacity(transports.len());

    for &transport in &transports {
        let mut pooled: Vec<Duration> = Vec::new();
        for trial in 0..TRIALS_PER_CELL {
            id_counter += ID_STEP;
            let id_base = id_counter;
            let slot = format!("phase1_sm_{}_t{trial}", transport.slot_tag());
            create_pgoutput_slot(&mut setup, &slot);
            let samples = rt.block_on(run_trial(
                &pg_repl_url,
                &pg_sql_url,
                &slot,
                publication,
                Arc::clone(&catalog),
                table_ids.orders,
                transport,
                id_base,
                &mut dml,
            ));
            println!(
                "  {} trial {}/{}: collected {}/{} events",
                transport.label(),
                trial + 1,
                TRIALS_PER_CELL,
                samples.len(),
                transport.events_per_trial(),
            );
            pooled.extend(samples);
            force_drop_slot(&mut setup, &slot);
        }
        let stats = LatencyStats::new(transport.label(), pooled);
        println!("{}", stats.text_summary());
        println!();
        pooled_stats.push(stats);
    }

    println!("---");
    println!();
    println!("### Smoothed Phase 1: pooled across {TRIALS_PER_CELL} trials per cell");
    println!();
    println!("{}", markdown_table_header());
    for stats in &pooled_stats {
        println!("{}", stats.markdown_row());
    }
    println!();

    // Architectural-claim invariant: push median must beat poll@100ms
    // and poll@1000ms. With pooled samples across multiple trials,
    // this is the load-bearing check; absolute numbers should be
    // stable across re-runs.
    let push_median = pooled_stats[0].median();
    let poll_100_median = pooled_stats[2].median();
    let poll_1000_median = pooled_stats[3].median();
    assert!(
        push_median < poll_100_median,
        "push median {push_median:?} must beat poll@100ms median {poll_100_median:?}"
    );
    assert!(
        push_median < poll_1000_median,
        "push median {push_median:?} must beat poll@1000ms median {poll_1000_median:?}"
    );
    println!("Smoothed Phase 1 architectural-claim check: PASS");
    println!(
        "  push median = {:.1} ms,  poll@100ms median = {:.1} ms (savings: {:.1} ms),  poll@1000ms median = {:.1} ms (savings: {:.1} ms)",
        common::ms_of(push_median),
        common::ms_of(poll_100_median),
        common::ms_of(poll_100_median) - common::ms_of(push_median),
        common::ms_of(poll_1000_median),
        common::ms_of(poll_1000_median) - common::ms_of(push_median),
    );
}
