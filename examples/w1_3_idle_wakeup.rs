//! Investigation: W1.3 post-idle push wake-up anomaly.
//!
//! Phase 1 W1.3 (3 s idle, then 1 INSERT, repeated 5 times) measured
//! a push median of ~30 ms — roughly 6x the wire-bound floor observed
//! in W1.1 (3.9 ms). The architectural claim is "push delivers at
//! wire RTT regardless of cadence", so this is the first cell in the
//! whole sweep where the claim looks wobbly.
//!
//! This example pins the cause down. Two hypotheses:
//!
//! 1. **PG wal sender quiesces.** After idle, the wal sender backs
//!    off; the first post-idle event has extra wake-up latency.
//!    Confirmable: shorter idle → lower latency; longer idle → higher
//!    latency.
//! 2. **The push source's status pump keeps the sender warm.** If
//!    `status_interval` is shorter than the idle window, the periodic
//!    `StandbyStatusUpdate` keeps the sender pingable. Confirmable:
//!    push with `status_interval = 1s` should beat push with
//!    `status_interval = 10s` under a 3 s idle.
//!
//! Methodology: for each `(idle_duration, status_interval)` cell,
//! run 5 trials. Each trial: create a fresh slot, connect a source,
//! sleep `idle_duration`, INSERT one row, measure
//! observed_at - commit_at, abort. Polling @ 100 ms is included as a
//! control to verify the polling cadence absorbs any wake-up cost
//! (a polling source has no concept of "wal sender wake-up": it
//! issues a SQL query every interval regardless).
//!
//! `commit_at` is captured BEFORE the INSERT call, matching the rest
//! of the workload-matrix benchmarks. So the measured latency
//! includes the INSERT round-trip (typically 1-2 ms) plus whatever
//! the transport contributes.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example w1_3_idle_wakeup --features pg-streaming
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
use subql::{
    EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource,
};

use common::{
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, create_publication_all,
    ms_of, parser_db, pg_connect, pg_port, pg_replication_url, pg_url, pg_with_wal2json,
    resolve_table_ids, setup_schema, spawn_full_event_receiver, EventKey,
};

/// Force-drop a replication slot. `pg_drop_replication_slot` refuses
/// to drop a slot whose `active_pid` is set, and `task.abort()` on the
/// streaming source's inner task doesn't reliably tear the TCP
/// connection down before the next trial starts. Terminate the
/// backend first, then drop. Tolerant of missing slots / already-dead
/// backends so the helper is safe to call unconditionally.
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

const TRIALS_PER_CELL: usize = 5;
const IDLE_DURATIONS: &[Duration] = &[
    Duration::from_millis(0),
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(3),
];
const PUSH_STATUS_INTERVALS: &[Duration] = &[Duration::from_secs(1), Duration::from_secs(10)];
/// Polling cadence used as a control. Polling has no wal-sender
/// concept; the inner task just queries on a ticker. Whatever it
/// shows is the polling baseline.
const CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(100);

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

#[derive(Debug, Clone, Copy)]
enum Transport {
    Push { status_interval: Duration },
    Poll { interval: Duration },
}

impl Transport {
    fn label(self) -> String {
        match self {
            Self::Push { status_interval } => {
                format!("push (status@{}ms)", status_interval.as_millis())
            }
            Self::Poll { interval } => format!("poll @ {}ms", interval.as_millis()),
        }
    }

    fn slot_tag(self) -> String {
        match self {
            Self::Push { status_interval } => format!("push_{}", status_interval.as_millis()),
            Self::Poll { interval } => format!("poll_{}", interval.as_millis()),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct CellSummary {
    samples: Vec<Duration>,
}

impl CellSummary {
    fn min(&self) -> Duration {
        *self.samples.iter().min().expect("non-empty samples")
    }

    fn median(&self) -> Duration {
        let mut s = self.samples.clone();
        s.sort();
        s[s.len() / 2]
    }

    fn max(&self) -> Duration {
        *self.samples.iter().max().expect("non-empty samples")
    }
}

/// Single trial: spawn the source against a fresh slot, sleep `idle`,
/// drive ONE insert, measure observed_at - commit_at. Aborts the
/// source task afterward. Returns `None` if the event was not
/// observed within `deadline`.
async fn trial_for_transport(
    pg_repl_url: &str,
    pg_sql_url: &str,
    slot: &str,
    publication: &str,
    catalog: Arc<sql_traits::structs::ParserDB>,
    orders_table_id: u32,
    transport: Transport,
    idle: Duration,
    id: i64,
    dml: &mut PgConnection,
) -> Option<Duration> {
    let event_key = (orders_table_id, id, EventKind::Insert);
    match transport {
        Transport::Push { status_interval } => {
            let config = PgStreamingConfig::new(pg_repl_url.to_string(), slot, publication)
                .status_interval(status_interval);
            let source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("connect push source");
            let (rx, task) = spawn_full_event_receiver(source);
            // Source-warmup window: let START_REPLICATION complete
            // before we start counting the idle window. Without this
            // the first trial's idle gets shortened by the handshake.
            tokio::time::sleep(Duration::from_millis(500)).await;
            tokio::time::sleep(idle).await;
            let commit_at = Instant::now();
            sql_query(insert_order_sql(id))
                .execute(dml)
                .unwrap_or_else(|e| panic!("post-idle insert id={id}: {e}"));
            let mut commits: HashMap<EventKey, Instant> = HashMap::new();
            commits.insert(event_key, commit_at);
            let deadline = Instant::now() + Duration::from_secs(5);
            let samples = collect_full_latencies(&commits, rx, deadline).await;
            task.abort();
            samples.into_iter().next()
        }
        Transport::Poll { interval } => {
            let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), slot, publication)
                .poll_interval(interval);
            let source = PollingPgCdcSource::connect(config, catalog)
                .await
                .expect("connect polling source");
            let (rx, task) = spawn_full_event_receiver(source);
            tokio::time::sleep(Duration::from_millis(200)).await;
            tokio::time::sleep(idle).await;
            let commit_at = Instant::now();
            sql_query(insert_order_sql(id))
                .execute(dml)
                .unwrap_or_else(|e| panic!("post-idle insert id={id}: {e}"));
            let mut commits: HashMap<EventKey, Instant> = HashMap::new();
            commits.insert(event_key, commit_at);
            let deadline = Instant::now() + Duration::from_secs(5) + interval * 4;
            let samples = collect_full_latencies(&commits, rx, deadline).await;
            task.abort();
            samples.into_iter().next()
        }
    }
}

fn main() {
    assert_docker_available();
    println!("W1.3 post-idle push wake-up investigation");
    println!(
        "{TRIALS_PER_CELL} trials per cell, push status_interval x idle duration matrix, polling control included."
    );
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "w1_3_pub";
    create_publication_all(&mut setup, publication);

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let mut transports: Vec<Transport> = Vec::new();
    for &status_interval in PUSH_STATUS_INTERVALS {
        transports.push(Transport::Push { status_interval });
    }
    transports.push(Transport::Poll {
        interval: CONTROL_POLL_INTERVAL,
    });

    let rt = current_thread_rt();
    let mut id_counter: i64 = 1;

    // results[transport_index][idle_index] = CellSummary
    let mut results: Vec<Vec<CellSummary>> =
        vec![vec![CellSummary::default(); IDLE_DURATIONS.len()]; transports.len()];

    for (ti, &transport) in transports.iter().enumerate() {
        for (ii, &idle) in IDLE_DURATIONS.iter().enumerate() {
            for trial in 0..TRIALS_PER_CELL {
                id_counter += 1;
                let id = id_counter;
                let slot = format!(
                    "w1_3_{}_idle{}_t{trial}",
                    transport.slot_tag(),
                    idle.as_millis(),
                );
                create_pgoutput_slot(&mut setup, &slot);
                let sample = rt.block_on(trial_for_transport(
                    &pg_repl_url,
                    &pg_sql_url,
                    &slot,
                    publication,
                    Arc::clone(&catalog),
                    table_ids.orders,
                    transport,
                    idle,
                    id,
                    &mut dml,
                ));
                match sample {
                    Some(d) => results[ti][ii].samples.push(d),
                    None => println!(
                        "  [warn] transport={} idle={}ms trial={}: no event observed within deadline",
                        transport.label(),
                        idle.as_millis(),
                        trial,
                    ),
                }
                // The trial's source has been aborted; terminate any
                // lingering wal sender and drop the slot so the next
                // trial doesn't bump `max_replication_slots`.
                force_drop_slot(&mut setup, &slot);
            }
            let cell = &results[ti][ii];
            println!(
                "{:<28}  idle={:>5}ms  n={:>2}  min={:>6.1}ms  median={:>6.1}ms  max={:>6.1}ms",
                transport.label(),
                idle.as_millis(),
                cell.samples.len(),
                ms_of(cell.min()),
                ms_of(cell.median()),
                ms_of(cell.max()),
            );
        }
        println!();
    }

    // Markdown table: rows = idle duration, cols = transport.
    println!("---");
    println!();
    println!("### Median post-idle latency (ms)");
    println!();
    let mut header = String::from("| idle (ms) |");
    let mut sep = String::from("| ---:|");
    for t in &transports {
        header.push_str(&format!(" {} |", t.label()));
        sep.push_str(" ---:|");
    }
    println!("{header}");
    println!("{sep}");
    for (ii, idle) in IDLE_DURATIONS.iter().enumerate() {
        let mut row = format!("| {} |", idle.as_millis());
        for tr in &results {
            row.push_str(&format!(" {:.1} |", ms_of(tr[ii].median())));
        }
        println!("{row}");
    }
    println!();

    // Interpretation hints — written into the output for the doc
    // capture so future readers see the test methodology and the
    // current verdict alongside the numbers.
    let push_1s_3s = results[0][IDLE_DURATIONS.len() - 1].median();
    let push_10s_3s = results[1][IDLE_DURATIONS.len() - 1].median();
    let poll_3s = results[2][IDLE_DURATIONS.len() - 1].median();
    println!("Headline: at 3s idle,");
    println!("  push (status@1s)  median = {:.1} ms", ms_of(push_1s_3s));
    println!("  push (status@10s) median = {:.1} ms", ms_of(push_10s_3s));
    println!("  poll @ 100ms     median = {:.1} ms", ms_of(poll_3s));
    if push_1s_3s < push_10s_3s / 2 {
        println!(
            "Verdict: shorter status_interval cuts post-idle latency by >=2x. The periodic-pump keeps the wal sender warm."
        );
    } else if push_10s_3s > poll_3s * 2 {
        println!(
            "Verdict: push is slower than polling after a 3s idle regardless of status_interval. The wake-up cost is upstream of subql (likely PG wal sender quiescence or kernel TCP idle)."
        );
    } else {
        println!(
            "Verdict: no large gap between push variants; the 30 ms tail observed in Phase 1 W1.3 may have been measurement noise. Re-run Phase 1 to confirm."
        );
    }
}
