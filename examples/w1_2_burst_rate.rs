//! Investigation: W1.2 high-rate push median tail.
//!
//! Phase 1 W1.2 drives 500 INSERTs at a 2 ms gap (~500 events/s for
//! ~1 s) and measured push median = 13.9 ms. Phase 3 W3.3 drives 1000
//! INSERTs at a 10 ms gap (~100 events/s sustained over 10 s) and
//! measured push median = 3.4 ms. Same architecture, same wire — the
//! gap (and therefore the per-second event rate) is what changes.
//!
//! The natural hypothesis: the push source's inner mpsc channel
//! (default `buffer_capacity = 1024`) backpressures under high event
//! rates because the receiver task forwarding events downstream
//! cannot keep up with the producer. When the channel fills, the
//! inner task blocks on `event_tx.send().await`, which stops draining
//! `CopyBothDuplex`, which stops draining TCP, which delays the next
//! event's delivery.
//!
//! This example sweeps:
//!
//! - `event_gap` in `{1 ms, 2 ms, 5 ms, 10 ms, 50 ms}` (rate in
//!   `{1000/s, 500/s, 200/s, 100/s, 20/s}`)
//! - `buffer_capacity` in `{256, 1024, 4096}`
//!
//! 200 events per cell. If the hypothesis holds:
//!
//! - At low event rate, median is wire-bound (~5 ms) for all buffer
//!   sizes.
//! - At high event rate, median climbs; larger buffer postpones the
//!   climb.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example w1_2_burst_rate --features pg-streaming
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
use subql::{EventKind, PgStreamingCdcSource, PgStreamingConfig};

use common::{
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, create_publication_all,
    ms_of, parser_db, pg_connect, pg_port, pg_replication_url, pg_with_wal2json, resolve_table_ids,
    setup_schema, spawn_full_event_receiver, EventKey,
};

const EVENTS_PER_CELL: i64 = 200;
const EVENT_GAPS_MS: &[u64] = &[1, 2, 5, 10, 50];
const BUFFER_CAPACITIES: &[usize] = &[256, 1024, 4096];

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

/// Force-drop a slot (mirrors the helper in `w1_3_idle_wakeup.rs`).
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

#[derive(Default, Clone)]
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

    fn p99(&self) -> Duration {
        let mut s = self.samples.clone();
        s.sort();
        let n = s.len();
        s[(n.saturating_sub(1)).min((n * 99) / 100)]
    }

    fn max(&self) -> Duration {
        *self.samples.iter().max().expect("non-empty samples")
    }
}

async fn drive_inserts_at(
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

async fn measure_cell(
    pg_repl_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    buffer_capacity: usize,
    orders_table_id: u32,
    gap: Duration,
    id_base: i64,
    dml: &mut PgConnection,
) -> CellSummary {
    let config =
        PgStreamingConfig::new(pg_repl_url, slot, publication).buffer_capacity(buffer_capacity);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let commit_times = drive_inserts_at(dml, id_base, EVENTS_PER_CELL, gap, orders_table_id).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    CellSummary { samples }
}

fn main() {
    assert_docker_available();
    println!("W1.2 high-rate push median investigation");
    println!("200 events per cell, sweeping event_gap x buffer_capacity (push only).");
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "w1_2_pub";
    create_publication_all(&mut setup, publication);

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let pg_repl_url = pg_replication_url(port);

    let rt = current_thread_rt();
    let mut id_counter: i64 = 1;

    // results[buffer_index][gap_index] = CellSummary
    let mut results: Vec<Vec<CellSummary>> =
        vec![vec![CellSummary::default(); EVENT_GAPS_MS.len()]; BUFFER_CAPACITIES.len()];

    for (bi, &buffer_capacity) in BUFFER_CAPACITIES.iter().enumerate() {
        for (gi, &gap_ms) in EVENT_GAPS_MS.iter().enumerate() {
            let gap = Duration::from_millis(gap_ms);
            id_counter += EVENTS_PER_CELL + 1;
            let id_base = id_counter;
            let slot = format!("w1_2_buf{buffer_capacity}_gap{gap_ms}");
            create_pgoutput_slot(&mut setup, &slot);
            let cell = rt.block_on(measure_cell(
                pg_repl_url.clone(),
                slot.clone(),
                publication.to_string(),
                Arc::clone(&catalog),
                buffer_capacity,
                table_ids.orders,
                gap,
                id_base,
                &mut dml,
            ));
            println!(
                "buffer={buffer_capacity:>5}  gap={gap_ms:>3}ms  rate={:>5}/s  n={:>3}  min={:>5.1}ms  median={:>6.1}ms  p99={:>6.1}ms  max={:>6.1}ms",
                1_000 / gap_ms.max(1),
                cell.samples.len(),
                ms_of(cell.min()),
                ms_of(cell.median()),
                ms_of(cell.p99()),
                ms_of(cell.max()),
            );
            results[bi][gi] = cell;
            force_drop_slot(&mut setup, &slot);
        }
        println!();
    }

    // Markdown table: rows = gap, cols = buffer.
    println!("---");
    println!();
    println!("### Median push latency (ms): rows = event gap, cols = buffer_capacity");
    println!();
    let mut header = String::from("| gap (ms) | rate (/s) |");
    let mut sep = String::from("| ---:| ---:|");
    for buf in BUFFER_CAPACITIES {
        header.push_str(&format!(" buf={buf} |"));
        sep.push_str(" ---:|");
    }
    println!("{header}");
    println!("{sep}");
    for (gi, gap_ms) in EVENT_GAPS_MS.iter().enumerate() {
        let mut row = format!("| {} | {} |", gap_ms, 1_000 / gap_ms.max(&1));
        for buf_results in &results {
            row.push_str(&format!(" {:.1} |", ms_of(buf_results[gi].median())));
        }
        println!("{row}");
    }
    println!();

    // Verdict.
    // Compare buf=256 vs buf=4096 at the tightest gap (1ms). If
    // larger buffer materially lowers the median, backpressure is
    // confirmed.
    let tightest_gap_idx = 0; // EVENT_GAPS_MS[0] = 1
    let smallest_buf_median = results[0][tightest_gap_idx].median();
    let largest_buf_median = results[BUFFER_CAPACITIES.len() - 1][tightest_gap_idx].median();
    let gap_ms = EVENT_GAPS_MS[tightest_gap_idx];
    println!(
        "Headline (gap = {gap_ms} ms, ~{}/s):",
        1_000 / gap_ms.max(1)
    );
    println!(
        "  buffer={}  median = {:.1} ms",
        BUFFER_CAPACITIES[0],
        ms_of(smallest_buf_median)
    );
    println!(
        "  buffer={}  median = {:.1} ms",
        BUFFER_CAPACITIES[BUFFER_CAPACITIES.len() - 1],
        ms_of(largest_buf_median)
    );
    if smallest_buf_median > largest_buf_median * 2 {
        println!(
            "Verdict: smaller buffer materially raises median at high rate. \
             The 13.9 ms tail in Phase 1 W1.2 is bounded-channel backpressure. \
             A larger `buffer_capacity` on `PgStreamingConfig` would have absorbed it."
        );
    } else if smallest_buf_median > Duration::from_millis(10) {
        println!(
            "Verdict: high event rate inflates median regardless of buffer size. \
             The cost is upstream of the bounded channel (parse cost or wire saturation), \
             not backpressure on the inner mpsc."
        );
    } else {
        println!(
            "Verdict: no large gap between buffer sizes; the 13.9 ms tail in Phase 1 W1.2 \
             may have been measurement noise. Re-run Phase 1 to confirm."
        );
    }
}
