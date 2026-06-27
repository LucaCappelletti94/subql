//! Phase 2 — Multi-table schema with I/U/D mix.
//!
//! Validates that the polling-vs-push verdict holds when the
//! operation mix isn't pure INSERTs. UPDATE / DELETE produce more
//! WAL bytes per event; multi-table commits stress the pgoutput
//! Relation cache; UPDATE-heavy workloads exercise the parser's
//! UPDATE path (where most CDC parser bugs hide).
//!
//! Three workloads, four transports:
//!
//! - W2.1 — Mixed I/U/D, steady. Rotating insert/update/delete on a
//!   pre-seeded `orders` row set at 1 event/s.
//! - W2.2 — Order-and-items transaction. Per commit, INSERT 1 row to
//!   `orders` plus 3 rows to `order_items` inside the same
//!   transaction. Tests how each transport handles per-commit batches
//!   of multi-table events.
//! - W2.3 — UPDATE-heavy. ~100 UPDATEs/s against a pre-seeded
//!   `users` row set. Stresses the parser's UPDATE path.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase2_mixed_iud --features pg-streaming
//! ```
//!
//! Pipe to `docs/benchmarks/phase2-<date>.md` to capture the run.

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
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, create_publication_all,
    markdown_table_header, parser_db, pg_connect, pg_port, pg_replication_url, pg_url,
    pg_with_wal2json, resolve_table_ids, setup_schema, spawn_full_event_receiver, EventKey,
    LatencyStats, TableIds,
};

const DRAIN_GRACE: Duration = Duration::from_secs(15);

const POLL_INTERVALS_MS: &[u64] = &[10, 100, 1_000];

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

#[derive(Debug, Clone, Copy)]
struct WorkloadDesc {
    name: &'static str,
    description: &'static str,
    id_base: i64,
}

const W2_1: WorkloadDesc = WorkloadDesc {
    name: "W2.1",
    description: "mixed I/U/D, steady (1 event/s rotating on a seeded row set)",
    id_base: 200_000,
};
const W2_2: WorkloadDesc = WorkloadDesc {
    name: "W2.2",
    description: "order-and-items transaction (1 orders row + 3 order_items rows per commit)",
    id_base: 300_000,
};
const W2_3: WorkloadDesc = WorkloadDesc {
    name: "W2.3",
    description: "UPDATE-heavy (~100 UPDATEs/s against users)",
    id_base: 400_000,
};

fn slot_name(workload: &WorkloadDesc, transport: Transport) -> String {
    format!(
        "phase2_{}_{}",
        workload.name.replace('.', "_").to_lowercase(),
        transport.slot_suffix()
    )
}

/// Insert a sentinel user (id=1) so every Phase 2 measurement run can
/// assume there is at least one real `users` row to reference.
fn insert_sentinel_user(conn: &mut PgConnection) {
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(conn)
    .expect("insert sentinel user");
}

// ----------------------------------------------------------------------
// W2.1 — Mixed I/U/D, steady
// ----------------------------------------------------------------------

/// W2.1: pre-seed 5 `orders` rows (NOT measured), then loop through
/// 15 events at 1 s gap, rotating INSERT / UPDATE / DELETE on
/// distinct rows so every event has a unique `(table, pk, kind)` key.
async fn drive_w2_1(
    conn: &mut PgConnection,
    id_base: i64,
    table_ids: TableIds,
) -> HashMap<EventKey, Instant> {
    const SEED_ROWS: i64 = 5;
    const COUNT: i64 = 15;
    // Seed rows id_base..id_base+SEED_ROWS so UPDATE/DELETE always
    // have something to act on. These are pre-existing data; their
    // INSERT events are NOT in the commit_times map (the slot is
    // created after seeding).
    for i in 0..SEED_ROWS {
        let id = id_base + i;
        sql_query(format!(
            "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
             VALUES ({id}, 1, 'pending', {price}, NOW())",
            price = id * 100
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("seed insert id={id}: {e}"));
    }

    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    // Insert new ids past the seeded range; update + delete the
    // seeded ids in a rotating pattern.
    for step in 0..COUNT {
        if step > 0 {
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }
        let commit_at = Instant::now();
        let (key, sql) = match step % 3 {
            0 => {
                // Insert a fresh row past the seeded range.
                let id = id_base + SEED_ROWS + step / 3;
                (
                    (table_ids.orders, id, EventKind::Insert),
                    format!(
                        "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                         VALUES ({id}, 1, 'paid', {price}, NOW())",
                        price = id * 100
                    ),
                )
            }
            1 => {
                // Update one of the seeded rows.
                let id = id_base + (step / 3) % SEED_ROWS;
                (
                    (table_ids.orders, id, EventKind::Update),
                    format!(
                        "UPDATE orders SET status = 'shipped', total_cents = {p}, updated_at = NOW() \
                         WHERE id = {id}",
                        p = id * 200
                    ),
                )
            }
            _ => {
                // Delete the row updated in the previous tick.
                let id = id_base + (step / 3) % SEED_ROWS;
                (
                    (table_ids.orders, id, EventKind::Delete),
                    format!("DELETE FROM orders WHERE id = {id}"),
                )
            }
        };
        sql_query(sql.as_str())
            .execute(conn)
            .unwrap_or_else(|e| panic!("step {step} `{sql}`: {e}"));
        // Only record the FIRST occurrence of a (table, pk, kind);
        // subsequent rotations of the same kind on the same pk would
        // collide on the EventKey. Step assignment above avoids that
        // by spreading ids and rotation, but guard anyway.
        commits.entry(key).or_insert(commit_at);
    }
    commits
}

// ----------------------------------------------------------------------
// W2.2 — Order-and-items transaction
// ----------------------------------------------------------------------

/// W2.2: 10 commits, each one transaction containing 1 INSERT into
/// `orders` plus 3 INSERTs into `order_items`. 1 commit per second.
/// Total: 40 events. Tests how each transport handles multi-table
/// per-commit batches.
async fn drive_w2_2(
    conn: &mut PgConnection,
    id_base: i64,
    table_ids: TableIds,
) -> HashMap<EventKey, Instant> {
    const COMMITS: i64 = 10;
    const ITEMS_PER_ORDER: i64 = 3;
    let mut commits: HashMap<EventKey, Instant> =
        HashMap::with_capacity((COMMITS * (1 + ITEMS_PER_ORDER)) as usize);
    for c in 0..COMMITS {
        if c > 0 {
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }
        let order_id = id_base + c;
        let item_id_base = id_base + (c + 1) * 100;
        let item_ids: Vec<i64> = (0..ITEMS_PER_ORDER).map(|i| item_id_base + i).collect();
        // pgoutput emits per-row events on COMMIT. The COMMIT-end
        // timestamp is the shared commit_at for every event in this
        // transaction; per-event latency is observed_at minus that.
        let commit_at = Instant::now();
        let mut stmts = format!(
            "BEGIN; \
             INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
             VALUES ({order_id}, 1, 'paid', {p}, NOW());",
            p = order_id * 100,
        );
        for &iid in &item_ids {
            stmts.push_str(&format!(
                " INSERT INTO order_items (id, order_id, product_name, quantity, price_cents) \
                  VALUES ({iid}, {order_id}, 'widget', 1, {p});",
                p = iid * 10,
            ));
        }
        stmts.push_str(" COMMIT;");
        // Multi-statement batch: diesel's `sql_query` rejects it
        // (prepared statements take exactly one command). We need
        // the libpq simple-query path; `SimpleConnection::batch_execute`
        // is exactly that.
        conn.batch_execute(stmts.as_str())
            .unwrap_or_else(|e| panic!("commit {c}: {e}"));
        commits.insert((table_ids.orders, order_id, EventKind::Insert), commit_at);
        for iid in item_ids {
            commits.insert((table_ids.order_items, iid, EventKind::Insert), commit_at);
        }
    }
    commits
}

// ----------------------------------------------------------------------
// W2.3 — UPDATE-heavy
// ----------------------------------------------------------------------

/// W2.3: pre-seed 100 `users` rows (NOT measured), then drive 500
/// UPDATEs at 10 ms gap (~100/s sustained for 5 s). Each UPDATE
/// targets a distinct user id so every event has a unique
/// `(users, pk, Update)` key.
async fn drive_w2_3(
    conn: &mut PgConnection,
    id_base: i64,
    table_ids: TableIds,
) -> HashMap<EventKey, Instant> {
    const SEED_ROWS: i64 = 100;
    const UPDATES: i64 = 500;
    for i in 0..SEED_ROWS {
        let id = id_base + i;
        sql_query(format!(
            "INSERT INTO users (id, email, name, last_login_at) \
             VALUES ({id}, 'user_{id}@example.invalid', 'user_{id}', NOW()) \
             ON CONFLICT (id) DO NOTHING"
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("seed user id={id}: {e}"));
    }

    // To avoid (user_id, Update) key collisions when 500 > 100
    // distinct seeded rows, UPDATE only the first 500 ids we COULD
    // touch — but we want 500 SAMPLES. Solution: each "update" assigns
    // a NEW user id past the seeded range, INSERT then UPDATE. That
    // would muddle the workload's intent (UPDATE-heavy, not
    // mixed-IUD). Alternative: re-seed 500 users; UPDATE each once.
    // We pick the latter: 500 seeded rows, each updated once. Wider
    // user table, same UPDATE pressure.
    for i in SEED_ROWS..UPDATES {
        let id = id_base + i;
        sql_query(format!(
            "INSERT INTO users (id, email, name, last_login_at) \
             VALUES ({id}, 'user_{id}@example.invalid', 'user_{id}', NOW()) \
             ON CONFLICT (id) DO NOTHING"
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("seed user id={id}: {e}"));
    }

    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(UPDATES as usize);
    for i in 0..UPDATES {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let id = id_base + i;
        let commit_at = Instant::now();
        sql_query(format!(
            "UPDATE users SET last_login_at = NOW(), name = 'user_{id}_v2' WHERE id = {id}"
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("update id={id}: {e}"));
        commits.insert((table_ids.users, id, EventKind::Update), commit_at);
    }
    commits
}

async fn drive_for_workload(
    w: &WorkloadDesc,
    table_ids: TableIds,
    dml: &mut PgConnection,
) -> HashMap<EventKey, Instant> {
    match w.name {
        "W2.1" => drive_w2_1(dml, w.id_base, table_ids).await,
        "W2.2" => drive_w2_2(dml, w.id_base, table_ids).await,
        "W2.3" => drive_w2_3(dml, w.id_base, table_ids).await,
        other => panic!("unknown workload {other}"),
    }
}

async fn measure_push(
    pg_replication_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    label: String,
    workload: WorkloadDesc,
    table_ids: TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PgStreamingConfig::new(pg_replication_url, slot, publication);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let commit_times = drive_for_workload(&workload, table_ids, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

async fn measure_poll(
    pg_url: String,
    slot: String,
    publication: String,
    catalog: Arc<sql_traits::structs::ParserDB>,
    interval: Duration,
    label: String,
    workload: WorkloadDesc,
    table_ids: TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PollingPgCdcConfig::new(pg_url, slot, publication).poll_interval(interval);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let commit_times = drive_for_workload(&workload, table_ids, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE + interval * 4;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

fn main() {
    assert_docker_available();
    println!("Phase 2 — Multi-table schema with I/U/D mix");
    println!("Mixed-DML, multi-table-commit, and UPDATE-heavy workloads vs the prior pure-INSERT baseline.");
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    setup_schema(&mut setup);
    insert_sentinel_user(&mut setup);
    let publication = "phase2_pub";
    create_publication_all(&mut setup, publication);

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let workloads = [W2_1, W2_2, W2_3];
    let transports = {
        let mut v = Vec::with_capacity(1 + POLL_INTERVALS_MS.len());
        v.push(Transport::Push);
        for &ms in POLL_INTERVALS_MS {
            v.push(Transport::Poll { interval_ms: ms });
        }
        v
    };

    let rt = current_thread_rt();
    let mut all_stats: Vec<(WorkloadDesc, Vec<LatencyStats>)> = Vec::new();

    for w in workloads {
        println!("=== {} — {} ===", w.name, w.description);
        let mut row: Vec<LatencyStats> = Vec::with_capacity(transports.len());
        for (ti, &t) in transports.iter().enumerate() {
            let slot = slot_name(&w, t);
            create_pgoutput_slot(&mut setup, &slot);
            let label = t.label();
            // Distinct id range per transport so successive runs of
            // the same workload don't collide on PKs across runs.
            let workload_for_run = WorkloadDesc {
                id_base: w.id_base + (ti as i64) * 10_000,
                ..w
            };
            let stats = rt.block_on(async {
                match t {
                    Transport::Push => {
                        measure_push(
                            pg_repl_url.clone(),
                            slot,
                            publication.to_string(),
                            Arc::clone(&catalog),
                            label,
                            workload_for_run,
                            table_ids,
                            &mut dml,
                        )
                        .await
                    }
                    Transport::Poll { interval_ms } => {
                        measure_poll(
                            pg_sql_url.clone(),
                            slot,
                            publication.to_string(),
                            Arc::clone(&catalog),
                            Duration::from_millis(interval_ms),
                            label,
                            workload_for_run,
                            table_ids,
                            &mut dml,
                        )
                        .await
                    }
                }
            });
            println!("{}", stats.text_summary());
            row.push(stats);
        }
        println!();
        all_stats.push((w, row));
    }

    println!("---");
    println!();
    for (w, row) in &all_stats {
        println!("### {} — {}", w.name, w.description);
        println!();
        println!("{}", markdown_table_header());
        for stats in row {
            println!("{}", stats.markdown_row());
        }
        println!();
    }

    // Architectural-claim sanity: across every Phase 2 workload, push
    // median must beat polling at 100 ms and 1 s. Phase 2 differs
    // from Phase 1 only in the DML mix, not in transport behavior.
    // If a workload inverts the verdict, that's a real engineering
    // finding worth investigating (it'd mean per-event WAL bytes
    // affect transport choice).
    for (w, row) in &all_stats {
        let push_median = row[0].median();
        let poll_100_median = row[2].median();
        let poll_1000_median = row[3].median();
        assert!(
            push_median < poll_100_median,
            "{} push median {push_median:?} must beat poll@100ms median {poll_100_median:?}",
            w.name
        );
        assert!(
            push_median < poll_1000_median,
            "{} push median {push_median:?} must beat poll@1000ms median {poll_1000_median:?}",
            w.name
        );
    }
    println!("Phase 2 architectural-claim check: PASS");
    println!("  push beats poll@100ms and poll@1000ms on every workload (median).");
}
