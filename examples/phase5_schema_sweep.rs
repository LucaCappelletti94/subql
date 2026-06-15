//! Phase 5 — Schema-size sweep.
//!
//! Tests whether schema complexity changes the polling-vs-push verdict.
//! pgoutput emits a `Relation` message on first contact with each
//! table; wide rows pay a per-event parse cost proportional to column
//! count; mixed access patterns (append-only audit + UPDATE-heavy
//! lookup) exercise both edges at once.
//!
//! Three workloads, four transports:
//!
//! - S5.1 — Wide rows. A single 50-column table mixing INT / TEXT /
//!   BIGINT / DOUBLE PRECISION / TIMESTAMPTZ / JSONB. 200 INSERTs at
//!   10 ms gap. Per-event parse cost should be higher than the 5-col
//!   `orders` baseline; transport choice should not change the
//!   verdict.
//! - S5.2 — Many tables. 15 narrow tables (`shop_t1..shop_t15`),
//!   each 4 columns. INSERT rotates across the 15 tables; the first
//!   15 events trigger pgoutput `Relation` messages, subsequent
//!   events hit the warm cache. 200 events total at 10 ms gap.
//! - S5.3 — Append-only audit + UPDATE-heavy lookup. Two tables:
//!   `audit_log` (INSERT-only, append) and `lookup_state` (pre-seeded
//!   with 50 rows, then UPDATEd). Workload rotates 4 inserts to
//!   `audit_log` for every 1 update to `lookup_state`. 250 events
//!   total at 10 ms gap.
//!
//! Phase 5 does NOT use the 3-table e-commerce schema from
//! `cdc_bench_common`. It defines its own schemas inline since they
//! are Phase-5-specific and would bloat the shared module.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase5_schema_sweep --features pg-streaming
//! ```
//!
//! Pipe to `docs/benchmarks/phase5-<date>.md` to capture the run.

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
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource,
};

use common::{
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, markdown_table_header,
    pg_connect, pg_port, pg_replication_url, pg_url, pg_with_wal2json, spawn_full_event_receiver,
    EventKey, LatencyStats,
};

const DRAIN_GRACE: Duration = Duration::from_secs(15);
const POLL_INTERVALS_MS: &[u64] = &[10, 100, 1_000];
const BUFFER_CAPACITY: usize = 4_096;

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

// ----------------------------------------------------------------------
// Schemas
// ----------------------------------------------------------------------

const NUM_SHOP_TABLES: usize = 15;

fn build_pg_ddl() -> Vec<String> {
    let mut stmts: Vec<String> = Vec::new();

    // S5.1 — wide_rows: 50 columns of mixed types. The id BIGINT PK
    // plus 10 INT, 10 TEXT, 10 BIGINT, 10 DOUBLE PRECISION, 5
    // TIMESTAMPTZ, 5 JSONB = 51 columns total.
    let mut wide = String::from("CREATE TABLE wide_rows (id BIGINT PRIMARY KEY");
    for i in 1..=10 {
        wide.push_str(&format!(", i{i:02} INT"));
    }
    for i in 1..=10 {
        wide.push_str(&format!(", t{i:02} TEXT"));
    }
    for i in 1..=10 {
        wide.push_str(&format!(", n{i:02} BIGINT"));
    }
    for i in 1..=10 {
        wide.push_str(&format!(", f{i:02} DOUBLE PRECISION"));
    }
    for i in 1..=5 {
        wide.push_str(&format!(", ts{i} TIMESTAMPTZ"));
    }
    for i in 1..=5 {
        wide.push_str(&format!(", j{i} JSONB"));
    }
    wide.push(')');
    stmts.push(wide);

    // S5.2 — 15 narrow tables. Same shape across tables to keep the
    // workload focused on relation-cache count, not per-event parse
    // cost differences.
    for k in 1..=NUM_SHOP_TABLES {
        stmts.push(format!(
            "CREATE TABLE shop_t{k} (id BIGINT PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL, c INT NOT NULL)"
        ));
    }

    // S5.3 — audit log + lookup pair.
    stmts.push(
        "CREATE TABLE audit_log (\
            id BIGINT PRIMARY KEY,\
            event_type TEXT NOT NULL,\
            actor_id BIGINT NOT NULL,\
            payload TEXT NOT NULL,\
            ts TIMESTAMPTZ NOT NULL\
         )"
        .into(),
    );
    stmts.push(
        "CREATE TABLE lookup_state (\
            id BIGINT PRIMARY KEY,\
            name TEXT NOT NULL,\
            value TEXT NOT NULL,\
            last_updated_at TIMESTAMPTZ NOT NULL\
         )"
        .into(),
    );

    // REPLICA IDENTITY FULL on every table (required for our UPDATE
    // observability semantics and matches the rest of the benchmarks).
    stmts.push("ALTER TABLE wide_rows REPLICA IDENTITY FULL".into());
    for k in 1..=NUM_SHOP_TABLES {
        stmts.push(format!("ALTER TABLE shop_t{k} REPLICA IDENTITY FULL"));
    }
    stmts.push("ALTER TABLE audit_log REPLICA IDENTITY FULL".into());
    stmts.push("ALTER TABLE lookup_state REPLICA IDENTITY FULL".into());

    stmts
}

fn build_parser_ddl() -> String {
    let mut ddl = String::new();

    ddl.push_str("CREATE TABLE wide_rows (id BIGINT PRIMARY KEY");
    for i in 1..=10 {
        ddl.push_str(&format!(", i{i:02} INT"));
    }
    for i in 1..=10 {
        ddl.push_str(&format!(", t{i:02} TEXT"));
    }
    for i in 1..=10 {
        ddl.push_str(&format!(", n{i:02} BIGINT"));
    }
    for i in 1..=10 {
        ddl.push_str(&format!(", f{i:02} DOUBLE PRECISION"));
    }
    for i in 1..=5 {
        ddl.push_str(&format!(", ts{i} TIMESTAMP"));
    }
    // JSONB → sqlparser's PostgreSQL dialect accepts it; subql maps
    // to ColumnType::Unknown. Phase 5 does not depend on per-cell
    // typed access for JSONB.
    for i in 1..=5 {
        ddl.push_str(&format!(", j{i} JSONB"));
    }
    ddl.push_str(");");

    for k in 1..=NUM_SHOP_TABLES {
        ddl.push_str(&format!(
            "CREATE TABLE shop_t{k} (id BIGINT PRIMARY KEY, a TEXT, b TEXT, c INT);"
        ));
    }
    ddl.push_str(
        "CREATE TABLE audit_log (\
            id BIGINT PRIMARY KEY,\
            event_type TEXT,\
            actor_id BIGINT,\
            payload TEXT,\
            ts TIMESTAMP\
         );",
    );
    ddl.push_str(
        "CREATE TABLE lookup_state (\
            id BIGINT PRIMARY KEY,\
            name TEXT,\
            value TEXT,\
            last_updated_at TIMESTAMP\
         );",
    );

    ddl
}

fn apply_pg_ddl(conn: &mut PgConnection) {
    for stmt in build_pg_ddl() {
        sql_query(stmt.as_str())
            .execute(conn)
            .unwrap_or_else(|e| panic!("apply DDL `{stmt}`: {e}"));
    }
}

fn create_publication(conn: &mut PgConnection, publication: &str) {
    let mut tables = vec![
        "wide_rows".to_string(),
        "audit_log".to_string(),
        "lookup_state".to_string(),
    ];
    for k in 1..=NUM_SHOP_TABLES {
        tables.push(format!("shop_t{k}"));
    }
    sql_query(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {}",
        tables.join(", ")
    ))
    .execute(conn)
    .expect("create publication");
}

#[derive(Debug, Clone, Copy)]
struct Phase5TableIds {
    wide_rows: u32,
    shop: [u32; NUM_SHOP_TABLES],
    audit_log: u32,
    lookup_state: u32,
}

fn resolve_phase5_table_ids(db: &ParserDB) -> Phase5TableIds {
    let mut shop = [0u32; NUM_SHOP_TABLES];
    for (k, slot) in shop.iter_mut().enumerate() {
        *slot = subql::catalog_helpers::table_id(db, &format!("shop_t{}", k + 1))
            .unwrap_or_else(|| panic!("shop_t{} table id", k + 1));
    }
    Phase5TableIds {
        wide_rows: subql::catalog_helpers::table_id(db, "wide_rows").expect("wide_rows table id"),
        shop,
        audit_log: subql::catalog_helpers::table_id(db, "audit_log").expect("audit_log table id"),
        lookup_state: subql::catalog_helpers::table_id(db, "lookup_state")
            .expect("lookup_state table id"),
    }
}

// ----------------------------------------------------------------------
// Workload drivers
// ----------------------------------------------------------------------

/// S5.1: 200 INSERTs into the 50-column wide_rows table at 10 ms gap.
async fn drive_s5_1(
    conn: &mut PgConnection,
    id_base: i64,
    ids: Phase5TableIds,
) -> HashMap<EventKey, Instant> {
    const COUNT: i64 = 200;
    const GAP: Duration = Duration::from_millis(10);
    // Build the column-name list once; for VALUES we will splat
    // deterministic content per row.
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    for c in 0..COUNT {
        if c > 0 {
            tokio::time::sleep(GAP).await;
        }
        let id = id_base + c;
        let mut sql = String::from("INSERT INTO wide_rows (id");
        for i in 1..=10 {
            sql.push_str(&format!(", i{i:02}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", t{i:02}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", n{i:02}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", f{i:02}"));
        }
        for i in 1..=5 {
            sql.push_str(&format!(", ts{i}"));
        }
        for i in 1..=5 {
            sql.push_str(&format!(", j{i}"));
        }
        sql.push_str(&format!(") VALUES ({id}"));
        for i in 1..=10 {
            let v = id + i64::from(i);
            sql.push_str(&format!(", {v}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", 't{i:02}_row_{id}'"));
        }
        for i in 1..=10 {
            let v = id * 1000 + i64::from(i);
            sql.push_str(&format!(", {v}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", {id}.{i:02}"));
        }
        for _ in 1..=5 {
            sql.push_str(", NOW()");
        }
        for i in 1..=5 {
            sql.push_str(&format!(", '{{\"k\":{i},\"id\":{id}}}'::jsonb"));
        }
        sql.push(')');
        let commit_at = Instant::now();
        sql_query(sql.as_str())
            .execute(conn)
            .unwrap_or_else(|e| panic!("wide insert id={id}: {e}"));
        commits.insert((ids.wide_rows, id, EventKind::Insert), commit_at);
    }
    commits
}

/// S5.2: 200 INSERTs rotating across `shop_t1..shop_t15` at 10 ms
/// gap. The first 15 events trigger pgoutput Relation messages
/// (cold cache); subsequent events use the warm cache.
async fn drive_s5_2(
    conn: &mut PgConnection,
    id_base: i64,
    ids: Phase5TableIds,
) -> HashMap<EventKey, Instant> {
    const COUNT: i64 = 200;
    const GAP: Duration = Duration::from_millis(10);
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    for c in 0..COUNT {
        if c > 0 {
            tokio::time::sleep(GAP).await;
        }
        let table_index = (c as usize) % NUM_SHOP_TABLES;
        let table_id = ids.shop[table_index];
        let table_name = format!("shop_t{}", table_index + 1);
        let id = id_base + c;
        let commit_at = Instant::now();
        sql_query(format!(
            "INSERT INTO {table_name} (id, a, b, c) VALUES ({id}, 'a_{id}', 'b_{id}', {c})"
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("{table_name} insert id={id}: {e}"));
        commits.insert((table_id, id, EventKind::Insert), commit_at);
    }
    commits
}

/// S5.3: append-only audit + UPDATE-heavy lookup. Seed 50 lookup_state
/// rows (not measured), then drive a 4:1 mix of (audit_log INSERT,
/// lookup_state UPDATE) for 250 events total at 10 ms gap.
async fn drive_s5_3(
    conn: &mut PgConnection,
    id_base: i64,
    ids: Phase5TableIds,
) -> HashMap<EventKey, Instant> {
    const SEED: i64 = 50;
    const COUNT: i64 = 250;
    const GAP: Duration = Duration::from_millis(10);
    // Pre-seed lookup_state with 50 rows. These INSERTs happen
    // BEFORE the slot is created — they are not in commit_times.
    for i in 0..SEED {
        let id = id_base + i;
        sql_query(format!(
            "INSERT INTO lookup_state (id, name, value, last_updated_at) \
             VALUES ({id}, 'name_{id}', 'v0', NOW())"
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("seed lookup_state id={id}: {e}"));
    }

    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    let mut audit_offset: i64 = 0;
    let mut update_offset: i64 = 0;
    for c in 0..COUNT {
        if c > 0 {
            tokio::time::sleep(GAP).await;
        }
        // 4:1 audit-insert to lookup-update.
        let do_audit = (c % 5) != 0;
        if do_audit {
            let id = id_base + 100_000 + audit_offset;
            audit_offset += 1;
            let commit_at = Instant::now();
            sql_query(format!(
                "INSERT INTO audit_log (id, event_type, actor_id, payload, ts) \
                 VALUES ({id}, 'click', 1, 'p_{id}', NOW())"
            ))
            .execute(conn)
            .unwrap_or_else(|e| panic!("audit insert id={id}: {e}"));
            commits.insert((ids.audit_log, id, EventKind::Insert), commit_at);
        } else {
            // Update the kth seeded row (k cycles through 0..SEED).
            let id = id_base + (update_offset % SEED);
            update_offset += 1;
            let commit_at = Instant::now();
            sql_query(format!(
                "UPDATE lookup_state SET value = 'v_{c}', last_updated_at = NOW() WHERE id = {id}"
            ))
            .execute(conn)
            .unwrap_or_else(|e| panic!("lookup update id={id}: {e}"));
            // First UPDATE on each seeded row records the EventKey;
            // later UPDATEs on the same row would collide.
            commits
                .entry((ids.lookup_state, id, EventKind::Update))
                .or_insert(commit_at);
        }
    }
    commits
}

#[derive(Debug, Clone, Copy)]
struct WorkloadDesc {
    name: &'static str,
    description: &'static str,
    id_base: i64,
}

const S5_1: WorkloadDesc = WorkloadDesc {
    name: "S5.1",
    description: "wide rows (50-column table, 200 INSERTs at 10ms gap)",
    id_base: 900_000,
};
const S5_2: WorkloadDesc = WorkloadDesc {
    name: "S5.2",
    description: "many tables (200 INSERTs rotating across 15 tables at 10ms gap)",
    id_base: 950_000,
};
const S5_3: WorkloadDesc = WorkloadDesc {
    name: "S5.3",
    description: "audit log + lookup (4:1 INSERT:UPDATE mix, 250 events at 10ms gap)",
    id_base: 1_000_000,
};

fn slot_name(workload: &WorkloadDesc, transport: Transport) -> String {
    format!(
        "phase5_{}_{}",
        workload.name.replace('.', "_").to_lowercase(),
        transport.slot_suffix()
    )
}

async fn drive_for_workload(
    w: &WorkloadDesc,
    ids: Phase5TableIds,
    dml: &mut PgConnection,
) -> HashMap<EventKey, Instant> {
    match w.name {
        "S5.1" => drive_s5_1(dml, w.id_base, ids).await,
        "S5.2" => drive_s5_2(dml, w.id_base, ids).await,
        "S5.3" => drive_s5_3(dml, w.id_base, ids).await,
        other => panic!("unknown workload {other}"),
    }
}

async fn measure_push(
    pg_replication_url: String,
    slot: String,
    publication: String,
    catalog: Arc<ParserDB>,
    label: String,
    workload: WorkloadDesc,
    ids: Phase5TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PgStreamingConfig::new(pg_replication_url, slot, publication)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let commit_times = drive_for_workload(&workload, ids, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

async fn measure_poll(
    pg_url: String,
    slot: String,
    publication: String,
    catalog: Arc<ParserDB>,
    interval: Duration,
    label: String,
    workload: WorkloadDesc,
    ids: Phase5TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PollingPgCdcConfig::new(pg_url, slot, publication)
        .poll_interval(interval)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let commit_times = drive_for_workload(&workload, ids, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE + interval * 4;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

fn main() {
    assert_docker_available();
    println!("Phase 5 — Schema-size sweep");
    println!("Wide rows, many tables, and append-only-plus-lookup vs the 5-col baseline.");
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    apply_pg_ddl(&mut setup);
    let publication = "phase5_pub";
    create_publication(&mut setup, publication);

    let catalog =
        Arc::new(ParserDB::parse::<PostgreSqlDialect>(&build_parser_ddl()).expect("parse DDL"));
    let table_ids = resolve_phase5_table_ids(&catalog);
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let workloads = [S5_1, S5_2, S5_3];
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
            // Distinct id range per transport to avoid PK collisions.
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

    // Architectural-claim sanity: schema complexity must not invert
    // the polling-vs-push verdict at sub-second cadence. Wide rows
    // and many-tables both pay extra parse cost equally across
    // transports.
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
    println!("Phase 5 architectural-claim check: PASS");
    println!(
        "  schema complexity (wide rows, many tables, mixed access) does not invert the verdict."
    );
}
