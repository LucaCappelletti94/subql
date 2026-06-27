//! Shared infrastructure for the `examples/phase{1..5}_*.rs` CDC
//! workload benchmarks.
//!
//! Provides everything the phase examples need to run a benchmark
//! against a real Postgres container and compare push vs. polling
//! transports over an arbitrary `CdcSource`:
//!
//! - Docker / Postgres container helpers (duplicated from
//!   `tests/common/`; examples cannot import from `tests/`).
//! - The 3-table e-commerce schema (`users` / `orders` / `order_items`)
//!   plus parser DDL and PG DDL setup helpers.
//! - Replication slot helpers (one publication over all three tables,
//!   one pgoutput slot per measurement run so transports do not steal
//!   events from each other).
//! - Phase 1 workload primitives: steady single-row inserts, sustained
//!   high-rate inserts, and idle-then-burst.
//! - Measurement primitives: [`LatencyStats`], a generic
//!   `spawn_event_receiver<S: CdcSource<Checkpoint = PgLsn>>(...)`
//!   helper, and `collect_latencies` to correlate event observations
//!   with commit timestamps.
//! - Markdown table output helpers so phase examples can pipe their
//!   stdout straight to `docs/benchmarks/phase{N}-<date>.md`.
//!
//! Each example file is its own cargo target; this module is shared
//! via Cargo's `#[path = "cdc_bench_common/mod.rs"]` example-sharing
//! pattern (see Cargo book § examples).

#![cfg(feature = "pg-streaming")]
// Examples are runnable measurement tools, not library code. The lint
// allowlist mirrors `tests/polling_vs_push_benchmark.rs`.
#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::too_many_lines,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::{sql_query, Connection, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{CdcSource, Cell, EventKind, PgLsn};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

// ----------------------------------------------------------------------
// Docker / Postgres container helpers
// ----------------------------------------------------------------------

const PG_IMAGE: &str = "subql-test/postgres-wal2json";
const PG_TAG: &str = "16";

fn ensure_image() {
    let output = std::process::Command::new("docker")
        .args(["images", "-q", &format!("{PG_IMAGE}:{PG_TAG}")])
        .output()
        .expect("docker images");
    if !output.stdout.is_empty() {
        return;
    }
    let dockerfile = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/Dockerfile.postgres"
    );
    let build_out = std::process::Command::new("docker")
        .args([
            "build",
            "-t",
            &format!("{PG_IMAGE}:{PG_TAG}"),
            "-f",
            dockerfile,
            ".",
        ])
        .output()
        .expect("docker build");
    assert!(
        build_out.status.success(),
        "Failed to build postgres-wal2json image: {}",
        String::from_utf8_lossy(&build_out.stderr)
    );
}

/// Preflight Docker. Panics with an actionable message if the daemon
/// is unreachable.
pub fn assert_docker_available() {
    let output = std::process::Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .unwrap_or_else(|e| panic!("docker preflight: `docker info` failed: {e}"));
    assert!(
        output.status.success(),
        "docker preflight failed: `docker info` exited with status {}.\n\
         Ensure Docker is running and the current user can access the daemon socket.\n\
         stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

/// Start a Postgres 16 container configured for logical replication.
pub fn pg_with_wal2json() -> Container<GenericImage> {
    ensure_image();
    GenericImage::new(PG_IMAGE, PG_TAG)
        .with_wait_for(WaitFor::message_on_stderr("ready to accept connections"))
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_USER", "subql_test")
        .with_env_var("POSTGRES_PASSWORD", "subql_test")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=384",
            "-c",
            "max_replication_slots=384",
            "-c",
            "max_connections=512",
        ])
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .expect("start postgres")
}

pub fn pg_url(port: u16) -> String {
    format!("postgres://subql_test:subql_test@127.0.0.1:{port}/testdb")
}

/// Alias for [`pg_url`] kept around so the push source's config reads
/// naturally; the streaming source flips on logical-replication mode
/// programmatically, so no extra query param is needed in the URL.
pub fn pg_replication_url(port: u16) -> String {
    pg_url(port)
}

pub fn pg_connect(port: u16) -> PgConnection {
    PgConnection::establish(&pg_url(port)).expect("PG connection")
}

pub fn pg_port(c: &Container<GenericImage>) -> u16 {
    c.get_host_port_ipv4(5432.tcp()).expect("pg port")
}

// ----------------------------------------------------------------------
// 3-table e-commerce schema (users / orders / order_items)
// ----------------------------------------------------------------------

/// DDL fed to `subql`'s parser-backed catalog. The column types match
/// the PG DDL below: BIGINT primary keys, TEXT strings, BIGINT for
/// money values, TIMESTAMPTZ for time columns (mapped to
/// `ColumnType::Unknown`, which is fine because Phase 1 only inspects
/// the BIGINT id column).
pub const PARSER_DDL: &str = "\
CREATE TABLE users (\
  id BIGINT PRIMARY KEY,\
  email TEXT NOT NULL,\
  name TEXT NOT NULL,\
  last_login_at TIMESTAMP\
);\
CREATE TABLE orders (\
  id BIGINT PRIMARY KEY,\
  user_id BIGINT NOT NULL,\
  status TEXT NOT NULL,\
  total_cents BIGINT NOT NULL,\
  updated_at TIMESTAMP\
);\
CREATE TABLE order_items (\
  id BIGINT PRIMARY KEY,\
  order_id BIGINT NOT NULL,\
  product_name TEXT NOT NULL,\
  quantity INT NOT NULL,\
  price_cents BIGINT NOT NULL\
);\
";

/// PG DDL statements applied in order. We skip foreign keys: they
/// don't affect CDC observability and would complicate workload
/// scripting (every order needs a pre-existing user; every order_item
/// needs a pre-existing order). Phase 2+ workloads insert their own
/// fixtures rather than depending on referential integrity.
pub const PG_DDL: &[&str] = &[
    "CREATE TABLE users (\
        id BIGINT PRIMARY KEY,\
        email TEXT NOT NULL,\
        name TEXT NOT NULL,\
        last_login_at TIMESTAMPTZ\
     )",
    "CREATE TABLE orders (\
        id BIGINT PRIMARY KEY,\
        user_id BIGINT NOT NULL,\
        status TEXT NOT NULL,\
        total_cents BIGINT NOT NULL,\
        updated_at TIMESTAMPTZ\
     )",
    "CREATE TABLE order_items (\
        id BIGINT PRIMARY KEY,\
        order_id BIGINT NOT NULL,\
        product_name TEXT NOT NULL,\
        quantity INT NOT NULL,\
        price_cents BIGINT NOT NULL\
     )",
    "ALTER TABLE users REPLICA IDENTITY FULL",
    "ALTER TABLE orders REPLICA IDENTITY FULL",
    "ALTER TABLE order_items REPLICA IDENTITY FULL",
];

/// All three table names. Used by [`create_publication_all`] and the
/// schema-reset helper.
pub const TABLES: &[&str] = &["users", "orders", "order_items"];

/// Run [`PG_DDL`] in order against the given connection.
pub fn setup_schema(conn: &mut PgConnection) {
    for stmt in PG_DDL {
        sql_query(*stmt)
            .execute(conn)
            .unwrap_or_else(|e| panic!("apply DDL `{stmt}`: {e}"));
    }
}

/// Build the parser-backed catalog used by both push and polling
/// transports for relation resolution.
pub fn parser_db() -> Arc<ParserDB> {
    Arc::new(ParserDB::parse::<PostgreSqlDialect>(PARSER_DDL).expect("parse PARSER_DDL"))
}

/// Truncate every table. Useful between workload runs so id ranges
/// stay clean and pgoutput Relation cache state stays warm.
pub fn truncate_all(conn: &mut PgConnection) {
    for table in TABLES {
        sql_query(format!("TRUNCATE TABLE {table} CASCADE"))
            .execute(conn)
            .unwrap_or_else(|e| panic!("truncate {table}: {e}"));
    }
}

// ----------------------------------------------------------------------
// Replication slot helpers
// ----------------------------------------------------------------------

/// Create a single publication over all three e-commerce tables. The
/// pgoutput protocol allows a slot to follow any subset of tables in
/// the publication; sharing one publication across all measurement
/// runs keeps slot setup terse.
pub fn create_publication_all(conn: &mut PgConnection, publication: &str) {
    sql_query(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {}",
        TABLES.join(", ")
    ))
    .execute(conn)
    .unwrap_or_else(|e| panic!("create publication {publication}: {e}"));
}

/// Create a pgoutput logical replication slot. Idempotent only within
/// a fresh container.
pub fn create_pgoutput_slot(conn: &mut PgConnection, slot: &str) {
    sql_query(format!(
        "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
    ))
    .execute(conn)
    .unwrap_or_else(|e| panic!("create pgoutput slot {slot}: {e}"));
}

/// Drop a slot. Tolerant of missing slots so a phase example can call
/// this on cleanup without first checking existence.
pub fn drop_slot(conn: &mut PgConnection, slot: &str) {
    let _ = sql_query(format!(
        "SELECT pg_drop_replication_slot('{slot}') \
         FROM pg_replication_slots WHERE slot_name = '{slot}'"
    ))
    .execute(conn);
}

// ----------------------------------------------------------------------
// LatencyStats
// ----------------------------------------------------------------------

/// Summary statistics over a vector of per-event latencies.
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub label: String,
    pub samples: Vec<Duration>,
}

impl LatencyStats {
    #[must_use]
    pub fn new(label: impl Into<String>, samples: Vec<Duration>) -> Self {
        Self {
            label: label.into(),
            samples,
        }
    }

    pub fn min(&self) -> Duration {
        self.sorted()[0]
    }

    pub fn median(&self) -> Duration {
        let s = self.sorted();
        s[s.len() / 2]
    }

    pub fn mean(&self) -> Duration {
        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }

    pub fn p99(&self) -> Duration {
        let s = self.sorted();
        let n = s.len();
        s[(n.saturating_sub(1)).min((n * 99) / 100)]
    }

    pub fn max(&self) -> Duration {
        let s = self.sorted();
        s[s.len() - 1]
    }

    fn sorted(&self) -> Vec<Duration> {
        let mut s = self.samples.clone();
        s.sort();
        s
    }

    /// One-line plain-text summary mirroring the prior one-shot
    /// benchmark's output format.
    #[must_use]
    pub fn text_summary(&self) -> String {
        format!(
            "{:<40} n={:>3}  min={:>7.1}ms  median={:>7.1}ms  mean={:>7.1}ms  p99={:>7.1}ms  max={:>7.1}ms",
            self.label,
            self.samples.len(),
            ms(self.min()),
            ms(self.median()),
            ms(self.mean()),
            ms(self.p99()),
            ms(self.max()),
        )
    }

    /// Markdown table row. Pair with [`markdown_table_header`].
    #[must_use]
    pub fn markdown_row(&self) -> String {
        format!(
            "| {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} |",
            self.label,
            self.samples.len(),
            ms(self.min()),
            ms(self.median()),
            ms(self.mean()),
            ms(self.p99()),
            ms(self.max()),
        )
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Public alias for [`ms`], usable by example files that want to print
/// extra labelled rows without going through [`LatencyStats`].
#[must_use]
pub fn ms_of(d: Duration) -> f64 {
    ms(d)
}

/// Markdown table header matching [`LatencyStats::markdown_row`].
#[must_use]
pub fn markdown_table_header() -> String {
    "| transport | n | min (ms) | median (ms) | mean (ms) | p99 (ms) | max (ms) |\n\
     | --- | ---:| ---:| ---:| ---:| ---:| ---:|"
        .to_string()
}

// ----------------------------------------------------------------------
// Generic event receiver + drive/collect plumbing
// ----------------------------------------------------------------------

/// Spawn a task that drains `source.next_event()` and forwards
/// `(table_id, pk_int, observed_at)` tuples to an unbounded channel
/// for every Insert / Update event with a BIGINT primary key. Other
/// kinds (Delete, Truncate) are dropped so callers correlating against
/// freshly-inserted rows aren't confused by tombstones from teardown.
///
/// Generic over any `CdcSource<Checkpoint = PgLsn>` so the same
/// receiver loop works for both the push and polling transports.
pub fn spawn_event_receiver<S>(
    mut source: S,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<(u32, i64, Instant)>,
    tokio::task::JoinHandle<()>,
)
where
    S: CdcSource<Checkpoint = PgLsn> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(u32, i64, Instant)>();
    let task = tokio::spawn(async move {
        loop {
            let next = source.next_event().await;
            let Ok(Some(ev)) = next else {
                return;
            };
            let observed_at = Instant::now();
            if !matches!(ev.kind(), EventKind::Insert | EventKind::Update) {
                continue;
            }
            let Some(row) = ev.new_row() else { continue };
            let Some(Cell::Int(id)) = row.get(0) else {
                continue;
            };
            if tx.send((ev.table_id(), *id, observed_at)).is_err() {
                return;
            }
        }
    });
    (rx, task)
}

/// A single Phase 1 insert statement plus the time it was committed at.
#[derive(Debug, Clone)]
pub struct CommitRecord {
    pub table_id_label: &'static str,
    pub id: i64,
    pub committed_at: Instant,
}

/// Build an INSERT statement against `orders` for a deterministic
/// (id, user_id, status, total_cents) tuple. `updated_at` is set to
/// `NOW()` so the row is plausibly real but we don't compare it.
fn insert_order_sql(id: i64) -> String {
    format!(
        "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
         VALUES ({id}, 1, 'pending', {price_cents}, NOW())",
        price_cents = id * 100
    )
}

/// W1.1 / W1.2 helper: drive `count` `orders` inserts spaced `gap`
/// apart, recording the wall-clock at which each commit returned.
/// Returns `(id -> committed_at)`.
///
/// Note: `gap` is the WALL-CLOCK gap between successive commits. Set
/// it larger than the source's polling interval so commits land at
/// varied offsets within polling cycles (the realistic case for a
/// long-running consumer).
pub async fn drive_inserts(
    conn: &mut PgConnection,
    id_base: i64,
    count: i64,
    gap: Duration,
) -> HashMap<i64, Instant> {
    let mut commits: HashMap<i64, Instant> = HashMap::with_capacity(count as usize);
    for offset in 0..count {
        if offset > 0 {
            tokio::time::sleep(gap).await;
        }
        let id = id_base + offset;
        let commit_at = Instant::now();
        sql_query(insert_order_sql(id))
            .execute(conn)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        commits.insert(id, commit_at);
    }
    commits
}

/// W1.3: do nothing for `idle` duration, then issue a single insert.
/// Returns `(id -> committed_at)` with a single entry. Idle worst-case
/// for polling: a polling cycle that JUST missed the commit waits
/// almost the full interval before observing it.
pub async fn drive_idle_then_burst(
    conn: &mut PgConnection,
    id_base: i64,
    idle: Duration,
    burst_count: i64,
) -> HashMap<i64, Instant> {
    tokio::time::sleep(idle).await;
    let mut commits: HashMap<i64, Instant> = HashMap::with_capacity(burst_count as usize);
    for offset in 0..burst_count {
        let id = id_base + offset;
        let commit_at = Instant::now();
        sql_query(insert_order_sql(id))
            .execute(conn)
            .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        commits.insert(id, commit_at);
    }
    commits
}

/// Wait on the event receiver until either every entry in
/// `commit_times` has been observed, or `deadline` elapses. Returns
/// the per-event COMMIT-to-observation latencies in observation
/// order. Events for ids outside `commit_times` (e.g. leftover events
/// from a prior run on the same slot) are skipped.
pub async fn collect_latencies(
    commit_times: &HashMap<i64, Instant>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(u32, i64, Instant)>,
    deadline: Instant,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(commit_times.len());
    let mut seen: HashSet<i64> = HashSet::new();
    while samples.len() < commit_times.len() && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(opt) = tokio::time::timeout(remaining, rx.recv()).await else {
            break;
        };
        let Some((_table, id, observed_at)) = opt else {
            break;
        };
        if !seen.insert(id) {
            continue;
        }
        if let Some(commit_at) = commit_times.get(&id) {
            samples.push(observed_at.saturating_duration_since(*commit_at));
        }
    }
    samples
}

// ----------------------------------------------------------------------
// Multi-event-kind receiver + collector (Phase 2+)
// ----------------------------------------------------------------------

/// Resolved table IDs for the 3-table e-commerce schema. Use
/// [`resolve_table_ids`] to populate from a parser-backed catalog;
/// pass the result into workload drivers that emit events across
/// multiple tables.
#[derive(Debug, Clone, Copy)]
pub struct TableIds {
    pub users: u32,
    pub orders: u32,
    pub order_items: u32,
}

/// Resolve the three e-commerce table names to subql's compact
/// `TableId` values via the parser-backed catalog. Panics if a table
/// is missing (the schema should always be set up by [`setup_schema`]
/// before this is called).
pub fn resolve_table_ids(db: &ParserDB) -> TableIds {
    TableIds {
        users: subql::catalog_helpers::table_id(db, "users").expect("users table id"),
        orders: subql::catalog_helpers::table_id(db, "orders").expect("orders table id"),
        order_items: subql::catalog_helpers::table_id(db, "order_items")
            .expect("order_items table id"),
    }
}

/// A single CDC event observation. Phase 2+ workloads correlate these
/// against per-`(table, pk, kind)` commit timestamps so Insert /
/// Update / Delete events on the same row can be tracked separately.
#[derive(Debug, Clone)]
pub struct EventObservation {
    pub table_id: u32,
    pub pk_int: i64,
    pub kind: EventKind,
    pub observed_at: Instant,
}

/// Key into a multi-event-kind commit-times map. The kind is part of
/// the key because the same `(table, pk)` may have several events in
/// a workload (e.g. W2.1 inserts, updates, then deletes the same row).
pub type EventKey = (u32, i64, EventKind);

/// Generic event receiver that surfaces Insert / Update / Delete
/// events (Truncate dropped). Uses `ev.pk()` for the primary key so
/// the receiver handles Delete events (which have no `new_row`)
/// uniformly with Insert and Update.
pub fn spawn_full_event_receiver<S>(
    mut source: S,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    tokio::task::JoinHandle<()>,
)
where
    S: CdcSource<Checkpoint = PgLsn> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<EventObservation>();
    let task = tokio::spawn(async move {
        loop {
            let next = source.next_event().await;
            let Ok(Some(ev)) = next else {
                return;
            };
            let observed_at = Instant::now();
            let kind = ev.kind();
            if !matches!(
                kind,
                EventKind::Insert | EventKind::Update | EventKind::Delete
            ) {
                continue;
            }
            let pk_cells = ev.pk().values();
            let Some(subql::Cell::Int(pk_int)) = pk_cells.first() else {
                continue;
            };
            let observation = EventObservation {
                table_id: ev.table_id(),
                pk_int: *pk_int,
                kind,
                observed_at,
            };
            if tx.send(observation).is_err() {
                return;
            }
        }
    });
    (rx, task)
}

/// Wait for every entry in `commit_times` to be observed, or
/// `deadline` to elapse. Returns per-event COMMIT-to-observation
/// latencies in observation order. Events not in `commit_times` (e.g.
/// from a prior run on the same slot) are skipped.
pub async fn collect_full_latencies(
    commit_times: &HashMap<EventKey, Instant>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    deadline: Instant,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(commit_times.len());
    let mut seen: HashSet<EventKey> = HashSet::new();
    while samples.len() < commit_times.len() && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(opt) = tokio::time::timeout(remaining, rx.recv()).await else {
            break;
        };
        let Some(ob) = opt else { break };
        let key = (ob.table_id, ob.pk_int, ob.kind);
        if !seen.insert(key) {
            continue;
        }
        if let Some(commit_at) = commit_times.get(&key) {
            samples.push(ob.observed_at.saturating_duration_since(*commit_at));
        }
    }
    samples
}

/// Slow-consumer receiver: like [`spawn_full_event_receiver`] but
/// the spawned task sleeps `per_event_delay` for every received event
/// before forwarding it. The sleep happens INSIDE the same task that
/// owns `source.next_event()`, so `next_event` is not polled during
/// the sleep — this is the load-bearing backpressure path. Push
/// backpressures via TCP (the inner task stops reading from
/// `CopyBothDuplex`); polling backpressures by skipping polls (the
/// inner task stops issuing `pg_logical_slot_get_binary_changes`).
///
/// `observed_at` is recorded AFTER the per-event delay, so the
/// per-event latency reported by [`collect_full_latencies`] is the
/// END-TO-END consumer-side latency (transport delivery + the
/// simulated processing time and any accumulated backlog wait).
pub fn spawn_slow_event_receiver<S>(
    mut source: S,
    per_event_delay: Duration,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    tokio::task::JoinHandle<()>,
)
where
    S: CdcSource<Checkpoint = PgLsn> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<EventObservation>();
    let task = tokio::spawn(async move {
        loop {
            let next = source.next_event().await;
            let Ok(Some(ev)) = next else {
                return;
            };
            tokio::time::sleep(per_event_delay).await;
            let observed_at = Instant::now();
            let kind = ev.kind();
            if !matches!(
                kind,
                EventKind::Insert | EventKind::Update | EventKind::Delete
            ) {
                continue;
            }
            let pk_cells = ev.pk().values();
            let Some(subql::Cell::Int(pk_int)) = pk_cells.first() else {
                continue;
            };
            let observation = EventObservation {
                table_id: ev.table_id(),
                pk_int: *pk_int,
                kind,
                observed_at,
            };
            if tx.send(observation).is_err() {
                return;
            }
        }
    });
    (rx, task)
}

// ----------------------------------------------------------------------
// Server-side load snapshots
// ----------------------------------------------------------------------

/// Snapshot of PG server-side counters at a moment in time. Pair two
/// snapshots with [`snapshot_delta`] to compute rates and totals over
/// a measurement window.
#[derive(Debug, Clone)]
pub struct ServerLoadSnapshot {
    pub taken_at: Instant,
    pub xact_commit: i64,
    pub xact_rollback: i64,
    pub tup_inserted: i64,
    pub tup_updated: i64,
    pub tup_deleted: i64,
    pub active_query_count: i64,
    pub active_pid_count: i64,
    pub slot_count: i64,
    pub current_wal_lsn_bytes: i64,
    pub per_slot_lag_bytes: Vec<(String, i64)>,
}

/// Take a [`ServerLoadSnapshot`] against the given DB connection.
pub fn snapshot_server_load(conn: &mut PgConnection) -> ServerLoadSnapshot {
    use diesel::sql_types::BigInt;

    #[derive(diesel::QueryableByName)]
    struct DbRow {
        #[diesel(sql_type = BigInt)]
        xact_commit: i64,
        #[diesel(sql_type = BigInt)]
        xact_rollback: i64,
        #[diesel(sql_type = BigInt)]
        tup_inserted: i64,
        #[diesel(sql_type = BigInt)]
        tup_updated: i64,
        #[diesel(sql_type = BigInt)]
        tup_deleted: i64,
    }
    let db: DbRow = sql_query(
        "SELECT \
            COALESCE(xact_commit, 0)::BIGINT AS xact_commit, \
            COALESCE(xact_rollback, 0)::BIGINT AS xact_rollback, \
            COALESCE(tup_inserted, 0)::BIGINT AS tup_inserted, \
            COALESCE(tup_updated, 0)::BIGINT AS tup_updated, \
            COALESCE(tup_deleted, 0)::BIGINT AS tup_deleted \
         FROM pg_stat_database WHERE datname = current_database()",
    )
    .get_result(conn)
    .expect("query pg_stat_database");

    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }
    let active_q: CountRow = sql_query(
        "SELECT COUNT(*)::BIGINT AS count FROM pg_stat_activity \
         WHERE state = 'active' AND backend_type = 'client backend'",
    )
    .get_result(conn)
    .expect("query active query count");

    let active_pid: CountRow = sql_query(
        "SELECT COUNT(*)::BIGINT AS count FROM pg_replication_slots \
         WHERE active_pid IS NOT NULL",
    )
    .get_result(conn)
    .expect("query active pid count");

    let slot_total: CountRow =
        sql_query("SELECT COUNT(*)::BIGINT AS count FROM pg_replication_slots")
            .get_result(conn)
            .expect("query slot count");

    #[derive(diesel::QueryableByName)]
    struct LsnRow {
        #[diesel(sql_type = BigInt)]
        lsn_bytes: i64,
    }
    let lsn: LsnRow =
        sql_query("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::BIGINT AS lsn_bytes")
            .get_result(conn)
            .expect("query current wal lsn");

    #[derive(diesel::QueryableByName)]
    struct SlotLagRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        slot_name: String,
        #[diesel(sql_type = BigInt)]
        lag_bytes: i64,
    }
    let slot_lags: Vec<SlotLagRow> = sql_query(
        "SELECT slot_name, \
            (pg_current_wal_lsn() - confirmed_flush_lsn)::BIGINT AS lag_bytes \
         FROM pg_replication_slots \
         WHERE confirmed_flush_lsn IS NOT NULL \
         ORDER BY slot_name",
    )
    .load(conn)
    .expect("query per-slot lag");

    ServerLoadSnapshot {
        taken_at: Instant::now(),
        xact_commit: db.xact_commit,
        xact_rollback: db.xact_rollback,
        tup_inserted: db.tup_inserted,
        tup_updated: db.tup_updated,
        tup_deleted: db.tup_deleted,
        active_query_count: active_q.count,
        active_pid_count: active_pid.count,
        slot_count: slot_total.count,
        current_wal_lsn_bytes: lsn.lsn_bytes,
        per_slot_lag_bytes: slot_lags
            .into_iter()
            .map(|r| (r.slot_name, r.lag_bytes))
            .collect(),
    }
}

/// Delta between two server-side snapshots: rates per second for
/// counter columns plus end values for gauges.
#[derive(Debug, Clone)]
pub struct ServerLoadDelta {
    pub window_secs: f64,
    pub xact_commit_per_sec: f64,
    pub xact_rollback_per_sec: f64,
    pub tup_inserted_per_sec: f64,
    pub tup_updated_per_sec: f64,
    pub tup_deleted_per_sec: f64,
    pub active_query_count_end: i64,
    pub active_pid_count_end: i64,
    pub slot_count_end: i64,
    pub wal_bytes_written: i64,
}

#[must_use]
pub fn snapshot_delta(before: &ServerLoadSnapshot, after: &ServerLoadSnapshot) -> ServerLoadDelta {
    let window_secs = after
        .taken_at
        .saturating_duration_since(before.taken_at)
        .as_secs_f64()
        .max(1e-6);
    #[allow(clippy::cast_precision_loss)]
    let rate = |a: i64, b: i64| -> f64 { (a - b) as f64 / window_secs };
    ServerLoadDelta {
        window_secs,
        xact_commit_per_sec: rate(after.xact_commit, before.xact_commit),
        xact_rollback_per_sec: rate(after.xact_rollback, before.xact_rollback),
        tup_inserted_per_sec: rate(after.tup_inserted, before.tup_inserted),
        tup_updated_per_sec: rate(after.tup_updated, before.tup_updated),
        tup_deleted_per_sec: rate(after.tup_deleted, before.tup_deleted),
        active_query_count_end: after.active_query_count,
        active_pid_count_end: after.active_pid_count,
        slot_count_end: after.slot_count,
        wal_bytes_written: after.current_wal_lsn_bytes - before.current_wal_lsn_bytes,
    }
}

// ----------------------------------------------------------------------
// TSV output helper
// ----------------------------------------------------------------------

/// Append-only TSV writer: every example writes one row per
/// (experiment, scale_key, cell_key, metric, value). Schema is
/// deliberately long-form so external tools (pandas, gnuplot) can
/// pivot freely without parsing free-text Markdown.
pub struct TsvWriter {
    file: std::fs::File,
}

impl TsvWriter {
    /// Open `path` for writing, truncating any existing file, and
    /// emit the schema header.
    pub fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let mut file = std::fs::File::create(path)?;
        std::io::Write::write_all(
            &mut file,
            b"experiment\tscale_key\tcell_key\tmetric\tvalue\n",
        )?;
        Ok(Self { file })
    }

    pub fn row(
        &mut self,
        experiment: &str,
        scale_key: &str,
        cell_key: &str,
        metric: &str,
        value: f64,
    ) {
        let line = format!("{experiment}\t{scale_key}\t{cell_key}\t{metric}\t{value:.6}\n");
        std::io::Write::write_all(&mut self.file, line.as_bytes()).expect("TSV row write");
    }

    pub fn row_int(
        &mut self,
        experiment: &str,
        scale_key: &str,
        cell_key: &str,
        metric: &str,
        value: i64,
    ) {
        let line = format!("{experiment}\t{scale_key}\t{cell_key}\t{metric}\t{value}\n");
        std::io::Write::write_all(&mut self.file, line.as_bytes()).expect("TSV row write");
    }
}

// ----------------------------------------------------------------------
// Multi-consumer harness
// ----------------------------------------------------------------------

/// Force-drop a replication slot. Pair with the per-trial slot
/// lifecycle so a single example can re-use slot names across trials
/// without hitting `max_replication_slots`.
pub fn force_drop_slot(conn: &mut PgConnection, slot: &str) {
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

/// Handle returned by the multi-consumer harness.
pub struct ConsumerHandle {
    pub slot_name: String,
    pub rx: tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
    pub task: tokio::task::JoinHandle<()>,
    pub task_exited_handle: Arc<core::sync::atomic::AtomicBool>,
}

/// Pre-create N pgoutput slots for push consumers. Use BEFORE
/// [`spawn_n_push_consumers`] so each source's `connect` validation
/// succeeds.
pub fn precreate_push_slots(conn: &mut PgConnection, label_prefix: &str, n: usize) -> Vec<String> {
    let mut names = Vec::with_capacity(n);
    for i in 0..n {
        let slot = format!("{label_prefix}_push_n{n}_c{i}");
        create_pgoutput_slot(conn, &slot);
        names.push(slot);
    }
    names
}

/// Mirror of [`precreate_push_slots`] for polling consumers; the
/// poll-interval enters the slot name so different cadence sweeps in
/// one example don't collide.
pub fn precreate_poll_slots(
    conn: &mut PgConnection,
    label_prefix: &str,
    poll_interval: Duration,
    n: usize,
) -> Vec<String> {
    let mut names = Vec::with_capacity(n);
    for i in 0..n {
        let slot = format!("{label_prefix}_poll{}_n{n}_c{i}", poll_interval.as_millis());
        create_pgoutput_slot(conn, &slot);
        names.push(slot);
    }
    names
}

/// Spawn `n` push consumers, each with its own pre-created slot.
pub async fn spawn_n_push_consumers(
    n: usize,
    label_prefix: &str,
    pg_repl_url: &str,
    publication: &str,
    catalog: Arc<ParserDB>,
    status_interval: Duration,
    buffer_capacity: usize,
) -> Vec<ConsumerHandle> {
    use subql::{PgStreamingCdcSource, PgStreamingConfig};
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let slot = format!("{label_prefix}_push_n{n}_c{i}");
        let config = PgStreamingConfig::new(pg_repl_url.to_string(), &slot, publication)
            .status_interval(status_interval)
            .buffer_capacity(buffer_capacity);
        let source = PgStreamingCdcSource::connect(config, Arc::clone(&catalog))
            .await
            .expect("connect push source");
        let task_exited_handle = source.task_exited_handle();
        let (rx, task) = spawn_full_event_receiver(source);
        handles.push(ConsumerHandle {
            slot_name: slot,
            rx,
            task,
            task_exited_handle,
        });
    }
    handles
}

/// Spawn `n` polling consumers, each with its own pre-created slot.
pub async fn spawn_n_poll_consumers(
    n: usize,
    label_prefix: &str,
    pg_sql_url: &str,
    publication: &str,
    catalog: Arc<ParserDB>,
    poll_interval: Duration,
    buffer_capacity: usize,
) -> Vec<ConsumerHandle> {
    use subql::{PollingPgCdcConfig, PollingPgCdcSource};
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let slot = format!("{label_prefix}_poll{}_n{n}_c{i}", poll_interval.as_millis());
        let config = PollingPgCdcConfig::new(pg_sql_url.to_string(), &slot, publication)
            .poll_interval(poll_interval)
            .buffer_capacity(buffer_capacity);
        let source = PollingPgCdcSource::connect(config, Arc::clone(&catalog))
            .await
            .expect("connect polling source");
        let task_exited_handle = source.task_exited_handle();
        let (rx, task) = spawn_full_event_receiver(source);
        handles.push(ConsumerHandle {
            slot_name: slot,
            rx,
            task,
            task_exited_handle,
        });
    }
    handles
}

/// Aggregate per-consumer event counts into a single throughput
/// summary `(total_events, events_per_second)`.
#[must_use]
pub fn aggregate_throughput(counts: &[u64], window: Duration) -> (u64, f64) {
    let total: u64 = counts.iter().sum();
    #[allow(clippy::cast_precision_loss)]
    let rate = total as f64 / window.as_secs_f64().max(1e-6);
    (total, rate)
}

// ============================================================================
// Mesh substrate
// ============================================================================
//
// Multi-PG benchmark primitives. The mesh OID sanity check
// (`examples/mesh_oid_sanity.rs`) confirmed that fresh PG containers
// running identical DDL deterministically assign matching PG OIDs
// AND that those OIDs translate to identical parser TableIds via a
// shared `Arc<ParserDB>`, so one catalog can be shared across the
// whole mesh and no `oid_remap` is needed.

/// One node in the mesh. The `container` field keeps the underlying
/// PG container alive; dropping a `MeshNode` shuts the container
/// down. `dml_conn` is a long-lived connection for setup queries
/// (publication, slots, snapshots). Writers open their own
/// short-lived connections via `sql_url`.
pub struct MeshNode {
    pub container: Container<GenericImage>,
    pub port: u16,
    pub source_id: u16,
    pub dml_conn: PgConnection,
    pub repl_url: String,
    pub sql_url: String,
}

/// Spawn `m` independent PG containers in sequence with identical
/// schema, each carrying the supplied publication. Returns one
/// `MeshNode` per container with monotonically assigned source_ids
/// starting at 0.
#[must_use]
pub fn spawn_mesh(m: usize, publication: &str) -> Vec<MeshNode> {
    let mut nodes = Vec::with_capacity(m);
    for idx in 0..m {
        let container = pg_with_wal2json();
        let port = pg_port(&container);
        let mut dml_conn = pg_connect(port);
        setup_schema(&mut dml_conn);
        create_publication_all(&mut dml_conn, publication);
        // Insert the sentinel users row so `orders` INSERTs at id=1
        // satisfy the FK. Slots are pre-created later so this
        // sentinel does not appear in the WAL stream.
        sql_query(
            "INSERT INTO users (id, email, name, last_login_at) \
             VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
             ON CONFLICT (id) DO NOTHING",
        )
        .execute(&mut dml_conn)
        .expect("insert mesh sentinel users row");
        nodes.push(MeshNode {
            container,
            port,
            source_id: u16::try_from(idx).expect("source_id fits in u16"),
            dml_conn,
            repl_url: pg_replication_url(port),
            sql_url: pg_url(port),
        });
    }
    nodes
}

/// Writer-side workload shape across the mesh.
#[derive(Debug, Clone, Copy)]
pub enum WriterProfile {
    /// Every PG receives the same per-PG event rate.
    Uniform { rate_per_pg: u64 },
    /// One PG drives `hot_rate`, the others drive `cold_rate`.
    Skewed {
        hot_idx: usize,
        hot_rate: u64,
        cold_rate: u64,
    },
}

impl WriterProfile {
    #[must_use]
    pub const fn rate_for(self, pg_idx: usize) -> u64 {
        match self {
            Self::Uniform { rate_per_pg } => rate_per_pg,
            Self::Skewed {
                hot_idx,
                hot_rate,
                cold_rate,
            } => {
                if pg_idx == hot_idx {
                    hot_rate
                } else {
                    cold_rate
                }
            }
        }
    }
}

/// A CDC event observation tagged with the source PG it came from.
/// Used downstream by [`MultiSourceFanIn`] so a single engine /
/// measurement loop can disambiguate cross-source events.
#[derive(Debug, Clone)]
pub struct TaggedEvent {
    pub source_id: u16,
    pub table_id: u32,
    pub pk_int: i64,
    pub kind: EventKind,
    pub observed_at: Instant,
}

/// Multi-source fan-in adapter. Owns one tokio task per source and
/// forwards all events into a single shared `mpsc::UnboundedReceiver`
/// with the source_id attached. The G2 geometry (one engine sees
/// events from many PGs) is implemented on top of this.
pub struct MultiSourceFanIn {
    pub rx: tokio::sync::mpsc::UnboundedReceiver<TaggedEvent>,
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl MultiSourceFanIn {
    /// Spawn one drain task per `(source_id, source)` pair. Events
    /// not carrying a PK Int (e.g. Truncate, or PKs with non-Int
    /// types) are dropped silently, matching [`spawn_full_event_receiver`].
    pub fn spawn<S>(sources: Vec<(u16, S)>) -> Self
    where
        S: CdcSource<Checkpoint = PgLsn> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<TaggedEvent>();
        let mut tasks = Vec::with_capacity(sources.len());
        for (source_id, mut source) in sources {
            let tx_clone = tx.clone();
            let task = tokio::spawn(async move {
                loop {
                    let next = source.next_event().await;
                    let Ok(Some(ev)) = next else {
                        return;
                    };
                    let observed_at = Instant::now();
                    let kind = ev.kind();
                    if !matches!(
                        kind,
                        EventKind::Insert | EventKind::Update | EventKind::Delete
                    ) {
                        continue;
                    }
                    let pk_cells = ev.pk().values();
                    let Some(Cell::Int(pk_int)) = pk_cells.first() else {
                        continue;
                    };
                    let event = TaggedEvent {
                        source_id,
                        table_id: ev.table_id(),
                        pk_int: *pk_int,
                        kind,
                        observed_at,
                    };
                    if tx_clone.send(event).is_err() {
                        return;
                    }
                }
            });
            tasks.push(task);
        }
        Self { rx, tasks }
    }

    /// Abort every drain task. Use after a measurement window closes.
    pub fn abort_all(&self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// Snapshot of the whole mesh at one wall-clock instant. Just a
/// `Vec<ServerLoadSnapshot>` indexed by source_id.
#[derive(Debug, Clone)]
pub struct MeshLoadSnapshot {
    pub per_node: Vec<ServerLoadSnapshot>,
}

/// Take a `MeshLoadSnapshot` by walking each node's `dml_conn` once.
pub fn snapshot_mesh_load(nodes: &mut [MeshNode]) -> MeshLoadSnapshot {
    let per_node = nodes
        .iter_mut()
        .map(|n| snapshot_server_load(&mut n.dml_conn))
        .collect();
    MeshLoadSnapshot { per_node }
}

/// Per-source commit-time map: writer thread records the wall-clock
/// instant at which each row was committed, keyed by
/// `(table_id, pk_int, EventKind::Insert)`.
pub type CommitMap = HashMap<EventKey, Instant>;

/// Drive one batched-INSERT writer thread per mesh node according to
/// `profile`. Returns the per-source commit maps once all writer
/// threads finish. Uses the OS-thread + `tokio::sync::oneshot` pattern
/// from `examples/scale_throughput.rs` so the current-thread tokio
/// runtime keeps driving the consumer side during the window.
///
/// `id_base` is the starting PK for source 0; each source_id reserves
/// 100_000_000 IDs above the previous, so PK collisions between
/// sources are impossible within any reasonable window.
pub fn mesh_writer_rt(
    nodes: &[MeshNode],
    profile: WriterProfile,
    duration: Duration,
    id_base: i64,
    orders_table_id: u32,
) -> impl std::future::Future<Output = Vec<CommitMap>> + Send + 'static {
    let plan: Vec<(u16, String, u64, i64)> = nodes
        .iter()
        .map(|n| {
            let rate = profile.rate_for(n.source_id as usize);
            let per_source_id_base = id_base + i64::from(n.source_id) * 100_000_000;
            (n.source_id, n.sql_url.clone(), rate, per_source_id_base)
        })
        .collect();
    drive_mesh_writers(plan, duration, orders_table_id)
}

async fn drive_mesh_writers(
    plan: Vec<(u16, String, u64, i64)>,
    duration: Duration,
    orders_table_id: u32,
) -> Vec<CommitMap> {
    use diesel::connection::SimpleConnection;

    let n_nodes = plan.len();
    let mut joins = Vec::with_capacity(n_nodes);
    for (source_id, pg_url, rate, per_source_id_base) in plan {
        let (tx, rx) = tokio::sync::oneshot::channel::<CommitMap>();
        std::thread::spawn(move || {
            let mut commits: CommitMap =
                HashMap::with_capacity(((rate as usize) * duration.as_secs() as usize).max(1024));
            if rate == 0 {
                let _ = tx.send(commits);
                return;
            }
            let mut conn = PgConnection::establish(&pg_url).expect("PG writer conn");
            let batch_size: u64 = if rate >= 10_000 {
                50
            } else if rate >= 5_000 {
                20
            } else if rate >= 1_000 {
                5
            } else {
                1
            };
            let batches_per_sec = rate.div_ceil(batch_size);
            let gap = Duration::from_secs_f64(1.0 / batches_per_sec as f64);
            let end = Instant::now() + duration;
            let mut id = per_source_id_base;
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
                    .unwrap_or_else(|e| panic!("source {source_id} insert id={id}: {e}"));
                    commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
                    id += 1;
                } else {
                    let mut stmts = String::from("BEGIN;");
                    let commit_at = Instant::now();
                    for _ in 0..batch_size {
                        use std::fmt::Write as _;
                        let _ = write!(
                            stmts,
                            " INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                              VALUES ({id}, 1, 'paid', {p}, NOW());",
                            p = id * 100
                        );
                        commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
                        id += 1;
                    }
                    stmts.push_str(" COMMIT;");
                    conn.batch_execute(&stmts)
                        .unwrap_or_else(|e| panic!("source {source_id} batch: {e}"));
                }
                if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                    std::thread::sleep(remaining);
                }
            }
            let _ = tx.send(commits);
        });
        joins.push((source_id, rx));
    }
    let mut out: Vec<CommitMap> = (0..n_nodes).map(|_| HashMap::new()).collect();
    for (source_id, rx) in joins {
        let commits = rx.await.unwrap_or_else(|_| HashMap::new());
        out[source_id as usize] = commits;
    }
    out
}

/// Compute per-source latency stats given the per-source commit maps
/// and a flat list of `TaggedEvent`s drained during the window. Each
/// returned `Vec<Duration>` is the per-source pooled latencies.
#[must_use]
pub fn pool_mesh_latencies(
    commits: &[CommitMap],
    observations: &[TaggedEvent],
) -> Vec<Vec<Duration>> {
    let mut out: Vec<Vec<Duration>> = (0..commits.len()).map(|_| Vec::new()).collect();
    let mut seen: Vec<HashSet<EventKey>> = (0..commits.len()).map(|_| HashSet::new()).collect();
    for ev in observations {
        let key = (ev.table_id, ev.pk_int, ev.kind);
        let idx = ev.source_id as usize;
        if idx >= commits.len() {
            continue;
        }
        if !seen[idx].insert(key) {
            continue;
        }
        if let Some(commit_at) = commits[idx].get(&key) {
            out[idx].push(ev.observed_at.saturating_duration_since(*commit_at));
        }
    }
    out
}
