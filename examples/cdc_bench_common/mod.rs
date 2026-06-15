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
            "max_wal_senders=32",
            "-c",
            "max_replication_slots=32",
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
