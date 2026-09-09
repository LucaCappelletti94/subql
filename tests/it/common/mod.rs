//! Shared helpers for the Docker-backed integration tests.
//!
//! One server per engine per nextest run, looked up by name and label from
//! every test process, and one database per test on it. Requires Docker. The
//! Postgres image `subql-test/postgres-wal2json:16` is built from
//! `tests/fixtures/Dockerfile.postgres` on first use.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use diesel::prelude::*;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, GenericImage, ImageExt, ReuseDirective};

// Shared round-trip dispatch machinery, only for test crates that enable
// the full apply stack. Empty (undeclared) for every other test.
#[cfg(all(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
pub mod dispatch;

const PG_IMAGE: &str = "subql-test/postgres-wal2json";
const PG_TAG: &str = "16";
const MAXWELL_IMAGE: &str = "zendesk/maxwell";
const MAXWELL_TAG: &str = "v1.44.0";
const PASSWORD: &str = "subql_test";
const ENGINE_LABEL: &str = "subql.test.engine";
const RUN_LABEL: &str = "subql.test.run";
/// Shared servers from other runs older than this are removed on acquisition.
const STALE_AFTER: Duration = Duration::from_secs(30 * 60);

/// The nextest run this process belongs to, or the process itself under
/// `cargo test`, which runs every test in one process anyway.
fn run_id() -> String {
    std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| std::process::id().to_string())
}

fn network_name(run: &str) -> String {
    format!("subql-net-{run}")
}

fn mysql_host(run: &str) -> String {
    format!("subql-mysql-{run}")
}

fn docker(args: &[&str]) -> std::process::Output {
    std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("docker {}: {e}", args.join(" ")))
}

fn ensure_image() {
    let output = docker(&["images", "-q", &format!("{PG_IMAGE}:{PG_TAG}")]);
    if !output.stdout.is_empty() {
        return;
    }
    let dockerfile = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/Dockerfile.postgres"
    );
    let build_out = docker(&[
        "build",
        "-t",
        &format!("{PG_IMAGE}:{PG_TAG}"),
        "-f",
        dockerfile,
        ".",
    ]);
    assert!(
        build_out.status.success(),
        "Failed to build postgres-wal2json image: {}",
        String::from_utf8_lossy(&build_out.stderr)
    );
}

/// Preflight Docker. Panics with an actionable message if the daemon is
/// unreachable.
pub fn assert_docker_available() {
    let output = docker(&["info", "--format", "{{.ServerVersion}}"]);
    assert!(
        output.status.success(),
        "docker preflight failed: `docker info` exited with status {}.\n\
         Ensure Docker is running and the current user can access the daemon socket.\n\
         stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

/// Multi-thread tokio runtime for the async e2e tests. Built at the sync
/// test boundary so blocking testcontainers setup runs before `block_on`,
/// then used to drive the async apply and re-exec paths. Multi-thread so
/// the connectors' `Send` futures are exercised across worker threads.
#[cfg(any(
    feature = "apply-patchset-postgres-async",
    feature = "apply-patchset-mysql-async",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-async-mysql",
))]
pub fn multi_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build multi-thread tokio runtime")
}

fn unix_now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_secs(),
    )
    .expect("seconds fit i64")
}

/// Remove our shared servers left behind by runs older than [`STALE_AFTER`],
/// and their networks. Nothing else can: testcontainers-rs has no reaper and
/// a reused container is never stopped on drop.
fn reap_stale() {
    let listing = docker(&[
        "ps",
        "-a",
        "--filter",
        &format!("label={ENGINE_LABEL}"),
        "--format",
        &format!("{{{{.ID}}}}\t{{{{.Label \"{RUN_LABEL}\"}}}}"),
    ]);
    let mine = run_id();
    let now = unix_now();
    for line in String::from_utf8_lossy(&listing.stdout).lines() {
        let Some((id, run)) = line.split_once('\t') else {
            continue;
        };
        if run == mine {
            continue;
        }
        let created = docker(&["inspect", "--format", "{{.Created}}", id]);
        let created = String::from_utf8_lossy(&created.stdout);
        let Ok(created) = chrono::DateTime::parse_from_rfc3339(created.trim()) else {
            continue;
        };
        if now - created.timestamp() < i64::try_from(STALE_AFTER.as_secs()).expect("fits") {
            continue;
        }
        docker(&["rm", "-f", id]);
        docker(&["network", "rm", &network_name(run)]);
    }
}

/// Look up or start this run's shared `engine` server and wait until `ready`
/// accepts its mapped host port.
///
/// Test processes race on the first acquisition: both miss the lookup, one
/// creation loses on the name and retries into a hit. The reuse path skips the
/// image's `WaitFor`, so readiness is always probed here.
fn shared_server(
    engine: &str,
    request: impl Fn() -> ContainerRequest<GenericImage>,
    container_port: u16,
    ready: impl Fn(u16) -> bool,
    timeout: Duration,
) -> u16 {
    reap_stale();
    let run = run_id();
    let name = format!("subql-{engine}-{run}");
    let deadline = Instant::now() + timeout;
    let container = loop {
        let attempt = request()
            .with_container_name(&name)
            .with_label(ENGINE_LABEL, engine)
            .with_label(RUN_LABEL, &run)
            .with_reuse(ReuseDirective::Always)
            .with_startup_timeout(timeout)
            .start();
        match attempt {
            Ok(container) => break container,
            Err(err) => {
                assert!(
                    Instant::now() < deadline,
                    "start shared {engine} server {name}: {err}"
                );
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    };
    let port = container
        .get_host_port_ipv4(container_port.tcp())
        .expect("mapped port");
    while !ready(port) {
        assert!(
            Instant::now() < deadline,
            "shared {engine} server {name} never became ready on port {port}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    port
}

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

fn fresh_db_name() -> String {
    format!(
        "t{}_{}",
        std::process::id(),
        DB_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

// Postgres

fn pg_request() -> ContainerRequest<GenericImage> {
    ensure_image();
    GenericImage::new(PG_IMAGE, PG_TAG)
        .with_wait_for(WaitFor::message_on_stderr("ready to accept connections"))
        .with_exposed_port(5432.tcp())
        // The data directory lives in container memory: initdb and every
        // fsync hit RAM, which is what a throwaway test server wants.
        .with_mount(Mount::tmpfs_mount("/var/lib/postgresql/data"))
        .with_env_var("POSTGRES_USER", "subql_test")
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_env_var("POSTGRES_DB", "postgres")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=128",
            "-c",
            "max_replication_slots=128",
            "-c",
            "max_connections=400",
            "-c",
            // PostgreSQL 16.15 made this an allow-list that replaces rather
            // than extends, so the two shipped plugins are repeated.
            "output_plugin_libraries=pgoutput,test_decoding,wal2json",
        ])
}

fn pg_url_for(port: u16, database: &str) -> String {
    format!("postgres://subql_test:{PASSWORD}@127.0.0.1:{port}/{database}")
}

fn pg_admin(port: u16) -> ConnectionResult<PgConnection> {
    PgConnection::establish(&pg_url_for(port, "postgres"))
}

/// One database on this run's shared Postgres, dropped with its slots when
/// the handle drops. Declare it before anything that connects to it.
pub struct PgDatabase {
    port: u16,
    name: String,
}

/// Acquire the shared Postgres and create a fresh database on it.
pub fn pg_database() -> PgDatabase {
    let port = shared_server(
        "pg",
        pg_request,
        5432,
        |port| pg_admin(port).is_ok(),
        Duration::from_secs(180),
    );
    let name = fresh_db_name();
    let mut admin = pg_admin(port).expect("PG admin connection");
    // DDL, which the query DSL does not express.
    diesel::sql_query(format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .expect("create test database");
    PgDatabase { port, name }
}

impl PgDatabase {
    /// libpq URL of this database.
    pub fn url(&self) -> String {
        pg_url_for(self.port, &self.name)
    }

    /// Establish a diesel [`PgConnection`] to this database.
    pub fn connect(&self) -> PgConnection {
        PgConnection::establish(&self.url()).expect("PG connection")
    }

    /// A replication slot name for this database. Slot names are cluster
    /// wide, so `base` is prefixed with the database name.
    pub fn slot(&self, base: &str) -> String {
        format!("{}_{base}", self.name)
    }

    /// Set a configuration parameter for new sessions on this database.
    #[cfg(feature = "pg-streaming")]
    pub fn set(&self, parameter: &str, value: &str) {
        let mut admin = pg_admin(self.port).expect("PG admin connection");
        // DDL, which the query DSL does not express.
        diesel::sql_query(format!(
            "ALTER DATABASE {} SET {parameter} = '{value}'",
            self.name
        ))
        .execute(&mut admin)
        .expect("alter database");
    }
}

diesel::table! {
    pg_catalog.pg_replication_slots (slot_name) {
        slot_name -> Text,
        database -> Nullable<Text>,
    }
}

impl Drop for PgDatabase {
    fn drop(&mut self) {
        let Ok(mut admin) = pg_admin(self.port) else {
            return;
        };
        // A database cannot be dropped while a logical slot is bound to it.
        let slots: Vec<String> = pg_replication_slots::table
            .filter(pg_replication_slots::database.eq(&self.name))
            .select(pg_replication_slots::slot_name)
            .load(&mut admin)
            .unwrap_or_default();
        for slot in slots {
            let _ = try_drop_slot(&mut admin, &slot);
        }
        // DDL, which the query DSL does not express.
        let _ = diesel::sql_query(format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .execute(&mut admin);
    }
}

/// Create a logical replication slot driven by `wal2json`. Name it through
/// [`PgDatabase::slot`].
pub fn create_slot(conn: &mut PgConnection, name: &str) {
    // Replication administration, which the query DSL does not express.
    diesel::sql_query(format!(
        "SELECT pg_create_logical_replication_slot('{name}', 'wal2json')"
    ))
    .execute(conn)
    .expect("create logical replication slot");
}

/// Drop a replication slot, waiting out a walsender that still holds it.
///
/// A streaming source releases its slot when its replication connection
/// closes, and the server notices that shortly after the client task ends, so
/// an immediate drop races the release and fails with `is active for PID`.
fn try_drop_slot(conn: &mut PgConnection, name: &str) -> QueryResult<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // Replication administration, which the query DSL does not express.
        match diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(conn)
        {
            Ok(_) => return Ok(()),
            Err(err) if err.to_string().contains("is active") && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

/// Drop a replication slot, waiting out a walsender that still holds it.
pub fn drop_slot(conn: &mut PgConnection, name: &str) {
    try_drop_slot(conn, name).unwrap_or_else(|err| panic!("drop replication slot {name}: {err}"));
}

/// Drain every queued WAL change from the named slot as wal2json v2 JSON
/// strings, in commit order. The options match what `parse_wal2json_v2`
/// expects: `format-version=2`, `include-pk=true`, and `include-lsn=true` so
/// each change carries the LSN that `MessageV2` surfaces as its checkpoint.
pub fn drain_slot(conn: &mut PgConnection, name: &str) -> Vec<String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        data: String,
    }
    // Set-returning replication function, which the query DSL does not express.
    let rows: Vec<Row> = diesel::sql_query(format!(
        "SELECT data FROM pg_logical_slot_get_changes(\
            '{name}', NULL, NULL, \
            'format-version', '2', \
            'include-pk', 'true', \
            'include-lsn', 'true'\
        )"
    ))
    .load(conn)
    .expect("pg_logical_slot_get_changes");
    rows.into_iter().map(|r| r.data).collect()
}

/// Create a Postgres `PUBLICATION` over a single table. Required before
/// a pgoutput logical replication slot can stream from that table.
#[cfg(any(feature = "pg-streaming", feature = "pgoutput-emit"))]
pub fn create_publication(conn: &mut PgConnection, publication: &str, table: &str) {
    // DDL, which the query DSL does not express.
    diesel::sql_query(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .execute(conn)
    .expect("create publication");
}

/// Create a logical replication slot driven by the built-in `pgoutput`
/// plugin. Pair with [`create_publication`] before draining.
#[cfg(any(feature = "pg-streaming", feature = "pgoutput-emit"))]
pub fn create_pgoutput_slot(conn: &mut PgConnection, name: &str) {
    // Replication administration, which the query DSL does not express.
    diesel::sql_query(format!(
        "SELECT pg_create_logical_replication_slot('{name}', 'pgoutput')"
    ))
    .execute(conn)
    .expect("create pgoutput slot");
}

// MySQL

fn mysql_request(run: &str) -> ContainerRequest<GenericImage> {
    // MySQL 8.0 prints "ready for connections" twice during startup, for the
    // bootstrap temp server and the real one. "port: 3306" is only in the
    // final message.
    GenericImage::new("mysql", "8.0")
        .with_wait_for(WaitFor::message_on_stderr("port: 3306"))
        .with_exposed_port(3306.tcp())
        // In-memory datadir, as for Postgres above. InnoDB initialization is
        // the bulk of a cold MySQL boot and it is all writes.
        .with_mount(Mount::tmpfs_mount("/var/lib/mysql"))
        .with_env_var("MYSQL_ROOT_PASSWORD", PASSWORD)
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--max-connections=400",
        ])
        .with_network(network_name(run))
}

fn mysql_url_for(port: u16, database: &str) -> String {
    format!("mysql://root:{PASSWORD}@127.0.0.1:{port}/{database}")
}

fn mysql_admin(port: u16) -> ConnectionResult<MysqlConnection> {
    MysqlConnection::establish(&mysql_url_for(port, "mysql"))
}

/// One database on this run's shared MySQL, dropped when the handle drops.
/// Declare it before anything that connects to it.
pub struct MysqlDatabase {
    port: u16,
    name: String,
    run: String,
    maxwell_count: AtomicU32,
}

/// Acquire the shared MySQL and create a fresh database on it.
pub fn mysql_database() -> MysqlDatabase {
    let run = run_id();
    let port = shared_server(
        "mysql",
        || mysql_request(&run),
        3306,
        |port| mysql_admin(port).is_ok(),
        Duration::from_secs(120),
    );
    let name = fresh_db_name();
    let mut admin = mysql_admin(port).expect("MySQL admin connection");
    // DDL, which the query DSL does not express.
    diesel::sql_query(format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .expect("create test database");
    MysqlDatabase {
        port,
        name,
        run,
        maxwell_count: AtomicU32::new(0),
    }
}

impl MysqlDatabase {
    /// diesel URL of this database.
    pub fn url(&self) -> String {
        mysql_url_for(self.port, &self.name)
    }

    /// Establish a diesel [`MysqlConnection`] to this database.
    pub fn connect(&self) -> MysqlConnection {
        MysqlConnection::establish(&self.url()).expect("MySQL connection")
    }

    fn maxwell_schema(&self) -> String {
        format!("maxwell_{}", self.name)
    }
}

impl Drop for MysqlDatabase {
    fn drop(&mut self) {
        let Ok(mut admin) = mysql_admin(self.port) else {
            return;
        };
        for database in [self.name.clone(), self.maxwell_schema()] {
            // DDL, which the query DSL does not express.
            let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {database}"))
                .execute(&mut admin);
        }
    }
}

/// Start a Maxwell daemon replicating `db` alone from the shared MySQL and
/// writing CDC as JSONL into `output_dir` (bind-mounted at `/output`).
/// `output_dir` must be world-writable so the in-container Maxwell process
/// can write it.
pub fn start_maxwell(db: &MysqlDatabase, output_dir: &str) -> Container<GenericImage> {
    // Every Maxwell on the shared server is its own replication client with
    // its own state schema. Other tests' databases are blacklisted, not just
    // excluded: Maxwell halts on DDL it cannot parse, whichever database it is
    // in, and a blacklist is the one filter that skips schema tracking.
    let instance = db.maxwell_count.fetch_add(1, Ordering::Relaxed);
    let replica_server_id = 2 + std::process::id() * 16 + instance;
    GenericImage::new(MAXWELL_IMAGE, MAXWELL_TAG)
        .with_wait_for(WaitFor::message_on_stderr("Binlog connected"))
        .with_network(network_name(&db.run))
        .with_mount(Mount::bind_mount(output_dir, "/output"))
        .with_cmd([
            "bin/maxwell".to_string(),
            "--producer=file".to_string(),
            "--output_file=/output/maxwell.jsonl".to_string(),
            "--output_primary_key_columns=true".to_string(),
            format!("--host={}", mysql_host(&db.run)),
            "--port=3306".to_string(),
            "--user=root".to_string(),
            format!("--password={PASSWORD}"),
            format!("--client_id={}_{instance}", db.name),
            format!("--replica_server_id={replica_server_id}"),
            format!("--schema_database={}", db.maxwell_schema()),
            format!(
                "--filter=blacklist: /^(?!({0}|{1})$).*/.*, exclude: *.*, include: {0}.*",
                db.name,
                db.maxwell_schema()
            ),
        ])
        .with_startup_timeout(Duration::from_secs(90))
        .start()
        .unwrap_or_else(|e| panic!("start maxwell for {}: {e}", db.name))
}

/// Poll the Maxwell JSONL output for row-change lines on `db`.`table` until
/// at least `expected` have arrived, returning them in file (commit) order.
/// Panics after a fixed timeout.
pub fn maxwell_collect(
    output_dir: &str,
    db: &MysqlDatabase,
    table: &str,
    expected: usize,
) -> Vec<String> {
    let path = std::path::Path::new(output_dir).join("maxwell.jsonl");
    let database_tag = format!("\"database\":\"{}\"", db.name);
    let table_tag = format!("\"table\":\"{table}\"");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if path.exists() {
            let content = std::fs::read_to_string(&path).unwrap_or_default();
            let matching: Vec<String> = content
                .lines()
                .filter(|line| {
                    line.contains(&database_tag)
                        && line.contains(&table_tag)
                        && (line.contains("\"type\":\"insert\"")
                            || line.contains("\"type\":\"update\"")
                            || line.contains("\"type\":\"delete\""))
                })
                .map(String::from)
                .collect();
            if matching.len() >= expected {
                return matching;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} Maxwell rows on {table} at {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

// OpenFGA

/// Mapped gRPC port of this run's shared OpenFGA. Blocking: call it from
/// `spawn_blocking` inside an async test.
#[cfg(all(feature = "visibility-openfga", feature = "testing"))]
pub fn openfga_port() -> u16 {
    shared_server(
        "openfga",
        || {
            // The image does not declare its gRPC port. The log line precedes
            // the listener, so callers still wait for a call to succeed.
            GenericImage::new("openfga/openfga", "v1.8.13")
                .with_wait_for(WaitFor::message_on_stdout("starting openfga service"))
                .with_exposed_port(8081.tcp())
                .with_cmd(["run"])
        },
        8081,
        |port| std::net::TcpStream::connect(("127.0.0.1", port)).is_ok(),
        Duration::from_secs(60),
    )
}

// Parked reads, for the re-execution connector tests.

#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql",
))]
pub use parked_reads::*;

#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql",
))]
mod parked_reads {
    use super::{MysqlDatabase, PgDatabase};
    use diesel::prelude::*;
    use std::time::{Duration, Instant};

    /// Advisory-lock key the parked-read helper gates on. Advisory locks are
    /// scoped to the database, so the constant is safe on a shared server.
    const PARK_KEY: i64 = 4242;

    /// Cross join that parks a read until [`park_a_read`] releases its gate.
    ///
    /// Postgres fixes a statement's snapshot before it executes, so a read that
    /// blocks here is already holding the snapshot it will answer from, and a
    /// commit landing while it waits is invisible to it. Append it to the read's
    /// SQL, after any `FROM`.
    pub const PARK: &str = "CROSS JOIN pg_advisory_xact_lock(4242)";

    /// The current WAL position, read from an ordinary connection.
    pub fn current_wal_lsn(conn: &mut PgConnection) -> subql::PgLsn {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Text)]
            v: String,
        }
        // Administrative function, which the query DSL does not express.
        let rows: Vec<Row> = diesel::sql_query("SELECT pg_current_wal_lsn()::text AS v")
            .load(conn)
            .expect("read the current WAL position");
        subql::PgLsn::parse(&rows[0].v).expect("parse the current WAL position")
    }

    diesel::table! {
        pg_catalog.pg_locks (pid) {
            pid -> Nullable<Integer>,
            locktype -> Text,
            database -> Nullable<Oid>,
            granted -> Bool,
        }
    }

    diesel::table! {
        pg_catalog.pg_database (oid) {
            oid -> Oid,
            datname -> Text,
        }
    }

    /// Backends of `database` blocked on an advisory lock, which is what a
    /// parked read looks like from another connection.
    fn parked_count(conn: &mut PgConnection, database: &str) -> i64 {
        let oid: u32 = pg_database::table
            .filter(pg_database::datname.eq(database))
            .select(pg_database::oid)
            .first(conn)
            .expect("resolve the database oid");
        pg_locks::table
            .filter(pg_locks::locktype.eq("advisory"))
            .filter(pg_locks::granted.eq(false))
            .filter(pg_locks::database.eq(oid))
            .count()
            .get_result(conn)
            .expect("read pg_locks")
    }

    /// Run `read` on another thread, commit `dml` while it is parked, and report
    /// what it returned together with the position that commit landed at.
    ///
    /// The read's SQL must carry [`PARK`], which holds it inside its own snapshot
    /// until the commit is done. A position taken before the snapshot therefore
    /// sits behind the returned one, and a position taken after sits at or ahead
    /// of it, so the two orderings are told apart from outside the call.
    pub fn park_a_read<T, F>(db: &PgDatabase, dml: &str, read: F) -> (T, subql::PgLsn)
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let mut gate = db.connect();
        let mut observer = db.connect();
        // Advisory lock function, which the query DSL does not express.
        diesel::sql_query(format!("SELECT pg_advisory_lock({PARK_KEY})"))
            .execute(&mut gate)
            .expect("take the gate");

        let reader = std::thread::spawn(read);
        let deadline = Instant::now() + Duration::from_secs(30);
        while parked_count(&mut observer, &db.name) == 0 {
            assert!(
                Instant::now() < deadline,
                "the read never parked on the gate, so its snapshot was never pinned"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        diesel::sql_query(dml)
            .execute(&mut observer)
            .expect("commit while the read is parked");
        let after_commit = current_wal_lsn(&mut observer);
        diesel::sql_query(format!("SELECT pg_advisory_unlock({PARK_KEY})"))
            .execute(&mut gate)
            .expect("release the gate");

        (reader.join().expect("parked read"), after_commit)
    }

    /// The current binlog coordinate, read from an ordinary connection.
    pub fn current_binlog_pos(conn: &mut MysqlConnection) -> subql::MysqlBinlogPos {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Text)]
            file: String,
            #[diesel(sql_type = diesel::sql_types::Unsigned<diesel::sql_types::BigInt>)]
            pos: u64,
        }
        // JSON extraction from a performance schema table, which the query DSL
        // does not express.
        let rows: Vec<Row> = diesel::sql_query(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(LOCAL, '$.binary_log_file')) AS file, \
         CAST(JSON_EXTRACT(LOCAL, '$.binary_log_position') AS UNSIGNED) AS pos \
         FROM performance_schema.log_status",
        )
        .load(conn)
        .expect("read log_status");
        let file = rows[0]
            .file
            .rsplit('.')
            .next()
            .and_then(|s| s.parse().ok())
            .expect("binlog file suffix");
        subql::MysqlBinlogPos {
            file,
            pos: u32::try_from(rows[0].pos).expect("binlog offset fits u32"),
        }
    }

    /// MySQL peer of [`park_a_read`], gated on a named user-level lock.
    ///
    /// `lock` must be unique per call: the parked read acquires it and its
    /// session keeps it, so a reused name would park the next read on itself.
    pub fn park_a_mysql_read<T, F>(
        db: &MysqlDatabase,
        lock: &str,
        dml: &str,
        read: F,
    ) -> (T, subql::MysqlBinlogPos)
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // User locks are server wide, so the name carries the database.
        let lock = format!("{}_{lock}", db.name);
        let mut gate = db.connect();
        let mut observer = db.connect();
        // User lock function, which the query DSL does not express.
        diesel::sql_query(format!("SELECT GET_LOCK('{lock}', 60) AS n"))
            .execute(&mut gate)
            .expect("take the gate");

        let reader = std::thread::spawn(read);
        let deadline = Instant::now() + Duration::from_secs(30);
        while mysql_parked_count(&mut observer, &db.name) == 0 {
            assert!(
                Instant::now() < deadline,
                "the read never parked on the gate, so its snapshot was never pinned"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        diesel::sql_query(dml)
            .execute(&mut observer)
            .expect("commit while the read is parked");
        let after_commit = current_binlog_pos(&mut observer);
        diesel::sql_query(format!("SELECT RELEASE_LOCK('{lock}') AS n"))
            .execute(&mut gate)
            .expect("release the gate");

        (reader.join().expect("parked read"), after_commit)
    }

    diesel::table! {
        information_schema.processlist (id) {
            id -> Unsigned<BigInt>,
            db -> Nullable<Text>,
            state -> Nullable<Text>,
        }
    }

    /// Sessions of `database` blocked on a user-level lock.
    fn mysql_parked_count(conn: &mut MysqlConnection, database: &str) -> i64 {
        processlist::table
            .filter(processlist::state.eq("User lock"))
            .filter(processlist::db.eq(database))
            .count()
            .get_result(conn)
            .expect("read processlist")
    }
}
