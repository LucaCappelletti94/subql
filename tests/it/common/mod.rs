//! Shared helpers for integration tests that need a real Postgres with
//! logical replication enabled.
//!
//! Each test that wants them does `mod common;` at the top of its file. Cargo
//! recompiles this module per integration-test crate. That's the price of
//! integration-test isolation and is fine for a handful of helpers.
//!
//! Requires Docker. The `pg_with_wal2json` helper builds the custom
//! `subql-test/postgres-wal2json:16` image from `tests/fixtures/Dockerfile.postgres`
//! on first call (cached by Docker's layer cache for subsequent runs).
#![allow(dead_code)] // a given test may use only a subset of these helpers

use std::time::Duration;

use diesel::{Connection, MysqlConnection, PgConnection, RunQueryDsl};
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, GenericImage, ImageExt};

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

/// Build the custom Postgres image with wal2json. Returns immediately if the
/// image is already present in the local Docker cache.
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

/// Preflight Docker. Panics with an actionable message if the daemon is
/// unreachable.
pub fn assert_docker_available() {
    let output = std::process::Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .unwrap_or_else(|e| panic!("docker preflight: `docker info` failed to execute: {e}"));
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
pub fn multi_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build multi-thread tokio runtime")
}

/// Spin up a Postgres 16 container with the wal2json output plugin and
/// `wal_level=logical`, waiting until the server is accepting connections.
///
/// `output_plugin_libraries` is what admits an output plugin at all. PostgreSQL
/// 16.15 made it an allow-list, so without it a wal2json slot is refused with
/// "library wal2json may not be used as an output plugin" however correctly the
/// plugin is installed. The list is set rather than added to, so the two entries
/// it ships with have to be repeated or the built-in `pgoutput` stops loading too.
pub fn pg_with_wal2json() -> Container<GenericImage> {
    ensure_image();
    GenericImage::new(PG_IMAGE, PG_TAG)
        .with_wait_for(WaitFor::message_on_stderr("ready to accept connections"))
        .with_exposed_port(5432.tcp())
        // The data directory lives in container memory: initdb and every
        // fsync hit RAM, which is what a throwaway test server wants.
        .with_mount(Mount::tmpfs_mount("/var/lib/postgresql/data"))
        .with_env_var("POSTGRES_USER", "subql_test")
        .with_env_var("POSTGRES_PASSWORD", "subql_test")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=4",
            "-c",
            "max_replication_slots=4",
            "-c",
            "output_plugin_libraries=pgoutput,test_decoding,wal2json",
        ])
        // 180s: a full-width parallel sweep starts a dozen containers at
        // once, and a cold Postgres under that contention can miss a 60s
        // ceiling while being perfectly healthy.
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .expect("start postgres")
}

/// Same as [`pg_with_wal2json`] but with a short `wal_sender_timeout`
/// so the server tears down the replication connection if the client
/// doesn't send a `StandbyStatusUpdate` within the given window.
/// Used to prove the periodic-pump path keeps the source alive across
/// idle periods.
pub fn pg_with_wal2json_impatient(wal_sender_timeout: Duration) -> Container<GenericImage> {
    ensure_image();
    let timeout_arg = format!("wal_sender_timeout={}ms", wal_sender_timeout.as_millis());
    GenericImage::new(PG_IMAGE, PG_TAG)
        .with_wait_for(WaitFor::message_on_stderr("ready to accept connections"))
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::tmpfs_mount("/var/lib/postgresql/data"))
        .with_env_var("POSTGRES_USER", "subql_test")
        .with_env_var("POSTGRES_PASSWORD", "subql_test")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=4",
            "-c",
            "max_replication_slots=4",
            "-c",
            "output_plugin_libraries=pgoutput,test_decoding,wal2json",
            "-c",
            &timeout_arg,
        ])
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .expect("start postgres")
}

/// Build the libpq URL for a Postgres container at the given mapped port.
pub fn pg_url(port: u16) -> String {
    format!("postgres://subql_test:subql_test@127.0.0.1:{port}/testdb")
}

/// Build the libpq URL for a Postgres container. Alias for
/// [`pg_url`] kept around so the pg_streaming e2e is explicit about
/// the connection being used for replication. `PgStreamingCdcSource`
/// flips on logical-replication mode programmatically, so no extra
/// query param is needed in the URL.
pub fn pg_replication_url(port: u16) -> String {
    pg_url(port)
}

/// Establish a diesel [`PgConnection`] against the container at `port`.
pub fn pg_connect(port: u16) -> PgConnection {
    PgConnection::establish(&pg_url(port)).expect("PG connection")
}

/// Mapped host port for a started Postgres container.
pub fn pg_port(c: &Container<GenericImage>) -> u16 {
    c.get_host_port_ipv4(5432.tcp()).expect("pg port")
}

/// Create a logical replication slot driven by `wal2json`. Idempotent only
/// within a fresh container. Calling twice with the same name on the same
/// instance errors.
pub fn create_slot(conn: &mut PgConnection, name: &str) {
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
/// Raw SQL because `pg_drop_replication_slot` is a Postgres administrative
/// function with no typed diesel representation.
pub fn drop_slot(conn: &mut PgConnection, name: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        match diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(conn)
        {
            Ok(_) => return,
            Err(err) => {
                let racing_walsender = err.to_string().contains("is active");
                assert!(
                    racing_walsender && std::time::Instant::now() < deadline,
                    "drop replication slot {name}: {err}"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Drain every queued WAL change from the named slot as wal2json v2 JSON
/// strings. Returns `Vec<String>` in commit order. Empty if there is nothing
/// pending. The format options match what subql's `parse_wal2json_v2` expects:
/// `format-version=2`, `include-pk=true`, and `include-lsn=true` so each
/// change carries the LSN that `MessageV2` surfaces as its checkpoint.
pub fn drain_slot(conn: &mut PgConnection, name: &str) -> Vec<String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        data: String,
    }
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
pub fn create_publication(conn: &mut PgConnection, publication: &str, table: &str) {
    diesel::sql_query(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .execute(conn)
    .expect("create publication");
}

/// Create a logical replication slot driven by the built-in `pgoutput`
/// plugin. Pair with [`create_publication`] before draining.
pub fn create_pgoutput_slot(conn: &mut PgConnection, name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_create_logical_replication_slot('{name}', 'pgoutput')"
    ))
    .execute(conn)
    .expect("create pgoutput slot");
}

// ---------------------------------------------------------------------------
// MySQL and Maxwell helpers
// ---------------------------------------------------------------------------

/// Base MySQL 8.0 container request with binary logging enabled (ROW
/// format, FULL row images) and port 3306 exposed for host connections.
/// Binary logging is required both for Maxwell replication and for the
/// binlog coordinate `SHOW MASTER STATUS` reports.
///
/// MySQL 8.0 prints "ready for connections" twice during startup (once for
/// the bootstrap temp server, once for the real one). We wait for the
/// "port: 3306" line, which only appears in the final ready message.
fn mysql_request() -> ContainerRequest<GenericImage> {
    GenericImage::new("mysql", "8.0")
        .with_wait_for(WaitFor::message_on_stderr("port: 3306"))
        .with_exposed_port(3306.tcp())
        // In-memory datadir, as for Postgres above. InnoDB initialization is
        // the bulk of a cold MySQL boot and it is all writes.
        .with_mount(Mount::tmpfs_mount("/var/lib/mysql"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "subql_test")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
        ])
        .with_startup_timeout(Duration::from_secs(120))
}

/// Spin up a standalone MySQL 8.0 container. See [`mysql_request`] for the
/// binary-logging setup.
pub fn mysql_8() -> Container<GenericImage> {
    mysql_request().start().expect("start mysql")
}

/// Spin up a MySQL 8.0 container attached to `network` under
/// `container_name`, so a sibling container (Maxwell) can reach it by
/// name. The mapped 3306 port is still exposed for host connections.
pub fn mysql_networked(network: &str, container_name: &str) -> Container<GenericImage> {
    mysql_request()
        .with_network(network)
        .with_container_name(container_name)
        .start()
        .unwrap_or_else(|e| {
            panic!("start networked mysql network={network} name={container_name}: {e}")
        })
}

/// Build the diesel URL for a MySQL container at the given mapped port.
pub fn mysql_url(port: u16) -> String {
    format!("mysql://root:subql_test@127.0.0.1:{port}/testdb")
}

/// Establish a diesel [`MysqlConnection`] against the container at `port`.
pub fn mysql_connect(port: u16) -> MysqlConnection {
    MysqlConnection::establish(&mysql_url(port)).expect("MySQL connection")
}

/// Mapped host port for a started MySQL container.
pub fn mysql_port(c: &Container<GenericImage>) -> u16 {
    c.get_host_port_ipv4(3306.tcp()).expect("mysql port")
}

const MAXWELL_IMAGE: &str = "zendesk/maxwell";
const MAXWELL_TAG: &str = "v1.44.0";

/// Start a Maxwell daemon on `network`, replicating from the MySQL
/// container named `mysql_name` and writing CDC as JSONL into `output_dir`
/// (bind-mounted at `/output`). `output_dir` must be world-writable so the
/// in-container Maxwell process can write it.
pub fn start_maxwell(network: &str, mysql_name: &str, output_dir: &str) -> Container<GenericImage> {
    let host_flag = format!("--host={mysql_name}");
    GenericImage::new(MAXWELL_IMAGE, MAXWELL_TAG)
        .with_wait_for(WaitFor::message_on_stderr("Binlog connected"))
        .with_network(network)
        .with_mount(Mount::bind_mount(output_dir, "/output"))
        .with_cmd([
            "bin/maxwell",
            "--producer=file",
            "--output_file=/output/maxwell.jsonl",
            "--output_primary_key_columns=true",
            &host_flag,
            "--port=3306",
            "--user=root",
            "--password=subql_test",
        ])
        .with_startup_timeout(Duration::from_secs(90))
        .start()
        .unwrap_or_else(|e| panic!("start maxwell network={network} mysql={mysql_name}: {e}"))
}

/// Poll the Maxwell JSONL output for row-change lines on `table` until at
/// least `expected` have arrived, returning them in file (commit) order.
/// Panics after a fixed timeout.
pub fn maxwell_collect(output_dir: &str, table: &str, expected: usize) -> Vec<String> {
    let path = std::path::Path::new(output_dir).join("maxwell.jsonl");
    let table_tag = format!("\"table\":\"{table}\"");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if path.exists() {
            let content = std::fs::read_to_string(&path).unwrap_or_default();
            let matching: Vec<String> = content
                .lines()
                .filter(|line| {
                    line.contains(&table_tag)
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
            std::time::Instant::now() < deadline,
            "timed out waiting for {expected} Maxwell rows on {table} at {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Advisory-lock key the parked-read helper gates on.
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
    let rows: Vec<Row> = diesel::sql_query("SELECT pg_current_wal_lsn()::text AS v")
        .load(conn)
        .expect("read the current WAL position");
    subql::PgLsn::parse(&rows[0].v).expect("parse the current WAL position")
}

/// Backends blocked on an advisory lock, which is what a parked read looks
/// like from another connection.
fn parked_count(conn: &mut PgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
    )
    .load(conn)
    .expect("read pg_locks");
    rows[0].n
}

/// Run `read` on another thread, commit `dml` while it is parked, and report
/// what it returned together with the position that commit landed at.
///
/// The read's SQL must carry [`PARK`], which holds it inside its own snapshot
/// until the commit is done. A position taken before the snapshot therefore
/// sits behind the returned one, and a position taken after sits at or ahead
/// of it, so the two orderings are told apart from outside the call.
pub fn park_a_read<T, F>(port: u16, dml: &str, read: F) -> (T, subql::PgLsn)
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let mut gate = pg_connect(port);
    let mut observer = pg_connect(port);
    diesel::sql_query(format!("SELECT pg_advisory_lock({PARK_KEY})"))
        .execute(&mut gate)
        .expect("take the gate");

    let reader = std::thread::spawn(read);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while parked_count(&mut observer) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
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
    port: u16,
    lock: &str,
    dml: &str,
    read: F,
) -> (T, subql::MysqlBinlogPos)
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let mut gate = mysql_connect(port);
    let mut observer = mysql_connect(port);
    diesel::sql_query(format!("SELECT GET_LOCK('{lock}', 60) AS n"))
        .execute(&mut gate)
        .expect("take the gate");

    let reader = std::thread::spawn(read);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while mysql_parked_count(&mut observer) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
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

/// Sessions blocked on a user-level lock.
fn mysql_parked_count(conn: &mut MysqlConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT count(*) AS n FROM information_schema.processlist WHERE state = 'User lock'",
    )
    .load(conn)
    .expect("read processlist");
    rows[0].n
}
