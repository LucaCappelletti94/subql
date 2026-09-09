//! Shared helpers for the Docker-backed integration tests.
//!
//! One server per engine per nextest run, looked up by name and label from
//! every test process, and one database per test on it. Requires Docker. The
//! Postgres image `subql-test/postgres-wal2json:16` is built from
//! `tests/fixtures/Dockerfile.postgres` on first use.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{ContainerRequest, GenericImage, ImageExt, ReuseDirective};

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

mod mysql;
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql",
))]
mod parked_reads;
mod pg;

pub use mysql::{maxwell_collect, mysql_database, start_maxwell};
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql",
))]
pub use parked_reads::{park_a_mysql_read, park_a_read, PARK};
#[cfg(feature = "pg-streaming")]
pub use pg::PgDatabase;
#[cfg(any(feature = "pg-streaming", feature = "pgoutput-emit"))]
pub use pg::{create_pgoutput_slot, create_publication};
pub use pg::{create_slot, drain_slot, drop_slot, pg_database};

/// Mapped gRPC port of this run's shared OpenFGA. Blocking: call it from
/// `spawn_blocking` inside an async test.
#[cfg(all(feature = "visibility-openfga", feature = "testing"))]
pub fn openfga_port() -> u16 {
    use testcontainers::core::WaitFor;
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
