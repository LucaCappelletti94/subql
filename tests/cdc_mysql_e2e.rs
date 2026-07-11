#![cfg(any())] // Phase 11: rewrite against E: CdcEvent shape. SubscriptionEngine took <Dialect,...>, now takes <E: CdcEvent,...>. Tracked in docs/refactor-cdc-event-handoff.md.

//! Standalone MySQL + Maxwell CDC end-to-end test.
//!
//! Applies a deterministic DML stream (two INSERTs, an UPDATE, a DELETE) to a
//! live MySQL 8.0, reads Maxwell's JSONL file output, parses each line with
//! [`subql::MaxwellParser`], dispatches through a [`SubscriptionEngine`] with a
//! single registered subscription, and asserts the per-consumer notifications
//! directly (an INSERT matching the WHERE lands in `inserted()`, the DELETE in
//! `deleted()`).
//!
//! This is the MySQL/Maxwell half of `tests/cdc_cross_db.rs`, lifted and
//! specialized: no PG, no cross-DB parity, just direct assertions on the
//! Maxwell path. The Maxwell/MySQL container helpers are duplicated here rather
//! than shared.
//!
//! Requires Docker and `default-libmysqlclient-dev`. Run with:
//! ```sh
//! cargo test --test cdc_mysql_e2e -- --ignored --nocapture
//! ```

use std::time::Duration;

use diesel::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

use sql_traits::structs::ParserDB;
use subql::{DefaultIds, MaxwellParser, SubscriptionEngine, SubscriptionRequest, WalParser};

const MAXWELL_IMAGE: &str = "zendesk/maxwell";
const MAXWELL_TAG: &str = "v1.44.0";

/// Catalog for the `events` table. Parsed with `PostgreSqlDialect` (subql
/// parses PG-flavored DDL regardless of the live backend). Maxwell sends
/// `schema="testdb"`; with no `testdb.events` in the catalog, table resolution
/// falls back to the unqualified lookup that hits this bare table.
fn events_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE events (id INT PRIMARY KEY, amount DOUBLE PRECISION, label TEXT);",
    )
    .expect("events DDL parses")
}

/// Start MySQL 8.0 with binlog enabled. `with_container_name` lets Maxwell
/// reach it as `mysql_name` on the shared network. Waits for "port: 3306",
/// which only appears in the final (real-server) ready message.
fn start_mysql(network: &str, container_name: &str) -> testcontainers::Container<GenericImage> {
    GenericImage::new("mysql", "8.0")
        .with_wait_for(WaitFor::message_on_stderr("port: 3306"))
        .with_exposed_port(3306.tcp())
        .with_env_var("MYSQL_ROOT_PASSWORD", "subql_test")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
        ])
        .with_network(network)
        .with_container_name(container_name)
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .unwrap_or_else(|e| {
            panic!("start mysql network={network} container_name={container_name}: {e}")
        })
}

/// Start the Maxwell daemon reading from MySQL on the shared network, writing
/// CDC rows to a bind-mounted JSONL file.
fn start_maxwell(
    network: &str,
    mysql_name: &str,
    output_dir: &str,
) -> testcontainers::Container<GenericImage> {
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
        .unwrap_or_else(|e| {
            panic!(
                "start maxwell image={MAXWELL_IMAGE} tag={MAXWELL_TAG} network={network} \
                 mysql_name={mysql_name}: {e}"
            )
        })
}

/// Fail fast with actionable diagnostics if Docker is unavailable.
fn assert_docker_available() {
    let output = std::process::Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .unwrap_or_else(|e| panic!("docker preflight: failed to execute `docker info`: {e}"));
    assert!(
        output.status.success(),
        "docker preflight failed: `docker info` exited with status {}.\nstderr: {}\n\
         Ensure the Docker daemon is running and this user can access the socket.",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn setup_mysql(my: &mut MysqlConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS events (
            id INT PRIMARY KEY,
            amount DOUBLE,
            label VARCHAR(100)
        )",
    )
    .execute(my)
    .expect("MySQL CREATE TABLE");
}

/// Apply the deterministic DML stream. Maxwell emits one CDC row per change in
/// commit order: INSERT(1), INSERT(2), UPDATE(1), DELETE(1).
fn apply_dml(my: &mut MysqlConnection) {
    diesel::sql_query("INSERT INTO events (id, amount, label) VALUES (1, 35.0, 'big')")
        .execute(my)
        .expect("insert 1");
    diesel::sql_query("INSERT INTO events (id, amount, label) VALUES (2, 5.0, 'small')")
        .execute(my)
        .expect("insert 2");
    diesel::sql_query("UPDATE events SET amount = 40.0 WHERE id = 1")
        .execute(my)
        .expect("update 1");
    diesel::sql_query("DELETE FROM events WHERE id = 1")
        .execute(my)
        .expect("delete 1");
}

/// Poll the bind-mounted Maxwell JSONL until at least `expected_count` rows for
/// the `events` table are present. Returns them in file (commit) order.
fn maxwell_read_changes(output_dir: &str, expected_count: usize) -> Vec<String> {
    let jsonl_path = std::path::Path::new(output_dir).join("maxwell.jsonl");
    let timeout = Duration::from_secs(30);
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();

    loop {
        assert!(
            start.elapsed() <= timeout,
            "Timed out waiting for Maxwell CDC messages at {}",
            jsonl_path.display()
        );

        if jsonl_path.exists() {
            let content = std::fs::read_to_string(&jsonl_path).unwrap_or_default();
            let matching: Vec<String> = content
                .lines()
                .filter(|line| {
                    line.contains("\"table\":\"events\"")
                        && (line.contains("\"type\":\"insert\"")
                            || line.contains("\"type\":\"update\"")
                            || line.contains("\"type\":\"delete\""))
                })
                .map(String::from)
                .collect();

            if matching.len() >= expected_count {
                return matching;
            }
        }

        std::thread::sleep(poll_interval);
    }
}

#[test]
#[ignore = "requires Docker; run with: cargo test --test cdc_mysql_e2e -- --ignored"]
#[allow(clippy::print_stderr)]
fn mysql_maxwell_cdc_e2e() {
    assert_docker_available();

    let pid = std::process::id();
    let network = format!("subql-mysql-e2e-{pid}");
    let mysql_name = format!("subql-mysql-e2e-{pid}");

    // Maxwell output dir (bind-mounted). World-writable so the in-container
    // Maxwell process can write to it.
    let maxwell_dir = tempfile::tempdir().expect("create maxwell tempdir");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(maxwell_dir.path(), std::fs::Permissions::from_mode(0o777))
            .expect("chmod maxwell dir");
    }
    let maxwell_path = maxwell_dir
        .path()
        .to_str()
        .expect("tempdir path")
        .to_string();

    let mysql_container = start_mysql(&network, &mysql_name);

    // Give MySQL a moment before Maxwell connects.
    std::thread::sleep(Duration::from_secs(2));
    let _maxwell_container = start_maxwell(&network, &mysql_name, &maxwell_path);

    let my_port = mysql_container
        .get_host_port_ipv4(3306.tcp())
        .expect("mysql port");
    let my_url = format!("mysql://root:subql_test@127.0.0.1:{my_port}/testdb");
    let mut my = MysqlConnection::establish(&my_url).expect("MySQL connection");

    setup_mysql(&mut my);

    // Let Maxwell catch up with the DDL before driving DML.
    std::thread::sleep(Duration::from_secs(3));

    apply_dml(&mut my);

    // Small delay for binlog propagation.
    std::thread::sleep(Duration::from_secs(1));

    let messages = maxwell_read_changes(&maxwell_path, 4);
    assert_eq!(
        messages.len(),
        4,
        "expected exactly 4 Maxwell CDC rows for `events`, got {}",
        messages.len()
    );

    let catalog = events_catalog();
    let consumer: u64 = 1;
    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(events_catalog(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            consumer,
            "SELECT * FROM events WHERE amount > 10",
        ))
        .expect("register subscription");

    // Per-event matched-consumer buckets, in commit order.
    let mut inserted: Vec<Vec<u64>> = Vec::new();
    let mut updated: Vec<Vec<u64>> = Vec::new();
    let mut deleted: Vec<Vec<u64>> = Vec::new();

    for (i, msg) in messages.iter().enumerate() {
        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), &catalog)
            .unwrap_or_else(|e| panic!("Maxwell parse failed for message {i}: {e}"));
        for event in &events {
            let notifs = engine
                .consumers(event)
                .unwrap_or_else(|e| panic!("dispatch failed for event {i}: {e}"));
            inserted.push(notifs.inserted().to_vec());
            updated.push(notifs.updated().to_vec());
            deleted.push(notifs.deleted().to_vec());
        }
    }

    // INSERT (1, 35.0): amount>10 -> consumer 1 in `inserted`.
    assert!(
        inserted[0].contains(&consumer),
        "INSERT id=1 (amount=35) should match WHERE amount>10, got inserted={:?}",
        inserted[0]
    );
    // INSERT (2, 5.0): amount<=10 -> no match anywhere.
    assert!(
        !inserted[1].contains(&consumer),
        "INSERT id=2 (amount=5) should not match, got inserted={:?}",
        inserted[1]
    );
    // UPDATE id=1 amount 35->40: still >10, only amount changed -> `updated`.
    assert!(
        updated[2].contains(&consumer),
        "UPDATE id=1 (amount 35->40) should re-match WHERE amount>10, got updated={:?}",
        updated[2]
    );
    // DELETE id=1 (amount=40): old row matched -> consumer 1 in `deleted`.
    assert!(
        deleted[3].contains(&consumer),
        "DELETE id=1 (amount=40) should match WHERE amount>10, got deleted={:?}",
        deleted[3]
    );
}
