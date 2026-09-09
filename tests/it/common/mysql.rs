//! The shared MySQL, one database per test on it, and a Maxwell per test.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use diesel::prelude::*;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, GenericImage, ImageExt};

use super::{fresh_db_name, network_name, run_id, shared_server, PASSWORD};

const MAXWELL_IMAGE: &str = "zendesk/maxwell";
const MAXWELL_TAG: &str = "v1.44.0";

fn mysql_host(run: &str) -> String {
    format!("subql-mysql-{run}")
}

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
    pub(super) name: String,
    run: String,
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
    MysqlDatabase { port, name, run }
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
    static MAXWELL_COUNTER: AtomicU32 = AtomicU32::new(0);
    // Unique across the server: the process is unique within a run and the
    // counter within the process. Folded into u32 for MySQL, never 0 or 1.
    let instance = MAXWELL_COUNTER.fetch_add(1, Ordering::Relaxed);
    let raw = u64::from(std::process::id()) * 1024 + u64::from(instance);
    let replica_server_id = 2 + u32::try_from(raw % u64::from(u32::MAX - 2)).expect("folded");
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
