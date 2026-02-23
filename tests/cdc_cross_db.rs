//! Cross-database CDC parity test.
//!
//! Proves that identical DML applied to PostgreSQL and MySQL produces identical
//! dispatch results when parsed through wal2json v2 and Maxwell respectively.
//!
//! Requires Docker and system libraries: `libpq-dev`, `default-libmysqlclient-dev`.
//! Run with:
//! ```sh
//! cargo test --test cdc_cross_db -- --ignored --nocapture
//! ```

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use diesel::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

use subql::{
    ColumnId, DefaultIds, MaxwellParser, SchemaCatalog, SubscriptionEngine, SubscriptionSpec,
    TableId, Wal2JsonV2Parser, WalParser,
};

// ============================================================================
// Diesel schema + model
// ============================================================================

diesel::table! {
    readings (sensor_id) {
        sensor_id -> Integer,
        temperature -> Double,
        humidity -> Double,
        location -> Varchar,
    }
}

#[derive(Insertable)]
#[diesel(table_name = readings)]
struct NewReading {
    sensor_id: i32,
    temperature: f64,
    humidity: f64,
    location: String,
}

// ============================================================================
// IoT Catalog
// ============================================================================

/// Schema catalog for the `readings` table across both PG and MySQL.
///
/// Resolves all name forms to the same `TableId(1)`:
/// - `"readings"` (bare name — tried first by `resolve_table`)
/// - `"public.readings"` (PG wal2json qualified form)
/// - `"testdb.readings"` (Maxwell `database.table` qualified form)
struct IoTCatalog {
    tables: HashMap<String, (TableId, usize)>,
    columns: HashMap<(TableId, String), ColumnId>,
    primary_keys: HashMap<TableId, Vec<ColumnId>>,
}

impl IoTCatalog {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert("readings".to_string(), (1, 4));
        tables.insert("public.readings".to_string(), (1, 4));
        tables.insert("testdb.readings".to_string(), (1, 4));

        let mut columns = HashMap::new();
        columns.insert((1, "sensor_id".to_string()), 0);
        columns.insert((1, "temperature".to_string()), 1);
        columns.insert((1, "humidity".to_string()), 2);
        columns.insert((1, "location".to_string()), 3);

        let mut primary_keys = HashMap::new();
        primary_keys.insert(1, vec![0]); // sensor_id is PK

        Self {
            tables,
            columns,
            primary_keys,
        }
    }
}

impl SchemaCatalog for IoTCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.tables.get(table_name).map(|(id, _)| *id)
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId> {
        self.columns
            .get(&(table_id, column_name.to_string()))
            .copied()
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.tables
            .values()
            .find(|(id, _)| *id == table_id)
            .map(|(_, arity)| *arity)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0)
    }

    fn primary_key_columns(&self, table_id: TableId) -> Option<&[ColumnId]> {
        self.primary_keys.get(&table_id).map(Vec::as_slice)
    }
}

// ============================================================================
// Container setup
// ============================================================================

const PG_IMAGE: &str = "subql-test/postgres-wal2json";
const PG_TAG: &str = "16";
const MAXWELL_IMAGE: &str = "zendesk/maxwell";
const MAXWELL_TAG: &str = "v1.44.0";

/// Build the custom Postgres image with wal2json (cached by Docker layer cache).
fn ensure_postgres_image() {
    let output = std::process::Command::new("docker")
        .args(["images", "-q", &format!("{PG_IMAGE}:{PG_TAG}")])
        .output()
        .expect("docker images");

    if !output.stdout.is_empty() {
        return; // Already built
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
        "Failed to build postgres-wal2json image"
    );
}

/// Start PostgreSQL with wal2json plugin.
fn start_postgres() -> testcontainers::Container<GenericImage> {
    ensure_postgres_image();

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
            "max_wal_senders=4",
            "-c",
            "max_replication_slots=4",
        ])
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .expect("start postgres")
}

/// Start MySQL 8.0 with binlog enabled.
///
/// Uses `with_container_name` so Maxwell can reach it as `mysql` on the shared network.
fn start_mysql(network: &str, container_name: &str) -> testcontainers::Container<GenericImage> {
    // Note: MySQL 8.0 prints "ready for connections" twice during startup
    // (once for temp server, once for real). We match "port: 3306" which
    // only appears in the final ready message.
    GenericImage::new("mysql", "8.0")
        .with_wait_for(WaitFor::message_on_stderr("port: 3306"))
        .with_exposed_port(3306.tcp())
        .with_env_var("MYSQL_ROOT_PASSWORD", "subql_test")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_env_var("MYSQL_USER", "subql_test")
        .with_env_var("MYSQL_PASSWORD", "subql_test")
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

/// Start Maxwell daemon, reading from MySQL on the shared network.
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
        "docker preflight failed: `docker info` exited with status {}.\nstdout: {}\nstderr: {}\n\
         Ensure Docker daemon is running and this user can access /var/run/docker.sock.",
        output.status,
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

// ============================================================================
// DDL + replication slot setup
// ============================================================================

fn setup_postgres(pg: &mut PgConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS readings (
            sensor_id INT PRIMARY KEY,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            location VARCHAR(100)
        )",
    )
    .execute(pg)
    .expect("PG CREATE TABLE");

    diesel::sql_query("ALTER TABLE readings REPLICA IDENTITY FULL")
        .execute(pg)
        .expect("PG REPLICA IDENTITY FULL");

    diesel::sql_query("SELECT pg_create_logical_replication_slot('subql_test', 'wal2json')")
        .execute(pg)
        .expect("PG create replication slot");
}

fn setup_mysql(my: &mut MysqlConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS readings (
            sensor_id INT PRIMARY KEY,
            temperature DOUBLE,
            humidity DOUBLE,
            location VARCHAR(100)
        )",
    )
    .execute(my)
    .expect("MySQL CREATE TABLE");
}

// ============================================================================
// DML operations (applied to both DBs)
// ============================================================================

fn apply_dml(pg: &mut PgConnection, my: &mut MysqlConnection) {
    use readings::dsl;

    // INSERT (1, 35.0, 45.0, 'warehouse-A')
    let r1 = NewReading {
        sensor_id: 1,
        temperature: 35.0,
        humidity: 45.0,
        location: "warehouse-A".into(),
    };
    diesel::insert_into(dsl::readings)
        .values(&r1)
        .execute(pg)
        .expect("PG insert 1");
    diesel::insert_into(dsl::readings)
        .values(&r1)
        .execute(my)
        .expect("MySQL insert 1");

    // INSERT (2, 28.0, 35.0, 'warehouse-B')
    let r2 = NewReading {
        sensor_id: 2,
        temperature: 28.0,
        humidity: 35.0,
        location: "warehouse-B".into(),
    };
    diesel::insert_into(dsl::readings)
        .values(&r2)
        .execute(pg)
        .expect("PG insert 2");
    diesel::insert_into(dsl::readings)
        .values(&r2)
        .execute(my)
        .expect("MySQL insert 2");

    // UPDATE readings SET temperature = 40.0 WHERE sensor_id = 1
    diesel::update(dsl::readings.filter(dsl::sensor_id.eq(1)))
        .set(dsl::temperature.eq(40.0))
        .execute(pg)
        .expect("PG update");
    diesel::update(dsl::readings.filter(dsl::sensor_id.eq(1)))
        .set(dsl::temperature.eq(40.0))
        .execute(my)
        .expect("MySQL update");

    // DELETE FROM readings WHERE sensor_id = 2
    diesel::delete(dsl::readings.filter(dsl::sensor_id.eq(2)))
        .execute(pg)
        .expect("PG delete");
    diesel::delete(dsl::readings.filter(dsl::sensor_id.eq(2)))
        .execute(my)
        .expect("MySQL delete");
}

// ============================================================================
// CDC capture: PostgreSQL via pg_logical_slot_get_changes
// ============================================================================

fn pg_read_changes(pg: &mut PgConnection) -> Vec<String> {
    #[derive(diesel::QueryableByName)]
    struct WalChange {
        #[diesel(sql_type = diesel::sql_types::Text)]
        data: String,
    }

    let changes: Vec<WalChange> = diesel::sql_query(
        "SELECT data FROM pg_logical_slot_get_changes(\
            'subql_test', NULL, NULL, \
            'format-version', '2', \
            'include-pk', 'true'\
        )",
    )
    .load(pg)
    .expect("pg_logical_slot_get_changes");

    changes.into_iter().map(|c| c.data).collect()
}

// ============================================================================
// CDC capture: MySQL via Maxwell file output
// ============================================================================

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
                    line.contains("\"table\":\"readings\"")
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

// ============================================================================
// Engine setup
// ============================================================================

fn setup_engine(
    catalog: &Arc<dyn SchemaCatalog>,
) -> SubscriptionEngine<PostgreSqlDialect, DefaultIds> {
    let mut engine = SubscriptionEngine::new(Arc::clone(catalog), PostgreSqlDialect {});

    let subscriptions = [
        (
            1_u64,
            1_u64,
            "SELECT * FROM readings WHERE temperature > 30",
        ),
        (
            2,
            2,
            "SELECT * FROM readings WHERE location = 'warehouse-A'",
        ),
        (
            3,
            3,
            "SELECT * FROM readings WHERE humidity < 40 AND temperature > 25",
        ),
        (4, 4, "SELECT * FROM readings WHERE sensor_id = 1"),
    ];

    for (sub_id, user_id, sql) in &subscriptions {
        engine
            .register(SubscriptionSpec {
                subscription_id: *sub_id,
                user_id: *user_id,
                session_id: None,
                sql: (*sql).to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap_or_else(|e| panic!("Failed to register subscription {sub_id}: {e}"));
    }

    engine
}

// ============================================================================
// Dispatch and collect matched users
// ============================================================================

fn dispatch_events(
    engine: &mut SubscriptionEngine<PostgreSqlDialect, DefaultIds>,
    parser: &dyn WalParser,
    messages: &[String],
    catalog: &dyn SchemaCatalog,
) -> Vec<BTreeSet<u64>> {
    let mut results = Vec::with_capacity(messages.len());

    for (i, msg) in messages.iter().enumerate() {
        let events = parser
            .parse_wal_message(msg.as_bytes(), catalog)
            .unwrap_or_else(|e| panic!("Failed to parse message {i}: {e}"));

        for event in &events {
            let users: BTreeSet<u64> = engine
                .users(event)
                .unwrap_or_else(|e| panic!("Dispatch failed for event {i}: {e}"))
                .collect();
            results.push(users);
        }
    }

    results
}

// ============================================================================
// Main test
// ============================================================================

#[test]
#[ignore = "requires Docker; run with: cargo test --test cdc_cross_db -- --ignored"]
#[allow(clippy::print_stderr)]
fn cross_db_cdc_parity() {
    assert_docker_available();

    // Unique names to avoid collisions with parallel test runs
    let pid = std::process::id();
    let network = format!("subql-test-{pid}");
    let mysql_name = format!("subql-mysql-{pid}");

    // Maxwell output directory (bind-mounted into the container).
    // Must be world-writable so the Maxwell process inside the container can write.
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

    // Start containers (PG is standalone; MySQL + Maxwell share a network)
    let pg_container = start_postgres();
    let mysql_container = start_mysql(&network, &mysql_name);

    // Give MySQL a moment before Maxwell connects
    std::thread::sleep(Duration::from_secs(2));
    let _maxwell_container = start_maxwell(&network, &mysql_name, &maxwell_path);

    // Get mapped host ports
    let pg_port = pg_container
        .get_host_port_ipv4(5432.tcp())
        .expect("pg port");
    let my_port = mysql_container
        .get_host_port_ipv4(3306.tcp())
        .expect("mysql port");

    // Connect via diesel
    let pg_url = format!("postgres://subql_test:subql_test@127.0.0.1:{pg_port}/testdb");
    let my_url = format!("mysql://subql_test:subql_test@127.0.0.1:{my_port}/testdb");

    let mut pg = PgConnection::establish(&pg_url).expect("PG connection");
    let mut my = MysqlConnection::establish(&my_url).expect("MySQL connection");

    // DDL setup
    setup_postgres(&mut pg);
    setup_mysql(&mut my);

    // Wait for Maxwell to catch up with DDL before DML
    std::thread::sleep(Duration::from_secs(3));

    // Apply DML to both databases
    apply_dml(&mut pg, &mut my);

    // Small delay for WAL/binlog propagation
    std::thread::sleep(Duration::from_secs(1));

    // Capture CDC events
    let pg_messages = pg_read_changes(&mut pg);
    let mx_messages = maxwell_read_changes(&maxwell_path, 4);

    // Set up engines — one per CDC source
    let catalog: Arc<dyn SchemaCatalog> = Arc::new(IoTCatalog::new());
    let mut pg_engine = setup_engine(&catalog);
    let mut mx_engine = setup_engine(&catalog);

    // Dispatch and collect results
    let pg_results = dispatch_events(&mut pg_engine, &Wal2JsonV2Parser, &pg_messages, &*catalog);
    let mx_results = dispatch_events(&mut mx_engine, &MaxwellParser, &mx_messages, &*catalog);

    // Expected matched user IDs per event.
    //
    // UPDATE optimization: only predicates depending on changed columns are
    // re-evaluated, so the UPDATE below (only temperature changed) only
    // matches user 1 (temperature > 30), not user 2 (location) or user 4
    // (sensor_id) even though the row still matches their predicates.
    let expected: Vec<BTreeSet<u64>> = vec![
        BTreeSet::from([1, 2, 4]), // INSERT (1, 35, 45, 'warehouse-A'): temp>30, loc match, sensor match
        BTreeSet::from([3]),       // INSERT (2, 28, 35, 'warehouse-B'): hum<40 AND temp>25
        BTreeSet::from([1]), // UPDATE sensor_id=1 temp=40: only temp changed → only temp>30 re-evaluated
        BTreeSet::from([3]), // DELETE sensor_id=2: old row matches hum<40 AND temp>25
    ];

    // Assert parity between PG and Maxwell results
    assert_eq!(
        pg_results.len(),
        mx_results.len(),
        "Event count mismatch: PG={}, Maxwell={}",
        pg_results.len(),
        mx_results.len()
    );
    assert_eq!(
        pg_results.len(),
        expected.len(),
        "Expected {} events, got {}",
        expected.len(),
        pg_results.len()
    );

    for (i, ((pg, mx), exp)) in pg_results
        .iter()
        .zip(&mx_results)
        .zip(&expected)
        .enumerate()
    {
        assert_eq!(
            pg, mx,
            "Event {i} parity failure: PG={pg:?}, Maxwell={mx:?}"
        );
        assert_eq!(pg, exp, "Event {i} expected {exp:?}, got PG={pg:?}");
    }
}
