//! The shared Postgres and one database per test on it.

use std::time::{Duration, Instant};

use diesel::prelude::*;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::{ContainerRequest, GenericImage, ImageExt};

use super::{ensure_image, fresh_db_name, shared_server, PASSWORD, PG_IMAGE, PG_TAG};

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
    pub(super) name: String,
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

/// The catalog DDL the three Postgres re-execution suites share.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
))]
pub const ORDERS_DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

/// The server-side DDL. `DOUBLE PRECISION` is the canonical 8-byte float, so
/// the `Nullable<Double>` row type in `DieselConnector` decodes cleanly.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
))]
pub const ORDERS_PG_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE PRECISION,
    quantity INT,
    status TEXT
)";

/// DDL, `REPLICA IDENTITY FULL`, seed rows, replication slot.
///
/// The seeds are inserted before the slot is created, so their WAL records
/// never reach it and a test observes only its own DML.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
))]
pub fn setup_orders(conn: &mut diesel::PgConnection, seed: &[(i64, f64)], slot: &str) {
    diesel::sql_query(ORDERS_PG_DDL)
        .execute(conn)
        .expect("CREATE TABLE");
    diesel::sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(conn)
        .expect("REPLICA IDENTITY FULL");
    for (id, price) in seed {
        diesel::sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) \
             VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(conn)
        .expect("seed insert");
    }
    super::create_slot(conn, slot);
}

/// One more row, for a commit that lands while a read is parked.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
))]
pub fn orders_insert(id: i64) -> String {
    format!("INSERT INTO orders (id, price, quantity, status) VALUES ({id}, 7.0, 1, 'paid')")
}

/// The in-process catalog the engine and the parser share.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
))]
pub fn orders_catalog() -> sql_traits::structs::ParserDB {
    sql_traits::structs::ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(ORDERS_DDL)
        .expect("parse DDL")
}
