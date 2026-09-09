//! Cross-database CDC parity test.
//!
//! Proves that identical DML applied to PostgreSQL and MySQL produces identical
//! dispatch results when parsed through wal2json v2 and Maxwell respectively.
//!
//! Requires Docker and system libraries: `libpq-dev`, `default-libmysqlclient-dev`.
//! Run with:
//! ```sh
//! cargo test --test it cdc_cross_db:: -- --ignored --nocapture
//! ```

use diesel::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;

use sql_traits::structs::ParserDB;

// Diesel schema + model

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

// IoT Catalog

/// The PostgreSQL catalog for the `readings` table.
///
/// Its default collation is deterministic, so text equality is byte
/// equality and subql answers it in process. MySQL needs its own
/// declaration, which is what [`iot_catalog_mysql`] is for.
///
/// One bare `readings` declaration serves both remaining callers:
/// - subscription registration (`SELECT * FROM readings WHERE ...`),
/// - wal2json (sends `schema="public"`, resolved as `public.readings`
///   which Postgres aliases to bare `readings`).
fn iot_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE readings (sensor_id INT PRIMARY KEY, temperature DOUBLE PRECISION, humidity DOUBLE PRECISION, location VARCHAR(100));",
    )
    .expect("iot fixture DDL parses")
}

/// The MySQL catalog, which has to name `location`'s collation.
///
/// MySQL 8's schema default is `utf8mb4_0900_ai_ci`, case- and
/// accent-insensitive, and subql refuses to answer equality under it in
/// process: measured here, registration returns
/// `CollationNotReproducible { column: 3, collation: None }` and routes
/// the subscription to a database read, which is the rule Phase C6
/// introduced. Byte comparison would have answered a question MySQL was
/// not asked. So the column declares `utf8mb4_bin`, under which equality
/// really is byte equality, and the parity this test is about becomes
/// meaningful rather than accidental.
fn iot_catalog_mysql() -> ParserDB {
    ParserDB::parse::<sqlparser::dialect::MySqlDialect>(
        "CREATE TABLE readings (sensor_id INT PRIMARY KEY, temperature DOUBLE, humidity DOUBLE, location VARCHAR(100) COLLATE utf8mb4_bin);",
    )
    .expect("iot fixture MySQL DDL parses")
}

mod dml_setup {
    use super::NewReading;
    use diesel::prelude::*;

    pub(super) fn setup_postgres(pg: &mut PgConnection, slot: &str) {
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

        crate::common::create_slot(pg, slot);
    }

    pub(super) fn setup_mysql(my: &mut MysqlConnection) {
        diesel::sql_query(
            "CREATE TABLE IF NOT EXISTS readings (
            sensor_id INT PRIMARY KEY,
            temperature DOUBLE,
            humidity DOUBLE,
            location VARCHAR(100) COLLATE utf8mb4_bin
        )",
        )
        .execute(my)
        .expect("MySQL CREATE TABLE");
    }

    // DML operations (applied to both DBs)

    pub(super) fn apply_dml(pg: &mut PgConnection, my: &mut MysqlConnection) {
        use super::readings::dsl;

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
}

mod engine_setup {
    use super::dml_setup::{apply_dml, setup_mysql, setup_postgres};
    use super::iot_catalog;
    use crate::common::{
        assert_docker_available, drain_slot, maxwell_collect, mysql_database, pg_database,
        start_maxwell,
    };
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
    use std::collections::BTreeSet;
    use subql::backend::{MySql, Postgres};
    use subql::{
        parse_maxwell, parse_wal2json_v2, DefaultIds, MaxwellEvent, MessageV2, SubscriptionEngine,
        SubscriptionRequest,
    };

    const SUBSCRIPTIONS: &[(u64, &str)] = &[
        (1, "SELECT * FROM readings WHERE temperature > 30"),
        (2, "SELECT * FROM readings WHERE location = 'warehouse-A'"),
        (
            3,
            "SELECT * FROM readings WHERE humidity < 40 AND temperature > 25",
        ),
        (4, "SELECT * FROM readings WHERE sensor_id = 1"),
    ];

    fn setup_pg_engine(catalog: ParserDB) -> SubscriptionEngine<MessageV2, DefaultIds, ParserDB> {
        let mut engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
        for (consumer_id, sql) in SUBSCRIPTIONS {
            engine
                .register(SubscriptionRequest::<DefaultIds, Postgres>::new(
                    *consumer_id,
                    *sql,
                ))
                .unwrap_or_else(|e| panic!("register PG subscription {consumer_id}: {e}"));
        }
        engine
    }

    fn setup_mysql_engine(
        catalog: ParserDB,
    ) -> SubscriptionEngine<MaxwellEvent, DefaultIds, ParserDB> {
        let mut engine = SubscriptionEngine::new(catalog, MySqlDialect {});
        for (consumer_id, sql) in SUBSCRIPTIONS {
            engine
                .register(SubscriptionRequest::<DefaultIds, MySql>::new(
                    *consumer_id,
                    *sql,
                ))
                .unwrap_or_else(|e| panic!("register MySQL subscription {consumer_id}: {e}"));
        }
        engine
    }

    // Dispatch and collect matched consumers

    fn dispatch_events<E>(
        engine: &mut SubscriptionEngine<E, DefaultIds, ParserDB>,
        parse: impl Fn(&[u8]) -> Result<Vec<E>, subql::WalParseError>,
        messages: &[String],
    ) -> Vec<BTreeSet<u64>>
    where
        E: subql::backend::CdcEvent,
        E::Backend: subql::compiler::literals::SqlLiteralParse,
    {
        let mut results = Vec::with_capacity(messages.len());

        for (i, msg) in messages.iter().enumerate() {
            let events = parse(msg.as_bytes())
                .unwrap_or_else(|e| panic!("Failed to parse message {i}: {e}"));

            for event in &events {
                let notifs = engine
                    .consumers(event)
                    .unwrap_or_else(|e| panic!("Dispatch failed for event {i}: {e}"));
                let consumers: BTreeSet<u64> = notifs
                    .inserted()
                    .iter()
                    .chain(notifs.deleted())
                    .chain(notifs.updated())
                    .copied()
                    .collect();
                results.push(consumers);
            }
        }

        results
    }

    // Main test

    #[test]
    #[ignore = "requires Docker; run with: cargo test --test it cdc_cross_db:: -- --ignored"]
    #[allow(clippy::print_stderr)]
    fn cross_db_cdc_parity() {
        assert_docker_available();

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

        let pg_db = pg_database();
        let my_db = mysql_database();

        let _maxwell_container = start_maxwell(&my_db, &maxwell_path);

        let mut pg = pg_db.connect();
        let mut my = my_db.connect();

        // DDL setup
        let slot = pg_db.slot("subql_test");
        setup_postgres(&mut pg, &slot);
        setup_mysql(&mut my);

        // Apply DML to both databases
        apply_dml(&mut pg, &mut my);

        // Capture CDC events
        let pg_messages = drain_slot(&mut pg, &slot);
        let mx_messages = maxwell_collect(&maxwell_path, &my_db, "readings", 4);

        // Set up engines, one per CDC source
        let mut pg_engine = setup_pg_engine(iot_catalog());
        let mut mx_engine = setup_mysql_engine(super::iot_catalog_mysql());

        // Dispatch and collect results
        let pg_results = dispatch_events(&mut pg_engine, parse_wal2json_v2, &pg_messages);
        let mx_results = dispatch_events(
            &mut mx_engine,
            |bytes| {
                parse_maxwell(bytes).map(|msgs| msgs.into_iter().map(MaxwellEvent::new).collect())
            },
            &mx_messages,
        );

        // Expected matched consumer IDs per event.
        //
        // Every subscription here is `SELECT *`, so an UPDATE that leaves a
        // subscription's predicate satisfied still changes the row image it holds.
        // The UPDATE below raises `temperature` on the row consumers 1, 2 and 4
        // match, so all three hear about it, exactly as they did for the INSERT of
        // that row. Consumer 3 wants `humidity < 40` and the row carries 45, so it
        // matches neither image and hears nothing.
        let expected: Vec<BTreeSet<u64>> = vec![
            BTreeSet::from([1, 2, 4]), // INSERT (1, 35, 45, 'warehouse-A'): temp>30, loc match, sensor match
            BTreeSet::from([3]),       // INSERT (2, 28, 35, 'warehouse-B'): hum<40 AND temp>25
            BTreeSet::from([1, 2, 4]), // UPDATE sensor_id=1 temp=40: still in all three views
            BTreeSet::from([3]),       // DELETE sensor_id=2: old row matches hum<40 AND temp>25
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
}
