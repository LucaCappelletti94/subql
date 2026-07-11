//! Docker-backed integration test for [`PgR2D2DieselConnector`].
//!
//! Exercises the pool-backed PG connector: a captured `MIN(price)`
//! subscription is registered, snapshot bootstraps via the pool, then a
//! DELETE-of-the-extreme drives a re-execution that round-trips through
//! the pool. Asserts the snapshot carries a real `PgLsn` and that the
//! ScalarUpdate emitted by re-execution carries the originating event's
//! LSN.
//!
//! Gated by `[[test]] required-features = ["executor-diesel-postgres-r2d2"]`.
//! Run with:
//!
//! ```sh
//! cargo test --test reexec_postgres_r2d2 \
//!     --features executor-diesel-postgres-r2d2 \
//!     -- --ignored --nocapture
//! ```
#![cfg(feature = "executor-diesel-postgres-r2d2")]
#![allow(clippy::unwrap_used)]

mod common;
use std::time::Duration;

use diesel::r2d2::ConnectionManager;
use diesel::{sql_query, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::reexec::{
    AutoResolvingEngine, PgR2D2DieselConnector, ReExecEngine, Registered, SnapshotResult,
};
use subql::backend::{CdcEvent, Postgres, Value};
use subql::{
    DefaultIds, SubscriptionEngine, SubscriptionRequest, Wal2JsonV2Event, Wal2JsonV2Parser,
    WalParser,
};

const SLOT: &str = "subql_test";
const DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

const PG_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE PRECISION,
    quantity INT,
    status TEXT
)";

fn setup_pg(conn: &mut PgConnection, seed: &[(i64, f64)]) {
    sql_query(PG_DDL).execute(conn).expect("CREATE TABLE");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(conn)
        .expect("REPLICA IDENTITY FULL");
    for (id, price) in seed {
        sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) \
             VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(conn)
        .expect("seed insert");
    }
    common::create_slot(conn, SLOT);
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn build_pool(port: u16) -> r2d2::Pool<ConnectionManager<PgConnection>> {
    let url = common::pg_url(port);
    let manager = ConnectionManager::<PgConnection>::new(url);
    r2d2::Pool::builder()
        .max_size(4)
        .connection_timeout(Duration::from_secs(10))
        .build(manager)
        .expect("build r2d2 pool")
}

fn build_engine(
    catalog: ParserDB,
    pool: r2d2::Pool<ConnectionManager<PgConnection>>,
) -> AutoResolvingEngine<Wal2JsonV2Event, DefaultIds, ParserDB, PgR2D2DieselConnector> {
    let inner = SubscriptionEngine::<Wal2JsonV2Event, DefaultIds, ParserDB>::new(
        catalog,
        PostgreSqlDialect {},
    );
    AutoResolvingEngine::new(ReExecEngine::new(inner), PgR2D2DieselConnector::new(pool))
}

fn parse_message(
    parser: &Wal2JsonV2Parser,
    catalog: &ParserDB,
    msg: &str,
) -> Vec<Wal2JsonV2Event> {
    parser
        .parse_wal_message(msg.as_bytes(), catalog)
        .expect("wal2json parse")
}

/// Snapshot through the pool returns the right value + an LSN tied to
/// the read transaction. Then a DELETE driven through real PG flows
/// back as a re-execution that goes through the pool.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn r2d2_pool_drives_snapshot_and_reexec() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let pool = build_pool(port);
    let mut engine = build_engine(catalog(), pool);

    let captured_qid = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered::ReExec { query_id, .. } => query_id,
        Registered::Engine(_) => panic!("expected ReExec"),
    };

    // Snapshot: must come back with value=5.0 and a non-zero LSN.
    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("query_id exists");
    let (value, snapshot_lsn) = match snap {
        SnapshotResult::Scalar(value, checkpoint) => (value, checkpoint),
        other => panic!("unexpected variant: {other:?}"),
    };
    assert_eq!(value, Value::Float(5.0));
    let snapshot_lsn = snapshot_lsn.expect("PgR2D2DieselConnector must report a checkpoint");
    assert!(
        snapshot_lsn > subql::PgLsn(0),
        "pg_current_wal_lsn() must be non-zero on a live server"
    );

    // Delete the current MIN row and drive a re-execution through the
    // pool. The new MIN is 9.0.
    sql_query("DELETE FROM orders WHERE id = 1")
        .execute(&mut conn_dml)
        .expect("delete id=1");

    let msgs = common::drain_slot(&mut conn_setup, SLOT);
    let parser = Wal2JsonV2Parser;
    let mut events: Vec<Wal2JsonV2Event> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(&parser, &catalog(), msg));
    }
    assert_eq!(events.len(), 1, "expected one DELETE event");

    let notifs = engine.consumers(&events[0]).expect("consumers dispatch");
    assert_eq!(notifs.scalar_updates.len(), 1);
    assert_eq!(notifs.scalar_updates[0].value, Value::Float(9.0));
    // The re-execution LSN should be at or after the snapshot LSN
    // (clock-monotonic). Use the event's checkpoint, propagated into
    // the ScalarUpdate.
    let event_lsn = events[0].checkpoint().copied().expect("wal2json event LSN");
    assert!(
        event_lsn >= snapshot_lsn,
        "post-snapshot WAL event LSN must be >= snapshot LSN"
    );
    assert_eq!(notifs.scalar_updates[0].checkpoint, Some(event_lsn));
}

/// `PgR2D2DieselConnector: Send + Sync` so it can move across async
/// tasks. Compile-time check.
#[test]
fn r2d2_connector_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PgR2D2DieselConnector>();
}
