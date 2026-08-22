//! Docker-backed integration test for [`AsyncAutoResolvingEngine`] +
//! [`PgAsyncDieselConnector`] against a real Postgres with logical
//! replication.
//!
//! Async twin of `tests/reexec_postgres.rs`. Drives the same production
//! path: an engine-supported subscription and a captured (MIN) subscription
//! coexist on one async engine, WAL events flow through wal2json, and the
//! `PgAsyncDieselConnector` re-executes the captured query against real
//! Postgres over a `diesel-async` pool, decoding a scalar
//! [`Value<Postgres>`](subql::backend::Value) and reading the WAL LSN in the
//! same transaction. The engine is driven on a multi-thread tokio runtime.
//!
//! Requires Docker. Tests are `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --test reexec_postgres_async \
//!     --features executor-diesel-async-postgres -- --ignored --nocapture
//! ```

#![cfg(feature = "executor-diesel-async-postgres")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::{sql_query, PgConnection, RunQueryDsl};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{
    AsyncAutoResolvingEngine, AsyncConnector, PgAsyncDieselConnector, ReExecEngine, Registered,
    SnapshotResult,
};
use subql::{parse_wal2json_v2, DefaultIds, MessageV2, SubscriptionEngine, SubscriptionRequest};

const SLOT: &str = "subql_test_async";
const DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";
const PG_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE PRECISION,
    quantity INT,
    status TEXT
)";

type Engine = AsyncAutoResolvingEngine<MessageV2, DefaultIds, ParserDB, PgAsyncDieselConnector>;

/// Build a `bb8` pool over `AsyncPgConnection` for the container at `port`.
async fn pg_async_pool(port: u16) -> Pool<AsyncPgConnection> {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(common::pg_url(port));
    Pool::builder()
        .build(manager)
        .await
        .expect("build async pg pool")
}

/// Same PG-side setup as the sync test: DDL, REPLICA IDENTITY FULL, seed
/// rows, then create the slot (so the seed WAL records never reach it).
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

fn build_engine(catalog: ParserDB, pool: Pool<AsyncPgConnection>) -> Engine {
    let inner =
        SubscriptionEngine::<MessageV2, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {});
    AsyncAutoResolvingEngine::new(ReExecEngine::new(inner), PgAsyncDieselConnector::new(pool))
}

fn parse_message(msg: &str) -> Vec<MessageV2> {
    parse_wal2json_v2(msg.as_bytes()).expect("wal2json parse")
}

/// Headline test: engine-supported and captured subscriptions coexist. An
/// INSERT matches the engine predicate only. A DELETE of the current MIN
/// spills a re-execution the async connector resolves against live PG,
/// returning the new MIN (9.0) as exactly one `ScalarUpdate`.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn engine_and_captured_paths_coexist_through_pg_async_connector() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    common::multi_thread_rt().block_on(async move {
        let pool = pg_async_pool(port).await;
        let mut engine = build_engine(catalog(), pool);

        let engine_consumer: u64 = 1;
        let engine_reg = engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(
                    engine_consumer,
                    "SELECT * FROM orders WHERE price > 8.0",
                ),
                (),
            )
            .expect("engine registration");
        assert!(matches!(engine_reg, Registered::Engine(_)));

        let captured_qid = match engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(
                    2u64,
                    "SELECT MIN(price) FROM orders",
                ),
                (),
            )
            .expect("captured registration")
        {
            Registered::ReExec { query_id, .. } => query_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(engine.install(captured_qid, Value::Float(5.0)));

        sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (3, 11.0, 1, 'paid')")
            .execute(&mut conn_dml)
            .expect("insert id=3");
        sql_query("DELETE FROM orders WHERE id = 1")
            .execute(&mut conn_dml)
            .expect("delete id=1");

        let msgs = common::drain_slot(&mut conn_setup, SLOT);
        assert!(
            !msgs.is_empty(),
            "expected at least one wal2json message after INSERT+DELETE"
        );

        let mut events: Vec<MessageV2> = Vec::new();
        for msg in &msgs {
            events.extend(parse_message(msg));
        }
        assert_eq!(
            events.len(),
            2,
            "expected exactly one INSERT and one DELETE event from wal2json (got {})",
            events.len()
        );

        let mut total_inserted = Vec::new();
        let mut total_scalar_updates = Vec::new();
        let mut total_triggers = 0usize;
        for event in &events {
            let notifs = engine.consumers(event).await.expect("consumers dispatch");
            total_inserted.extend(notifs.engine.inserted().iter().copied());
            total_scalar_updates.extend(notifs.scalar_updates);
            total_triggers += notifs.triggers.len();
        }

        assert!(
            total_inserted.contains(&engine_consumer),
            "consumer {engine_consumer} expected in `inserted`, got {total_inserted:?}"
        );
        assert_eq!(
            total_scalar_updates.len(),
            1,
            "expected exactly one ScalarUpdate, got {}",
            total_scalar_updates.len()
        );
        assert_eq!(total_scalar_updates[0].query_id, captured_qid);
        assert_eq!(total_scalar_updates[0].value, Value::Float(9.0));
        assert_eq!(total_triggers, 0, "auto-resolving engine drains triggers");
    });
}

/// `PgAsyncDieselConnector` snapshot reads value plus a non-zero `PgLsn`
/// inside one transaction, mirroring the sync snapshot test.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn snapshot_reads_value_and_lsn_from_pg_async() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    common::multi_thread_rt().block_on(async move {
        let pool = pg_async_pool(port).await;
        let mut engine = build_engine(catalog(), pool);

        let captured_qid = match engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(
                    1u64,
                    "SELECT MIN(price) FROM orders",
                ),
                (),
            )
            .expect("captured registration")
        {
            Registered::ReExec { query_id, .. } => query_id,
            other => panic!("expected ReExec, got {other:?}"),
        };

        let snap = engine
            .snapshot(captured_qid)
            .await
            .expect("snapshot")
            .expect("query_id exists");
        let (value, checkpoint) = match snap {
            SnapshotResult::Scalar(value, checkpoint) => (value, checkpoint),
            other => panic!("unexpected snapshot variant: {other:?}"),
        };
        assert_eq!(value, Value::Float(5.0), "MIN(price) snapshot value");

        let lsn = checkpoint.expect("PgAsyncDieselConnector must report a checkpoint");
        assert!(
            lsn > subql::PgLsn(0),
            "pg_current_wal_lsn() should be non-zero on a live server, got {lsn:?}"
        );
    });
}

/// The multi-column aggregate seed decodes correctly through the async PG
/// connector, mirroring the sync path. Integer column, so `SUM` promotes to
/// `bigint` and the double cast must still yield `f64`.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn execute_scalar_row_decodes_integer_aggregate_seed_async() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    sql_query("CREATE TABLE nums (id INT PRIMARY KEY, amount INT)")
        .execute(&mut setup)
        .expect("CREATE TABLE nums");
    for (id, amount) in [(1, 2), (2, 4), (3, 6)] {
        sql_query(format!(
            "INSERT INTO nums (id, amount) VALUES ({id}, {amount})"
        ))
        .execute(&mut setup)
        .expect("seed insert");
    }

    let db =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE nums (id INT PRIMARY KEY, amount INT);")
            .expect("parse nums DDL");
    let mut engine =
        SubscriptionEngine::<MessageV2, DefaultIds, ParserDB>::new(db, PostgreSqlDialect {});
    let bundle = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(
            1u64,
            "SELECT VAR_POP(amount) FROM nums",
        ))
        .expect("register aggregate")
        .aggregate_bootstrap
        .expect("aggregate carries a bootstrap");

    common::multi_thread_rt().block_on(async move {
        let pool = pg_async_pool(port).await;
        let connector = PgAsyncDieselConnector::new(pool);
        let (row, checkpoint) = connector
            .execute_scalar_row(&bundle.sql, &bundle.kinds, &())
            .await
            .expect("execute_scalar_row");
        assert_eq!(
            row,
            vec![Value::Float(12.0), Value::Float(56.0), Value::Int(3)]
        );
        assert!(checkpoint.is_some());
    });
}
