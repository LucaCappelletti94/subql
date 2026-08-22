//! Docker-backed integration test for [`AsyncAutoResolvingEngine`] +
//! [`MysqlAsyncDieselConnector`] against a real MySQL 8.0 with binary
//! logging.
//!
//! Async twin of `tests/reexec_mysql.rs`. MySQL has no in-tree CDC source,
//! so the displacement test hand-builds the event via [`TestEvent<MySql>`]
//! instead of decoding one from a replication stream. The point is the
//! same: prove the async connector re-executes the captured query's SQL
//! against live MySQL over a `diesel-async` pool and decodes a scalar
//! [`Value`] correctly, reading the binlog coordinate in the same
//! transaction. The engine is driven on a multi-thread tokio runtime.
//!
//! Requires Docker. Tests are `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --test reexec_mysql_async \
//!     --features executor-diesel-async-mysql -- --ignored --nocapture
//! ```

#![cfg(feature = "executor-diesel-async-mysql")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::{sql_query, MysqlConnection, RunQueryDsl};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncMysqlConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, Value};
use subql::reexec::{
    AsyncAutoResolvingEngine, AsyncConnector, MysqlAsyncDieselConnector, ReExecEngine, Registered,
    SnapshotResult,
};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

type Engine =
    AsyncAutoResolvingEngine<TestEvent<MySql>, DefaultIds, ParserDB, MysqlAsyncDieselConnector>;

const DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";
const MYSQL_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE,
    quantity INT,
    status TEXT
)";

/// Build a `bb8` pool over `AsyncMysqlConnection` for the container at `port`.
async fn mysql_async_pool(port: u16) -> Pool<AsyncMysqlConnection> {
    let manager =
        AsyncDieselConnectionManager::<AsyncMysqlConnection>::new(common::mysql_url(port));
    Pool::builder()
        .build(manager)
        .await
        .expect("build async mysql pool")
}

fn setup_mysql(conn: &mut MysqlConnection, seed: &[(i64, f64)]) {
    sql_query(MYSQL_DDL).execute(conn).expect("CREATE TABLE");
    for (id, price) in seed {
        sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) \
             VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(conn)
        .expect("seed insert");
    }
}

fn catalog() -> ParserDB {
    ParserDB::parse::<MySqlDialect>(DDL).expect("parse DDL")
}

fn build_engine(catalog: ParserDB, pool: Pool<AsyncMysqlConnection>) -> Engine {
    let inner =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, ParserDB>::new(catalog, MySqlDialect {});
    AsyncAutoResolvingEngine::new(
        ReExecEngine::new(inner),
        MysqlAsyncDieselConnector::new(pool),
    )
}

/// Column order matches the catalog: id=0, price=1, quantity=2, status=3.
fn orders_row(id: i64, price: f64) -> Vec<Value<MySql>> {
    vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(1),
        Value::String("paid".into()),
    ]
}

/// Snapshot reads value plus a binlog coordinate through the async
/// connector, mirroring the sync snapshot test.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn snapshot_reads_value_and_binlog_pos_from_mysql_async() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    common::multi_thread_rt().block_on(async move {
        let pool = mysql_async_pool(port).await;
        let mut engine = build_engine(catalog(), pool);

        let captured_qid = match engine
            .register(
                SubscriptionRequest::<DefaultIds, MySql>::new(
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

        let pos = checkpoint.expect("MysqlAsyncDieselConnector must report a binlog checkpoint");
        assert!(
            pos.pos > 0,
            "log_status byte offset should be non-zero on a live server, got {pos:?}"
        );
    });
}

/// A DELETE that displaces the current MIN spills a re-execution the async
/// connector resolves against live MySQL, returning the new MIN (9.0).
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn delete_displacing_extreme_resolves_via_mysql_async_connector() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let mut conn_dml = common::mysql_connect(port);
    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let cat = catalog();
    let table_id: TableId = catalog_helpers::table_id(&cat, "orders").expect("resolve orders");

    common::multi_thread_rt().block_on(async move {
        let pool = mysql_async_pool(port).await;
        let mut engine = build_engine(cat, pool);

        let captured_qid = match engine
            .register(
                SubscriptionRequest::<DefaultIds, MySql>::new(
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
        assert!(engine.install(captured_qid, Value::Float(5.0)));

        sql_query("DELETE FROM orders WHERE id = 1")
            .execute(&mut conn_dml)
            .expect("delete id=1");

        let event =
            TestEvent::<MySql>::delete(table_id, orders_row(1, 5.0)).with_pk_columns([0u16]);

        let notifs = engine.consumers(&event).await.expect("consumers dispatch");
        assert_eq!(
            notifs.scalar_updates.len(),
            1,
            "expected exactly one ScalarUpdate, got {}",
            notifs.scalar_updates.len()
        );
        assert_eq!(notifs.scalar_updates[0].query_id, captured_qid);
        assert_eq!(
            notifs.scalar_updates[0].value,
            Value::Float(9.0),
            "connector re-queried live MySQL for the new MIN"
        );
        assert!(
            notifs.triggers.is_empty(),
            "auto-resolving engine drains triggers"
        );
    });
}

/// The multi-column aggregate seed decodes correctly through the async
/// MySQL connector. MySQL promotes `SUM(int)` to `DECIMAL`; the double cast
/// must still yield `f64`.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn execute_scalar_row_decodes_integer_aggregate_seed_async() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut setup = common::mysql_connect(port);
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

    let db = ParserDB::parse::<MySqlDialect>("CREATE TABLE nums (id INT PRIMARY KEY, amount INT);")
        .expect("parse nums DDL");
    let mut engine =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, ParserDB>::new(db, MySqlDialect {});
    let bundle = engine
        .register(SubscriptionRequest::<DefaultIds, MySql>::new(
            1u64,
            "SELECT VAR_POP(amount) FROM nums",
        ))
        .expect("register aggregate")
        .aggregate_bootstrap
        .expect("aggregate carries a bootstrap");

    common::multi_thread_rt().block_on(async move {
        let pool = mysql_async_pool(port).await;
        let connector = MysqlAsyncDieselConnector::new(pool);
        let (row, _checkpoint) = connector
            .execute_scalar_row(&bundle.sql, &bundle.kinds, &())
            .await
            .expect("execute_scalar_row");
        assert_eq!(
            row,
            vec![Value::Float(12.0), Value::Float(56.0), Value::Int(3)]
        );
    });
}
