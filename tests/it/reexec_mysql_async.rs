//! Docker-backed integration test for [`AutoResolvingEngine`] with [`AsyncMode`] +
//! [`MysqlAsyncDieselConnector`] against a real MySQL 8.0 with binary
//! logging.
//!
//! Async twin of `tests/it/reexec_mysql.rs`. MySQL has no in-tree CDC source,
//! so the displacement test hand-builds the event via [`TestEvent<MySql>`]
//! instead of decoding one from a replication stream. The point is the
//! same: prove the async connector re-executes the captured query's SQL
//! against live MySQL over a `diesel-async` pool and decodes a scalar
//! [`Value`] correctly, reading the binlog coordinate just before the read's
//! transaction. The engine is driven on a multi-thread tokio runtime.
//!
//! Requires Docker. Tests are `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --test it reexec_mysql_async:: \
//!     --features executor-diesel-async-mysql -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use std::sync::Arc;

use diesel::{sql_query, MysqlConnection, RunQueryDsl};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncMysqlConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, ScalarFamily, Value};
use subql::reexec::{
    AsyncConnector, AsyncMode, AutoResolvingEngine, MysqlAsyncDieselConnector, SessionSetup,
    SnapshotResult,
};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

type Engine = AutoResolvingEngine<
    TestEvent<MySql>,
    DefaultIds,
    ParserDB,
    AsyncMode<MysqlAsyncDieselConnector>,
>;

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

/// One more row, for a commit that lands while a read is parked.
fn insert(id: i64) -> String {
    format!("INSERT INTO orders (id, price, quantity, status) VALUES ({id}, 7.0, 1, 'paid')")
}

fn catalog() -> ParserDB {
    ParserDB::parse::<MySqlDialect>(DDL).expect("parse DDL")
}

fn build_engine(catalog: ParserDB, pool: Pool<AsyncMysqlConnection>) -> Engine {
    let inner =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, ParserDB>::new(catalog, MySqlDialect {});
    AutoResolvingEngine::new(inner, AsyncMode::new(MysqlAsyncDieselConnector::new(pool)))
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
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };

        let snap = engine
            .snapshot(captured_qid)
            .await
            .expect("snapshot")
            .expect("subscription_id exists");
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
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(subql::Install::install(
            &mut engine,
            captured_qid,
            subql::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<subql::NoCheckpoint>
            }
        )
        .is_ok());

        sql_query("DELETE FROM orders WHERE id = 1")
            .execute(&mut conn_dml)
            .expect("delete id=1");

        let event =
            TestEvent::<MySql>::delete(table_id, orders_row(1, 5.0)).with_pk_columns([0u16]);

        engine.apply(&event).expect("apply");
        let notifs = engine.resolve_collect().await.expect("consumers dispatch");
        assert_eq!(
            notifs.scalar_updates.len(),
            1,
            "expected exactly one ScalarUpdate, got {}",
            notifs.scalar_updates.len()
        );
        assert_eq!(notifs.scalar_updates[0].subscription_id, captured_qid);
        assert_eq!(
            notifs.scalar_updates[0].value,
            Value::Float(9.0),
            "connector re-queried live MySQL for the new MIN"
        );
        assert!(
            notifs.transitions.is_empty(),
            "a plain scalar re-read changes no tier"
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
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("aggregate carries a bootstrap");

    common::multi_thread_rt().block_on(async move {
        let pool = mysql_async_pool(port).await;
        let connector = MysqlAsyncDieselConnector::new(pool);
        let (row, _checkpoint) = connector
            .execute_scalar_row(&bundle.query.as_read_query(), &bundle.kinds, &())
            .await
            .expect("execute_scalar_row");
        // (sum, squared deviations, count) = (12, 8, 3). Eight because
        // the seed reads the engine's own `VAR_POP(amount) * COUNT(amount)`
        // rather than a sum of squares, which would be 56.
        assert_eq!(
            row,
            vec![
                Value::Decimal(bigdecimal::BigDecimal::from(12)),
                Value::Float(8.0),
                Value::Int(3)
            ]
        );
    });
}

/// Both position-carrying reads report a position taken before their snapshot.
///
/// The contract a caller replays from: a position at or behind the snapshot
/// re-delivers changes the snapshot already holds, while a position ahead of
/// it silently drops a transaction the snapshot never saw. A user-level lock
/// parks each read inside its own snapshot, a commit lands while it waits, and
/// the returned coordinate has to sit behind that commit. The page read is not
/// here because this connector reports no coordinate for it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_read_reports_a_position_taken_before_its_snapshot() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);
    let mut conn = common::mysql_connect(port);
    setup_mysql(&mut conn, &[(1, 5.0)]);

    let rt = common::multi_thread_rt();
    let connector = Arc::new(MysqlAsyncDieselConnector::new(
        rt.block_on(mysql_async_pool(port)),
    ));

    let held = Arc::clone(&connector);
    let on = rt.handle().clone();
    // Each lock name carries a column, so MySQL cannot fold the condition to a
    // constant and take the lock before its read view exists. The gate holds
    // the name the lowest id builds, which a clustered-index scan reaches
    // first.
    let sql = "SELECT count(*) AS v FROM orders WHERE GET_LOCK(CONCAT('park_scalar_', id), 60) = 1";
    let ((value, position), after_commit) =
        common::park_a_mysql_read(port, "park_scalar_1", &insert(2), move || {
            on.block_on(async move {
                held.execute_scalar(
                    &subql::reexec::ReadQuery::without_binds(sql),
                    ScalarFamily::Int,
                    &(),
                )
                .await
            })
            .expect("scalar read")
        });
    assert_eq!(
        value,
        Value::Int(1),
        "the scalar read's snapshot holds one row"
    );
    assert!(
        position.expect("binary logging is on, so a coordinate is reported") < after_commit,
        "the scalar read's position must sit behind the commit at {after_commit:?}"
    );

    let held = Arc::clone(&connector);
    let on = rt.handle().clone();
    let sql = "SELECT count(*) AS c0 FROM orders WHERE GET_LOCK(CONCAT('park_seed_', id), 60) = 1";
    let ((values, position), after_commit) =
        common::park_a_mysql_read(port, "park_seed_1", &insert(3), move || {
            on.block_on(async move {
                held.execute_scalar_row(
                    &subql::reexec::ReadQuery::without_binds(sql),
                    &[ScalarFamily::Int],
                    &(),
                )
                .await
            })
            .expect("seed read")
        });
    assert_eq!(
        values,
        vec![Value::Int(2)],
        "the seed read's snapshot holds two rows"
    );
    assert!(
        position.expect("binary logging is on, so a coordinate is reported") < after_commit,
        "the seed read's position must sit behind the commit at {after_commit:?}"
    );
}

/// A borrowed-list session setup, mirroring what a caller builds per read.
struct MarkerSetup(Vec<String>);

impl SessionSetup for MarkerSetup {
    fn setup_statements(&self) -> &[String] {
        &self.0
    }
}

/// The session-setup seam reaches the async MySQL `read_page`, the one shipped
/// path the inventory finds transaction-free. A non-empty setup runs before the
/// caller's SQL (observed through a session variable the read then reads back),
/// and an empty setup on a fresh connection runs nothing. The transaction-open
/// conditional itself (a transaction only when the setup is non-empty) is
/// pinned exactly by the generic connector's SQLite instrumentation test in
/// `tests/it/session_setup.rs`, since MySQL exposes no clean in-transaction flag a
/// read can select.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn session_setup_runs_on_the_transaction_free_read_page() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    common::multi_thread_rt().block_on(async move {
        let read_marker = "SELECT @@max_sort_length AS v";
        let setup = MarkerSetup(vec!["SET SESSION max_sort_length = 1234".to_string()]);
        let with = MysqlAsyncDieselConnector::with_session_setup(mysql_async_pool(port).await);
        let page = with
            .read_page(
                &subql::reexec::ReadQuery::without_binds(read_marker),
                4096,
                &setup,
            )
            .await
            .expect("read with setup");
        assert_eq!(
            page.value.rows[0][0],
            Value::Int(1234),
            "the setup ran on read_page before the caller's SQL"
        );

        // A fresh pool that never ran the setter reads the server default,
        // which is not the value the setup would have installed.
        let plain = MysqlAsyncDieselConnector::new(mysql_async_pool(port).await);
        let page = plain
            .read_page(
                &subql::reexec::ReadQuery::without_binds(read_marker),
                4096,
                &(),
            )
            .await
            .expect("read with empty setup");
        assert_ne!(
            page.value.rows[0][0],
            Value::Int(1234),
            "an empty setup runs nothing, so the default stands"
        );
    });
}
