//! Docker-backed integration test for [`AutoResolvingEngine`] +
//! [`MysqlDieselConnector`] against a real MySQL 8.0 with binary logging.
//!
//! Mirrors `tests/reexec_postgres.rs` for the MySQL backend. MySQL has no
//! in-tree CDC source (Maxwell lives outside the crate, exercised by
//! `tests/cdc_mysql_e2e.rs`), so the displacement test hand-builds the
//! event via [`TestEvent<MySql>`] instead of decoding one from a
//! replication stream. The point is the same: prove the connector
//! re-executes the captured query's SQL against live MySQL and decodes
//! a scalar [`Value`] correctly.
//!
//! Requires Docker. Tests are `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --test reexec_mysql --features executor-diesel-mysql \
//!     -- --ignored --nocapture
//! ```
#![cfg(feature = "executor-diesel-mysql")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::{sql_query, MysqlConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, ScalarKind, Value};
use subql::reexec::{
    AutoResolvingEngine, Connector, MysqlDieselConnector, ReExecEngine, Registered, SnapshotResult,
};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

type Engine = AutoResolvingEngine<TestEvent<MySql>, DefaultIds, ParserDB, MysqlDieselConnector>;

/// Catalog DDL shared by the engine + planner. `FLOAT` maps to subql's
/// `Float` scalar kind under the MySQL parser.
const DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

/// MySQL-side DDL that actually runs on the server. Uses `DOUBLE` (MySQL's
/// 8-byte float) so the diesel `Nullable<Double>` row type decodes cleanly.
const MYSQL_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE,
    quantity INT,
    status TEXT
)";

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

fn build_engine(catalog: ParserDB, conn_exec: MysqlConnection) -> Engine {
    let inner =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, ParserDB>::new(catalog, MySqlDialect {});
    AutoResolvingEngine::new(
        ReExecEngine::new(inner),
        MysqlDieselConnector::new(conn_exec),
    )
}

/// Build the full `orders` row image for the hand-built delete event.
/// Column order matches the catalog: id=0, price=1, quantity=2, status=3.
fn orders_row(id: i64, price: f64) -> Vec<Value<MySql>> {
    vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(1),
        Value::String("paid".into()),
    ]
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn scaffold_registers_both_and_executes_scalar() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let conn_exec = common::mysql_connect(port);

    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

    let engine_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(
                1u64,
                "SELECT * FROM orders WHERE price > 8.0",
            ),
            (),
        )
        .expect("engine-supported registration");
    match engine_reg {
        Registered::Engine(_) => {}
        other => panic!("expected the Engine variant, got {other:?}"),
    }

    let captured_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration");
    match captured_reg {
        Registered::ReExec { query_id, .. } => {
            assert!(engine.install(query_id, Value::Float(5.0)));
        }
        other => panic!("expected ReExec variant, got {other:?}"),
    }

    let (value, _checkpoint) = engine
        .connector()
        .execute_scalar("SELECT MIN(price) AS v FROM orders", ScalarKind::Float, &())
        .expect("connector executes");
    assert_eq!(value, Value::Float(5.0), "live MIN(price) decode");
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn snapshot_reads_value_and_binlog_pos_from_mysql() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let conn_exec = common::mysql_connect(port);

    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

    let captured_qid = match engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered::ReExec { query_id, .. } => query_id,
        other => panic!("expected ReExec, got {other:?}"),
    };

    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("query_id exists");
    let (value, checkpoint) = match snap {
        SnapshotResult::Scalar(value, checkpoint) => (value, checkpoint),
        other => panic!("unexpected snapshot variant: {other:?}"),
    };
    assert_eq!(value, Value::Float(5.0), "MIN(price) snapshot value");

    let pos = checkpoint.expect("MysqlDieselConnector must report a binlog checkpoint");
    assert!(
        pos.pos > 0,
        "SHOW MASTER STATUS byte offset should be non-zero on a live server, got {pos:?}"
    );
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn delete_displacing_extreme_resolves_via_mysql_connector() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let mut conn_dml = common::mysql_connect(port);
    let conn_exec = common::mysql_connect(port);

    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let cat = catalog();
    let table_id: TableId = catalog_helpers::table_id(&cat, "orders").expect("resolve orders");

    let mut engine = build_engine(cat, conn_exec);

    let captured_qid = match engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(1u64, "SELECT MIN(price) FROM orders"),
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

    let event = TestEvent::<MySql>::delete(table_id, orders_row(1, 5.0)).with_pk_columns([0u16]);

    let notifs = engine.consumers(&event).expect("consumers dispatch");
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
}

/// The multi-column aggregate seed decodes correctly through the real MySQL
/// connector. MySQL promotes `SUM` over an integer column to `DECIMAL`, so
/// the connector's `CAST(... AS DOUBLE)` must still decode the sum and
/// sum-of-squares as `f64` (the case in-memory SQLite cannot exercise).
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn execute_scalar_row_decodes_integer_aggregate_seed() {
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

    // MySQL SUM(int) -> DECIMAL, cast to DOUBLE; sum=12, sum_sq=56, count=3.
    let connector = MysqlDieselConnector::new(common::mysql_connect(port));
    let (row, _checkpoint) = connector
        .execute_scalar_row(&bundle.sql, &bundle.kinds, &())
        .expect("execute_scalar_row");
    assert_eq!(
        row,
        vec![Value::Float(12.0), Value::Float(56.0), Value::Int(3)]
    );
}
