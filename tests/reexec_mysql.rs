#![cfg(any())] // Phase 11: rewrite against Value<B> + TestEvent<B>. Retired Cell/WalEvent/RowImage/PrimaryKey/ColumnType api. Tracked in docs/refactor-cdc-event-handoff.md.

//! Docker-backed integration test for [`AutoResolvingEngine`] +
//! [`MysqlDieselConnector`] against a real MySQL 8.0 with binary logging.
//!
//! Mirrors `tests/reexec_postgres.rs` for the MySQL backend. MySQL has no
//! in-tree CDC source (Maxwell lives outside the crate, exercised by
//! `tests/cdc_mysql_e2e.rs`), so the displacement test hand-builds the
//! `WalEvent` instead of decoding one from a replication stream. The point is
//! the same: prove the connector re-executes the captured query's SQL against
//! live MySQL and decodes a scalar [`Cell`] correctly.
//!
//! The catalog is parsed with `PostgreSqlDialect` regardless of backend
//! (subql parses PG-flavored DDL into its in-process catalog; the MySQL DDL
//! that actually runs on the server is separate).
//!
//! Requires Docker. Tests are `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --test reexec_mysql --features executor-diesel-mysql \
//!     -- --ignored --nocapture
//! ```
//!
//! Gated via `[[test]] required-features = ["executor-diesel-mysql"]` in
//! `Cargo.toml`.
#![cfg(feature = "executor-diesel-mysql")]
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use diesel::{sql_query, MysqlConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::reexec::{
    AutoResolvingEngine, Connector, MysqlDieselConnector, ReExecEngine, Registered, SnapshotResult,
};
use subql::{
    catalog_helpers, Cell, ColumnType, DefaultIds, RowImage, SubscriptionEngine,
    SubscriptionRequest, WalEvent,
};

/// Catalog DDL shared by the engine + planner. Parsed with `PostgreSqlDialect`.
/// Uses `FLOAT` (which sqlparser maps to subql's `Float` column type) so the
/// captured `MIN(price)` plan decodes through the connector's `Nullable<Double>`
/// row.
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

/// Set up the MySQL side: DDL + seed rows.
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

/// Build the in-process catalog the engine + planner share.
fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn build_engine(
    catalog: ParserDB,
    conn_exec: MysqlConnection,
) -> AutoResolvingEngine<PostgreSqlDialect, DefaultIds, ParserDB, MysqlDieselConnector> {
    let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
        catalog,
        PostgreSqlDialect {},
    );
    AutoResolvingEngine::new(
        ReExecEngine::new(inner),
        MysqlDieselConnector::new(conn_exec),
    )
}

/// Build the full `orders` row image for the hand-built delete event.
/// Column order matches the catalog: id=0, price=1, quantity=2, status=3.
fn orders_row(id: i64, price: f64) -> RowImage {
    RowImage {
        cells: Arc::from([
            Cell::Int(id),
            Cell::Float(price),
            Cell::Int(1),
            Cell::String(Arc::from("paid")),
        ]),
    }
}

/// Harness health check + connector round-trip. Container starts, two MySQL
/// connections, catalog builds, both subscription kinds register, and the
/// connector executes a live `MIN(price)` returning the seeded extreme. Proves
/// the live re-execution + scalar decode path works end to end.
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
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE price > 8.0"),
            (),
        )
        .expect("engine-supported registration");
    match engine_reg {
        Registered::Engine(_) => {}
        Registered::ReExec { .. } => panic!("expected Engine variant"),
    }

    let captured_reg = engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration");
    match captured_reg {
        Registered::ReExec { query_id, .. } => {
            assert!(engine.install(query_id, Cell::Float(5.0)));
        }
        Registered::Engine(_) => panic!("expected ReExec variant"),
    }

    // The connector re-executes against live MySQL and decodes the scalar.
    let (value, _checkpoint) = engine
        .connector()
        .execute_scalar("SELECT MIN(price) AS v FROM orders", ColumnType::Float, &())
        .expect("connector executes");
    assert_eq!(value, Cell::Float(5.0), "live MIN(price) decode");
}

/// `MysqlDieselConnector::snapshot` reads the value and a binlog coordinate.
///
/// Seeds `(1, 5.0), (2, 9.0)`, registers `MIN(price)`, then calls
/// `engine.snapshot(qid)` BEFORE any CDC events. The result should be
/// `SnapshotResult::Scalar(Cell::Float(5.0), Some(MysqlBinlogPos { .. }))`.
/// Binary logging is enabled (`--log-bin`), so `SHOW MASTER STATUS` returns a
/// row and the checkpoint is present. We assert it is `Some` and that the
/// byte offset is plausible (non-zero on a live server that has written DDL +
/// seed rows to the binlog).
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
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered::ReExec { query_id, .. } => query_id,
        Registered::Engine(_) => panic!("expected ReExec"),
    };

    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("query_id exists");
    let (value, checkpoint) = match snap {
        SnapshotResult::Scalar(value, checkpoint) => (value, checkpoint),
        SnapshotResult::Rows(_, _) => panic!("expected Scalar variant"),
        other => panic!("unexpected snapshot variant: {other:?}"),
    };
    assert_eq!(value, Cell::Float(5.0), "MIN(price) snapshot value");

    let pos = checkpoint.expect("MysqlDieselConnector must report a binlog checkpoint");
    assert!(
        pos.pos > 0,
        "SHOW MASTER STATUS byte offset should be non-zero on a live server, got {pos:?}"
    );
}

/// Displacement of the current extreme via a hand-built `WalEvent`.
///
/// Seeds `(1, 5.0), (2, 9.0)` and installs MIN=5.0. Deletes id=1 on the live
/// MySQL connection (so the new MIN on the server is 9.0). Then hand-builds a
/// DELETE `WalEvent` for id=1 (price=5.0) and dispatches it: the engine sees
/// the current extreme leave, spills a trigger, and the auto-resolving engine
/// re-queries live MySQL through the connector, which returns 9.0. Asserts
/// exactly one `ScalarUpdate` carrying `Cell::Float(9.0)`.
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
    let resolved =
        catalog_helpers::resolve_table(&cat, "orders", &["id", "price", "quantity", "status"])
            .expect("resolve orders");
    let table_id = resolved.table_id;
    let pk_col = *resolved
        .primary_key
        .first()
        .expect("orders has a primary key");

    let mut engine = build_engine(cat, conn_exec);

    let captured_qid = match engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered::ReExec { query_id, .. } => query_id,
        Registered::Engine(_) => panic!("expected ReExec"),
    };
    assert!(engine.install(captured_qid, Cell::Float(5.0)));

    // Remove the current MIN on the live server so the connector re-query
    // returns the next-smallest row (9.0).
    sql_query("DELETE FROM orders WHERE id = 1")
        .execute(&mut conn_dml)
        .expect("delete id=1");

    // Hand-build the DELETE event the engine would otherwise decode from a
    // CDC stream. `NoCheckpoint` is fine: `consumers` is generic over the
    // event's checkpoint type.
    let event: WalEvent = WalEvent::builder(table_id)
        .delete()
        .pk_cell(pk_col, Cell::Int(1))
        .old_row(orders_row(1, 5.0))
        .build()
        .expect("build delete event");

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
        Cell::Float(9.0),
        "connector re-queried live MySQL for the new MIN"
    );
    assert!(
        notifs.triggers.is_empty(),
        "auto-resolving engine drains triggers"
    );
}
