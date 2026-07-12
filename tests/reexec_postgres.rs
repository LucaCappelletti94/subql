//! Docker-backed integration test for [`AutoResolvingEngine`] +
//! [`DieselConnector`] against a real Postgres with logical replication.
//!
//! Verifies the production path end to end:
//! * an engine-supported subscription and a captured (MIN) subscription
//!   coexist on one engine. Each gets the right kind of notification when
//!   the corresponding WAL event arrives.
//! * The [`DieselConnector`] re-executes the captured query's SQL against
//!   real Postgres and decodes a scalar [`Value<Postgres>`] correctly.
//!
//! Requires Docker. Tests are `#[ignore]`d so default `cargo test` does
//! not spin up containers. Run with:
//!
//! ```sh
//! cargo test --test reexec_postgres --features executor-diesel \
//!     -- --ignored --nocapture
//! ```
//!
//! Gated via `[[test]] required-features = ["executor-diesel-postgres"]`
//! in `Cargo.toml`. The companion test against in-memory SQLite is
//! `tests/reexec_diesel.rs`. That one is a plumbing smoke check, this one
//! is the production-path proof and pulls LSN information from
//! `pg_current_wal_lsn()` inside snapshot transactions.
#![cfg(feature = "executor-diesel-postgres")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::{sql_query, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Postgres, Value};
use subql::reexec::{
    AutoResolvingEngine, Connector, PgDieselConnector, ReExecEngine, Registered, SnapshotResult,
};
use subql::{
    DefaultIds, SubscriptionEngine, SubscriptionRequest, Wal2JsonV2Event, Wal2JsonV2Parser,
    WalParser,
};

const SLOT: &str = "subql_test";
const DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

/// PG-side DDL used in the actual container. Uses DOUBLE PRECISION (the
/// canonical PG type for an 8-byte float) so the diesel `Nullable<Double>`
/// row type in `DieselConnector` decodes cleanly.
const PG_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE PRECISION,
    quantity INT,
    status TEXT
)";

/// Set up the PG side: DDL, REPLICA IDENTITY FULL, seed rows, replication
/// slot. The slot starts empty (the seeds are inserted **before** creating
/// the slot, so their WAL records never reach it). After this returns we
/// can drive test-specific DML and observe it cleanly.
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

/// Build the in-process catalog the engine + parser share.
fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn build_engine(
    catalog: ParserDB,
    conn_exec: PgConnection,
) -> AutoResolvingEngine<Wal2JsonV2Event, DefaultIds, ParserDB, PgDieselConnector> {
    let inner = SubscriptionEngine::<Wal2JsonV2Event, DefaultIds, ParserDB>::new(
        catalog,
        PostgreSqlDialect {},
    );
    AutoResolvingEngine::new(ReExecEngine::new(inner), PgDieselConnector::new(conn_exec))
}

/// Parse one wal2json v2 message into zero or more [`Wal2JsonV2Event`]s.
/// The parser emits 0 events for relation-only messages (begin/commit/
/// relation), which the test must tolerate.
#[allow(dead_code)] // used by the step-3 / step-4 tests
fn parse_message(parser: &Wal2JsonV2Parser, catalog: &ParserDB, msg: &str) -> Vec<Wal2JsonV2Event> {
    parser
        .parse_wal_message(msg.as_bytes(), catalog)
        .expect("wal2json parse")
}

/// Harness health check: container starts, three PG connections, catalog
/// builds, registration of both subscriptions returns the right `Registered`
/// variants. No DML, no slot drain. Proves the harness is healthy without
/// asserting CDC semantics.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn scaffold_registers_both_subscription_kinds() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let _conn_dml = common::pg_connect(port);
    let conn_exec = common::pg_connect(port);

    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

    let engine_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT * FROM orders WHERE price > 8.0",
            ),
            (),
        )
        .expect("engine-supported registration");
    match engine_reg {
        Registered::Engine(_) => {}
        Registered::ReExec { .. } => panic!("expected Engine variant"),
    }

    let captured_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration");
    match captured_reg {
        Registered::ReExec { query_id, .. } => {
            assert!(engine.install(query_id, Value::Float(5.0)));
        }
        Registered::Engine(_) => panic!("expected ReExec variant"),
    }

    // Sanity: connector still callable (proves the PG connection survived
    // the construction).
    let _ = engine
        .connector()
        .execute_scalar(
            "SELECT MIN(price) AS v FROM orders",
            subql::backend::ScalarKind::Float,
            &(),
        )
        .expect("connector executes");
}

/// Test 1 - the headline integration test.
///
/// Register an engine-supported subscription (`SELECT * FROM orders WHERE
/// price > 8.0`, consumer=1) and a captured subscription (`SELECT MIN(price)
/// FROM orders`, consumer=2) on the same `AutoResolvingEngine`. Drive an
/// INSERT (id=3 price=11.0) and a DELETE-of-the-current-extreme (id=1
/// price=5.0) via real PG. The INSERT must match the engine subscription
/// only (no re-execution). The DELETE removes the current MIN, so the
/// engine spills `NeedsReexecution`. The auto-resolving engine calls the
/// `DieselConnector`, which queries PG and returns the new MIN
/// (`9.0`, the smallest remaining row). The connector must be called
/// exactly once across the batch.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn engine_and_captured_paths_coexist_through_pg_connector() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    let conn_exec = common::pg_connect(port);

    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

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
            SubscriptionRequest::<DefaultIds, Postgres>::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered::ReExec { query_id, .. } => query_id,
        Registered::Engine(_) => panic!("expected ReExec"),
    };
    assert!(engine.install(captured_qid, Value::Float(5.0)));

    // The replication slot was created after the seed inserts, so it's
    // already empty. No pre-drain needed.

    // Drive the DML the test cares about.
    sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (3, 11.0, 1, 'paid')")
        .execute(&mut conn_dml)
        .expect("insert id=3");
    sql_query("DELETE FROM orders WHERE id = 1")
        .execute(&mut conn_dml)
        .expect("delete id=1");

    // Pull WAL output and feed it through the parser + engine.
    let msgs = common::drain_slot(&mut conn_setup, SLOT);
    assert!(
        !msgs.is_empty(),
        "expected at least one wal2json message after INSERT+DELETE"
    );

    let parser = Wal2JsonV2Parser;
    let mut events: Vec<Wal2JsonV2Event> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(&parser, &catalog(), msg));
    }
    assert_eq!(
        events.len(),
        2,
        "expected exactly one INSERT and one DELETE event from wal2json (got {})",
        events.len()
    );

    // Accumulate notifications across the batch.
    let mut total_inserted = Vec::new();
    let mut total_deleted = Vec::new();
    let mut total_scalar_updates = Vec::new();
    let mut total_triggers = 0usize;
    for event in &events {
        let notifs = engine.consumers(event).expect("consumers dispatch");
        total_inserted.extend(notifs.engine.inserted().iter().copied());
        total_deleted.extend(notifs.engine.deleted().iter().copied());
        total_scalar_updates.extend(notifs.scalar_updates);
        total_triggers += notifs.triggers.len();
    }

    // Engine-supported subscription: the INSERT id=3 (price=11.0) matches
    // the `> 8.0` predicate -> `inserted`. The DELETE id=1 (price=5.0) does
    // NOT match -> nothing for consumer 1 on that event.
    assert!(
        total_inserted.contains(&engine_consumer),
        "consumer {engine_consumer} expected in `inserted`, got {total_inserted:?}"
    );
    assert!(
        !total_deleted.contains(&engine_consumer),
        "consumer {engine_consumer} should not be in `deleted` for this scenario, got {total_deleted:?}"
    );

    // Captured subscription: exactly one ScalarUpdate, value = new MIN (9.0,
    // the smallest remaining row after the delete). No triggers under the
    // auto-resolving engine.
    assert_eq!(
        total_scalar_updates.len(),
        1,
        "expected exactly one ScalarUpdate, got {}",
        total_scalar_updates.len()
    );
    assert_eq!(total_scalar_updates[0].query_id, captured_qid);
    assert_eq!(total_scalar_updates[0].value, Value::Float(9.0));
    assert_eq!(total_triggers, 0, "auto-resolving engine drains triggers");
}

/// Test 2 - UPDATE displaces the current extreme.
///
/// Seed `(1, 5.0)` so MIN=5.0. UPDATE it to `price=20.0`. With
/// `REPLICA IDENTITY FULL` the wal2json output carries the full old row, so
/// the maintenance state machine detects "extreme value left the predicate"
/// (the current MIN row's value changed) and spills `NeedsReexecution`. The
/// `DieselConnector` queries PG and returns the new MIN, which is `20.0`
/// (the only remaining row).
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn update_displacing_extreme_resolves_via_pg_connector() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    let conn_exec = common::pg_connect(port);

    setup_pg(&mut conn_setup, &[(1, 5.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

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
    assert!(engine.install(captured_qid, Value::Float(5.0)));

    sql_query("UPDATE orders SET price = 20.0 WHERE id = 1")
        .execute(&mut conn_dml)
        .expect("update id=1 price=20.0");

    let msgs = common::drain_slot(&mut conn_setup, SLOT);
    let parser = Wal2JsonV2Parser;
    let mut events: Vec<Wal2JsonV2Event> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(&parser, &catalog(), msg));
    }
    assert_eq!(events.len(), 1, "expected exactly one UPDATE event");

    let notifs = engine.consumers(&events[0]).expect("consumers dispatch");
    assert_eq!(notifs.scalar_updates.len(), 1, "expected one ScalarUpdate");
    assert_eq!(notifs.scalar_updates[0].query_id, captured_qid);
    assert_eq!(notifs.scalar_updates[0].value, Value::Float(20.0));
    assert!(notifs.triggers.is_empty());
}

/// Test 3 - PgDieselConnector::snapshot returns a real PgLsn.
///
/// Seeds the table with `(1, 5.0), (2, 9.0)`, registers `MIN(price)`, then
/// calls `engine.snapshot(qid)` BEFORE any CDC events. The result should be
/// `SnapshotResult::Scalar(Value::Float(5.0), Some(PgLsn(_)))` where the
/// LSN is non-zero (PG always has a position). Subsequent dispatches then
/// see 5.0 as the current MIN without any further connector calls because
/// `snapshot` already installed the value.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn snapshot_reads_value_and_lsn_from_pg() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let conn_exec = common::pg_connect(port);

    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

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

    // Snapshot reads value + LSN inside a single transaction.
    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("query_id exists");
    let (value, checkpoint) = match snap {
        SnapshotResult::Scalar(value, checkpoint) => (value, checkpoint),
        other => panic!("unexpected snapshot variant: {other:?}"),
    };
    assert_eq!(value, Value::Float(5.0), "MIN(price) snapshot value");

    let lsn = checkpoint.expect("PgDieselConnector must report a checkpoint");
    assert!(
        lsn > subql::PgLsn(0),
        "pg_current_wal_lsn() should be non-zero on a live server, got {lsn:?}"
    );
}
