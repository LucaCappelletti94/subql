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
//! cargo test --test it reexec_postgres:: --features executor-diesel-postgres \
//!     -- --ignored --nocapture
//! ```
//!
//! Gated via `#[cfg(feature = "executor-diesel-postgres")]` in
//! `tests/it/main.rs`. The companion test against in-memory SQLite is
//! `tests/it/reexec_diesel.rs`. That one is a plumbing smoke check, this one
//! is the production-path proof and reads `pg_current_wal_lsn()` just before
//! each snapshot transaction.
#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::reexec::{
    AutoResolvingEngine, Connector, PgDieselConnector, SessionSetup, SnapshotResult, SyncMode,
};
use subql::{
    parse_wal2json_v2, DefaultIds, MessageV2, Registered, SubscriptionEngine, SubscriptionRequest,
    Tier,
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

/// One more row, for a commit that lands while a read is parked.
fn insert(id: i64) -> String {
    format!("INSERT INTO orders (id, price, quantity, status) VALUES ({id}, 7.0, 1, 'paid')")
}

/// Build the in-process catalog the engine + parser share.
fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn build_engine(
    catalog: ParserDB,
    conn_exec: PgConnection,
) -> AutoResolvingEngine<MessageV2, DefaultIds, ParserDB, SyncMode<PgDieselConnector>> {
    let inner =
        SubscriptionEngine::<MessageV2, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {});
    AutoResolvingEngine::new(inner, SyncMode(PgDieselConnector::new(conn_exec)))
}

/// Parse one wal2json v2 message into zero or more [`MessageV2`]s.
/// The parser emits 0 events for relation-only messages (begin/commit/
/// relation), which the test must tolerate.
fn parse_message(msg: &str) -> Vec<MessageV2> {
    parse_wal2json_v2(msg.as_bytes()).expect("wal2json parse")
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
        Registered {
            tier: Tier::InProcess(_),
            ..
        } => {}
        other => panic!("expected the Engine variant, got {other:?}"),
    }

    let captured_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration");
    match captured_reg {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => {
            assert!(subql::Install::install(
                &mut engine,
                subscription_id,
                subql::ScalarInstall {
                    value: Value::Float(5.0),
                    checkpoint: None::<subql::NoCheckpoint>
                }
            )
            .is_ok());
        }
        other => panic!("expected ReExec variant, got {other:?}"),
    }

    // Sanity: connector still callable (proves the PG connection survived
    // the construction).
    let _ = engine
        .connector()
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds("SELECT MIN(price) AS v FROM orders"),
            subql::backend::BuiltinKind::Float,
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
    assert!(matches!(
        engine_reg,
        Registered {
            tier: Tier::InProcess(_),
            ..
        }
    ));

    let captured_qid = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(2u64, "SELECT MIN(price) FROM orders"),
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
    assert_eq!(total_scalar_updates[0].subscription_id, captured_qid);
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

    sql_query("UPDATE orders SET price = 20.0 WHERE id = 1")
        .execute(&mut conn_dml)
        .expect("update id=1 price=20.0");

    let msgs = common::drain_slot(&mut conn_setup, SLOT);
    let mut events: Vec<MessageV2> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(msg));
    }
    assert_eq!(events.len(), 1, "expected exactly one UPDATE event");

    let notifs = engine.consumers(&events[0]).expect("consumers dispatch");
    assert_eq!(notifs.scalar_updates.len(), 1, "expected one ScalarUpdate");
    assert_eq!(notifs.scalar_updates[0].subscription_id, captured_qid);
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };

    // Snapshot reads value + LSN inside a single transaction.
    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("subscription_id exists");
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

/// The multi-column aggregate seed decodes correctly through the real PG
/// connector. Uses an INTEGER column so `SUM` promotes to `bigint` (the
/// cross-backend case in-memory SQLite cannot exercise): the connector's
/// double cast must still decode the sum and sum-of-squares as `f64`.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn execute_scalar_row_decodes_integer_aggregate_seed() {
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

    // Register to obtain the seed bundle (sql + kinds).
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
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("aggregate carries a bootstrap");

    // sum=12 (bigint), sum_sq=56 (bigint), count=3, decoded through the
    // double cast into (Float, Float, Int).
    let connector = PgDieselConnector::new(common::pg_connect(port));
    let (row, checkpoint) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&bundle.sql),
            &bundle.kinds,
            &(),
        )
        .expect("execute_scalar_row");
    assert_eq!(
        row,
        vec![Value::Float(12.0), Value::Float(56.0), Value::Int(3)]
    );
    // The read is LSN-anchored like execute_scalar.
    assert!(checkpoint.is_some());
}

/// A key column whose name needs quoting must still be readable.
///
/// Postgres folds an unquoted identifier to lower case, so a column created as
/// `"OrderId"` is not found by `OrderId`. Building the scoped read's identifiers
/// from catalog names without quoting them registered the subscription cleanly
/// and then failed on every single change with `column "orderid" does not
/// exist`. SQLite cannot show this: its identifiers are case-insensitive.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_key_column_needing_quotes_is_still_readable() {
    use subql::testing::TestEvent;

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut setup = common::pg_connect(port);
    sql_query(r#"CREATE TABLE quoted ("OrderId" INT PRIMARY KEY, status TEXT)"#)
        .execute(&mut setup)
        .expect("create");
    sql_query("INSERT INTO quoted VALUES (1, 'paid'), (2, 'void')")
        .execute(&mut setup)
        .expect("seed");

    let cat = ParserDB::parse::<PostgreSqlDialect>(
        r#"CREATE TABLE quoted ("OrderId" INT PRIMARY KEY, status TEXT);"#,
    )
    .expect("catalog");
    let table = subql::catalog_helpers::table_id(&cat, "quoted").expect("quoted");
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        cat,
        PostgreSqlDialect {},
    );
    let mut engine = AutoResolvingEngine::new(
        inner,
        SyncMode(PgDieselConnector::new(common::pg_connect(port))),
    );
    engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT * FROM quoted WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("captured");

    let event = TestEvent::<Postgres>::update(
        table,
        vec![Value::Int(1), Value::String("paid".into())],
        vec![Value::Int(1), Value::String("paid".into())],
    )
    .with_pk_columns([0u16]);
    let notifications = engine
        .consumers(&event)
        .expect("a quoted key column must be readable, not fail on every change");

    assert_eq!(notifications.row_deltas.len(), 1);
    assert_eq!(notifications.row_deltas[0].key, vec![Value::Int(1)]);
    assert!(
        notifications.row_deltas[0].row.is_some(),
        "the row still matches, so it arrives as itself"
    );
}

/// Every read reports a position taken before its own snapshot opened.
///
/// The contract a caller replays from: a position at or behind the snapshot
/// re-delivers changes the snapshot already holds, which keyed application
/// absorbs, while a position ahead of it silently drops a transaction the
/// snapshot never saw. An advisory lock parks each read inside its own
/// snapshot, a commit lands while it waits, and the returned position has to
/// sit behind that commit. The row count is asserted too, because a read that
/// somehow saw the commit would make the position comparison meaningless.
///
/// A connector each, because this one owns its connection outright and a
/// parked read holds it for the length of the park.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_read_reports_a_position_taken_before_its_snapshot() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 5.0)]);

    let sql = format!("SELECT count(*)::bigint AS v FROM orders {}", common::PARK);
    let ((value, position), after_commit) = common::park_a_read(port, &insert(2), move || {
        PgDieselConnector::new(common::pg_connect(port))
            .execute_scalar(
                &subql::reexec::ReadQuery::without_binds(&sql),
                BuiltinKind::Int,
                &(),
            )
            .expect("scalar read")
    });
    assert_eq!(
        value,
        Value::Int(1),
        "the scalar read's snapshot holds one row"
    );
    assert!(
        position.expect("a PG connector reports a position") < after_commit,
        "the scalar read's position must sit behind the commit at {after_commit:?}"
    );

    let sql = format!("SELECT id FROM orders {} ORDER BY id", common::PARK);
    let (page, after_commit) = common::park_a_read(port, &insert(3), move || {
        PgDieselConnector::new(common::pg_connect(port))
            .read_page(&subql::reexec::ReadQuery::without_binds(&sql), 1 << 20, &())
            .expect("page read")
    });
    assert_eq!(
        page.value.rows.len(),
        2,
        "the page's snapshot holds two rows"
    );
    assert!(
        page.checkpoint.expect("a PG connector reports a position") < after_commit,
        "the page read's position must sit behind the commit at {after_commit:?}"
    );

    let sql = format!("SELECT count(*)::bigint AS c0 FROM orders {}", common::PARK);
    let ((values, position), after_commit) = common::park_a_read(port, &insert(4), move || {
        PgDieselConnector::new(common::pg_connect(port))
            .execute_scalar_row(
                &subql::reexec::ReadQuery::without_binds(&sql),
                &[BuiltinKind::Int],
                &(),
            )
            .expect("seed read")
    });
    assert_eq!(
        values,
        vec![Value::Int(3)],
        "the seed read's snapshot holds three rows"
    );
    assert!(
        position.expect("a PG connector reports a position") < after_commit,
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

/// The session-setup seam runs its statements inside each read's transaction on
/// the single-connection `PgDieselConnector`. `SET LOCAL` takes hold only for
/// the transaction that runs it, so a read that sees the value proves the setup
/// ran in that read's own transaction.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn session_setup_runs_inside_each_read_transaction_sync_pg() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let read_marker = "SELECT current_setting('app.marker', true) AS v";
    let setup = MarkerSetup(vec!["SET LOCAL app.marker = 'seen'".to_string()]);
    let connector: PgDieselConnector<MarkerSetup> =
        PgDieselConnector::with_session_setup(common::pg_connect(port));

    let (value, _) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            BuiltinKind::String,
            &setup,
        )
        .expect("scalar read");
    assert_eq!(
        value,
        Value::String("seen".into()),
        "execute_scalar setup takes hold"
    );

    let page = connector
        .read_page(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            4096,
            &setup,
        )
        .expect("page read");
    assert_eq!(
        page.value.rows[0][0],
        Value::String("seen".into()),
        "read_page setup takes hold"
    );

    let plain = PgDieselConnector::new(common::pg_connect(port));
    let (value, _) = plain
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            BuiltinKind::String,
            &(),
        )
        .expect("scalar read");
    assert_eq!(value, Value::Null, "an empty setup leaves the marker unset");
}
