//! Docker-backed integration test for [`PgR2D2DieselConnector`].
//!
//! Exercises the pool-backed PG connector: a captured `MIN(price)`
//! subscription is registered, snapshot bootstraps via the pool, then a
//! DELETE-of-the-extreme drives a re-execution that round-trips through
//! the pool. Asserts the snapshot carries a real `PgLsn` and that the
//! ScalarUpdate emitted by re-execution carries the originating event's
//! LSN.
//!
//! Gated by `#[cfg(feature = "executor-diesel-postgres-r2d2")]` in
//! `tests/it/main.rs`.
//! Run with:
//!
//! ```sh
//! cargo test --test it reexec_postgres_r2d2:: \
//!     --features executor-diesel-postgres-r2d2 \
//!     -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used)]

use crate::common;
use std::sync::Arc;
use std::time::Duration;

use diesel::r2d2::ConnectionManager;
use diesel::{sql_query, ExpressionMethods, PgConnection, QueryDsl, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Postgres, ScalarFamily, Value};
use subql::reexec::{
    AutoResolvingEngine, Connector, PgR2D2DieselConnector, SessionSetup, SnapshotResult, SyncMode,
};
use subql::{
    parse_wal2json_v2, AggregateResultValue, AggregateValueChange, DefaultIds,
    MaintenanceStopReason, MessageV2, Registered, SubscriptionEngine, SubscriptionRequest, Tier,
    TierKind,
};

mod grouped_schema {
    diesel::table! {
        orders (id) {
            id -> Integer,
            price -> Double,
            quantity -> Integer,
            status -> Text,
            bucket -> Binary,
        }
    }
}

/// One `BigInt` column, for the `count(*)` and `pg_backend_pid()` probes the
/// cursor tests make against `pg_stat_activity`.
#[derive(diesel::QueryableByName)]
struct Counted {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// Count of whatever `sql` counts, read from an observing connection.
fn scalar(observer: &mut PgConnection, sql: &str) -> i64 {
    let rows: Vec<Counted> = sql_query(sql).load(observer).expect("observe");
    rows[0].n
}

/// The server-side process id of whichever connection asks. Comparing it before
/// and after tells connection reuse from discard-and-reopen exactly, where a
/// connection count only tells it approximately.
fn backend_pid(conn: &mut PgConnection) -> i64 {
    let rows: Vec<Counted> = sql_query("SELECT pg_backend_pid()::bigint AS n")
        .load(conn)
        .expect("read backend pid");
    rows[0].n
}

/// Backends parked inside a transaction, which is what a stranded cursor looks
/// like from outside.
const IDLE_IN_TXN: &str = "SELECT count(*) AS n FROM pg_stat_activity \
     WHERE state = 'idle in transaction' AND xact_start IS NOT NULL";

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

/// One more row, for a commit that lands while a read is parked.
fn insert(id: i64) -> String {
    format!("INSERT INTO orders (id, price, quantity, status) VALUES ({id}, 7.0, 1, 'paid')")
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
) -> AutoResolvingEngine<MessageV2, DefaultIds, ParserDB, SyncMode<PgR2D2DieselConnector>> {
    let inner =
        SubscriptionEngine::<MessageV2, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {});
    AutoResolvingEngine::new(inner, SyncMode(PgR2D2DieselConnector::new(pool)))
}

fn parse_message(msg: &str) -> Vec<MessageV2> {
    parse_wal2json_v2(msg.as_bytes()).expect("wal2json parse")
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };

    // Snapshot: must come back with value=5.0 and a non-zero LSN.
    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("subscription_id exists");
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
    let mut events: Vec<MessageV2> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(msg));
    }
    assert_eq!(events.len(), 1, "expected one DELETE event");
    engine.apply(&events[0]).expect("apply dispatch");
    let notifs = engine.resolve_collect().expect("consumers dispatch");
    assert_eq!(notifs.scalar_updates.len(), 1);
    assert_eq!(notifs.scalar_updates[0].value, Value::Float(9.0));
    // The re-execution LSN should be at or after the snapshot LSN
    // (clock-monotonic). Use the event's checkpoint, propagated into
    // the ScalarUpdate.
    let event_lsn = events[0].checkpoint().expect("wal2json event LSN");
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

/// Pages of a keyless result come from one cursor in one transaction, which is
/// the only way successive pages describe a single instant. Asserts the whole
/// lifecycle against real Postgres: every row arrives exactly once across
/// pages, the byte budget bounds each page, the last page says so, and a
/// concurrent write is invisible because the snapshot predates it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_cursor_pages_one_snapshot_of_a_keyless_result() {
    use subql::reexec::{Connector, CursorError, CursorId};

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let seed: Vec<(i64, f64)> = (1..=40_u32)
        .map(|id| (i64::from(id), f64::from(id)))
        .collect();
    setup_pg(&mut conn_setup, &seed);

    let connector = PgR2D2DieselConnector::new(build_pool(port));
    // DISTINCT has no key to resume from, which is what cursors exist for.
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::owned(
                "SELECT DISTINCT id, status FROM orders WHERE id > $1 ORDER BY id".to_string(),
                vec![Value::Int(0)],
            ),
            &(),
        )
        .expect("open cursor");

    // A write committed after the cursor opened must not appear in its pages.
    let mut conn_dml = common::pg_connect(port);
    sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (999, 1.0, 1, 'late')")
        .execute(&mut conn_dml)
        .expect("concurrent insert");

    let mut ids = Vec::new();
    let mut pages = 0;
    loop {
        let page = connector.fetch_cursor(cursor, 96).expect("fetch page");
        pages += 1;
        assert!(
            page.checkpoint.is_some(),
            "a cursor's pages carry the snapshot's position"
        );
        for row in &page.value.rows {
            match row[0] {
                Value::Int(id) => ids.push(id),
                ref other => panic!("id should decode as an integer, got {other:?}"),
            }
        }
        if !page.value.more {
            break;
        }
        assert!(pages < 100, "the cursor should finish");
    }

    assert!(pages > 1, "a 40-row result should not fit one 96-byte page");
    assert_eq!(ids, (1..=40).collect::<Vec<_>>(), "every row, exactly once");

    connector.close_cursor(cursor).expect("close cursor");
    // Closing twice is not an error, so an abandoned read cannot leak a
    // transaction through a double close.
    connector.close_cursor(cursor).expect("close is idempotent");
    assert!(matches!(
        connector.fetch_cursor(cursor, 96),
        Err(CursorError::Unknown(_))
    ));
    assert!(matches!(
        connector.fetch_cursor(CursorId(9999), 96),
        Err(CursorError::Unknown(_))
    ));
}

/// End to end for the whole-re-read tier: a query the engine refuses because
/// its filter is outside the predicate language is captured instead, and a
/// change to the table it reads delivers the answer again as pages.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_captured_query_delivers_its_rows_again_when_the_table_changes() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    // Enough rows that the answer cannot fit one page, so the test exercises
    // paging rather than trivially passing on a single page.
    let seed: Vec<(i64, f64)> = (1..=40_u32)
        .map(|id| (i64::from(id), f64::from(id)))
        .collect();
    setup_pg(&mut conn_setup, &seed);

    // `lower(status)` is a function call the in-process predicate language
    // cannot evaluate, so the engine refuses and the wrapper captures. DISTINCT
    // is keyless, so the capture cannot resume by changed key and lands on the
    // whole re-read tier.
    let mut engine = build_engine(catalog(), build_pool(port)).with_max_page_bytes(64);
    let registered = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT DISTINCT id, status FROM orders WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("a filter outside the language is captured, not refused");
    let (subscription_id, tables) = match registered {
        Registered {
            subscription_id,
            tier: Tier::WholeRows { tables, .. },
            ..
        } => (subscription_id, tables),
        other => panic!("expected a whole-re-read capture, got {other:?}"),
    };
    assert_eq!(tables.len(), 1, "one table triggers this query");

    // A change to that table means the answer may have moved.
    sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (41, 1.0, 1, 'paid')")
        .execute(&mut conn_dml)
        .expect("insert");
    let msgs = common::drain_slot(&mut conn_setup, SLOT);
    let mut events: Vec<MessageV2> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(msg));
    }
    assert_eq!(events.len(), 1, "expected one INSERT event");

    let mut delivered: Vec<i64> = Vec::new();
    let mut generations = Vec::new();
    let mut finals = 0;
    let mut pages = 0;
    for event in &events {
        engine.apply(event).expect("apply");
    }
    let notifications = engine.resolve_collect().expect("dispatch");
    for update in &notifications.rows_updates {
        assert_eq!(update.subscription_id, subscription_id);
        generations.push(update.generation);
        pages += 1;
        if !update.more {
            finals += 1;
        }
        for row in &update.rows {
            match row[0] {
                Value::Int(id) => delivered.push(id),
                ref other => panic!("id should decode as an integer, got {other:?}"),
            }
        }
    }
    assert!(
        notifications.scalar_updates.is_empty(),
        "this tier delivers rows, not a scalar"
    );

    delivered.sort_unstable();
    assert_eq!(
        delivered,
        (1..=41_i64).collect::<Vec<_>>(),
        "the answer is delivered again in full, including the new row"
    );
    assert!(
        pages > 1,
        "a 41-row answer does not fit one 64-byte page, got {pages}"
    );
    assert_eq!(finals, 1, "exactly one page closes the re-read");
    assert!(
        generations.windows(2).all(|w| w[0] == w[1]),
        "pages of one re-read share a generation, got {generations:?}"
    );
    assert_eq!(generations.first().copied(), Some(1), "the first re-read");
}

/// A captured query has an answer before anything changes. Without a bootstrap
/// a subscription against a quiet database sits empty holding a correct answer
/// it was never told, which is a worse failure than a refusal because it looks
/// like success.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_captured_query_snapshots_its_answer_at_registration() {
    use subql::reexec::SnapshotResult;

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let seed: Vec<(i64, f64)> = (1..=30_u32)
        .map(|id| (i64::from(id), f64::from(id)))
        .collect();
    setup_pg(&mut conn_setup, &seed);

    // DISTINCT is keyless, so the capture lands on the whole re-read tier.
    let mut engine = build_engine(catalog(), build_pool(port)).with_max_page_bytes(64);
    let subscription_id = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT DISTINCT id, status FROM orders WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::WholeRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected a whole-re-read capture, got {other:?}"),
    };

    let snapshot = engine
        .snapshot(subscription_id)
        .expect("snapshot reads")
        .expect("the query exists");
    match snapshot {
        SnapshotResult::Rows {
            columns,
            rows,
            checkpoint,
        } => {
            assert_eq!(columns, vec!["id", "status"]);
            // Every row, though the budget forced several round trips to get
            // them, and all from one snapshot so concatenating them is sound.
            let ids: Vec<i64> = rows
                .iter()
                .map(|row| match row[0] {
                    Value::Int(id) => id,
                    ref other => panic!("id should decode as an integer, got {other:?}"),
                })
                .collect();
            assert_eq!(ids, (1..=30_i64).collect::<Vec<_>>());
            assert!(
                checkpoint.is_some(),
                "the answer is anchored to a position in the change stream"
            );
        }
        other => panic!("a whole-re-read capture snapshots as rows, got {other:?}"),
    }
}

/// A join is triggered by a change to either side. Routing keys on a table set
/// rather than one table, and this is what proves it: nothing else in the suite
/// registers a capture over more than one table.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_joined_capture_is_triggered_by_either_table() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);
    sql_query("CREATE TABLE couriers (status TEXT PRIMARY KEY, name TEXT)")
        .execute(&mut conn_setup)
        .expect("create couriers");
    sql_query("ALTER TABLE couriers REPLICA IDENTITY FULL")
        .execute(&mut conn_setup)
        .expect("replica identity");
    sql_query("INSERT INTO couriers VALUES ('paid', 'ana')")
        .execute(&mut conn_setup)
        .expect("seed courier");

    let joined_catalog = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT); \
         CREATE TABLE couriers (status TEXT PRIMARY KEY, name TEXT);",
    )
    .expect("parse joined DDL");
    let mut engine = build_engine(joined_catalog, build_pool(port));

    let tables = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT o.id, c.name FROM orders o JOIN couriers c ON c.status = o.status \
                 ORDER BY o.id",
            ),
            (),
        )
        .expect("a join is captured, not refused")
    {
        Registered {
            tier: Tier::WholeRows { tables, .. },
            ..
        } => tables,
        other => panic!("expected a whole-re-read capture, got {other:?}"),
    };
    assert_eq!(tables.len(), 2, "both sides of the join trigger it");

    // Drain the slot so only the changes below are read.
    let _ = common::drain_slot(&mut conn_setup, SLOT);

    for (label, dml) in [
        (
            "left side",
            "INSERT INTO orders (id, price, quantity, status) VALUES (3, 1.0, 1, 'paid')",
        ),
        ("right side", "INSERT INTO couriers VALUES ('late', 'bo')"),
    ] {
        sql_query(dml).execute(&mut conn_dml).expect(label);
        let msgs = common::drain_slot(&mut conn_setup, SLOT);
        let mut delivered = 0;
        for msg in &msgs {
            for event in parse_message(msg) {
                engine.apply(&event).expect("apply");
            }
        }
        let notifications = engine.resolve_collect().expect("dispatch");
        delivered += notifications.rows_updates.len();
        assert!(
            delivered > 0,
            "a change to the {label} should re-read the join, got no pages"
        );
    }
}

/// `pg_current_wal_lsn()` is not bound to the transaction snapshot.
///
/// This is the fact every position-carrying read depends on, so it is pinned
/// rather than asserted in a comment. Inside one `REPEATABLE READ` transaction
/// the position advances when another connection commits, while that commit
/// stays invisible to the snapshot. A position taken after the rows can
/// therefore sit ahead of the snapshot it claims to describe, and a caller
/// replaying the change stream from it would skip that commit entirely.
///
/// The connectors read the position before opening the snapshot for exactly
/// this reason, and the parked-read test below pins that per read. This one
/// pins the premise underneath: if a future PostgreSQL made the function
/// snapshot-bound, it fails and the ordering constraint can be revisited.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_wal_position_is_not_bound_to_the_transaction_snapshot() {
    #[derive(diesel::QueryableByName, Debug)]
    struct Lsn {
        #[diesel(sql_type = diesel::sql_types::Text)]
        lsn: String,
    }
    #[derive(diesel::QueryableByName, Debug)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut reader = common::pg_connect(port);
    let mut writer = common::pg_connect(port);

    setup_pg(&mut reader, &[(1, 5.0)]);

    sql_query("BEGIN").execute(&mut reader).expect("begin");
    sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut reader)
        .expect("isolation");
    // Establish the snapshot, then read the position it supposedly matches.
    let rows_before: Vec<Count> = sql_query("SELECT count(*) AS n FROM orders")
        .load(&mut reader)
        .expect("snapshot read");
    let at_snapshot: Vec<Lsn> = sql_query("SELECT pg_current_wal_lsn()::text AS lsn")
        .load(&mut reader)
        .expect("position at snapshot");

    sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (2, 7.0, 1, 'paid')")
        .execute(&mut writer)
        .expect("concurrent commit");

    let rows_after: Vec<Count> = sql_query("SELECT count(*) AS n FROM orders")
        .load(&mut reader)
        .expect("second snapshot read");
    let after_commit: Vec<Lsn> = sql_query("SELECT pg_current_wal_lsn()::text AS lsn")
        .load(&mut reader)
        .expect("position after commit");
    sql_query("COMMIT").execute(&mut reader).expect("commit");

    assert_eq!(
        rows_before[0].n, rows_after[0].n,
        "the snapshot does not see the concurrent commit, which is the point of \
         REPEATABLE READ"
    );
    assert_ne!(
        at_snapshot[0].lsn, after_commit[0].lsn,
        "yet the position advanced, so it is not snapshot-bound: a position read \
         after the rows would sit ahead of the snapshot and a replay from it \
         would lose the commit"
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
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_read_reports_a_position_taken_before_its_snapshot() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 5.0)]);
    let connector = Arc::new(PgR2D2DieselConnector::new(build_pool(port)));

    let held = Arc::clone(&connector);
    let sql = format!("SELECT count(*)::bigint AS v FROM orders {}", common::PARK);
    let ((value, position), after_commit) = common::park_a_read(port, &insert(2), move || {
        held.execute_scalar(
            &subql::reexec::ReadQuery::without_binds(&sql),
            ScalarFamily::Int,
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

    let held = Arc::clone(&connector);
    let sql = format!("SELECT id FROM orders {} ORDER BY id", common::PARK);
    let (page, after_commit) = common::park_a_read(port, &insert(3), move || {
        held.read_page(&subql::reexec::ReadQuery::without_binds(&sql), 1 << 20, &())
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

    let held = Arc::clone(&connector);
    let sql = format!("SELECT count(*)::bigint AS c0 FROM orders {}", common::PARK);
    let ((values, position), after_commit) = common::park_a_read(port, &insert(4), move || {
        held.execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&sql),
            &[ScalarFamily::Int],
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

/// An abandoned cursor must end its transaction and give its connection back.
///
/// A cursor pins a pooled connection inside an open transaction, so a read
/// that never reaches its close would hold both, and a permanently open read
/// transaction pins Postgres's `xmin` horizon and stops vacuum from reclaiming
/// dead tuples database-wide. `Drop` on the cursor is the mechanism, and the
/// pool has to stay alive to observe it: dropping the pool closes every socket,
/// which ends the transaction no matter what the cursor did.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_abandoned_cursor_ends_its_transaction_and_keeps_its_connection() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 10.0), (2, 20.0), (3, 30.0)]);

    // Observed from a separate connection, never through the pool under test:
    // that pool's own query reads as `active`, not `idle in transaction`.
    let mut observer = common::pg_connect(port);

    // One connection, so an abandoned cursor cannot hide behind a spare.
    let url = common::pg_url(port);
    let pool = r2d2::Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_secs(10))
        .build(ConnectionManager::<PgConnection>::new(url))
        .expect("build pool");

    // The pool holds exactly one connection, so this is the process the cursor
    // will run on and the one that must come back.
    let pooled_pid = {
        let mut only = pool.get().expect("pool pre-fills one connection");
        backend_pid(&mut only)
    };
    let connector = PgR2D2DieselConnector::new(pool.clone());
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT id, price FROM orders ORDER BY id"),
            &(),
        )
        .expect("open cursor");
    let page = connector
        .fetch_cursor(cursor, 1)
        .expect("fetch one bounded page");
    assert!(
        page.value.more,
        "a one-byte budget must leave the read unfinished, else nothing is abandoned"
    );
    assert_eq!(
        scalar(&mut observer, IDLE_IN_TXN),
        1,
        "an open cursor holds exactly one transaction while it is alive"
    );

    // Abandon the read: never close, drop the connector. The pool outlives it,
    // so every socket stays open and the cursor's own cleanup is what shows.
    drop(connector);
    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(
        scalar(&mut observer, IDLE_IN_TXN),
        0,
        "an abandoned cursor must not leave a transaction open on a live pool"
    );
    // The pool can still serve, which a stranded connection would block
    // forever on a one-connection pool.
    let mut reused = pool.get().expect("the pool still has its connection");
    assert_eq!(
        backend_pid(&mut reused),
        pooled_pid,
        "the same connection must come back reusable, not be discarded and reopened"
    );
    assert_eq!(
        scalar(&mut observer, IDLE_IN_TXN),
        0,
        "the reused connection must not carry the abandoned transaction"
    );
}

/// A cursor whose read failed must be gone, not merely unavailable.
///
/// After a failure its server-side state is unknown, so it is dropped rather
/// than left registered, and later calls behave as they do for a cursor that
/// was never opened.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_cursor_whose_read_failed_reports_as_unknown() {
    use subql::reexec::CursorError;

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 10.0), (2, 20.0), (3, 30.0)]);
    let mut observer = common::pg_connect(port);

    let connector = PgR2D2DieselConnector::new(build_pool(port));
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT id, price FROM orders ORDER BY id"),
            &(),
        )
        .expect("open cursor");

    // Drain the buffered rows so the next fetch reaches the server, otherwise
    // breaking the connection proves nothing.
    loop {
        let page = connector.fetch_cursor(cursor, 1).expect("drain");
        if !page.value.more {
            break;
        }
    }
    sql_query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE state = 'idle in transaction' AND xact_start IS NOT NULL",
    )
    .execute(&mut observer)
    .expect("terminate the cursor's backend");

    assert!(
        connector.fetch_cursor(cursor, 1).is_err(),
        "a fetch on a terminated backend must fail rather than answer"
    );
    let after = connector.fetch_cursor(cursor, 1);
    assert!(
        matches!(after, Err(CursorError::Unknown(_))),
        "a cursor whose fetch failed must be gone, got {after:?}"
    );
    connector
        .close_cursor(cursor)
        .expect("closing a gone cursor is idempotent");
}

/// Delegates everything, and panics part way through a paged read.
struct PanicMidRead {
    inner: PgR2D2DieselConnector,
    fetches: parking_lot::Mutex<usize>,
}

impl Connector for PanicMidRead {
    type AuthContext = ();
    type Error = <PgR2D2DieselConnector as Connector>::Error;
    type Checkpoint = <PgR2D2DieselConnector as Connector>::Checkpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        kind: subql::backend::ScalarFamily,
        auth: &(),
    ) -> Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error> {
        self.inner.execute_scalar(query, kind, auth)
    }

    fn read_page(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        max_bytes: usize,
        auth: &(),
    ) -> Result<
        subql::reexec::Snapshot<subql::reexec::RowPage<Postgres>, Self::Checkpoint>,
        Self::Error,
    > {
        self.inner.read_page(query, max_bytes, auth)
    }

    fn execute_scalar_row(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        kinds: &[subql::backend::ScalarFamily],
        auth: &(),
    ) -> Result<
        (Vec<Value<Postgres>>, Option<Self::Checkpoint>),
        subql::reexec::ScalarRowError<Self::Error>,
    > {
        self.inner.execute_scalar_row(query, kinds, auth)
    }

    fn open_cursor(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        auth: &(),
    ) -> Result<subql::reexec::CursorId, subql::reexec::CursorError<Self::Error>> {
        self.inner.open_cursor(query, auth)
    }

    fn fetch_cursor(
        &self,
        cursor: subql::reexec::CursorId,
        max_bytes: usize,
    ) -> Result<
        subql::reexec::Snapshot<subql::reexec::RowPage<Postgres>, Self::Checkpoint>,
        subql::reexec::CursorError<Self::Error>,
    > {
        let seen = {
            let mut fetches = self.fetches.lock();
            *fetches += 1;
            *fetches
        };
        assert!(seen != 2, "simulated failure part way through a paged read");
        self.inner.fetch_cursor(cursor, max_bytes)
    }

    fn close_cursor(
        &self,
        cursor: subql::reexec::CursorId,
    ) -> Result<(), subql::reexec::CursorError<Self::Error>> {
        self.inner.close_cursor(cursor)
    }
}

/// A panic during a read must not strand the cursor's transaction.
///
/// Unwinding runs only destructors, and `PgCursor::drop` cannot help: the
/// connector's registry holds the handle, so the entry outlives the unwind. The
/// guard in the engine's read loop is the only thing that closes it, so the
/// panic has to happen inside that loop, which means inside a connector call
/// the loop makes.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_panic_during_a_read_leaves_no_transaction_behind() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 10.0), (2, 20.0), (3, 30.0)]);
    let mut observer = common::pg_connect(port);

    let connector = PanicMidRead {
        inner: PgR2D2DieselConnector::new(build_pool(port)),
        fetches: parking_lot::Mutex::new(0),
    };
    let cat = catalog();
    let table = subql::catalog_helpers::table_id(&cat, "orders").expect("orders");
    let inner =
        SubscriptionEngine::<subql::testing::TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            cat,
            PostgreSqlDialect {},
        );
    // One row per page, so the read needs a second fetch and reaches the panic.
    let mut engine = AutoResolvingEngine::new(inner, SyncMode(connector)).with_max_page_bytes(1);
    engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT DISTINCT id, price FROM orders WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("captured");

    let event = subql::testing::TestEvent::<Postgres>::update(
        table,
        vec![
            Value::Int(1),
            Value::Float(10.0),
            Value::Int(1),
            Value::String("paid".into()),
        ],
        vec![
            Value::Int(1),
            Value::Float(11.0),
            Value::Int(1),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply succeeds");
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = engine.resolve_collect();
    }));
    assert!(unwound.is_err(), "the read must have panicked");

    assert_eq!(
        scalar(&mut observer, IDLE_IN_TXN),
        0,
        "the guard must close the cursor as the panic unwinds through the read loop"
    );
}

/// A keyed capture has starting rows too.
///
/// The tier serves later changes one row at a time, but a subscriber still has
/// to be told what the answer was before anything changed, and it cannot
/// reconstruct that from deltas: no change ever fires for a row that was
/// already in the answer and stayed there. Without this the subscription looks
/// empty and correct at the same time.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_keyed_capture_snapshots_its_rows() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn_setup = common::pg_connect(port);
    let seed: Vec<(i64, f64)> = (1..=12_u32)
        .map(|id| (i64::from(id), f64::from(id)))
        .collect();
    setup_pg(&mut conn_setup, &seed);
    // One row outside the filter, so a snapshot that ignored the WHERE would
    // deliver it and be caught.
    sql_query("UPDATE orders SET status = 'void' WHERE id = 7")
        .execute(&mut conn_setup)
        .expect("take one row out of the answer");

    // A page budget below one row's size, so the answer only arrives complete
    // if the read pages. A single-page read would pass with the paging deleted.
    let mut engine = build_engine(catalog(), build_pool(port)).with_max_page_bytes(1);
    let subscription_id = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT * FROM orders WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::KeyedRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected a keyed capture, got {other:?}"),
    };

    let snapshot = engine
        .snapshot(subscription_id)
        .expect("snapshot reads")
        .expect("the query exists");
    match snapshot {
        SnapshotResult::Rows {
            columns,
            rows,
            checkpoint,
        } => {
            assert_eq!(columns, vec!["id", "price", "quantity", "status"]);
            let ids: Vec<i64> = rows
                .iter()
                .map(|row| match row[0] {
                    Value::Int(id) => id,
                    ref other => panic!("id should decode as an integer, got {other:?}"),
                })
                .collect();
            assert_eq!(
                ids,
                (1..=12_i64).filter(|id| *id != 7).collect::<Vec<_>>(),
                "every matching row and only the matching rows"
            );
            assert!(
                checkpoint.is_some(),
                "the starting rows are anchored to a position in the change stream, \
                 which is what lets a consumer replay from there without a gap"
            );
        }
        other => panic!("a keyed capture snapshots as rows, got {other:?}"),
    }
}

/// A keyless change replaces a keyed read and the sync connector resolves the
/// complete row read in the same dispatch.
#[test]
#[ignore = "requires Docker, run with --ignored"]
fn a_keyless_change_transitions_and_runs_the_sync_replacement_read() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn_setup = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), build_pool(port));
    let subscription_id = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT * FROM orders WHERE lower(status) = 'paid'",
            ),
            (),
        )
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::KeyedRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected a keyed capture, got {other:?}"),
    };

    let events = parse_message(r#"{"action":"U","schema":"public","table":"orders"}"#);
    let notifications = engine
        .apply(&events[0])
        .expect("keyless change transitions");
    let resolved = engine.resolve_collect().expect("keyless change re-reads");

    assert_eq!(notifications.transitions.len(), 1);
    assert_eq!(
        notifications.transitions[0].subscription_id,
        subscription_id
    );
    assert_eq!(notifications.transitions[0].from, TierKind::KeyedRows);
    let Tier::WholeRows { tables, .. } = &notifications.transitions[0].to else {
        panic!("expected whole-row replacement")
    };
    assert_eq!(tables.len(), 1);
    assert_eq!(
        notifications.transitions[0].reason,
        MaintenanceStopReason::KeyedChangeWithoutKey {
            table_id: tables[0],
        }
    );
    let mut ids: Vec<_> = resolved
        .rows_updates
        .iter()
        .flat_map(|update| &update.rows)
        .map(|row| match row[0] {
            Value::Int(id) => id,
            ref other => panic!("id should decode as an integer, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(engine.pending_read_count(), 0);
}

/// The sync wrapper seeds grouped extrema and resolves a displaced group.
#[test]
#[ignore = "requires Docker, run with --ignored"]
fn grouped_min_snapshots_and_rereads_one_group_sync() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 5.0), (2, 9.0), (3, 11.0)]);
    // Diesel has no DDL query builder.
    sql_query(r"ALTER TABLE orders ADD COLUMN bucket BYTEA NOT NULL DEFAULT '\x01'")
        .execute(&mut conn)
        .expect("add byte group");
    diesel::update(grouped_schema::orders::table.find(3))
        .set(grouped_schema::orders::bucket.eq(vec![2u8]))
        .execute(&mut conn)
        .expect("seed second group");
    let _ = common::drain_slot(&mut conn, SLOT);

    let grouped_catalog = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (
            id INT PRIMARY KEY,
            price FLOAT,
            quantity INT,
            status TEXT,
            bucket BYTEA
        );",
    )
    .expect("parse grouped catalog");
    let mut engine = build_engine(grouped_catalog, build_pool(port));
    let subscription = match engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT bucket, MIN(price) FROM orders GROUP BY bucket",
            ),
            (),
        )
        .expect("grouped min registers")
    {
        Registered {
            subscription_id,
            tier: Tier::GroupedScalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected grouped scalar tier, got {other:?}"),
    };
    let snapshot = engine
        .snapshot(subscription)
        .expect("snapshot reads")
        .expect("subscription exists");
    let SnapshotResult::GroupedAggregate { updates, .. } = snapshot else {
        panic!("expected grouped aggregate snapshot")
    };
    assert_eq!(updates.len(), 2);
    let bucket_one = updates
        .iter()
        .find(|update| {
            update.change
                == AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Float(5.0)))
        })
        .and_then(|update| update.group.clone())
        .expect("first byte group");

    diesel::delete(grouped_schema::orders::table.find(1))
        .execute(&mut conn)
        .expect("delete current minimum");
    let events: Vec<_> = common::drain_slot(&mut conn, SLOT)
        .iter()
        .flat_map(|message| parse_message(message))
        .collect();
    engine.apply(&events[0]).expect("apply");
    let output = engine.resolve_collect().expect("group re-read");
    assert_eq!(engine.pending_read_count(), 0, "no pending reads");
    assert_eq!(output.aggregate_updates.len(), 1);
    assert_eq!(
        output.aggregate_updates[0].group.as_ref(),
        Some(&bucket_one)
    );
    assert_eq!(
        output.aggregate_updates[0].change,
        AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Float(9.0)))
    );
}

/// An aggregate over a Postgres integer narrower than `bigint` decodes: the
/// connector casts the read to eight bytes rather than requiring the caller
/// to spell `::bigint` in the SQL.
#[test]
#[ignore = "requires Docker"]
fn a_scalar_over_a_narrow_integer_column_decodes() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 5.0), (2, 9.0)]);
    sql_query("CREATE TABLE probe (small INT, tiny SMALLINT)")
        .execute(&mut conn)
        .expect("probe table");
    sql_query("INSERT INTO probe VALUES (7, 3), (9, 4)")
        .execute(&mut conn)
        .expect("probe rows");

    let connector = PgR2D2DieselConnector::new(build_pool(port));
    let (value, _) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds("SELECT MIN(quantity) FROM orders"),
            ScalarFamily::Int,
            &(),
        )
        .expect("INT column decodes");
    assert_eq!(value, Value::Int(1));
    let (value, _) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds("SELECT MIN(small) FROM probe"),
            ScalarFamily::Int,
            &(),
        )
        .expect("INT column decodes");
    assert_eq!(value, Value::Int(7));
    let (value, _) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds("SELECT MAX(tiny) FROM probe"),
            ScalarFamily::Int,
            &(),
        )
        .expect("SMALLINT column decodes");
    assert_eq!(value, Value::Int(4));
}

/// A borrowed-list session setup, mirroring what a caller builds per read.
struct MarkerSetup(Vec<String>);

impl SessionSetup for MarkerSetup {
    fn setup_statements(&self) -> &[String] {
        &self.0
    }
}

/// The session-setup seam runs its statements inside the transaction that
/// serves each read, the cursor's held transaction included. `SET LOCAL` takes
/// hold only inside a transaction and only until it ends, so a read that sees
/// the value proves the setup ran in that read's own transaction, before the
/// caller's SQL. An empty setup leaves the marker unset.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn session_setup_runs_inside_each_read_transaction() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 5.0)]);

    let read_marker = "SELECT current_setting('app.marker', true) AS v";
    let setup = MarkerSetup(vec!["SET LOCAL app.marker = 'seen'".to_string()]);
    let connector: PgR2D2DieselConnector<MarkerSetup> =
        PgR2D2DieselConnector::with_session_setup(build_pool(port));

    let (value, _) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            ScalarFamily::String,
            &setup,
        )
        .expect("scalar read");
    assert_eq!(
        value,
        Value::String("seen".into()),
        "execute_scalar setup takes hold in its transaction"
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
        "read_page setup takes hold in its transaction"
    );

    // The cursor's held transaction: setup runs at open, before the DECLARE, so
    // the cursor's snapshot carries the value.
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            &setup,
        )
        .expect("open cursor");
    let page = connector.fetch_cursor(cursor, 4096).expect("fetch");
    assert_eq!(
        page.value.rows[0][0],
        Value::String("seen".into()),
        "the cursor's held transaction ran the setup before the DECLARE"
    );
    connector.close_cursor(cursor).expect("close");

    // No setup: the marker is never set, so current_setting is NULL.
    let plain = PgR2D2DieselConnector::new(build_pool(port));
    let (value, _) = plain
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            ScalarFamily::String,
            &(),
        )
        .expect("scalar read");
    assert_eq!(value, Value::Null, "an empty setup leaves the marker unset");
}

/// Backends parked inside a transaction, aborted or not. A failed cursor open
/// aborts its transaction before it is rolled back, and an aborted one is a
/// distinct `pg_stat_activity` state, so the plain probe above would miss it.
const IDLE_IN_ANY_TXN: &str = "SELECT count(*) AS n FROM pg_stat_activity \
     WHERE state LIKE 'idle in transaction%' AND xact_start IS NOT NULL";

/// A backend running a cursor `FETCH` right now, which is what a reader
/// holding the cursor looks like from another connection.
const FETCH_RUNNING: &str = "SELECT count(*) AS n FROM pg_stat_activity \
     WHERE state = 'active' AND query LIKE 'FETCH FORWARD%'";

/// A cursor's held transaction is the same read snapshot every other read
/// gets: repeatable read, and read only. The cursor's own SQL is what
/// observes it, evaluated when the rows are fetched.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_cursor_holds_a_read_only_repeatable_read_snapshot() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 5.0)]);

    let connector = PgR2D2DieselConnector::new(build_pool(port));
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds(
                "SELECT current_setting('transaction_isolation') AS iso, \
                 current_setting('transaction_read_only') AS ro",
            ),
            &(),
        )
        .expect("open cursor");
    let page = connector.fetch_cursor(cursor, 4096).expect("fetch");
    assert_eq!(
        page.value.rows[0],
        vec![
            Value::String("repeatable read".into()),
            Value::String("on".into())
        ],
        "the cursor pages one repeatable-read snapshot that refuses writes"
    );
    connector.close_cursor(cursor).expect("close");

    // And a write reached through the cursor is refused for being read only,
    // not merely absent. `DECLARE` rejects a data-modifying `WITH` outright,
    // so the write has to be one a plain `SELECT` can carry: `nextval` is.
    sql_query("CREATE SEQUENCE probe_seq")
        .execute(&mut conn)
        .expect("create the sequence");
    let writing = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT nextval('probe_seq') AS n"),
            &(),
        )
        .expect("the declaration itself is accepted");
    let refused = connector
        .fetch_cursor(writing, 4096)
        .expect_err("advancing a sequence inside the read snapshot is refused");
    assert!(
        refused.to_string().contains("read-only transaction"),
        "refused for being read-only rather than for any other reason, got {refused}"
    );
}

/// One page of a cursor as its integer first column, with the flag saying
/// whether more rows are waiting. A tight budget, so a result of any size
/// arrives over several pages.
fn page_of_ids(
    connector: &PgR2D2DieselConnector,
    cursor: subql::reexec::CursorId,
) -> (Vec<i64>, bool) {
    let page = connector.fetch_cursor(cursor, 32).expect("fetch a page");
    let ids = page
        .value
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int(id) => *id,
            other => panic!("id should decode as an integer, got {other:?}"),
        })
        .collect();
    (ids, page.value.more)
}

/// Two cursors open at once are two cursors: distinct ids, distinct
/// server-side names, and pages that do not bleed into each other. They share
/// nothing but the pool.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn two_cursors_open_at_once_page_independently() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    let seed: Vec<(i64, f64)> = (1..=6_u32)
        .map(|id| (i64::from(id), f64::from(id)))
        .collect();
    setup_pg(&mut conn, &seed);

    let connector = PgR2D2DieselConnector::new(build_pool(port));
    let (ascending, descending) = open_ordered_cursors(&connector);
    assert_ne!(
        ascending, descending,
        "each open must hand back its own cursor id"
    );

    let (up, down) = drain_interleaved(&connector, ascending, descending);
    assert_eq!(
        up,
        (1..=6).collect::<Vec<_>>(),
        "the ascending cursor's rows"
    );
    assert_eq!(
        down,
        (1..=6).rev().collect::<Vec<_>>(),
        "the descending cursor's rows, unmixed with the other's"
    );
    connector.close_cursor(ascending).expect("close ascending");
    connector
        .close_cursor(descending)
        .expect("close descending");
}

/// One cursor over the ids ascending and one over them descending, so a page
/// from either says plainly which cursor answered.
fn open_ordered_cursors(
    connector: &PgR2D2DieselConnector,
) -> (subql::reexec::CursorId, subql::reexec::CursorId) {
    let ascending = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT id FROM orders ORDER BY id"),
            &(),
        )
        .expect("open the ascending cursor");
    let descending = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT id FROM orders ORDER BY id DESC"),
            &(),
        )
        .expect("open the descending cursor");
    (ascending, descending)
}

/// Page both cursors to exhaustion, one page each in turn, so a shared
/// registration would show up as one cursor answering both readers.
fn drain_interleaved(
    connector: &PgR2D2DieselConnector,
    ascending: subql::reexec::CursorId,
    descending: subql::reexec::CursorId,
) -> (Vec<i64>, Vec<i64>) {
    let mut up = Vec::new();
    let mut down = Vec::new();
    loop {
        let (ascending_page, ascending_more) = page_of_ids(connector, ascending);
        let (descending_page, descending_more) = page_of_ids(connector, descending);
        up.extend(ascending_page);
        down.extend(descending_page);
        assert!(
            up.len() <= 6 && down.len() <= 6,
            "both cursors should finish"
        );
        if !ascending_more && !descending_more {
            return (up, down);
        }
    }
}

/// A cursor is serial, so a second reader arriving while one is in flight is
/// told the cursor is busy rather than made to wait on a resource it cannot
/// queue for. Waiting would hide the caller's own bug in a stall.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_second_reader_of_one_cursor_is_told_it_is_busy() {
    use std::time::Instant;
    use subql::reexec::CursorError;

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 1.0), (2, 2.0), (3, 3.0)]);

    let connector = Arc::new(PgR2D2DieselConnector::new(build_pool(port)));
    // One second of server-side sleep per row, so the first read holds the
    // cursor for seconds while the second one asks.
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds(
                "SELECT id, pg_sleep(1) IS NULL AS slept FROM orders ORDER BY id",
            ),
            &(),
        )
        .expect("open cursor");

    let reader = {
        let connector = Arc::clone(&connector);
        std::thread::spawn(move || connector.fetch_cursor(cursor, 1 << 20))
    };

    // Wait for the server to report the reader's `FETCH` running, which is
    // the only moment that proves the reader holds the cursor. A channel send
    // before the call would prove only that the thread was about to ask, and
    // then this thread could take the cursor first and do the slow read
    // itself, leaving nothing to contend with.
    let mut observer = common::pg_connect(port);
    let deadline = Instant::now() + Duration::from_secs(30);
    while scalar(&mut observer, FETCH_RUNNING) == 0 {
        assert!(
            Instant::now() < deadline,
            "the first reader never reached the server"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let contended = connector.fetch_cursor(cursor, 1 << 20);
    assert!(
        matches!(contended, Err(CursorError::Busy(id)) if id == cursor),
        "a second reader must be told this cursor is busy, naming it, rather \
         than blocked until the first read ends, got {contended:?}"
    );

    reader
        .join()
        .expect("the reading thread finishes")
        .expect("the first read succeeds");
    connector.close_cursor(cursor).expect("close");
}

/// A cursor that fails to open rolls its transaction back: it keeps the pooled
/// connection, and leaves no backend parked in a transaction. Dropping the
/// connection would also end the transaction, at the cost of the connection.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_cursor_that_fails_to_open_keeps_its_connection_and_leaves_no_transaction() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    setup_pg(&mut conn, &[(1, 1.0)]);
    let mut observer = common::pg_connect(port);

    // One connection, so the pid before and after names the same slot.
    let manager = ConnectionManager::<PgConnection>::new(common::pg_url(port));
    let pool = r2d2::Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_secs(10))
        .build(manager)
        .expect("build a single-connection pool");
    let before = backend_pid(&mut pool.get().expect("borrow the connection"));

    let connector = PgR2D2DieselConnector::new(pool.clone());
    let failed = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT id FROM no_such_table"),
            &(),
        )
        .expect_err("a cursor over a missing table cannot open");
    assert!(
        failed.to_string().contains("no_such_table"),
        "the failure names what the database refused, got {failed}"
    );

    assert_eq!(
        scalar(&mut observer, IDLE_IN_ANY_TXN),
        0,
        "a failed open leaves no backend parked in a transaction"
    );
    assert_eq!(
        backend_pid(&mut pool.get().expect("borrow the connection again")),
        before,
        "and the connection is reusable rather than discarded"
    );
}

/// Every page of a cursor names its columns, including the last one. The
/// terminal `FETCH` returns no rows, and a page whose names came from it would
/// arrive unlabelled at a caller that decodes rows by column name.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_page_of_a_cursor_names_its_columns() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn = common::pg_connect(port);
    let seed: Vec<(i64, f64)> = (1..=8_u32)
        .map(|id| (i64::from(id), f64::from(id)))
        .collect();
    setup_pg(&mut conn, &seed);

    let connector = PgR2D2DieselConnector::new(build_pool(port));
    let cursor = connector
        .open_cursor(
            &subql::reexec::ReadQuery::without_binds("SELECT id, status FROM orders ORDER BY id"),
            &(),
        )
        .expect("open cursor");

    let mut pages = 0;
    loop {
        // A tight budget, so the rows arrive over several pages and the last
        // one is reached with the buffer already empty.
        let page = connector.fetch_cursor(cursor, 24).expect("fetch a page");
        pages += 1;
        assert_eq!(
            page.value.columns,
            vec!["id".to_string(), "status".to_string()],
            "page {pages} must name its columns, last page included"
        );
        if !page.value.more {
            break;
        }
        assert!(pages < 50, "the cursor should finish");
    }
    assert!(pages > 1, "the budget should have split this result");
    connector.close_cursor(cursor).expect("close");
}
