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
use subql::backend::{CdcEvent, Postgres, Value};
use subql::reexec::{
    AutoResolvingEngine, PgR2D2DieselConnector, ReExecEngine, Registered, SnapshotResult,
};
use subql::{parse_wal2json_v2, DefaultIds, MessageV2, SubscriptionEngine, SubscriptionRequest};

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
) -> AutoResolvingEngine<MessageV2, DefaultIds, ParserDB, PgR2D2DieselConnector> {
    let inner =
        SubscriptionEngine::<MessageV2, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {});
    AutoResolvingEngine::new(ReExecEngine::new(inner), PgR2D2DieselConnector::new(pool))
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
        Registered::ReExec { query_id, .. } => query_id,
        other => panic!("expected ReExec, got {other:?}"),
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
    let mut events: Vec<MessageV2> = Vec::new();
    for msg in &msgs {
        events.extend(parse_message(msg));
    }
    assert_eq!(events.len(), 1, "expected one DELETE event");

    let notifs = engine.consumers(&events[0]).expect("consumers dispatch");
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
        .open_cursor("SELECT DISTINCT id, status FROM orders ORDER BY id", &())
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
    use subql::reexec::Registered;

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

    // `lower(status)` is a function call, which the in-process predicate
    // language cannot evaluate, so the engine refuses and the wrapper captures.
    let mut engine = build_engine(catalog(), build_pool(port)).with_max_page_bytes(64);
    let registered = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(
                1u64,
                "SELECT id, status FROM orders WHERE lower(status) = 'paid' ORDER BY id",
            ),
            (),
        )
        .expect("a filter outside the language is captured, not refused");
    let (query_id, tables) = match registered {
        Registered::Captured {
            query_id, tables, ..
        } => (query_id, tables),
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
        let notifications = engine.consumers(event).expect("dispatch");
        for update in &notifications.rows_updates {
            assert_eq!(update.query_id, query_id);
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
    }

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
