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

/// One `BigInt` column, for the `count(*)` and `pg_backend_pid()` probes the
/// cursor tests make against `pg_stat_activity`.
#[derive(diesel::QueryableByName)]
struct Counted {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// Count of whatever `sql` counts, read from an observing sync connection.
fn count(observer: &mut PgConnection, sql: &str) -> i64 {
    let rows: Vec<Counted> = RunQueryDsl::load(sql_query(sql), observer).expect("observe");
    rows[0].n
}

/// Backends parked inside a transaction, which is what a cancelled read leaves
/// behind when its transaction is invisible to the pool.
const IDLE_IN_TXN: &str = "SELECT count(*) AS n FROM pg_stat_activity \
     WHERE state = 'idle in transaction' AND xact_start IS NOT NULL";

/// Poll a future exactly once with a waker that does nothing.
///
/// Deterministic suspension: a network round trip cannot complete in one poll,
/// and unlike a timer this does not depend on the runtime's timer granularity
/// being coarser than a local database round trip, which it is not.
fn poll_once<F: core::future::Future>(fut: &mut core::pin::Pin<Box<F>>) -> bool {
    struct NoopWake;
    impl std::task::Wake for NoopWake {
        fn wake(self: std::sync::Arc<Self>) {}
    }
    let waker = std::sync::Arc::new(NoopWake).into();
    let mut cx = core::task::Context::from_waker(&waker);
    fut.as_mut().poll(&mut cx).is_pending()
}

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

/// A cancelled row-returning read must not poison the pool.
///
/// This is the case a destructor cannot cover: ending a transaction needs I/O,
/// and dropping a future runs no I/O. What makes it safe is opening the
/// cursor's transaction through diesel's transaction manager, so the pool sees
/// the released connection is still in a transaction and discards it. With a
/// raw `BEGIN` diesel's depth stays zero, the pool believes the connection is
/// clean, and the next caller silently inherits an open transaction.
#[test]
#[ignore = "requires docker"]
fn a_cancelled_read_does_not_hand_its_transaction_to_the_next_caller() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut setup = common::pg_connect(port);
    setup_pg(&mut setup, &[(1, 10.0), (2, 20.0), (3, 30.0)]);

    let mut observer = common::pg_connect(port);

    common::multi_thread_rt().block_on(async move {
        // One connection, so the next caller must reuse whatever the cancelled
        // read left behind. That is the whole question.
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(common::pg_url(port));
        let pool: Pool<AsyncPgConnection> = Pool::builder()
            .max_size(1)
            .build(manager)
            .await
            .expect("build pool");
        let connector = PgAsyncDieselConnector::new(pool.clone());

        // Cancel the read between opening the cursor and closing it, which is
        // what an elapsed deadline or a lost `select!` race does.
        let cancelled = async {
            let cursor = connector
                .open_cursor("SELECT id, price FROM orders ORDER BY id", &())
                .await
                .expect("open cursor");
            let page = connector
                .fetch_cursor(cursor, 1)
                .await
                .expect("fetch one bounded page");
            assert!(page.value.more, "the read must be left unfinished");
            tokio::time::sleep(core::time::Duration::from_secs(30)).await;
            connector.close_cursor(cursor).await.expect("close");
        };
        tokio::select! {
            () = cancelled => panic!("the read was supposed to be cancelled"),
            () = tokio::time::sleep(core::time::Duration::from_millis(400)) => {}
        }

        // The cursor is still registered, holding its connection: nothing ran.
        // Dropping the connector is what releases it, exactly as dropping the
        // engine would.
        drop(connector);
        tokio::time::sleep(core::time::Duration::from_millis(500)).await;

        assert_eq!(
            count(&mut observer, IDLE_IN_TXN),
            0,
            "a cancelled read must leave no transaction open"
        );

        // And the pool is still usable, with a session that carries nothing.
        let mut next = pool.get().await.expect("pool still serves");
        diesel_async::RunQueryDsl::execute(
            sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (99, 1.0, 1, 'x')"),
            &mut next,
        )
        .await
        .expect("insert on the next checkout");
        drop(next);
        tokio::time::sleep(core::time::Duration::from_millis(300)).await;
        assert_eq!(
            count(
                &mut observer,
                "SELECT count(*) AS n FROM orders WHERE id = 99"
            ),
            1,
            "the next caller's write must survive, not be eaten by an inherited transaction"
        );
    });
}

/// A cursor is serial, and the two ways a fetch can find it unavailable are
/// different things the caller needs told apart.
///
/// A concurrent fetch must say the cursor is busy, not that it does not exist,
/// or a caller goes looking for a close it never made. A fetch that fails
/// leaves the cursor's server-side state unknown, so it is dropped rather than
/// re-registered, and the next fetch must say it is gone.
#[test]
#[ignore = "requires docker"]
fn a_busy_cursor_and_a_broken_one_report_differently() {
    use subql::reexec::CursorError;

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut setup = common::pg_connect(port);
    setup_pg(&mut setup, &[(1, 10.0), (2, 20.0), (3, 30.0)]);
    let mut observer = common::pg_connect(port);

    common::multi_thread_rt().block_on(async move {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(common::pg_url(port));
        let pool: Pool<AsyncPgConnection> = Pool::builder()
            .max_size(2)
            .build(manager)
            .await
            .expect("build pool");
        let connector = std::sync::Arc::new(PgAsyncDieselConnector::new(pool));

        let cursor = connector
            .open_cursor("SELECT id, price FROM orders ORDER BY id", &())
            .await
            .expect("open cursor");

        // Deterministic contention rather than a race between two tasks: one
        // poll leaves the first read in flight holding the cursor, and the
        // second call then has to say what it finds.
        let mut holding = Box::pin(connector.fetch_cursor(cursor, 1));
        assert!(
            poll_once(&mut holding),
            "one poll must leave the first read in flight, else nothing contends"
        );
        let contended = connector.fetch_cursor(cursor, 1).await;
        assert!(
            matches!(contended, Err(CursorError::Busy(_))),
            "a second reader must be told the cursor is busy, not that it is \
             unknown, got {contended:?}"
        );
        holding.await.expect("the first read finishes");

        // Drain the buffered rows first. A fetch served from the leftover
        // buffer never touches the server, so terminating the backend before
        // the buffer is empty would prove nothing.
        loop {
            let page = connector
                .fetch_cursor(cursor, 1)
                .await
                .expect("drain the buffer");
            if !page.value.more {
                break;
            }
        }

        // Now break the cursor's connection underneath it, so its next fetch
        // fails for real, and check the cursor is not left registered.
        {
            use diesel::RunQueryDsl as S;
            S::execute(
                sql_query(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE state = 'idle in transaction' AND xact_start IS NOT NULL",
                ),
                &mut observer,
            )
            .expect("terminate the cursor's backend");
        }
        let failed = connector.fetch_cursor(cursor, 1).await;
        assert!(
            failed.is_err(),
            "a fetch on a terminated backend must fail rather than answer"
        );
        let after = connector.fetch_cursor(cursor, 1).await;
        assert!(
            matches!(after, Err(CursorError::Unknown(_))),
            "a cursor whose fetch failed must be gone, not busy or alive, got {after:?}"
        );
        // Closing what is already gone is not an error, so an abandoned read
        // cannot fail through a double close.
        connector
            .close_cursor(cursor)
            .await
            .expect("closing a gone cursor is idempotent");
    });
}

/// Closing a cursor while a read of it is in flight must not orphan it.
///
/// The read holds the cursor outside the registry for the duration, so a close
/// arriving meanwhile finds nothing and could report success while the read
/// then puts the cursor back. That would leave a pooled connection and an open
/// repeatable-read transaction held for the connector's whole life, and a
/// long-lived read transaction stops Postgres reclaiming dead tuples anywhere.
#[test]
#[ignore = "requires docker"]
fn closing_a_cursor_during_a_read_does_not_orphan_it() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut setup = common::pg_connect(port);
    setup_pg(&mut setup, &[(1, 10.0), (2, 20.0), (3, 30.0)]);
    let mut observer = common::pg_connect(port);

    common::multi_thread_rt().block_on(async move {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(common::pg_url(port));
        let pool: Pool<AsyncPgConnection> = Pool::builder()
            .max_size(4)
            .build(manager)
            .await
            .expect("build pool");
        let connector = PgAsyncDieselConnector::new(pool);

        let cursor = connector
            .open_cursor("SELECT id, price FROM orders ORDER BY id", &())
            .await
            .expect("open cursor");

        // Deterministically suspend a read mid-flight: one poll cannot finish a
        // network round trip, and a zero deadline returns without dropping the
        // future, so the cursor is genuinely in use when the close arrives.
        let mut reading = Box::pin(connector.fetch_cursor(cursor, 1));
        assert!(
            poll_once(&mut reading),
            "one poll must leave the read in flight, else nothing is being raced"
        );

        connector
            .close_cursor(cursor)
            .await
            .expect("closing a cursor being read is not an error");

        // Let the read finish. It must not resurrect the closed cursor.
        let _ = reading.await;
        tokio::time::sleep(core::time::Duration::from_millis(500)).await;
        assert_eq!(
            count(&mut observer, IDLE_IN_TXN),
            0,
            "a cursor closed during a read must not be put back holding a transaction"
        );
        let after = connector.fetch_cursor(cursor, 1).await;
        assert!(
            matches!(after, Err(subql::reexec::CursorError::Unknown(_))),
            "a closed cursor must stay closed, got {after:?}"
        );
    });
}

/// A cancelled read must not leave its cursor id poisoned.
///
/// Cancellation runs no cleanup, so anything the read marked has to be undone
/// by a destructor. A cursor id stuck in a "being read" state answers every
/// later caller with "busy" forever, for a cursor that no longer exists.
#[test]
#[ignore = "requires docker"]
fn a_cancelled_read_does_not_poison_its_cursor_id() {
    use subql::reexec::CursorError;

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut setup = common::pg_connect(port);
    setup_pg(&mut setup, &[(1, 10.0), (2, 20.0), (3, 30.0)]);

    common::multi_thread_rt().block_on(async move {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(common::pg_url(port));
        let pool: Pool<AsyncPgConnection> = Pool::builder()
            .max_size(4)
            .build(manager)
            .await
            .expect("build pool");
        let connector = PgAsyncDieselConnector::new(pool);

        let cursor = connector
            .open_cursor("SELECT id, price FROM orders ORDER BY id", &())
            .await
            .expect("open cursor");

        // Cancel deterministically: poll once, which cannot complete a network
        // round trip, then drop the future. A timer race is not reliable here,
        // because tokio's timer granularity is coarser than a local round trip.
        {
            let mut reading = Box::pin(connector.fetch_cursor(cursor, 1));
            assert!(
                poll_once(&mut reading),
                "one poll must leave the read in flight, else nothing is cancelled"
            );
        }

        // The cursor is gone, because a cancelled read cannot put it back and
        // its connection is discarded. So say it is gone, not that it is busy.
        let after = connector.fetch_cursor(cursor, 1).await;
        assert!(
            matches!(after, Err(CursorError::Unknown(_))),
            "a cancelled read must leave the cursor unknown, not busy forever, got {after:?}"
        );
        connector
            .close_cursor(cursor)
            .await
            .expect("closing a cancelled cursor is idempotent");
    });
}

/// The keyed tier delivers per-row changes through the async engine.
///
/// Everything below this point had only been proven on the sync engine or at
/// the connector's own surface. This drives a real change through the async
/// engine's resolve and checks what a consumer actually receives.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_keyed_tier_delivers_row_deltas_through_the_async_engine() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    common::multi_thread_rt().block_on(async move {
        let pool = pg_async_pool(port).await;
        let mut engine = build_engine(catalog(), pool);

        // `lower(status)` is a function call the in-process language cannot
        // evaluate, over one table with a primary key, so this is the keyed tier.
        let query_id = match engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(
                    1u64,
                    "SELECT * FROM orders WHERE lower(status) = 'paid'",
                ),
                (),
            )
            .expect("captured")
        {
            Registered::Captured {
                query_id,
                tier: subql::reexec::CaptureTier::ChangedRowsOnly,
                ..
            } => query_id,
            other => panic!("expected a keyed capture, got {other:?}"),
        };

        // Row 2 leaves the answer, row 1 stays in it.
        sql_query("UPDATE orders SET status = 'void' WHERE id = 2")
            .execute(&mut conn_dml)
            .expect("update");
        sql_query("UPDATE orders SET price = 7.0 WHERE id = 1")
            .execute(&mut conn_dml)
            .expect("update");
        let msgs = common::drain_slot(&mut conn_setup, SLOT);
        let events: Vec<MessageV2> = msgs.iter().flat_map(|m| parse_message(m)).collect();
        assert!(!events.is_empty(), "the slot must carry both updates");

        let mut deltas = Vec::new();
        for event in &events {
            let notifs = engine.consumers(event).await.expect("dispatch");
            assert!(
                notifs.scalar_updates.is_empty(),
                "a row query is not a scalar, got {:?}",
                notifs.scalar_updates
            );
            assert!(
                notifs.rows_updates.is_empty(),
                "the keyed tier delivers per-row deltas, never whole pages: a page here \
                 means the tier was decided again at resolve time and got it wrong"
            );
            deltas.extend(notifs.row_deltas);
        }

        assert!(
            deltas.iter().all(|d| d.query_id == query_id),
            "every delta belongs to the registered query"
        );
        let removed: Vec<_> = deltas
            .iter()
            .filter(|d| d.row.is_none())
            .map(|d| d.key.clone())
            .collect();
        let upserted: Vec<_> = deltas
            .iter()
            .filter(|d| d.row.is_some())
            .map(|d| d.key.clone())
            .collect();
        assert_eq!(
            removed,
            vec![vec![Value::Int(2)]],
            "row 2 stopped matching, so it arrives as a removal"
        );
        assert_eq!(
            upserted,
            vec![vec![Value::Int(1)]],
            "row 1 still matches, so it arrives as itself"
        );
        let row1 = deltas
            .iter()
            .find(|d| d.row.is_some())
            .and_then(|d| d.row.clone())
            .expect("row 1's current values");
        assert!(
            row1.contains(&Value::Float(7.0)),
            "the delivered row carries the new price, got {row1:?}"
        );
    });
}

/// The whole-re-read tier delivers pages through the async engine.
///
/// This is the path that needs a cursor, so it exercises the async cursor from
/// the engine rather than from a test calling the connector directly.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_whole_reread_tier_delivers_pages_through_the_async_engine() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0), (3, 11.0)]);

    common::multi_thread_rt().block_on(async move {
        let pool = pg_async_pool(port).await;
        // One row per page, so the cursor has to page and `more` has to be
        // right, which one big page would never test.
        let mut engine = build_engine(catalog(), pool).with_max_page_bytes(1);

        // `DISTINCT` has no key to resume from, so this is the whole-re-read
        // tier rather than the keyed one.
        let query_id = match engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(
                    1u64,
                    "SELECT DISTINCT status FROM orders WHERE lower(status) = 'paid'",
                ),
                (),
            )
            .expect("captured")
        {
            Registered::Captured {
                query_id,
                tier: subql::reexec::CaptureTier::WholeReread,
                ..
            } => query_id,
            other => panic!("expected a whole-re-read capture, got {other:?}"),
        };

        sql_query("INSERT INTO orders (id, price, quantity, status) VALUES (4, 1.0, 1, 'paid')")
            .execute(&mut conn_dml)
            .expect("insert");
        let msgs = common::drain_slot(&mut conn_setup, SLOT);
        let events: Vec<MessageV2> = msgs.iter().flat_map(|m| parse_message(m)).collect();
        assert!(!events.is_empty(), "the slot must carry the insert");

        let mut pages = Vec::new();
        for event in &events {
            let notifs = engine.consumers(event).await.expect("dispatch");
            assert!(
                notifs.row_deltas.is_empty(),
                "this tier delivers pages, not per-row deltas"
            );
            pages.extend(notifs.rows_updates);
        }

        assert!(!pages.is_empty(), "a change must produce at least one page");
        assert!(
            pages.iter().all(|p| p.query_id == query_id),
            "every page belongs to the registered query"
        );
        let generation = pages[0].generation;
        assert!(
            pages.iter().all(|p| p.generation == generation),
            "pages of one re-read share a generation, got {:?}",
            pages.iter().map(|p| p.generation).collect::<Vec<_>>()
        );
        assert!(
            !pages[pages.len() - 1].more,
            "the last page must say there is no more"
        );
        assert!(
            pages[..pages.len() - 1].iter().all(|p| p.more),
            "every page but the last must say there is more"
        );
        let rows: Vec<_> = pages.iter().flat_map(|p| p.rows.clone()).collect();
        assert_eq!(
            rows,
            vec![vec![Value::String("paid".into())]],
            "DISTINCT over the matching rows is one row, got {rows:?}"
        );
    });
}

/// The async batch path delivers row tiers, and a change carrying no key still
/// fails loudly there.
///
/// `consumers_batch` is separate code from `consumers` on this engine, and the
/// one thing it does differently, coalescing several events per query into one
/// read, is exactly what the keyed tier depends on. The keyless refusal was
/// pinned on the sync engine only.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_async_batch_path_delivers_row_deltas_and_refuses_a_keyless_change() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut conn_setup = common::pg_connect(port);
    let mut conn_dml = common::pg_connect(port);
    setup_pg(&mut conn_setup, &[(1, 5.0), (2, 9.0), (3, 11.0)]);

    common::multi_thread_rt().block_on(async move {
        let pool = pg_async_pool(port).await;
        let mut engine = build_engine(catalog(), pool);
        let query_id = match engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(
                    1u64,
                    "SELECT * FROM orders WHERE lower(status) = 'paid'",
                ),
                (),
            )
            .expect("captured")
        {
            Registered::Captured {
                query_id,
                tier: subql::reexec::CaptureTier::ChangedRowsOnly,
                ..
            } => query_id,
            other => panic!("expected the keyed tier, got {other:?}"),
        };

        // Two rows leave the answer, one stays, all in one batch.
        sql_query("UPDATE orders SET status = 'void' WHERE id IN (2, 3)")
            .execute(&mut conn_dml)
            .expect("update");
        sql_query("UPDATE orders SET price = 6.0 WHERE id = 1")
            .execute(&mut conn_dml)
            .expect("update");
        let msgs = common::drain_slot(&mut conn_setup, SLOT);
        let events: Vec<MessageV2> = msgs.iter().flat_map(|m| parse_message(m)).collect();
        assert!(events.len() >= 3, "the slot must carry all three updates");

        let outcome = engine.consumers_batch(&events).await.expect("batch");
        assert!(
            outcome.scalar_updates.is_empty() && outcome.rows_updates.is_empty(),
            "the keyed tier delivers per-row deltas only"
        );
        assert!(
            outcome.row_deltas.iter().all(|d| d.query_id == query_id),
            "every delta belongs to the registered query"
        );
        let mut removed: Vec<_> = outcome
            .row_deltas
            .iter()
            .filter(|d| d.row.is_none())
            .map(|d| d.key.clone())
            .collect();
        removed.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(
            removed,
            vec![vec![Value::Int(2)], vec![Value::Int(3)]],
            "both rows that stopped matching arrive as removals, coalesced into one read"
        );
        assert_eq!(
            outcome
                .row_deltas
                .iter()
                .filter(|d| d.row.is_some())
                .map(|d| d.key.clone())
                .collect::<Vec<_>>(),
            vec![vec![Value::Int(1)]],
            "the row that still matches arrives as itself, exactly once"
        );

        // A change carrying no readable key cannot be asked about, so it must
        // be reported rather than silently leaving the subscription stale.
        // An update carrying neither a new image nor an identity: nothing to
        // read a key out of.
        let keyless = parse_message(r#"{"action":"U","schema":"public","table":"orders"}"#);
        assert_eq!(keyless.len(), 1, "the probe must parse as one message");
        match engine.consumers(&keyless[0]).await {
            Err(subql::reexec::ReExecError::Dispatch(
                subql::DispatchError::KeyedChangeWithoutKey(_),
            )) => {}
            Err(other) => panic!("expected a keyless refusal, got {other:?}"),
            Ok(_) => panic!("a change with no readable key must not resolve quietly"),
        }
    });
}
