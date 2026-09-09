//! Docker-backed end-to-end tests for [`PgStreamingCdcSource`].
//!
//! Each test is `#[ignore]` so default `cargo test` does not require
//! Docker. Run with:
//!
//! ```sh
//! cargo test --test it pg_streaming_e2e:: --features pg-streaming \
//!     -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::items_after_statements
)]

use crate::common;

use std::time::{Duration, Instant};

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::CdcEvent;
use subql::{CdcSource, EventKind, PgLsn, PgStreamingCdcSource, PgStreamingConfig};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

/// How long a test waits for something that must arrive.
///
/// Liveness only: no assertion below measures how fast anything is, so this
/// is deliberately generous. A short wait measures the runner instead of the
/// code, and a shared CI runner starting eight Postgres containers stalls for
/// seconds at a time.
const ARRIVAL: Duration = Duration::from_secs(30);

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

/// Open a replication-mode connection and validate it accepts
/// `IDENTIFY_SYSTEM`. No DML, no streaming yet. Bare constructor
/// contract.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn connect_against_real_pg() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_connect");
    let publication = "subql_pg_streaming_connect_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_secs(10))
        .buffer_capacity(1024);

    current_thread_rt().block_on(async move {
        let _source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect succeeds against live PG with valid slot");
    });

    common::drop_slot(&mut setup, &slot);
}

/// Query the slot's `confirmed_flush_lsn` via a side connection and
/// parse the `XXXX/YYYY` text form into a [`PgLsn`].
fn confirmed_flush_lsn(conn: &mut diesel::PgConnection, slot: &str) -> Option<PgLsn> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        confirmed_flush_lsn: String,
    }
    // pg_lsn has no Diesel type mapping; the ::text cast requires raw SQL
    let rows: Vec<Row> = diesel::sql_query(format!(
        "SELECT confirmed_flush_lsn::text AS confirmed_flush_lsn \
         FROM pg_replication_slots WHERE slot_name = '{slot}'"
    ))
    .load(conn)
    .expect("query pg_replication_slots");
    rows.into_iter()
        .next()
        .and_then(|r| PgLsn::parse(&r.confirmed_flush_lsn))
}

/// Drive an INSERT in a side connection and assert `source.next_event()`
/// returns the corresponding typed CDC event without waiting for a tick.
///
/// The claim is that delivery is wire-driven, so the ceiling is stated
/// against the configuration rather than against the clock: the status
/// interval is set to thirty seconds and the event must arrive within a sixth
/// of it. An implementation that surfaced events on its interval instead
/// would blow that by five seconds, while a wire-driven one has three orders
/// of magnitude of headroom, measured at four milliseconds on an idle machine
/// and fifteen under contention. The observed latency is reported rather than
/// asserted, because a wall-clock number measures the runner: a ceiling of
/// two hundred milliseconds failed CI on 2026-09-07 while passing locally.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn next_event_delivers_an_insert_without_waiting_for_a_tick() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_next_event");
    let publication = "subql_pg_streaming_next_event_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    // Status interval far above the ceiling so a tick-driven event cannot pass the latency test.
    const STATUS_INTERVAL: Duration = Duration::from_secs(30);
    const CEILING: Duration = Duration::from_secs(5);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config =
        PgStreamingConfig::new(db.url(), &slot, publication).status_interval(STATUS_INTERVAL);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Commit before any next_event() poll so the timestamp precedes the wait.
        let commit_at = Instant::now();
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");

        let event = tokio::time::timeout(CEILING, source.next_event())
            .await
            .expect("the event must arrive on the wire, not on the status interval")
            .expect("next_event must not error")
            .expect("source must not have shut down");
        // Reported only; wall-clock includes the blocking INSERT and runner scheduling.
        let observed_latency = commit_at.elapsed();

        assert_eq!(
            event.kind(),
            EventKind::Insert,
            "first event must be the INSERT we just issued, got {:?}",
            event.kind()
        );
        println!(
            "COMMIT-to-event latency: {}us (ceiling: {}s, status interval: {}s)",
            observed_latency.as_micros(),
            CEILING.as_secs(),
            STATUS_INTERVAL.as_secs()
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Explicit `ack(upto)` advances the slot's
/// `confirmed_flush_lsn` on the server. Without acking, the slot
/// retains all WAL since slot creation. Acking releases it. The
/// load-bearing claim: an `ack` issued by the caller produces a
/// visible server-side state change within a short window.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn ack_advances_confirmed_flush_lsn() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_ack");
    let publication = "subql_pg_streaming_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    // slot also needed inside the async block for the confirmed_flush_lsn probe
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        for id in 1..=3 {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }

        let mut last_lsn = PgLsn(0);
        for _ in 0..3 {
            let ev = tokio::time::timeout(ARRIVAL, source.next_event())
                .await
                .expect("next_event timeout")
                .expect("next_event err")
                .expect("source closed");
            assert_eq!(ev.kind(), EventKind::Insert);
            last_lsn = ev.checkpoint().expect("XLogData carries an LSN");
        }
        assert!(
            last_lsn.0 > 0,
            "events must carry a non-zero LSN; got {last_lsn:?}"
        );

        source.ack(last_lsn).await.expect("ack");

        // Loop is a liveness check; a no-op ack would never advance the LSN.
        let deadline = Instant::now() + ARRIVAL;
        let mut advanced = None;
        while Instant::now() < deadline {
            if let Some(observed) = confirmed_flush_lsn(&mut probe, &slot_inner) {
                if observed >= last_lsn {
                    advanced = Some(observed);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let observed = advanced.unwrap_or_else(|| {
            panic!(
                "confirmed_flush_lsn never reached {last_lsn:?}: \
                 ack() must surface a StandbyStatusUpdate to the server"
            )
        });
        let lsn_hi = u32::try_from(last_lsn.0 >> 32).expect("high 32 bits fit u32");
        let lsn_lo = u32::try_from(last_lsn.0 & 0xFFFF_FFFF).expect("low 32 bits fit u32");
        let obs_hi = u32::try_from(observed.0 >> 32).expect("high 32 bits fit u32");
        let obs_lo = u32::try_from(observed.0 & 0xFFFF_FFFF).expect("low 32 bits fit u32");
        println!("ack({lsn_hi}/{lsn_lo}) -> confirmed_flush_lsn {obs_hi}/{obs_lo}");
    });

    common::drop_slot(&mut setup, &slot);
}

/// The periodic status-update pump bumps the observability counter while the
/// source is idle: no DML, no acks, no server-side timeout games. The claim is
/// that the inner task fires its interval arm independently of the consumer
/// and the server, so the test polls until the counter climbs rather than
/// counting ticks inside a fixed window, which counts the runner's scheduling
/// as much as the pump's.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn pump_increments_status_update_counter_during_idle() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_pump_counter");
    let publication = "subql_pg_streaming_pump_counter_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));

    current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        assert_eq!(
            source.status_updates_sent(),
            0,
            "counter should start at 0 after connect"
        );

        // A pump that never fires never reaches 3, however long this waits.
        let deadline = Instant::now() + ARRIVAL;
        let mut observed = 0;
        while Instant::now() < deadline {
            observed = source.status_updates_sent();
            if observed >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            observed >= 3,
            "the periodic pump must emit while idle, counter reached {observed}"
        );
        println!("status_updates_sent while idle: {observed}");
    });

    common::drop_slot(&mut setup, &slot);
}

/// Idle through an impatient server's `wal_sender_timeout`
/// and confirm the connection survives. PG runs with
/// `wal_sender_timeout=3s`. The source's periodic pump
/// (`status_interval=500ms`) sends a `StandbyStatusUpdate` ~10 times
/// during the idle period, enough to keep the connection alive. After
/// 5s idle, a freshly-issued INSERT must still be deliverable through
/// `next_event`.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn connection_survives_wal_sender_timeout() {
    common::assert_docker_available();
    let db = common::pg_database();
    db.set("wal_sender_timeout", "3s");
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_survive");
    let publication = "subql_pg_streaming_survive_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(500));

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        tokio::time::sleep(Duration::from_secs(5)).await;

        // status_updates_sent counts pumps, acks, and keepalive replies alike; delivery is the claim.
        let pumped = source.status_updates_sent();

        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert after idle");

        let ev = tokio::time::timeout(ARRIVAL, source.next_event())
            .await
            .expect("next_event timeout: connection likely torn down")
            .expect("next_event err: connection likely torn down")
            .expect("source closed: connection likely torn down");
        assert_eq!(ev.kind(), EventKind::Insert);
        println!(
            "survived 5s idle with wal_sender_timeout=3s, \
             {pumped} pumps emitted, insert still flowed through"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Bounded back-pressure. With `buffer_capacity = 4` and 100
/// inserts in flight, the inner task's event-push must block on
/// `event_tx.send().await` while the channel is full, then resume as
/// the consumer drains. No events dropped, order preserved end-to-end.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn back_pressure_under_slow_consumer_preserves_order_and_count() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_backpressure");
    let publication = "subql_pg_streaming_backpressure_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    // Tiny buffer forces back-pressure: 100 inserts >> 4-slot channel.
    let config = PgStreamingConfig::new(db.url(), &slot, publication).buffer_capacity(4);

    const N: i32 = 100;

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Insert before draining so the source hits back-pressure before the consumer polls.
        for id in 1..=N {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let schema = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let n = usize::try_from(N).expect("N fits usize");
        let mut observed_ids = Vec::with_capacity(n);
        for _ in 0..N {
            let ev = tokio::time::timeout(ARRIVAL, source.next_event())
                .await
                .expect("timeout draining events")
                .expect("next_event err")
                .expect("source closed");
            assert_eq!(ev.kind(), EventKind::Insert);
            let id = match ev
                .value_at(&schema, subql::backend::RowKind::New, 0)
                .unwrap()
            {
                subql::backend::Value::Int(v) => v,
                other => panic!("expected int id, got {other:?}"),
            };
            observed_ids.push(id);
        }

        assert_eq!(
            observed_ids.len(),
            n,
            "must receive all {N} events; got {}",
            observed_ids.len()
        );
        let expected: Vec<i64> = (1..=i64::from(N)).collect();
        assert_eq!(
            observed_ids, expected,
            "events must arrive in commit order under back-pressure"
        );
        println!("back-pressure: drained {N} events through a 4-slot buffer in commit order");
    });

    common::drop_slot(&mut setup, &slot);
}

/// Dropping the source cleanly shuts down the inner task.
/// No panic, no leaked task. The `task_exited` flag flips to true
/// within a small window after `drop(source)` because the cooperative
/// shutdown signal makes the task break out of its loop on the next
/// `select!` poll.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn drop_source_shuts_down_inner_task() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_shutdown");
    let publication = "subql_pg_streaming_shutdown_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));

    current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Clone before drop so we can observe shutdown after the source is gone.
        let task_exited = source.task_exited_handle();

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !task_exited.load(std::sync::atomic::Ordering::Relaxed),
            "inner task must still be running before drop"
        );

        // Shutdown is cooperative; the biased select! polls the cancel arm before the WAL read.
        drop(source);

        // Loop is a liveness check; a leaked task never flips the flag.
        let deadline = Instant::now() + ARRIVAL;
        let mut observed_exit = false;
        while Instant::now() < deadline {
            if task_exited.load(std::sync::atomic::Ordering::Relaxed) {
                observed_exit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            observed_exit,
            "inner task never exited after dropping the source"
        );
        println!("inner task exited cleanly after source drop");
    });

    common::drop_slot(&mut setup, &slot);
}

/// `events_received` counter is incremented as the inner task
/// pushes events to the consumer channel. Symmetric counter exists on
/// `PollingPgCdcSource` so generic benchmark code can read throughput
/// on either transport.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn events_received_counter_tracks_pushed_events() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_events_counter");
    let publication = "subql_pg_streaming_events_counter_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        assert_eq!(
            source.events_received(),
            0,
            "counter starts at 0 before any events have been pushed"
        );

        const N: i32 = 5;
        for id in 1..=N {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }
        for _ in 0..N {
            let ev = tokio::time::timeout(ARRIVAL, source.next_event())
                .await
                .expect("timeout")
                .expect("err")
                .expect("source closed");
            assert_eq!(ev.kind(), EventKind::Insert);
        }

        let observed = source.events_received();
        assert_eq!(
            observed,
            u64::try_from(N).unwrap(),
            "counter must equal the number of events pushed"
        );
        println!("events_received after {N} inserts: {observed}");
    });

    common::drop_slot(&mut setup, &slot);
}
