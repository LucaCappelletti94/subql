#![cfg(any())] // Phase 11: rewrite against Value<B> + TestEvent<B>. Retired Cell/WalEvent/RowImage/PrimaryKey/ColumnType api. Tracked in docs/refactor-cdc-event-handoff.md.

//! Docker-backed end-to-end tests for [`PgStreamingCdcSource`].
//!
//! Each test is `#[ignore]` so default `cargo test` does not require
//! Docker. Run with:
//!
//! ```sh
//! cargo test --test pg_streaming_e2e --features pg-streaming \
//!     -- --ignored --nocapture
//! ```

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::items_after_statements
)]

mod common;

use std::time::{Duration, Instant};

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{CdcSource, Cell, EventKind, PgLsn, PgStreamingCdcSource, PgStreamingConfig};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

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
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_connect";
    let publication = "subql_pg_streaming_connect_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication)
        .status_interval(Duration::from_secs(10))
        .buffer_capacity(1024);

    current_thread_rt().block_on(async move {
        let _source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect succeeds against live PG with valid slot");
    });

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// Query the slot's `confirmed_flush_lsn` via a side connection and
/// parse the `XXXX/YYYY` text form into a [`PgLsn`].
fn confirmed_flush_lsn(conn: &mut diesel::PgConnection, slot: &str) -> Option<PgLsn> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        confirmed_flush_lsn: String,
    }
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

/// Drive an INSERT in a side connection, assert
/// `source.next_event().await` returns the corresponding `WalEvent` with
/// COMMIT-to-event-delivery latency under the budget. This latency is
/// wire-bound, not interval-bound.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn next_event_delivers_insert_within_latency_budget() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_next_event";
    let publication = "subql_pg_streaming_next_event_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication);

    // Latency budget for COMMIT-to-event delivery: wire-bound (single-digit
    // ms on a loopback), not interval-bound. Generous headroom because the
    // first event after START_REPLICATION includes the server's handshake
    // and relation-cache prelude.
    const LATENCY_BUDGET: Duration = Duration::from_millis(200);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Drive the INSERT BEFORE we start polling, so the COMMIT timestamp
        // is captured strictly before next_event() begins waiting.
        let commit_at = Instant::now();
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");

        // LATENCY_BUDGET is a hard ceiling: the source must deliver the
        // event within that window without polling at any interval.
        let event = tokio::time::timeout(LATENCY_BUDGET, source.next_event())
            .await
            .expect("next_event must return within the latency budget")
            .expect("next_event must not error")
            .expect("source must not have shut down");
        let observed_latency = commit_at.elapsed();

        assert_eq!(
            event.kind(),
            EventKind::Insert,
            "first event must be the INSERT we just issued, got {:?}",
            event.kind()
        );
        println!(
            "COMMIT-to-event latency: {}us (budget: {}ms)",
            observed_latency.as_micros(),
            LATENCY_BUDGET.as_millis()
        );
    });

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
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
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    let mut probe = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_ack";
    let publication = "subql_pg_streaming_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication);

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Drive a small batch of events. Each carries its own LSN.
        for id in 1..=3 {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }

        // Drain three events, capture the final LSN.
        let mut last_lsn = PgLsn(0);
        for _ in 0..3 {
            let ev = tokio::time::timeout(Duration::from_secs(2), source.next_event())
                .await
                .expect("next_event timeout")
                .expect("next_event err")
                .expect("source closed");
            assert_eq!(ev.kind(), EventKind::Insert);
            last_lsn = *ev.checkpoint().expect("XLogData carries an LSN");
        }
        assert!(
            last_lsn.0 > 0,
            "events must carry a non-zero LSN; got {last_lsn:?}"
        );

        // Acknowledge up to the last seen LSN. The inner task should
        // emit a StandbyStatusUpdate immediately; PG should advance
        // `confirmed_flush_lsn` to at most this value within a short
        // window.
        source.ack(last_lsn).await.expect("ack");

        // Poll the slot's confirmed_flush_lsn for up to 2s, asserting
        // it advances to at least `last_lsn`. A no-op `ack` would
        // make this poll loop time out.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut advanced = None;
        while Instant::now() < deadline {
            if let Some(observed) = confirmed_flush_lsn(&mut probe, slot) {
                if observed >= last_lsn {
                    advanced = Some(observed);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let observed = advanced.unwrap_or_else(|| {
            panic!(
                "confirmed_flush_lsn did not reach {last_lsn:?} within 2s: \
                 ack() must surface a StandbyStatusUpdate to the server"
            )
        });
        println!(
            "ack({}/{}) -> confirmed_flush_lsn {}/{}",
            (last_lsn.0 >> 32) as u32,
            (last_lsn.0 & 0xFFFF_FFFF) as u32,
            (observed.0 >> 32) as u32,
            (observed.0 & 0xFFFF_FFFF) as u32,
        );
    });

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// The periodic status-update pump bumps the observability
/// counter on idle. Fast (~500ms), no DML, no acks, no server-side
/// timeout games. Asserts the inner task fires the interval arm
/// independently of consumer or server activity.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn pump_increments_status_update_counter_during_idle() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_pump_counter";
    let publication = "subql_pg_streaming_pump_counter_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication)
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

        // Idle 500ms. With status_interval=100ms we expect ~5 ticks.
        // Demand >=3 to keep headroom against runtime jitter on CI.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let observed = source.status_updates_sent();
        assert!(
            observed >= 3,
            "periodic pump must emit during idle: \
             counter = {observed} after 500ms with 100ms interval"
        );
        println!("status_updates_sent after 500ms idle: {observed}");
    });

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
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
    let container = common::pg_with_wal2json_impatient(Duration::from_secs(3));
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_survive";
    let publication = "subql_pg_streaming_survive_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication)
        .status_interval(Duration::from_millis(500));

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Idle for 5s, longer than the 3s wal_sender_timeout. The
        // server tears down replication connections that go silent
        // for that long. The periodic pump should keep us alive.
        tokio::time::sleep(Duration::from_secs(5)).await;

        // If the pump worked, the counter should reflect ~10 pumps
        // (5s / 500ms). Sanity-check it climbed past 5.
        let pumped = source.status_updates_sent();
        assert!(
            pumped >= 5,
            "expected periodic pump to fire repeatedly during idle; got {pumped}"
        );

        // Drive an INSERT now. The connection must still be alive to
        // deliver it.
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert after idle");

        let ev = tokio::time::timeout(Duration::from_secs(1), source.next_event())
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

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// Bounded back-pressure. With `buffer_capacity = 4` and 100
/// inserts in flight, the inner task's event-push must block on
/// `event_tx.send().await` while the channel is full, then resume as
/// the consumer drains. No events dropped, order preserved end-to-end.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn back_pressure_under_slow_consumer_preserves_order_and_count() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_backpressure";
    let publication = "subql_pg_streaming_backpressure_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    // Tiny buffer forces back-pressure: 100 inserts >> 4-slot channel.
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication)
        .buffer_capacity(4);

    const N: i32 = 100;

    current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Burst 100 inserts before draining anything. By the time we
        // first await `next_event`, the source's inner task will have
        // begun reading them from CopyBoth and pushing to the bounded
        // mpsc; with capacity 4, it will hit `send().await` and yield
        // back to the consumer for cooperation.
        for id in 1..=N {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }

        // Let the task fill its buffer + start back-pressuring.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Drain all N events. If the inner task drops events under
        // back-pressure, fewer than N arrive. If ordering is broken,
        // the observed ids won't be 1..=N.
        let mut observed_ids = Vec::with_capacity(N as usize);
        for _ in 0..N {
            let ev = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .expect("timeout draining events")
                .expect("next_event err")
                .expect("source closed");
            assert_eq!(ev.kind(), EventKind::Insert);
            let row = ev.new_row().expect("INSERT carries new_row");
            let id = match row.get(0) {
                Some(Cell::Int(v)) => *v,
                other => panic!("expected Cell::Int id, got {other:?}"),
            };
            observed_ids.push(id);
        }

        assert_eq!(
            observed_ids.len(),
            N as usize,
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

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
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
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_shutdown";
    let publication = "subql_pg_streaming_shutdown_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication)
        .status_interval(Duration::from_millis(100));

    current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");

        // Clone the task-exited handle BEFORE dropping the source so
        // we can observe shutdown from outside its lifetime.
        let task_exited = source.task_exited_handle();

        // Confirm the inner task is actually running first. Give it
        // a couple of pump cadences.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !task_exited.load(std::sync::atomic::Ordering::Relaxed),
            "inner task must still be running before drop"
        );

        // Drop the source. The cooperative shutdown signal fires; the
        // inner task's select! polls the shutdown arm first (biased)
        // and breaks out of the loop. The drop guard sets the flag.
        drop(source);

        // Poll for up to 500ms for task_exited to flip. Production
        // should see this within milliseconds; we allow headroom.
        let deadline = Instant::now() + Duration::from_millis(500);
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
            "inner task did not exit within 500ms after dropping the source"
        );
        println!("inner task exited cleanly after source drop");
    });

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}

/// `events_received` counter is incremented as the inner task
/// pushes events to the consumer channel. Symmetric counter exists on
/// `PollingPgCdcSource` so generic benchmark code can read throughput
/// on either transport.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn events_received_counter_tracks_pushed_events() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_pg_streaming_events_counter";
    let publication = "subql_pg_streaming_events_counter_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication);

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
        // Drain all events so we're sure the inner task has pushed them.
        for _ in 0..N {
            let ev = tokio::time::timeout(Duration::from_secs(2), source.next_event())
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

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}
