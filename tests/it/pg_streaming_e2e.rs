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

use diesel::{sql_query, Connection, ExpressionMethods, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::CdcEvent;
use subql::{
    CdcSource, EventKind, PgChangeEvent, PgCommit, PgCommitPosition, PgLsn, PgStreamingCdcSource,
    PgStreamingConfig, SourceItem, TimelineSwitch,
};

diesel::table! {
    orders (id) {
        id -> Integer,
        price -> Double,
    }
}

/// Insert one row per id in a single statement, so in one transaction.
fn insert(conn: &mut PgConnection, ids: &[i32]) {
    let rows: Vec<_> = ids
        .iter()
        .map(|&id| (orders::id.eq(id), orders::price.eq(f64::from(id))))
        .collect();
    diesel::insert_into(orders::table)
        .values(&rows)
        .execute(conn)
        .expect("insert");
}

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

/// Drive an INSERT in a side connection and assert `source.next_item()`
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
fn next_item_delivers_an_insert_without_waiting_for_a_tick() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_next_item");
    let publication = "subql_pg_streaming_next_item_pub";
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

        // Commit before any next_item() poll so the timestamp precedes the wait.
        let commit_at = Instant::now();
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");

        let event = tokio::time::timeout(CEILING, source.next_item())
            .await
            .expect("the event must arrive on the wire, not on the status interval")
            .expect("next_item must not error")
            .and_then(SourceItem::into_event)
            .expect("the row arrives before its commit");
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

/// Acknowledging a commit moves the slot's `confirmed_flush_lsn` to exactly
/// the end the commit named, so a consumer that stored that end before
/// acknowledging holds the position the slot resumes from.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_a_commit_moves_the_slot_to_the_end_it_named() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_ack");
    let publication = "subql_pg_streaming_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1]);
        let row = next_row(&mut source).await;
        let commit = next_commit(&mut source).await;
        assert_eq!(
            commit.position(),
            PgCommitPosition::at_commit(row.position().commit_lsn()),
            "the commit follows its row"
        );
        assert!(commit.end_lsn() > row.position().commit_lsn());

        source.ack(commit.position()).await.expect("ack the commit");
        let flushed = flushed_after_ack(
            &source,
            |lsn| lsn >= commit.end_lsn(),
            &mut probe,
            &slot_inner,
        )
        .await;
        assert_eq!(flushed, commit.end_lsn());
        assert_eq!(source.acknowledged_position(), Some(commit.end_lsn()));
    });

    common::drop_slot(&mut setup, &slot);
}

/// Acknowledging every row of a transaction without its commit leaves the
/// slot where it started, however many status updates go out.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_every_row_without_the_commit_never_moves_the_slot() {
    const STATUS_INTERVAL: Duration = Duration::from_millis(100);
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_rows_only");
    let publication = "subql_pg_streaming_rows_only_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config =
        PgStreamingConfig::new(db.url(), &slot, publication).status_interval(STATUS_INTERVAL);
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let started = confirmed_flush_lsn(&mut probe, &slot_inner).expect("the slot exists");
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1, 2]);
        next_row(&mut source).await;
        let last = next_row(&mut source).await;
        next_commit(&mut source).await;

        source.ack(last.position()).await.expect("ack the last row");
        flushed_after_ack(&source, |_| true, &mut probe, &slot_inner).await;
        let before = source.status_updates_sent();
        let deadline = Instant::now() + ARRIVAL;
        while source.status_updates_sent() < before + 5 {
            assert!(Instant::now() < deadline, "the status pump stalled");
            tokio::time::sleep(STATUS_INTERVAL).await;
        }
        assert_eq!(
            confirmed_flush_lsn(&mut probe, &slot_inner),
            Some(started),
            "rows alone must not release their transaction"
        );
        assert_eq!(source.acknowledged_position(), Some(started));
    });

    common::drop_slot(&mut setup, &slot);
}

type Item = SourceItem<PgChangeEvent, PgCommit>;

/// Commit the newer transaction while the older one is open, then the
/// older, the shape every concurrent writer produces.
fn interleave(older: &mut PgConnection, newer: &mut PgConnection, rows_in_the_older: usize) {
    let older_ids: Vec<i32> = (0..rows_in_the_older)
        .map(|id| 100 + i32::try_from(id).expect("few rows"))
        .collect();
    older
        .transaction::<_, diesel::result::Error, _>(|older| {
            insert(older, &older_ids);
            insert(newer, &[1]);
            Ok(())
        })
        .expect("commit the older");
}

/// The items an older transaction committing after a newer one delivers.
fn interleaved_items(rows_in_the_older: usize) -> Vec<Item> {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut older = db.connect();
    let mut newer = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_commit_order");
    let publication = "subql_pg_streaming_commit_order_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);

    let items = current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        interleave(&mut older, &mut newer, rows_in_the_older);
        let mut items = Vec::new();
        // The newer's row and commit, then the older's rows and commit.
        for _ in 0..rows_in_the_older + 3 {
            items.push(next_item(&mut source).await);
        }
        items
    });
    common::drop_slot(&mut setup, &slot);
    items
}

const fn item_position(item: &Item) -> PgCommitPosition {
    match item {
        SourceItem::Event(event) => event.position(),
        SourceItem::Commit(commit) => commit.position(),
    }
}

/// The next item, which must arrive.
async fn next_item(source: &mut PgStreamingCdcSource) -> Item {
    tokio::time::timeout(ARRIVAL, source.next_item())
        .await
        .expect("the item arrives")
        .expect("next_item must not error")
        .expect("source must not have shut down")
}

/// The next item, which must be a row.
async fn next_row(source: &mut PgStreamingCdcSource) -> PgChangeEvent {
    match next_item(source).await {
        SourceItem::Event(event) => event,
        SourceItem::Commit(commit) => panic!("expected a row, got {commit:?}"),
    }
}

/// The next item, which must be a commit.
async fn next_commit(source: &mut PgStreamingCdcSource) -> PgCommit {
    match next_item(source).await {
        SourceItem::Commit(commit) => commit,
        SourceItem::Event(event) => panic!("expected a commit, got {event:?}"),
    }
}

/// Each transaction's rows and then its commit arrive in commit order, so
/// the newer transaction's commit comes before the older one's rows.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn items_follow_commit_order_across_interleaved_transactions() {
    let items = interleaved_items(1);
    let shape: Vec<bool> = items
        .iter()
        .map(|item| matches!(item, SourceItem::Commit(_)))
        .collect();
    assert_eq!(
        shape,
        [false, true, false, true],
        "row, commit, row, commit: {items:?}"
    );
    assert!(
        item_position(&items[0]) < item_position(&items[2]),
        "the newer transaction committed first, so its row must order first: {items:?}"
    );
}

/// Every delivered item orders strictly after the one before it, including the rows inside one
/// transaction and the commit after them, so a consumer resuming from any position it holds misses
/// nothing after it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn positions_strictly_increase_through_a_multi_row_transaction() {
    let positions: Vec<PgCommitPosition> = interleaved_items(2).iter().map(item_position).collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "positions must strictly increase in delivery order: {positions:?}"
    );
}

/// Acknowledging the first of two interleaved commits releases the slot to
/// exactly that commit's end, before the second transaction.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_the_first_of_interleaved_commits_releases_exactly_its_end() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut older = db.connect();
    let mut newer = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_interleaved_ack");
    let publication = "subql_pg_streaming_interleaved_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        interleave(&mut older, &mut newer, 1);
        next_row(&mut source).await;
        let first = next_commit(&mut source).await;
        next_row(&mut source).await;
        let second = next_commit(&mut source).await;
        assert!(first.end_lsn() < second.end_lsn());

        source
            .ack(first.position())
            .await
            .expect("ack the first commit");
        let flushed = flushed_after_ack(
            &source,
            |lsn| lsn >= first.end_lsn(),
            &mut probe,
            &slot_inner,
        )
        .await;
        assert_eq!(flushed, first.end_lsn());
    });

    common::drop_slot(&mut setup, &slot);
}

/// Connect, waiting out the walsender of a source just dropped, which holds
/// the slot until it notices.
async fn connect_when_free(config: &PgStreamingConfig) -> PgStreamingCdcSource {
    let deadline = Instant::now() + ARRIVAL;
    loop {
        let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        match PgStreamingCdcSource::connect(config.clone(), catalog).await {
            Ok(source) => return source,
            Err(err) if err.to_string().contains("is active") && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => panic!("connect: {err}"),
        }
    }
}

/// Wait until the source has reported an acknowledgement at least `past`
/// far, then return the slot's `confirmed_flush_lsn` as the server holds it.
async fn flushed_after_ack(
    source: &PgStreamingCdcSource,
    past: impl Fn(PgLsn) -> bool,
    probe: &mut diesel::PgConnection,
    slot: &str,
) -> PgLsn {
    let deadline = Instant::now() + ARRIVAL;
    while !source.acknowledged_position().is_some_and(&past) {
        assert!(
            Instant::now() < deadline,
            "the acknowledgement never reached the server"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    loop {
        let flushed = confirmed_flush_lsn(probe, slot).expect("the slot exists");
        if past(flushed) {
            return flushed;
        }
        assert!(
            Instant::now() < deadline,
            "the slot never took the acknowledgement"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Acknowledging part of a transaction leaves the slot before it, so a fresh
/// source receives the whole transaction again, commit and end included, and
/// acknowledging its commit releases it, so a fresh source never sees it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn acknowledging_part_of_a_transaction_never_releases_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_partial_ack");
    let publication = "subql_pg_streaming_partial_ack_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    let slot_inner = slot.clone();

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1, 2]);
        let first = next_row(&mut source).await.position();
        let second = next_row(&mut source).await.position();
        let commit = next_commit(&mut source).await;

        source.ack(first).await.expect("ack the first row");
        let flushed = flushed_after_ack(&source, |_| true, &mut probe, &slot_inner).await;
        assert!(
            flushed < first.commit_lsn(),
            "the slot passed a transaction the consumer holds half of: {flushed:?} against {first:?}"
        );
        drop(source);
        let mut source = connect_when_free(&config).await;
        let again = [
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
        ];
        assert_eq!(
            again,
            [first, second],
            "the half-acknowledged transaction must come back whole"
        );
        assert_eq!(next_commit(&mut source).await, commit, "with the same end");

        source.ack(commit.position()).await.expect("ack the commit");
        flushed_after_ack(&source, |lsn| lsn >= commit.end_lsn(), &mut probe, &slot_inner).await;
        drop(source);
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[3]);
        let after = next_row(&mut source).await.position();
        assert!(
            after > commit.position(),
            "the acknowledged transaction must not come back, got {after:?}"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Resuming from a position delivers exactly the items after it, none
/// skipped and none repeated: from a row, the rest of its transaction and
/// its commit with the same end, from the last row, the commit alone, and
/// from the commit, the next transaction.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn resuming_from_a_position_delivers_exactly_the_items_after_it() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_resume");
    let publication = "subql_pg_streaming_resume_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let config = PgStreamingConfig::new(db.url(), &slot, publication);

    current_thread_rt().block_on(async move {
        let mut source = connect_when_free(&config).await;
        insert(&mut dml, &[1, 2, 3]);
        let rows = [
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
        ];
        let commit = next_commit(&mut source).await;
        drop(source);

        let mut source = connect_when_free(&config.clone().start(Some(rows[0]))).await;
        let rest = [
            next_row(&mut source).await.position(),
            next_row(&mut source).await.position(),
        ];
        assert_eq!(rest, rows[1..], "the rest of the transaction, once each");
        assert_eq!(
            next_commit(&mut source).await,
            commit,
            "then its commit, same end"
        );
        insert(&mut dml, &[4]);
        let next = next_row(&mut source).await.position();
        assert!(
            next > commit.position(),
            "then the next transaction, got {next:?}"
        );
        drop(source);

        let mut source = connect_when_free(&config.clone().start(Some(rows[2]))).await;
        assert_eq!(
            next_commit(&mut source).await,
            commit,
            "resuming after the last row delivers the commit it had not had"
        );
        drop(source);

        let mut source = connect_when_free(&config.start(Some(commit.position()))).await;
        assert_eq!(
            next_row(&mut source).await.position(),
            next,
            "resuming after the commit starts at the next transaction"
        );
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
/// `next_item`.
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

        let ev = tokio::time::timeout(ARRIVAL, source.next_item())
            .await
            .expect("next_item timeout: connection likely torn down")
            .expect("next_item err: connection likely torn down")
            .and_then(SourceItem::into_event)
            .expect("source closed or yielded a commit first: connection likely torn down");
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
        while observed_ids.len() < n {
            let SourceItem::Event(ev) = next_item(&mut source).await else {
                continue;
            };
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
        assert!(
            !source.task_exited(),
            "and the source says so itself, which is what an operator asks"
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
/// pushes row events to the consumer channel, and commits do not count.
/// Symmetric counter exists on `PollingPgCdcSource` so generic benchmark
/// code can read throughput on either transport.
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
            assert_eq!(next_row(&mut source).await.kind(), EventKind::Insert);
            next_commit(&mut source).await;
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

/// The source says its task died, without being dropped to find out.
///
/// An operator asks a live source whether its inner task is still there,
/// and the answer matters most when the server ended the connection
/// rather than the caller ending the source. Terminating the slot's
/// backend is that case.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_source_reports_a_task_the_server_killed() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = db.slot("subql_pg_streaming_killed");
    let publication = "subql_pg_streaming_killed_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication)
        .status_interval(Duration::from_millis(100));

    let (mut setup, slot) = current_thread_rt().block_on(async move {
        let mut source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        assert!(!source.task_exited(), "the task is there to begin with");

        // Only this slot's backend, named by the slot rather than by a
        // sweep, so no other test's connection is touched.
        sql_query(format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{slot}' AND active_pid IS NOT NULL"
        ))
        .execute(&mut setup)
        .expect("terminate the slot's backend");

        // Drive the source so the loop notices the connection is gone.
        let deadline = Instant::now() + ARRIVAL;
        while Instant::now() < deadline {
            if source.task_exited() {
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), source.next_item()).await;
        }

        assert!(
            source.task_exited(),
            "a live source reports the task the server took from it"
        );
        (setup, slot)
    });
    common::drop_slot(&mut setup, &slot);
}

#[derive(diesel::QueryableByName)]
struct ControlSystem {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    system_identifier: i64,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    timeline: i32,
}

/// The cluster's identity as plain SQL reports it, outside the replication protocol.
fn control_system(conn: &mut diesel::PgConnection) -> (u64, u32) {
    // The DSL cannot put a set-returning function in FROM.
    let row = sql_query(
        "SELECT s.system_identifier, c.timeline_id AS timeline \
         FROM pg_control_system() s, pg_control_checkpoint() c",
    )
    .get_result::<ControlSystem>(conn)
    .expect("pg_control_system");
    (
        u64::try_from(row.system_identifier).expect("system id positive"),
        u32::try_from(row.timeline).expect("timeline positive"),
    )
}

/// Parse a raw history file without the code under test.
fn raw_history_switches(content: &str) -> Vec<TimelineSwitch> {
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut fields = line.split('\t');
            TimelineSwitch {
                timeline: fields.next().unwrap().trim().parse().unwrap(),
                switch_lsn: PgLsn::parse(fields.next().unwrap().trim()).unwrap(),
            }
        })
        .collect()
}

fn docker_ok(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("docker {}: {e}", args.join(" ")));
    assert!(
        out.status.success(),
        "docker {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn wait_until(what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + ARRIVAL;
    while !probe() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {ARRIVAL:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// A promoted point-in-time clone of the shared server.
struct PitrCluster {
    container: String,
    volume: String,
    port: u16,
}

impl PitrCluster {
    /// Clone the shared server with `pg_basebackup`, recover to the backup's consistency point and promote to timeline 2.
    fn promoted(name: &str, source_container: &str) -> Self {
        let mut clone = Self {
            container: format!("subql-pitr-{name}"),
            volume: format!("subql-pitr-{name}-vol"),
            port: 0,
        };
        let image = common::pg_image_ref();
        docker_ok(&["volume", "create", &clone.volume]);
        // Postgres refuses an immediate target without a restore_command, and consistency never consults it.
        let seed = r#"pg_basebackup -h 127.0.0.1 -U subql_test -D /dst -X stream \
               && printf "recovery_target = 'immediate'\nrestore_command = 'false'\n" >> /dst/postgresql.auto.conf \
               && touch /dst/recovery.signal"#;
        docker_ok(&[
            "run",
            "--rm",
            "--network",
            &format!("container:{source_container}"),
            "-v",
            &format!("{}:/dst", clone.volume),
            "-e",
            "PGPASSWORD=subql_test",
            &image,
            "sh",
            "-c",
            seed,
        ]);
        let data = format!("{}:/var/lib/postgresql/data", clone.volume);
        let mut run = vec![
            "run",
            "-d",
            "--name",
            &clone.container,
            "-v",
            &data,
            "-p",
            "127.0.0.1:0:5432",
            "-e",
            "POSTGRES_USER=subql_test",
            "-e",
            "POSTGRES_PASSWORD=subql_test",
            &image,
        ];
        run.extend(common::pg::PG_COMMAND);
        docker_ok(&run);
        // PG16 pauses at the recovery target, and resuming promotes.
        let in_recovery = |want: &str| {
            let out = std::process::Command::new("docker")
                .args([
                    "exec",
                    &clone.container,
                    "psql",
                    "user=subql_test dbname=postgres",
                    "-tAc",
                    "SELECT pg_is_in_recovery()",
                ])
                .output()
                .expect("docker runs");
            out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == want
        };
        wait_until("clone reached the paused recovery point", || {
            in_recovery("t")
        });
        docker_ok(&[
            "exec",
            &clone.container,
            "psql",
            "user=subql_test dbname=postgres",
            "-c",
            "SELECT pg_wal_replay_resume()",
        ]);
        wait_until("clone promoted", || in_recovery("f"));
        let mapped = docker_ok(&["port", &clone.container, "5432/tcp"]);
        clone.port = mapped
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .and_then(|p| p.parse().ok())
            .expect("mapped port");
        clone
    }

    /// libpq URL of the test database on the clone.
    fn url(&self, database: &str) -> String {
        common::pg::PgDatabase::url_at(self.port, database)
    }

    /// The clone's raw `00000002.history`, read off its disk.
    fn raw_history(&self) -> String {
        docker_ok(&[
            "exec",
            &self.container,
            "sh",
            "-c",
            "find /var/lib/postgresql/data/pg_wal -name 00000002.history -exec cat {} +",
        ])
    }
}

impl Drop for PitrCluster {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
        let _ = std::process::Command::new("docker")
            .args(["volume", "rm", &self.volume])
            .output();
    }
}

/// `cluster_identity` matches `pg_control_system` on timeline 1, with no history.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn cluster_identity_of_the_running_cluster() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    let slot = db.slot("subql_pg_streaming_identity");
    let publication = "subql_pg_streaming_identity_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, &slot);
    let (system_id, timeline) = control_system(&mut setup);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(db.url(), &slot, publication);
    current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect");
        let identity = source.cluster_identity();
        assert_eq!(identity.system_id, system_id);
        assert_eq!(identity.timeline, timeline);
        assert_eq!(identity.history, Vec::<TimelineSwitch>::new());
    });

    common::drop_slot(&mut setup, &slot);
}

/// A promoted point-in-time clone reports timeline 2 and its own history file's switch point.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn cluster_identity_of_a_promoted_point_in_time_clone() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    // Recovery discards restored logical slots, so only the publication is created before the clone.
    let publication = "subql_pg_streaming_pitr_pub";
    common::create_publication(&mut setup, publication, "orders");

    let clone = PitrCluster::promoted(db.name(), db.container());
    let mut on_clone =
        diesel::PgConnection::establish(&clone.url(db.name())).expect("connect to clone");
    let slot = db.slot("subql_pg_streaming_pitr");
    common::create_pgoutput_slot(&mut on_clone, &slot);
    let (system_id, _) = control_system(&mut on_clone);
    let expected = raw_history_switches(&clone.raw_history());
    assert!(!expected.is_empty(), "history file names the branch");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let config = PgStreamingConfig::new(clone.url(db.name()), &slot, publication);
    current_thread_rt().block_on(async move {
        let source = PgStreamingCdcSource::connect(config, catalog)
            .await
            .expect("connect to promoted clone");
        let identity = source.cluster_identity();
        assert_eq!(identity.timeline, 2);
        assert_eq!(identity.history, expected);
        // Promotion keeps the system id, which is why the timeline is needed.
        assert_eq!(identity.system_id, system_id);
    });
}
