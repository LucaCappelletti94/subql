//! The polling source's slot protocol, against a real Postgres.
//!
//! [`subql::CdcSource::ack`] promises that an event is retained until it is
//! acknowledged, so these pin where the slot's `confirmed_flush_lsn` moves
//! rather than only what a consumer happens to receive. The position is the
//! contract, and reading it is the only way to tell a source that retains
//! from one that has already thrown the events away.

#![allow(clippy::unwrap_used, clippy::print_stdout)]

use crate::common;

use std::time::Duration;

use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind};
use subql::{CdcSource, EventKind, PollingPgCdcConfig, PollingPgCdcSource};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

#[derive(QueryableByName, Debug)]
struct SlotPosition {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    confirmed_flush_lsn: Option<String>,
}

#[derive(QueryableByName, Debug)]
struct WalDistance {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    distance: i64,
}

/// Bytes of WAL the server has written since `from`.
fn wal_since(conn: &mut PgConnection, from: &str) -> u64 {
    let rows: Vec<WalDistance> = sql_query(format!(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{from}')::int8 AS distance"
    ))
    .load(conn)
    .expect("read the WAL distance");
    u64::try_from(rows[0].distance).expect("the server does not run backwards")
}

/// `pg_replication_slots` is a server catalog view, which the typed DSL has
/// no schema for in this suite, and the slot position is the very thing
/// under test.
fn confirmed_flush(conn: &mut PgConnection, slot: &str) -> Option<String> {
    let rows: Vec<SlotPosition> = sql_query(format!(
        "SELECT confirmed_flush_lsn::text AS confirmed_flush_lsn \
         FROM pg_replication_slots WHERE slot_name = '{slot}'"
    ))
    .load(conn)
    .expect("read the slot position");
    rows.into_iter()
        .next()
        .and_then(|row| row.confirmed_flush_lsn)
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn config(url: String, slot: &str, publication: &str) -> PollingPgCdcConfig {
    PollingPgCdcConfig::new(url, slot, publication).poll_interval(Duration::from_millis(50))
}

/// Set up a table, a publication and a slot, and hand back the slot name.
fn fixture(conn: &mut PgConnection, name: &str, publication: &str, slot: &str) {
    sql_query(PG_DDL).execute(conn).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(conn)
        .expect("REPLICA IDENTITY FULL");
    common::create_publication(conn, publication, "orders");
    common::create_pgoutput_slot(conn, slot);
    println!("fixture ready for {name}");
}

/// An event nobody acknowledged is still in the slot, so a source that dies
/// with it buffered has lost nothing. This is what the trait's retention
/// promise means, and a consuming read cannot keep it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_unacknowledged_event_replays_after_the_source_is_dropped() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_polling_unacked");
    let publication = "subql_polling_unacked_pub";
    fixture(&mut setup, "unacked replay", publication, &slot);

    common::multi_thread_rt().block_on(async {
        {
            let mut source =
                PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                    .await
                    .expect("connect polling source");
            sql_query("INSERT INTO orders VALUES (1, 5.0)")
                .execute(&mut dml)
                .expect("insert");
            let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .expect("the first source receives the insert")
                .expect("no source error")
                .expect("the source is open");
            assert_eq!(event.kind(), EventKind::Insert);
            // Dropped without an ack, as a crash or a shutdown drops it.
        }

        let mut replay =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("reconnect polling source");
        let event = tokio::time::timeout(Duration::from_secs(5), replay.next_event())
            .await
            .expect("the unacknowledged insert is still in the slot")
            .expect("no source error")
            .expect("the source is open");
        assert_eq!(
            event.kind(),
            EventKind::Insert,
            "the event nobody acknowledged arrives again"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// An acknowledged event is gone from the slot, and only what came after it
/// replays. Retention that never ends would be a different defect.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_acknowledged_event_does_not_replay_and_a_later_one_does() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_polling_acked");
    let publication = "subql_polling_acked_pub";
    fixture(&mut setup, "acked replay", publication, &slot);

    common::multi_thread_rt().block_on(async {
        {
            let mut source =
                PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                    .await
                    .expect("connect polling source");
            sql_query("INSERT INTO orders VALUES (1, 5.0)")
                .execute(&mut dml)
                .expect("first insert");
            let first = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .expect("the first insert arrives")
                .expect("no source error")
                .expect("the source is open");
            let upto = first
                .checkpoint()
                .expect("a polled event carries its position");
            source.ack(upto).await.expect("the ack reaches the source");
            // Give the loop an iteration to carry the ack to the server.
            tokio::time::sleep(Duration::from_millis(400)).await;
        }

        sql_query("INSERT INTO orders VALUES (2, 6.0)")
            .execute(&mut dml)
            .expect("second insert");

        let mut replay =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("reconnect polling source");
        let event = tokio::time::timeout(Duration::from_secs(5), replay.next_event())
            .await
            .expect("the second insert arrives")
            .expect("no source error")
            .expect("the source is open");
        let id = event
            .value_at(&catalog(), RowKind::New, 0)
            .expect("the new image carries the key");
        assert_eq!(
            id,
            subql::backend::Value::Int(2),
            "the acknowledged row is not served again, the later one is"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// The position moves when the consumer says it has the data, and not when
/// the source merely read it. This is the protocol itself rather than a
/// symptom of it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_slot_moves_on_an_acknowledgement_and_not_on_a_poll() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let mut probe = db.connect();
    let slot = db.slot("subql_polling_position");
    let publication = "subql_polling_position_pub";
    fixture(&mut setup, "slot position", publication, &slot);

    common::multi_thread_rt().block_on(async {
        let mut source =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("connect polling source");
        let before = confirmed_flush(&mut probe, &slot);

        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");
        let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
            .await
            .expect("the insert arrives")
            .expect("no source error")
            .expect("the source is open");

        // Several more polls have certainly run by now, and none of them may
        // have moved the position.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            confirmed_flush(&mut probe, &slot),
            before,
            "reading the slot must not advance it"
        );

        let upto = event
            .checkpoint()
            .expect("a polled event carries its position");
        source.ack(upto).await.expect("the ack reaches the source");

        let mut moved = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if confirmed_flush(&mut probe, &slot) != before {
                moved = true;
                break;
            }
        }
        assert!(moved, "the acknowledgement must advance the slot");
    });

    common::drop_slot(&mut setup, &slot);
}

/// Peeking re-reads what is still unacknowledged on every poll, so the loop
/// has to deliver each event once however many times it sees it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_repeated_peek_delivers_each_event_once() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_polling_once");
    let publication = "subql_polling_once_pub";
    fixture(&mut setup, "exactly once", publication, &slot);

    common::multi_thread_rt().block_on(async {
        let mut source =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("connect polling source");
        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");
        let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
            .await
            .expect("the insert arrives")
            .expect("no source error")
            .expect("the source is open");
        assert_eq!(event.kind(), EventKind::Insert);

        // Nothing is acknowledged, so every later poll peeks this same
        // transaction again.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let again = tokio::time::timeout(Duration::from_millis(500), source.next_event()).await;
        assert!(
            again.is_err(),
            "an unacknowledged event must not be delivered twice, got {again:?}"
        );
        assert_eq!(
            source.events_received(),
            1,
            "the loop saw the transaction repeatedly and delivered it once"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// One long-lived source keeps delivering.
///
/// A commit reports the position the next transaction begins at, so a loop
/// that skipped past the last position it saw would go deaf after its first
/// batch. Every test above reconnects, so none of them would notice. Nothing
/// is acknowledged here, so the deafness cannot be hidden by a release.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_long_lived_source_keeps_delivering_later_transactions() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_polling_long_lived");
    let publication = "subql_polling_long_lived_pub";
    fixture(&mut setup, "long lived", publication, &slot);

    common::multi_thread_rt().block_on(async {
        let mut source =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("connect polling source");

        for id in 1..=3 {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)"))
                .execute(&mut dml)
                .expect("insert");
            let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .unwrap_or_else(|_| panic!("insert {id} arrives on the same source"))
                .expect("no source error")
                .expect("the source is open");
            let seen = event
                .value_at(&catalog(), RowKind::New, 0)
                .expect("the new image carries the key");
            assert_eq!(
                seen,
                subql::backend::Value::Int(i64::from(id)),
                "each transaction arrives in turn"
            );
        }
    });

    common::drop_slot(&mut setup, &slot);
}

/// Every change of a multi-statement transaction arrives.
///
/// Both shapes are covered, three rows in one statement and three statements
/// in one transaction. A transaction's first change shares the position its
/// `begin` carries, so anything keyed on that position alone could swallow a
/// sibling row.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_change_of_one_transaction_is_delivered() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_polling_multi");
    let publication = "subql_polling_multi_pub";
    fixture(&mut setup, "multi statement", publication, &slot);

    common::multi_thread_rt().block_on(async {
        let mut source =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("connect polling source");
        sql_query("INSERT INTO orders VALUES (1, 1.0), (2, 2.0), (3, 3.0)")
            .execute(&mut dml)
            .expect("three rows in one statement");
        dml.transaction::<_, diesel::result::Error, _>(|conn| {
            for id in 4..=6 {
                sql_query(format!("INSERT INTO orders VALUES ({id}, {id}.0)")).execute(conn)?;
            }
            Ok(())
        })
        .expect("three statements in one transaction");

        let mut seen = Vec::new();
        for _ in 0..6 {
            let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .unwrap_or_else(|_| panic!("all three changes arrive, got {seen:?}"))
                .expect("no source error")
                .expect("the source is open");
            let id = event
                .value_at(&catalog(), RowKind::New, 0)
                .expect("the new image carries the key");
            seen.push(id);
        }
        assert_eq!(
            seen,
            (1..=6)
                .map(|id| subql::backend::Value::Int(i64::from(id)))
                .collect::<Vec<_>>(),
            "one statement of three rows and one transaction of three statements"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// Every row of a `COPY` arrives. Postgres writes a copied batch as one
/// multi-insert record, so its rows share one WAL position, and anything
/// keyed on that position alone could swallow all but the first.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_row_of_a_copied_batch_is_delivered() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_polling_copy");
    let publication = "subql_polling_copy_pub";
    fixture(&mut setup, "copied batch", publication, &slot);

    common::multi_thread_rt().block_on(async {
        let mut source =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("connect polling source");
        // COPY has no typed DSL form, and a program source keeps the rows in the statement.
        sql_query(r#"COPY orders (id, price) FROM PROGRAM 'printf "1\t1\n2\t2\n3\t3\n"'"#)
            .execute(&mut dml)
            .expect("copy three rows");

        let mut seen = Vec::new();
        for _ in 0..3 {
            let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .unwrap_or_else(|_| panic!("all three copied rows arrive, got {seen:?}"))
                .expect("no source error")
                .expect("the source is open");
            seen.push(
                event
                    .checkpoint()
                    .expect("a polled event carries a position"),
            );
        }
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "each copied row has a position of its own: {seen:?}"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// An older transaction that commits after a newer one orders after it, and
/// its rows order among themselves.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn polled_checkpoints_follow_commit_order_across_interleaved_transactions() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut older = db.connect();
    let mut newer = db.connect();
    let slot = db.slot("subql_polling_commit_order");
    let publication = "subql_polling_commit_order_pub";
    fixture(&mut setup, "commit order", publication, &slot);

    common::multi_thread_rt().block_on(async {
        let mut source =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("connect polling source");
        sql_query("BEGIN").execute(&mut older).expect("begin");
        for id in [100, 101] {
            sql_query(format!("INSERT INTO orders VALUES ({id}, 1.0)"))
                .execute(&mut older)
                .expect("older insert");
        }
        sql_query("INSERT INTO orders VALUES (1, 2.0)")
            .execute(&mut newer)
            .expect("newer insert");
        sql_query("COMMIT")
            .execute(&mut older)
            .expect("commit the older");

        let mut delivered = Vec::new();
        for _ in 0..3 {
            let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
                .await
                .unwrap_or_else(|_| panic!("all three rows arrive, got {delivered:?}"))
                .expect("no source error")
                .expect("the source is open");
            let id = event
                .value_at(&catalog(), RowKind::New, 0)
                .expect("the new image carries the key");
            delivered.push((
                id,
                event
                    .checkpoint()
                    .expect("a polled event carries a position"),
            ));
        }
        let ids: Vec<_> = delivered.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(
            ids,
            [1, 100, 101].map(subql::backend::Value::Int),
            "the newer transaction committed first"
        );
        assert!(
            delivered.windows(2).all(|pair| pair[0].1 < pair[1].1),
            "positions strictly increase in delivery order: {delivered:?}"
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// The streaming source reports its distance behind the server's WAL end.
///
/// Acknowledging is what releases a slot, so a consumer that never
/// acknowledges retains WAL until the server's volume fills, and the only
/// symptom is a transport error at a layer with no visible connection to the
/// cause. The distance between the WAL end the server reports and the
/// position acknowledged is what makes that alertable beforehand.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_streaming_source_reports_what_its_slot_is_holding() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_streaming_retention");
    let publication = "subql_streaming_retention_pub";
    fixture(&mut setup, "streaming retention", publication, &slot);

    let start = confirmed_flush(&mut setup, &slot).expect("the slot has a position");

    common::multi_thread_rt().block_on(async {
        let mut source = subql::PgStreamingCdcSource::connect(
            subql::PgStreamingConfig::new(db.url(), &slot, publication),
            catalog(),
        )
        .await
        .expect("connect streaming source");
        // Relational throughout. The figure is a distance to the server's
        // own WAL end, which every other database on a shared server moves,
        // so an absolute reading races the whole cluster.
        let base = source.unacknowledged_bytes();
        assert!(source.acknowledged_position().is_none());

        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");
        let event = tokio::time::timeout(Duration::from_secs(5), source.next_event())
            .await
            .expect("the insert arrives")
            .expect("no source error")
            .expect("the source is open");

        // A data frame carries the position of the record in it, measured
        // as equal to its start on every frame of this suite, and the first
        // record of a transaction sits where the slot already was. Only the
        // commit frame, or a keepalive, moves the figure, so growth is a
        // bounded wait rather than a reading taken the instant one lands.
        let mut held = base;
        let started = std::time::Instant::now();
        while held <= base && started.elapsed() < Duration::from_secs(10) {
            if let Ok(polled) =
                tokio::time::timeout(Duration::from_millis(100), source.next_event()).await
            {
                polled.expect("no source error");
            }
            held = source.unacknowledged_bytes();
        }
        assert!(
            held > base,
            "an unacknowledged event leaves the source behind the server's \
             WAL end, {held} against {base}"
        );
        // Read after the figure, so the server can only have moved further
        // on. A gauge reporting an absolute position rather than a distance
        // blows past this, however busy the rest of the cluster is.
        let written = wal_since(&mut setup, &start);
        assert!(
            held <= written,
            "the figure is a distance from the position this slot started \
             at, so it cannot exceed the {written} bytes written since, \
             got {held}"
        );
        assert!(
            source.acknowledged_position().is_none(),
            "nothing has been acknowledged yet"
        );

        let upto = event.checkpoint().expect("the event carries its position");
        source.ack(upto).await.expect("the ack reaches the source");
        // The held figure is a distance to the server's WAL end, which every
        // other database on a shared server also moves, so the acknowledged
        // position is what this can assert rather than a fall in the figure.
        // The acknowledgement releases the whole transaction, so the position
        // reported lands past its commit.
        let released = |position: Option<subql::PgLsn>| {
            position.is_some_and(|flushed| flushed > upto.commit_lsn())
        };
        for _ in 0..40 {
            if released(source.acknowledged_position()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            released(source.acknowledged_position()),
            "the acknowledgement is what releases the slot, and it is reported, got {:?} for {upto:?}",
            source.acknowledged_position()
        );
    });

    common::drop_slot(&mut setup, &slot);
}

/// The lag figure moves on WAL the publication never carries.
///
/// A keepalive is what carries the server's own WAL end, so a source whose
/// publication is idle still learns that it has fallen behind. Without that,
/// a slot held open beside a busy neighbour reads as caught up right up to
/// the moment the volume fills, which is the case the figure exists for.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn an_idle_publication_still_reports_the_source_falling_behind() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut setup = db.connect();
    let mut dml = db.connect();
    let slot = db.slot("subql_streaming_idle_lag");
    let publication = "subql_streaming_idle_lag_pub";
    fixture(&mut setup, "streaming idle lag", publication, &slot);
    // Outside the publication, so writing to it produces WAL the source is
    // never sent. Only a keepalive can disclose it.
    sql_query("CREATE TABLE unpublished (id INT PRIMARY KEY, bulk TEXT)")
        .execute(&mut setup)
        .expect("create the unpublished table");

    common::multi_thread_rt().block_on(async {
        let mut source = subql::PgStreamingCdcSource::connect(
            subql::PgStreamingConfig::new(db.url(), &slot, publication)
                .status_interval(Duration::from_millis(100)),
            catalog(),
        )
        .await
        .expect("connect streaming source");
        let base = source.unacknowledged_bytes();

        sql_query(
            "INSERT INTO unpublished SELECT g, repeat('x', 4000) \
             FROM generate_series(1, 200) AS g",
        )
        .execute(&mut dml)
        .expect("write outside the publication");

        let started = std::time::Instant::now();
        let mut grew = base;
        while started.elapsed() < Duration::from_secs(30) {
            // next_event drives the frame loop, and must not produce one.
            match tokio::time::timeout(Duration::from_millis(200), source.next_event()).await {
                Err(_) => {}
                Ok(Err(e)) => panic!("the source failed while idle: {e}"),
                Ok(Ok(other)) => panic!("the publication is idle, got {other:?}"),
            }
            grew = source.unacknowledged_bytes();
            if grew > base {
                break;
            }
        }
        println!(
            "idle lag moved {base} -> {grew} after {:?}",
            started.elapsed()
        );
        assert!(
            grew > base,
            "a keepalive carries the server's WAL end, so an idle source \
             still learns it is behind, stayed at {grew}"
        );
    });

    common::drop_slot(&mut setup, &slot);
}
