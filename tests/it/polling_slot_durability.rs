//! The polling source's slot protocol, against a real Postgres.
//!
//! [`subql::CdcSource::ack`] promises that an item is retained until it is
//! acknowledged, so these pin where the slot's `confirmed_flush_lsn` moves
//! rather than only what a consumer happens to receive. The position is the
//! contract, and reading it is the only way to tell a source that retains
//! from one that has already thrown the items away.

#![allow(clippy::unwrap_used, clippy::print_stdout)]

use crate::common;

use std::time::Duration;

use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind};
use subql::{
    CdcSource, EventKind, PgChangeEvent, PgCommit, PgCommitPosition, PgLsn, PollingPgCdcConfig,
    PollingPgCdcSource, SourceItem,
};

type Item = SourceItem<PgChangeEvent, PgCommit>;

/// The next item, which must arrive within five seconds.
async fn next_item<S>(source: &mut S, what: &str) -> Item
where
    S: CdcSource<Event = PgChangeEvent, Commit = PgCommit>,
{
    tokio::time::timeout(Duration::from_secs(5), source.next_item())
        .await
        .unwrap_or_else(|_| panic!("{what} arrives"))
        .expect("no source error")
        .expect("the source is open")
}

/// The next item, which must be a row.
async fn next_row<S>(source: &mut S, what: &str) -> PgChangeEvent
where
    S: CdcSource<Event = PgChangeEvent, Commit = PgCommit>,
{
    match next_item(source, what).await {
        SourceItem::Event(event) => event,
        SourceItem::Commit(commit) => panic!("expected {what}, got {commit:?}"),
    }
}

/// The next item, which must be a commit.
async fn next_commit<S>(source: &mut S, what: &str) -> PgCommit
where
    S: CdcSource<Event = PgChangeEvent, Commit = PgCommit>,
{
    match next_item(source, what).await {
        SourceItem::Commit(commit) => commit,
        SourceItem::Event(event) => panic!("expected {what}, got {event:?}"),
    }
}

const fn position(item: &Item) -> PgCommitPosition {
    match item {
        SourceItem::Event(event) => event.position(),
        SourceItem::Commit(commit) => commit.position(),
    }
}

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
            let event = next_row(&mut source, "the insert on the first source").await;
            assert_eq!(event.kind(), EventKind::Insert);
            // Dropped without an ack, as a crash or a shutdown drops it.
        }

        let mut replay =
            PollingPgCdcSource::connect(config(db.url(), &slot, publication), catalog())
                .await
                .expect("reconnect polling source");
        let event = next_row(&mut replay, "the unacknowledged insert, still in the slot").await;
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
            next_row(&mut source, "the first insert").await;
            let commit = next_commit(&mut source, "the first insert's commit").await;
            source
                .ack(commit.position())
                .await
                .expect("the ack reaches the source");
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
        let event = next_row(&mut replay, "the second insert").await;
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

/// The position moves to the end of a commit when the consumer acknowledges
/// that commit, and not when the source merely read it or the consumer
/// acknowledged its rows. This is the protocol itself rather than a symptom
/// of it.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn the_slot_moves_on_an_acknowledged_commit_and_not_on_a_poll_or_a_row() {
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
        let event = next_row(&mut source, "the insert").await;
        let commit = next_commit(&mut source, "its commit").await;

        source
            .ack(event.position())
            .await
            .expect("the ack reaches the source");
        // Several more polls have certainly run by now, and none of them may
        // have moved the position.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            confirmed_flush(&mut probe, &slot),
            before,
            "reading the slot or acknowledging a row must not advance it"
        );

        source
            .ack(commit.position())
            .await
            .expect("the ack reaches the source");
        let mut flushed = None;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            flushed = confirmed_flush(&mut probe, &slot);
            if flushed != before {
                break;
            }
        }
        assert_eq!(
            flushed.as_deref().and_then(PgLsn::parse),
            Some(commit.end_lsn()),
            "the acknowledged commit moves the slot to its end"
        );
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
        let event = next_row(&mut source, "the insert").await;
        assert_eq!(event.kind(), EventKind::Insert);
        next_commit(&mut source, "its commit").await;

        // Nothing is acknowledged, so every later poll peeks this same
        // transaction again.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let again = tokio::time::timeout(Duration::from_millis(500), source.next_item()).await;
        assert!(
            again.is_err(),
            "an unacknowledged item must not be delivered twice, got {again:?}"
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
            let event = next_row(&mut source, &format!("insert {id} on the same source")).await;
            next_commit(&mut source, &format!("the commit of insert {id}")).await;
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
        let mut commits = 0;
        for _ in 0..8 {
            match next_item(&mut source, &format!("every item, got {seen:?}")).await {
                SourceItem::Event(event) => seen.push(
                    event
                        .value_at(&catalog(), RowKind::New, 0)
                        .expect("the new image carries the key"),
                ),
                SourceItem::Commit(_) => commits += 1,
            }
        }
        assert_eq!(commits, 2, "one commit per transaction");
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
        for _ in 0..4 {
            seen.push(position(
                &next_item(&mut source, &format!("every copied row, got {seen:?}")).await,
            ));
        }
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "each copied row has a position of its own, and the commit follows them: {seen:?}"
        );
        assert_eq!(seen[3], PgCommitPosition::at_commit(seen[0].commit_lsn()));
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
        for _ in 0..5 {
            delivered.push(next_item(&mut source, &format!("every item, got {delivered:?}")).await);
        }
        let ids: Vec<_> = delivered
            .iter()
            .map(|item| match item {
                SourceItem::Event(event) => Some(
                    event
                        .value_at(&catalog(), RowKind::New, 0)
                        .expect("the new image carries the key"),
                ),
                SourceItem::Commit(_) => None,
            })
            .collect();
        assert_eq!(
            ids,
            [Some(1), None, Some(100), Some(101), None]
                .map(|id| id.map(subql::backend::Value::Int)),
            "the newer transaction and its commit, then the older one's rows and commit"
        );
        assert!(
            delivered
                .windows(2)
                .all(|pair| position(&pair[0]) < position(&pair[1])),
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
        next_row(&mut source, "the insert").await;

        // A data frame carries the position of the record in it, measured
        // as equal to its start on every frame of this suite, and the first
        // record of a transaction sits where the slot already was. Only the
        // commit frame, or a keepalive, moves the figure, so growth is a
        // bounded wait rather than a reading taken the instant one lands.
        let mut commit = None;
        let mut held = base;
        let started = std::time::Instant::now();
        while held <= base && started.elapsed() < Duration::from_secs(10) {
            if let Ok(polled) =
                tokio::time::timeout(Duration::from_millis(100), source.next_item()).await
            {
                if let Some(SourceItem::Commit(polled)) = polled.expect("no source error") {
                    commit = Some(polled);
                }
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

        let commit = match commit {
            Some(commit) => commit,
            None => next_commit(&mut source, "the insert's commit").await,
        };
        source
            .ack(commit.position())
            .await
            .expect("the ack reaches the source");
        // The held figure is a distance to the server's WAL end, which every
        // other database on a shared server also moves, so the acknowledged
        // position is what this can assert rather than a fall in the figure.
        for _ in 0..40 {
            if source.acknowledged_position().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            source.acknowledged_position(),
            Some(commit.end_lsn()),
            "the acknowledged commit releases the slot to its end, and it is reported"
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
            // next_item drives the frame loop, and must not produce one.
            match tokio::time::timeout(Duration::from_millis(200), source.next_item()).await {
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
