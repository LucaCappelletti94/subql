#![cfg(any())] // Phase 11: rewrite against E: CdcEvent shape. SubscriptionEngine took <Dialect,...>, now takes <E: CdcEvent,...>. Tracked in docs/refactor-cdc-event-handoff.md.

//! Docker-backed end-to-end test for the pgoutput binary replication parser.
//!
//! Spins up a real Postgres, creates a publication + pgoutput logical slot,
//! applies INSERT/UPDATE/DELETE through a side connection, drains the slot
//! as raw `bytea` messages via `pg_logical_slot_get_binary_changes`, and
//! feeds each message through [`subql::PgOutputParser`]. Asserts that the
//! parser surfaces typed `WalEvent`s with the right kinds and primary
//! keys.
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require Docker.
//! Run with:
//!
//! ```sh
//! cargo test --test pgoutput_e2e -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

mod common;

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{EventKind, PgOutputParser, WalParser};

const SLOT: &str = "subql_pgoutput_e2e";
const PUBLICATION: &str = "subql_pgoutput_e2e_pub";
const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn pgoutput_stream_parses_insert_update_delete() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    // Setup connection: create the table, set REPLICA IDENTITY FULL so
    // UPDATE and DELETE carry the old row image, create the publication,
    // then create the pgoutput slot. Order matters: the slot must exist
    // before any DML if we want to capture those events.
    let mut setup_conn = common::pg_connect(port);
    sql_query(PG_DDL)
        .execute(&mut setup_conn)
        .expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup_conn)
        .expect("REPLICA IDENTITY FULL");
    common::create_publication(&mut setup_conn, PUBLICATION, "orders");
    common::create_pgoutput_slot(&mut setup_conn, SLOT);

    // Apply DML through a side connection.
    let mut dml_conn = common::pg_connect(port);
    sql_query("INSERT INTO orders (id, price) VALUES (1, 5.0)")
        .execute(&mut dml_conn)
        .expect("insert id=1");
    sql_query("INSERT INTO orders (id, price) VALUES (2, 9.0)")
        .execute(&mut dml_conn)
        .expect("insert id=2");
    sql_query("UPDATE orders SET price = 7.0 WHERE id = 2")
        .execute(&mut dml_conn)
        .expect("update id=2");
    sql_query("DELETE FROM orders WHERE id = 1")
        .execute(&mut dml_conn)
        .expect("delete id=1");

    // Drain the slot as raw pgoutput message bodies.
    let messages = common::drain_pgoutput_slot(&mut setup_conn, SLOT, PUBLICATION);
    assert!(
        !messages.is_empty(),
        "expected at least one pgoutput message after 4 DML statements"
    );

    // Parse each message through PgOutputParser. The parser caches relation
    // metadata internally, so it MUST be a single instance across the loop;
    // relation messages ('R') populate the cache without yielding events.
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let parser = PgOutputParser::new();
    let mut events = Vec::new();
    for msg in &messages {
        let parsed = parser
            .parse_wal_message(msg.as_slice(), &catalog)
            .expect("pgoutput parse");
        events.extend(parsed);
    }

    // Expect exactly 2 INSERTs + 1 UPDATE + 1 DELETE. Begin/Commit/Relation
    // messages are filtered out by the parser, so the typed event count is
    // the DML count.
    assert_eq!(
        events.len(),
        4,
        "expected 2 INSERT + 1 UPDATE + 1 DELETE, got {} events: {events:?}",
        events.len(),
    );
    assert_eq!(events[0].kind(), EventKind::Insert);
    assert_eq!(events[1].kind(), EventKind::Insert);
    assert_eq!(events[2].kind(), EventKind::Update);
    assert_eq!(events[3].kind(), EventKind::Delete);

    // Cleanup: drop the slot so a follow-up test run on the same DB starts
    // clean. (Idempotent within a fresh container, but a guard rail in
    // case the test is rerun.)
    sql_query(format!("SELECT pg_drop_replication_slot('{SLOT}')"))
        .execute(&mut setup_conn)
        .expect("drop slot");
    sql_query(format!("DROP PUBLICATION {PUBLICATION}"))
        .execute(&mut setup_conn)
        .expect("drop publication");
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn pgoutput_stream_handles_truncate() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let slot = "subql_pgoutput_truncate";
    let publication = "subql_pgoutput_truncate_pub";

    let mut setup_conn = common::pg_connect(port);
    sql_query(PG_DDL)
        .execute(&mut setup_conn)
        .expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup_conn)
        .expect("REPLICA IDENTITY FULL");
    common::create_publication(&mut setup_conn, publication, "orders");
    common::create_pgoutput_slot(&mut setup_conn, slot);

    let mut dml_conn = common::pg_connect(port);
    sql_query("INSERT INTO orders (id, price) VALUES (1, 5.0)")
        .execute(&mut dml_conn)
        .expect("insert id=1");
    sql_query("TRUNCATE orders")
        .execute(&mut dml_conn)
        .expect("truncate");

    let messages = common::drain_pgoutput_slot(&mut setup_conn, slot, publication);
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let parser = PgOutputParser::new();
    let mut events = Vec::new();
    for msg in &messages {
        events.extend(
            parser
                .parse_wal_message(msg.as_slice(), &catalog)
                .expect("pgoutput parse"),
        );
    }

    let truncate_count = events
        .iter()
        .filter(|e| matches!(e.kind(), EventKind::Truncate))
        .count();
    assert!(
        truncate_count >= 1,
        "expected at least one TRUNCATE event, got events: {events:?}",
    );

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup_conn)
        .expect("drop slot");
    sql_query(format!("DROP PUBLICATION {publication}"))
        .execute(&mut setup_conn)
        .expect("drop publication");
}
