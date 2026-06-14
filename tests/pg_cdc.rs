//! Docker-backed integration test for [`PgCdcReader`].
//!
//! Exercises the polling-based PostgreSQL CDC reader: creates a slot,
//! applies a few DML statements through a side PgConnection, drains the
//! slot into typed `WalEvent<PgLsn>`s, and advances the slot to the last
//! observed LSN.
//!
//! Gated by `[[test]] required-features = ["pg-cdc"]`. Run with:
//!
//! ```sh
//! cargo test --test pg_cdc --features pg-cdc -- --ignored --nocapture
//! ```
#![cfg(feature = "pg-cdc")]
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::wal::PgCdcReader;
use subql::{EventKind, PgLsn};

const SLOT: &str = "subql_pg_cdc_test";
const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn drains_inserts_and_deletes_with_increasing_lsn() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    // Setup connection: creates the table and ensures REPLICA IDENTITY FULL.
    let mut setup_conn = common::pg_connect(port);
    sql_query(PG_DDL)
        .execute(&mut setup_conn)
        .expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup_conn)
        .expect("REPLICA IDENTITY FULL");

    // DML connection (separate from the reader's own connection).
    let mut dml_conn = common::pg_connect(port);

    // Reader connection. The reader owns it for its lifetime.
    let reader_conn = common::pg_connect(port);
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let mut reader = PgCdcReader::new(reader_conn, SLOT.to_string(), catalog);

    let created = reader.ensure_slot().expect("ensure_slot");
    assert!(created, "slot must be freshly created");

    // Insert two rows + delete one. wal2json includes LSN per message.
    sql_query("INSERT INTO orders (id, price) VALUES (1, 5.0)")
        .execute(&mut dml_conn)
        .expect("insert id=1");
    sql_query("INSERT INTO orders (id, price) VALUES (2, 9.0)")
        .execute(&mut dml_conn)
        .expect("insert id=2");
    sql_query("DELETE FROM orders WHERE id = 1")
        .execute(&mut dml_conn)
        .expect("delete id=1");

    // Drain. Expect exactly three events: 2 INSERTs + 1 DELETE.
    let events = reader.next_batch().expect("next_batch");
    assert_eq!(
        events.len(),
        3,
        "expected 2 INSERT + 1 DELETE, got {}",
        events.len()
    );
    assert_eq!(events[0].kind(), EventKind::Insert);
    assert_eq!(events[1].kind(), EventKind::Insert);
    assert_eq!(events[2].kind(), EventKind::Delete);

    // Every event must carry a `Some(PgLsn(_))` and LSNs must be
    // monotonically non-decreasing in commit order.
    let lsns: Vec<PgLsn> = events
        .iter()
        .map(|e| e.checkpoint().copied().expect("wal2json LSN"))
        .collect();
    assert!(lsns.windows(2).all(|w| w[0] <= w[1]), "LSNs: {lsns:?}");

    // next_batch is idempotent (peek-based): a second drain with no new
    // DML returns the same events. Only `advance` releases them.
    let repeated = reader.next_batch().expect("second drain");
    assert_eq!(
        repeated.len(),
        3,
        "peek-based drain must be idempotent until advance"
    );

    // Advance the slot to the last observed LSN. We don't assert on the
    // post-advance drain because wal2json's per-message LSN refers to
    // the change's WAL position while the slot's confirmed_flush_lsn
    // tracks transaction commit boundaries; the relationship between
    // the two is wal2json-version-specific. The assertion here is just
    // that advance succeeds without error.
    let last_lsn = *lsns.last().unwrap();
    reader.advance(last_lsn).expect("advance");

    // Cleanup.
    reader.drop_slot().expect("drop_slot");
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn ensure_slot_is_idempotent() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup_conn = common::pg_connect(port);
    sql_query(PG_DDL)
        .execute(&mut setup_conn)
        .expect("create table");

    let reader_conn = common::pg_connect(port);
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let mut reader = PgCdcReader::new(reader_conn, SLOT.to_string(), catalog);

    assert!(reader.ensure_slot().expect("first ensure_slot"));
    assert!(
        !reader.ensure_slot().expect("second ensure_slot"),
        "second call must be a no-op"
    );
    reader.drop_slot().expect("drop");
}
