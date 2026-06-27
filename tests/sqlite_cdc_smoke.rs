//! Step-1 smoke test for the in-process [`SqliteCdcSource`].
//!
//! Builds a source against an in-memory SQLite, registers a single PG
//! `CREATE TABLE` against both the engine catalog (`ParserDB`) and the
//! SQLite-side schema, performs one INSERT through the source, drains
//! the resulting event stream, and asserts that exactly one
//! [`WalEvent::Insert`](subql::WalEvent::Insert) was produced with the
//! catalog-derived `table_id`, `pk`, and `new_row`.
//!
//! Gated via `[[test]] required-features = ["sqlite-cdc"]` in
//! `Cargo.toml`, so default `cargo test` skips it.
#![cfg(feature = "sqlite-cdc")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::{catalog_helpers, CdcSource, Cell, EventKind, SqliteCdcConfig, SqliteCdcSource};

const PG_DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

const SQLITE_DDL: &str = "CREATE TABLE orders (\
    id INTEGER PRIMARY KEY, \
    price REAL, \
    quantity INTEGER, \
    status TEXT)";

fn open_sqlite_with_schema() -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut conn).unwrap();
    conn
}

#[tokio::test]
async fn insert_surfaces_as_wal_event_insert() {
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap());
    let conn = open_sqlite_with_schema();

    let mut source = SqliteCdcSource::new(conn, Arc::clone(&catalog), SqliteCdcConfig::default())
        .expect("source construction must succeed");

    let rows = source
        .execute("INSERT INTO orders (id, price, quantity, status) VALUES (7, 9.5, 1, 'paid')")
        .expect("INSERT must succeed");
    assert_eq!(rows, 1, "INSERT touched one row");

    let event = source
        .next_event()
        .await
        .expect("next_event must succeed")
        .expect("the shadow log should hold exactly one event");

    let expected_table_id = catalog_helpers::table_id(catalog.as_ref(), "orders").unwrap();

    assert_eq!(event.kind(), EventKind::Insert);
    assert_eq!(event.table_id(), expected_table_id);
    assert_eq!(event.pk().columns(), &[0]);
    assert_eq!(event.pk().values(), &[Cell::Int(7)]);

    let new_row = event.new_row().expect("INSERT carries a new_row image");
    assert_eq!(new_row.cells.len(), 4);
    assert_eq!(new_row.cells[0], Cell::Int(7));
    assert_eq!(new_row.cells[1], Cell::Float(9.5));
    assert_eq!(new_row.cells[2], Cell::Int(1));
    assert_eq!(new_row.cells[3], Cell::String(Arc::from("paid")));

    let trailing = source.next_event().await.expect("queue is now empty");
    assert!(
        trailing.is_none(),
        "exactly one event should have been queued"
    );
}
