//! Step-2 DML coverage for the in-process [`SqliteCdcSource`].
//!
//! Exercises INSERT / UPDATE / DELETE through the trigger-installed
//! shadow log used by the production source. Two table shapes are
//! covered: a single-column INTEGER PRIMARY KEY, and a composite
//! PRIMARY KEY spanning two columns. Both shapes prove the trigger
//! capture, the PK derivation in the engine-facing `WalEvent`, and the
//! `changed_columns` diff produced when UPDATE touches a strict subset
//! of the row.
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

const SINGLE_PK_PG_DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";
const SINGLE_PK_SQLITE_DDL: &str =
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, price REAL, status TEXT)";

const COMPOSITE_PK_PG_DDL: &str = "CREATE TABLE items (\
    region_id INT, item_id INT, name TEXT, \
    PRIMARY KEY (region_id, item_id));";
const COMPOSITE_PK_SQLITE_DDL: &str = "CREATE TABLE items (\
    region_id INTEGER, item_id INTEGER, name TEXT, \
    PRIMARY KEY (region_id, item_id))";

fn open_with(sqlite_ddl: &str) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query(sqlite_ddl).execute(&mut conn).unwrap();
    conn
}

fn make_source(sqlite_ddl: &str, pg_ddl: &str) -> SqliteCdcSource {
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(pg_ddl).unwrap());
    let conn = open_with(sqlite_ddl);
    SqliteCdcSource::new(conn, catalog, SqliteCdcConfig::default()).unwrap()
}

#[tokio::test]
async fn single_pk_insert_update_delete_round_trip() {
    let mut source = make_source(SINGLE_PK_SQLITE_DDL, SINGLE_PK_PG_DDL);
    let table_id =
        catalog_helpers::table_id(source.catalog(), "orders").expect("orders table resolves");

    source
        .execute("INSERT INTO orders (id, price, status) VALUES (1, 5.0, 'paid')")
        .unwrap();
    source
        .execute("UPDATE orders SET price = 9.0, status = 'shipped' WHERE id = 1")
        .unwrap();
    source.execute("DELETE FROM orders WHERE id = 1").unwrap();

    let insert = source.next_event().await.unwrap().unwrap();
    assert_eq!(insert.kind(), EventKind::Insert);
    assert_eq!(insert.table_id(), table_id);
    assert_eq!(insert.pk().columns(), &[0]);
    assert_eq!(insert.pk().values(), &[Cell::Int(1)]);
    let new = insert.new_row().expect("INSERT carries new_row");
    assert_eq!(new.cells[0], Cell::Int(1));
    assert_eq!(new.cells[1], Cell::Float(5.0));
    assert_eq!(new.cells[2], Cell::String(Arc::from("paid")));

    let update = source.next_event().await.unwrap().unwrap();
    assert_eq!(update.kind(), EventKind::Update);
    assert_eq!(update.table_id(), table_id);
    assert_eq!(update.pk().values(), &[Cell::Int(1)]);
    let old = update.old_row().expect("UPDATE carries old_row");
    let new = update.new_row().expect("UPDATE carries new_row");
    assert_eq!(old.cells[1], Cell::Float(5.0));
    assert_eq!(old.cells[2], Cell::String(Arc::from("paid")));
    assert_eq!(new.cells[1], Cell::Float(9.0));
    assert_eq!(new.cells[2], Cell::String(Arc::from("shipped")));
    assert_eq!(
        update.changed_columns(),
        &[1, 2],
        "price and status changed, id did not"
    );

    let delete = source.next_event().await.unwrap().unwrap();
    assert_eq!(delete.kind(), EventKind::Delete);
    assert_eq!(delete.table_id(), table_id);
    assert_eq!(delete.pk().values(), &[Cell::Int(1)]);
    let removed = delete.old_row().expect("DELETE carries old_row");
    assert_eq!(removed.cells[0], Cell::Int(1));
    assert_eq!(removed.cells[1], Cell::Float(9.0));
    assert_eq!(removed.cells[2], Cell::String(Arc::from("shipped")));

    assert!(source.next_event().await.unwrap().is_none());
}

#[tokio::test]
async fn with_pg_ddl_translates_and_applies_schema() {
    let conn = SqliteConnection::establish(":memory:").unwrap();
    let mut source =
        SqliteCdcSource::with_pg_ddl(conn, SINGLE_PK_PG_DDL, SqliteCdcConfig::default()).unwrap();
    let table_id =
        catalog_helpers::table_id(source.catalog(), "orders").expect("orders table resolves");

    source
        .execute("INSERT INTO orders (id, price, status) VALUES (7, 1.0, 'paid')")
        .unwrap();

    let event = source.next_event().await.unwrap().unwrap();
    assert_eq!(event.kind(), EventKind::Insert);
    assert_eq!(event.table_id(), table_id);
    assert_eq!(event.pk().values(), &[Cell::Int(7)]);
}

#[tokio::test]
async fn composite_pk_insert_update_delete_round_trip() {
    let mut source = make_source(COMPOSITE_PK_SQLITE_DDL, COMPOSITE_PK_PG_DDL);
    let table_id =
        catalog_helpers::table_id(source.catalog(), "items").expect("items table resolves");

    source
        .execute("INSERT INTO items (region_id, item_id, name) VALUES (1, 100, 'widget')")
        .unwrap();
    source
        .execute("UPDATE items SET name = 'gadget' WHERE region_id = 1 AND item_id = 100")
        .unwrap();
    source
        .execute("DELETE FROM items WHERE region_id = 1 AND item_id = 100")
        .unwrap();

    let insert = source.next_event().await.unwrap().unwrap();
    assert_eq!(insert.kind(), EventKind::Insert);
    assert_eq!(insert.table_id(), table_id);
    assert_eq!(insert.pk().columns(), &[0, 1]);
    assert_eq!(insert.pk().values(), &[Cell::Int(1), Cell::Int(100)]);

    let update = source.next_event().await.unwrap().unwrap();
    assert_eq!(update.kind(), EventKind::Update);
    assert_eq!(update.pk().columns(), &[0, 1]);
    assert_eq!(update.pk().values(), &[Cell::Int(1), Cell::Int(100)]);
    let old = update.old_row().expect("UPDATE carries old_row");
    let new = update.new_row().expect("UPDATE carries new_row");
    assert_eq!(old.cells[2], Cell::String(Arc::from("widget")));
    assert_eq!(new.cells[2], Cell::String(Arc::from("gadget")));
    assert_eq!(update.changed_columns(), &[2]);

    let delete = source.next_event().await.unwrap().unwrap();
    assert_eq!(delete.kind(), EventKind::Delete);
    assert_eq!(delete.pk().values(), &[Cell::Int(1), Cell::Int(100)]);
    let removed = delete.old_row().expect("DELETE carries old_row");
    assert_eq!(removed.cells[2], Cell::String(Arc::from("gadget")));

    assert!(source.next_event().await.unwrap().is_none());
}
