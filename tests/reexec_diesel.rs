#![cfg(any())] // Phase 11: rewrite against Value<B> + TestEvent<B>. Retired Cell/WalEvent/RowImage/PrimaryKey/ColumnType api. Tracked in docs/refactor-cdc-event-handoff.md.

//! Integration test for [`DieselConnector`]: drives a full `MIN(price)`
//! subscription through the [`AutoResolvingEngine`] against an in-memory
//! SQLite database. Verifies that:
//!
//! 1. Deleting the current extreme triggers a re-execution that goes
//!    through the Diesel connector, decodes the new MIN, installs it, and
//!    surfaces a [`ScalarUpdate`] (no triggers under the auto-resolving
//!    engine).
//! 2. A `MIN` over an empty result set decodes as [`Cell::Null`].
//!
//! The lib's unit tests cover the "no DB call on insert above the
//! extreme" optimization with a `MockConnector`. Here we only assert the
//! Diesel side of the round-trip. Each test seeds SQLite with whatever
//! state the connector is expected to read at re-execution time, then
//! dispatches a CDC event that forces the engine to call the connector.
//!
//! Gated via `[[test]] required-features = ["executor-diesel"]` in
//! `Cargo.toml`, so default `cargo test` does not try to compile it.
#![cfg(feature = "executor-diesel")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::reexec::{AutoResolvingEngine, DieselConnector, ReExecEngine, Registered};
use subql::{Cell, DefaultIds, RowImage, SubscriptionEngine, SubscriptionRequest, WalEvent};

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn sqlite_with(rows: &[(i64, f64)]) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query(
        "CREATE TABLE orders (\
            id INTEGER PRIMARY KEY, \
            price REAL, \
            quantity INTEGER, \
            status TEXT)",
    )
    .execute(&mut conn)
    .unwrap();
    for (id, price) in rows {
        sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(&mut conn)
        .unwrap();
    }
    conn
}

fn row(id: i64, price: f64) -> RowImage {
    RowImage {
        cells: Arc::from([
            Cell::Int(id),
            Cell::Float(price),
            Cell::Int(1),
            Cell::String(Arc::from("paid")),
        ]),
    }
}

fn delete_event(id: i64, price: f64) -> WalEvent {
    WalEvent::builder(0)
        .delete()
        .pk_cell(0, Cell::Int(id))
        .old_row(row(id, price))
        .build()
        .unwrap()
}

fn engine(
    conn: SqliteConnection,
) -> AutoResolvingEngine<PostgreSqlDialect, DefaultIds, ParserDB, DieselConnector<SqliteConnection>>
{
    let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
        catalog(),
        PostgreSqlDialect {},
    );
    AutoResolvingEngine::new(ReExecEngine::new(inner), DieselConnector::new(conn))
}

fn register_min(
    e: &mut AutoResolvingEngine<
        PostgreSqlDialect,
        DefaultIds,
        ParserDB,
        DieselConnector<SqliteConnection>,
    >,
    bootstrap: Cell,
) -> u64 {
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered::ReExec { query_id, .. } => query_id,
        Registered::Engine(_) => panic!("expected ReExec, got Engine"),
    };
    assert!(e.install(qid, bootstrap));
    qid
}

#[test]
fn delete_of_extreme_resolves_via_diesel_connector() {
    // SQLite reflects the state AFTER id=1 has been deleted: just id=2 with
    // price=9.0. The model previously had {1: 5.0, 2: 9.0} - we bootstrap
    // with 5.0 to mirror that. The delete event then forces the connector to
    // re-query SQLite, which now contains only id=2 with price=9.0.
    let conn = sqlite_with(&[(2, 9.0)]);
    let mut e = engine(conn);
    let qid = register_min(&mut e, Cell::Float(5.0));

    let n = e.consumers(&delete_event(1, 5.0)).unwrap();
    assert!(
        n.triggers.is_empty(),
        "AutoResolvingEngine drains triggers internally"
    );
    assert_eq!(n.scalar_updates.len(), 1, "expected MIN to be re-executed");
    assert_eq!(n.scalar_updates[0].query_id, qid);
    assert_eq!(
        n.scalar_updates[0].value,
        Cell::Float(9.0),
        "Diesel returned the new MIN"
    );
}

#[test]
fn empty_set_min_decodes_as_null() {
    // SQLite has no rows. The connector re-executes MIN(price) and gets a
    // single SQL-NULL row, which decodes as Cell::Null.
    let conn = sqlite_with(&[]);
    let mut e = engine(conn);
    let qid = register_min(&mut e, Cell::Float(5.0));

    let n = e.consumers(&delete_event(1, 5.0)).unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].query_id, qid);
    assert_eq!(
        n.scalar_updates[0].value,
        Cell::Null,
        "MIN over empty set -> SQL NULL -> Cell::Null"
    );
}
