//! End-to-end SQLite test: `register_follow_insert` + `SqliteCdcSource`.
//!
//! Wires the two together against one shared SQLite database and asserts the
//! follow does not merely register but actually *receives the inserted row's
//! delta*: execute `INSERT ... RETURNING id` (follow registered), let the
//! source's `AFTER INSERT` trigger capture it into the shadow log, drain the
//! `WalEvent`, dispatch it, and confirm the follower is notified.
//!
//! A shared temp-file database is used (not `:memory:`) so the writer connection
//! and the CDC source's connection see the same schema (triggers) and shadow log.
#![cfg(all(feature = "sqlite-cdc", feature = "diesel-typed-sqlite"))]
#![allow(clippy::unwrap_used)]

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{DefaultIds, EventKind, SqliteCdcConfig, SqliteCdcSource, SubscriptionEngine};

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

const CATALOG_DDL: &str = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);";

#[test]
fn follow_insert_then_receive_cdc_delta() {
    let path = std::env::temp_dir().join(format!("subql_follow_cdc_{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let db = path.to_str().unwrap();

    // CDC source over the shared file: create the table, then install triggers.
    let mut source_conn = SqliteConnection::establish(db).unwrap();
    diesel::sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .execute(&mut source_conn)
        .unwrap();
    let source_catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap();
    let mut source =
        SqliteCdcSource::new(source_conn, source_catalog, SqliteCdcConfig::default()).unwrap();

    // Engine over the same schema.
    let engine_catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap();
    let mut engine =
        SubscriptionEngine::<_, DefaultIds, _>::new(engine_catalog, PostgreSqlDialect {});

    // Execute the insert on a second connection to the same file, and follow the
    // minted row. The insert fires the source's AFTER INSERT trigger.
    let mut writer = SqliteConnection::establish(db).unwrap();
    let follow = engine
        .register_follow_insert(
            1,
            diesel::insert_into(users::table).values(users::name.eq("ann")),
            &mut writer,
        )
        .unwrap();
    assert_eq!(follow.len(), 1);

    // Drain the captured change and dispatch it.
    let event = source
        .poll_next_event()
        .unwrap()
        .expect("the insert should have produced a CDC event");
    assert_eq!(event.kind(), EventKind::Insert);

    let output = engine.dispatch(&event).unwrap();

    // The follow (WHERE id = 1) receives the inserted row as a row-in delta.
    assert!(
        output.notifications().inserted().contains(&1),
        "follower (consumer 1) should receive the inserted row's delta, got notified: {:?}",
        output.notified()
    );

    let _ = std::fs::remove_file(&path);
}
