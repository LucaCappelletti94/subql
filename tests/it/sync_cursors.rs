//! Cursors on the sync diesel connectors.
//!
//! A keyless answer, and the first answer of a keyed read, has no key to
//! resume from, so its pages must describe one instant. A sync connector
//! owns one connection, which cannot hold a transaction open across calls
//! while other reads run on it, so its cursor reads the whole answer in one
//! snapshot at open and pages it from memory.
#![allow(clippy::unwrap_used)]

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use subql::backend::{SQLite, Value};
use subql::reexec::{Connector as _, CursorError, DieselConnector, ReadQuery};

/// Two connections to one in-memory database, the first holding forty rows.
fn database(name: &str) -> (SqliteConnection, SqliteConnection) {
    let url = format!("file:{name}?mode=memory&cache=shared");
    let mut writer = SqliteConnection::establish(&url).unwrap();
    // Table setup, which the typed DSL does not express.
    diesel::sql_query("CREATE TABLE readings (id INTEGER PRIMARY KEY, label TEXT)")
        .execute(&mut writer)
        .unwrap();
    for id in 1..=40 {
        diesel::sql_query(format!("INSERT INTO readings VALUES ({id}, 'l{}')", id % 7))
            .execute(&mut writer)
            .unwrap();
    }
    let reader = SqliteConnection::establish(&url).unwrap();
    (writer, reader)
}

#[test]
fn a_cursor_pages_one_snapshot_of_a_keyless_result() {
    let (mut writer, reader) = database("sync_cursor_snapshot");
    let connector: DieselConnector<SqliteConnection, SQLite> = DieselConnector::new(reader);
    let cursor = connector
        .open_cursor(
            &ReadQuery::without_binds("SELECT DISTINCT id, label FROM readings ORDER BY id"),
            &(),
        )
        .unwrap();
    diesel::sql_query("INSERT INTO readings VALUES (999, 'late')")
        .execute(&mut writer)
        .unwrap();

    let mut ids = Vec::new();
    let mut pages = 0;
    loop {
        let page = connector.fetch_cursor(cursor, 64).unwrap();
        pages += 1;
        assert_eq!(
            page.value.columns,
            vec!["id", "label"],
            "every page names its columns"
        );
        assert!(!page.value.rows.is_empty(), "a page makes progress");
        for row in &page.value.rows {
            let Value::Int(id) = row[0] else {
                panic!("id decodes as an integer, got {:?}", row[0]);
            };
            ids.push(id);
        }
        if !page.value.more {
            break;
        }
        assert!(pages < 100, "the cursor finishes");
    }
    assert!(pages > 1, "a 64-byte budget splits forty rows");
    assert_eq!(
        ids,
        (1..=40).collect::<Vec<i64>>(),
        "every row once, none written after"
    );
    connector.close_cursor(cursor).unwrap();
}

#[test]
fn a_closed_cursor_is_unknown_and_closes_again() {
    let (_writer, reader) = database("sync_cursor_closed");
    let connector: DieselConnector<SqliteConnection, SQLite> = DieselConnector::new(reader);
    let cursor = connector
        .open_cursor(&ReadQuery::without_binds("SELECT id FROM readings"), &())
        .unwrap();
    connector.close_cursor(cursor).unwrap();
    assert!(matches!(
        connector.fetch_cursor(cursor, 64),
        Err(CursorError::Unknown(id)) if id == cursor
    ));
    connector.close_cursor(cursor).unwrap();
}

#[test]
fn a_cursor_that_fails_to_open_reports_the_database() {
    let (_writer, reader) = database("sync_cursor_failed");
    let connector: DieselConnector<SqliteConnection, SQLite> = DieselConnector::new(reader);
    assert!(matches!(
        connector.open_cursor(&ReadQuery::without_binds("SELECT nope FROM readings"), &()),
        Err(CursorError::Connector(_))
    ));
}

#[test]
fn two_cursors_page_independently() {
    let (_writer, reader) = database("sync_cursor_two");
    let connector: DieselConnector<SqliteConnection, SQLite> = DieselConnector::new(reader);
    let low = connector
        .open_cursor(
            &ReadQuery::without_binds("SELECT id FROM readings WHERE id <= 3 ORDER BY id"),
            &(),
        )
        .unwrap();
    let high = connector
        .open_cursor(
            &ReadQuery::without_binds("SELECT id FROM readings WHERE id > 38 ORDER BY id"),
            &(),
        )
        .unwrap();
    let first = |cursor| connector.fetch_cursor(cursor, 1).unwrap().value.rows[0][0].clone();
    assert_eq!(first(low), Value::Int(1));
    assert_eq!(first(high), Value::Int(39));
    assert_eq!(first(low), Value::Int(2));
    assert_eq!(first(high), Value::Int(40));
}
