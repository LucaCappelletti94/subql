//! A result set whose shape is not known at compile time decodes column by
//! column, which is what lets a captured query of arbitrary shape be read.
//!
//! `MILESTONES.md` blamed diesel for this being impossible: "Row-set decoding
//! through diesel's `sql_query` wants the schema at compile time: each column
//! needs its own typed accessor." It does not. `QueryableByName::build`
//! receives the row, and `Row::field_count`, `Row::get`, `Field::field_name`
//! and `Field::value` are public and ungated, so a decoder can walk a row it
//! has never seen. This test is that claim's refutation, run against real
//! SQLite in-process.
//!
//! The queries here go through `sql_query` on purpose rather than the typed
//! DSL: a statement whose column list is unknown until runtime is exactly the
//! case the typed DSL cannot express, and is the case this code path exists to
//! serve.
#![cfg(feature = "diesel-typed-sqlite")]
#![allow(clippy::unwrap_used)]

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use subql::backend::{SQLite, Value};
use subql::diesel_decode::DynamicRow;

fn conn() -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    // Table setup, which the typed DSL does not express.
    diesel::sql_query(
        "CREATE TABLE readings (id INTEGER PRIMARY KEY, label TEXT, amount REAL, raw BLOB, absent TEXT)",
    )
    .execute(&mut conn)
    .expect("create readings");
    diesel::sql_query("INSERT INTO readings VALUES (7, 'north', 1.5, X'00FF10', NULL)")
        .execute(&mut conn)
        .expect("insert reading");
    conn
}

/// Every storage class SQLite can hand back, plus a NULL, decoded off a
/// statement whose shape the compiler never saw.
#[test]
fn a_row_of_unknown_shape_decodes_every_storage_class() {
    let mut conn = conn();
    let rows: Vec<DynamicRow<SQLite>> = diesel::sql_query("SELECT * FROM readings")
        .load(&mut conn)
        .expect("load rows of unknown shape");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row.columns,
        vec!["id", "label", "amount", "raw", "absent"],
        "the column names come from the database, not from a schema"
    );
    assert_eq!(
        row.values,
        vec![
            Value::Int(7),
            Value::String("north".into()),
            Value::Float(1.5),
            Value::Bytes(vec![0x00, 0xff, 0x10]),
            Value::Null,
        ]
    );
}

/// A projection no `table!` describes: an expression, an alias, and a computed
/// column. This is the shape the catch-all tier exists for, and the one a
/// compile-time row struct cannot name.
#[test]
fn a_computed_projection_decodes_under_its_own_aliases() {
    let mut conn = conn();
    let rows: Vec<DynamicRow<SQLite>> = diesel::sql_query(
        "SELECT id * 2 AS doubled, upper(label) AS shout, length(raw) AS width FROM readings",
    )
    .load(&mut conn)
    .expect("load computed projection");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns, vec!["doubled", "shout", "width"]);
    assert_eq!(
        rows[0].values,
        vec![Value::Int(14), Value::String("NORTH".into()), Value::Int(3),]
    );
}

/// Several rows arrive in order, and an empty result is empty rather than an
/// error, since a captured query whose answer went empty is a normal outcome.
#[test]
fn row_counts_follow_the_result() {
    let mut conn = conn();
    diesel::sql_query("INSERT INTO readings VALUES (8, 'south', 2.5, X'01', NULL)")
        .execute(&mut conn)
        .expect("insert second reading");

    let rows: Vec<DynamicRow<SQLite>> = diesel::sql_query("SELECT id FROM readings ORDER BY id")
        .load(&mut conn)
        .expect("load two rows");
    assert_eq!(
        rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![vec![Value::Int(7)], vec![Value::Int(8)]]
    );

    let none: Vec<DynamicRow<SQLite>> = diesel::sql_query("SELECT id FROM readings WHERE id > 100")
        .load(&mut conn)
        .expect("load no rows");
    assert!(none.is_empty());
}
