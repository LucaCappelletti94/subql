//! E2E test for [`subql::SubscriptionEngine::apply_patchset`] against a
//! SQLite target.
//!
//! The trivial round-trip: source (SQLite session-extension bytes) and
//! target (SQLite database) speak the same affinity-based type system,
//! so every value falls through
//! [`sqlite_diff_rs::DefaultBinder`] with no per-type dispatch. This test
//! confirms the wiring: the [`SqliteAdapter`] resolves column names from
//! the subql catalog, the engine method drives the batch through diesel,
//! and INSERT / UPDATE / DELETE round-trip through an in-memory SQLite
//! database.
//!
//! No Docker required; runs on the default `cargo test` path.

#![allow(clippy::unwrap_used)]

use diesel::{sql_query, Connection, QueryableByName, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{
    DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};
use sqlparser::dialect::SQLiteDialect;
use subql::patchset::SqliteAdapter;
use subql::{DefaultIds, SubscriptionEngine};
// SQLite CDC changeset event is the natural E: CdcEvent for a
// SQLite-target engine. Any CdcEvent whose Backend is SQLite works,
// including the test-only TestEvent<SQLite>.
use subql::testing::TestEvent;

const DDL: &str =
    "CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT, active INTEGER, blob_col BLOB);";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    active: i64,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    blob_col: Vec<u8>,
}

#[test]
#[allow(clippy::too_many_lines)]
fn apply_patchset_sqlite_roundtrip_insert_update_delete() {
    let mut conn = SqliteConnection::establish(":memory:").expect("open in-memory sqlite");
    sql_query(DDL).execute(&mut conn).expect("create table");

    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<TestEvent<subql::backend::SQLite>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, SQLiteDialect {});
    let adapter = SqliteAdapter::new(engine.database());

    let things = SimpleTable::new("things", &["id", "name", "active", "blob_col"], &[0]);

    // Round 1: INSERT two rows spanning all four SQLite affinities.
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(things.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, "alice".to_owned())
                .unwrap()
                .set(2, 1_i64)
                .unwrap()
                .set(3, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]))
                .unwrap(),
        )
        .insert(
            Insert::from(things.clone())
                .set(0, 2_i64)
                .unwrap()
                .set(1, "bob".to_owned())
                .unwrap()
                .set(2, 0_i64)
                .unwrap()
                .set(3, Value::Blob(vec![0x01, 0x02])) // arbitrary length blob
                .unwrap(),
        );

    let n = engine
        .apply_patchset(&inserts, &mut conn, &adapter)
        .expect("apply inserts");
    assert_eq!(n, 2, "two rows inserted");

    let rows: Vec<ThingRow> =
        sql_query("SELECT id, name, active, blob_col FROM things ORDER BY id")
            .load(&mut conn)
            .expect("load");
    assert_eq!(
        rows,
        vec![
            ThingRow {
                id: 1,
                name: "alice".to_owned(),
                active: 1,
                blob_col: vec![0xDE, 0xAD, 0xBE, 0xEF],
            },
            ThingRow {
                id: 2,
                name: "bob".to_owned(),
                active: 0,
                blob_col: vec![0x01, 0x02],
            },
        ]
    );

    // Round 2: UPDATE id=2 name and active.
    let updates = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(
        Update::<_, PatchsetFormat, String, Vec<u8>>::from(things.clone())
            .set(0, 2_i64)
            .unwrap()
            .set(1, "bobbles".to_owned())
            .unwrap()
            .set(2, 1_i64)
            .unwrap()
            .set(3, Value::Blob(vec![0x03, 0x04]))
            .unwrap(),
    );
    let n = engine
        .apply_patchset(&updates, &mut conn, &adapter)
        .expect("apply updates");
    assert_eq!(n, 1);

    let row: ThingRow = sql_query("SELECT id, name, active, blob_col FROM things WHERE id = 2")
        .get_result(&mut conn)
        .expect("load id=2");
    assert_eq!(
        row,
        ThingRow {
            id: 2,
            name: "bobbles".to_owned(),
            active: 1,
            blob_col: vec![0x03, 0x04],
        }
    );

    // Round 3: DELETE id=1.
    let deletes = PatchSet::<SimpleTable, String, Vec<u8>>::new().delete(PatchDelete::<
        SimpleTable,
        String,
        Vec<u8>,
    >::new(
        things,
        vec![Value::Integer(1)],
    ));
    let n = engine
        .apply_patchset(&deletes, &mut conn, &adapter)
        .expect("apply deletes");
    assert_eq!(n, 1);

    let remaining: Vec<ThingRow> =
        sql_query("SELECT id, name, active, blob_col FROM things ORDER BY id")
            .load(&mut conn)
            .expect("load remaining");
    assert_eq!(
        remaining,
        vec![ThingRow {
            id: 2,
            name: "bobbles".to_owned(),
            active: 1,
            blob_col: vec![0x03, 0x04],
        }]
    );
}
