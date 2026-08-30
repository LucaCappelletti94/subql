//! Docker-backed E2E test for [`subql::SubscriptionEngine::apply_patchset`].
//!
//! Spins up a real Postgres, creates a `things (id INT PK, active BOOL)`
//! table, builds an SQLite session patchset with INSERT / UPDATE / DELETE
//! ops that touch the `active` column, applies the patchset through
//! `apply_patchset` with the [`PgAdapter`](subql::patchset::PgAdapter),
//! and asserts the rows land in PG with the correct `bool` values (no
//! `CAST` wrapper, no wire-format bind-type mismatch).
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require
//! Docker. Run with:
//!
//! ```sh
//! cargo test --test it apply_patchset_pg_e2e:: --features apply-patchset-postgres -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{
    DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::PgAdapter;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN);";
const PG_DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN)";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    active: bool,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_bool_roundtrip_insert_update_delete() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut conn).expect("create table");

    // subql catalog mirrors the PG DDL so the adapter can resolve
    // "things.active -> BOOLEAN" for dispatch.
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});

    // SQLite session table descriptor. Column 0 is the PK.
    let things = SimpleTable::new("things", &["id", "active"], &[0]);

    // Round 1: INSERT id=1 active=true, INSERT id=2 active=false.
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(things.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, 1_i64)
                .unwrap(),
        )
        .insert(
            Insert::from(things.clone())
                .set(0, 2_i64)
                .unwrap()
                .set(1, 0_i64)
                .unwrap(),
        );

    let adapter = PgAdapter::new(engine.database());
    let n = engine
        .apply_patchset(&inserts, &mut conn, &adapter)
        .expect("apply inserts");
    assert_eq!(n, 2, "two rows inserted");

    let rows: Vec<ThingRow> = sql_query("SELECT id, active FROM things ORDER BY id")
        .load(&mut conn)
        .expect("load");
    assert_eq!(
        rows,
        vec![
            ThingRow {
                id: 1,
                active: true
            },
            ThingRow {
                id: 2,
                active: false
            },
        ]
    );

    let updates = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(
        Update::<_, PatchsetFormat, String, Vec<u8>>::from(things.clone())
            .set(0, 2_i64)
            .unwrap()
            .set(1, 1_i64)
            .unwrap(),
    );
    let n = engine
        .apply_patchset(&updates, &mut conn, &adapter)
        .expect("apply updates");
    assert_eq!(n, 1, "one row updated");

    let row: ThingRow = sql_query("SELECT id, active FROM things WHERE id = 2")
        .get_result(&mut conn)
        .expect("load");
    assert_eq!(
        row,
        ThingRow {
            id: 2,
            active: true
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
    assert_eq!(n, 1, "one row deleted");

    let remaining: Vec<ThingRow> = sql_query("SELECT id, active FROM things ORDER BY id")
        .load(&mut conn)
        .expect("load");
    assert_eq!(
        remaining,
        vec![ThingRow {
            id: 2,
            active: true
        }]
    );
}
