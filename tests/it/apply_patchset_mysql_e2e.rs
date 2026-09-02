//! Docker-backed E2E test for
//! [`subql::SubscriptionEngine::apply_patchset`] against MySQL.
//!
//! Spins up a real MySQL 8, creates a `things (id INT PK, active BOOL)`
//! table, builds an SQLite session patchset with INSERT / UPDATE / DELETE
//! ops that touch the `active` column, applies the patchset through
//! `apply_patchset` with the
//! [`MysqlAdapter`](subql::patchset::MysqlAdapter), and asserts the rows
//! land in MySQL with the correct `bool` values (no `CAST` wrapper, no
//! wire-format bind-type mismatch).
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require
//! Docker. Run with:
//!
//! ```sh
//! cargo test --test it apply_patchset_mysql_e2e:: --features apply-patchset-mysql -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{
    DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};
use sqlparser::dialect::MySqlDialect;
use subql::patchset::MysqlAdapter;
use subql::wal::MaxwellMessage;
use subql::{DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN);";
const MYSQL_DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN)";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    active: bool,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_bool_roundtrip_insert_update_delete_mysql() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn = common::mysql_connect(port);
    sql_query(MYSQL_DDL)
        .execute(&mut conn)
        .expect("create table");

    // subql catalog mirrors the MySQL DDL so the adapter can resolve
    // "things.active -> BOOLEAN" for dispatch.
    let catalog = ParserDB::parse::<MySqlDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<MaxwellMessage, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, MySqlDialect {});

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

    let adapter = MysqlAdapter::new(engine.database()).expect("the catalog indexes");
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

    // Round 2: UPDATE id=2 active=true.
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
