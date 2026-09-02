//! Docker-backed E2E for the async apply entry points that
//! `apply_patchset_pg_async_e2e` does not reach:
//! [`SubscriptionEngine::apply_diffset_bytes_async`](subql::SubscriptionEngine::apply_diffset_bytes_async)
//! (both the patchset and changeset dispatch arms) and
//! [`SubscriptionEngine::apply_changeset_async`](subql::SubscriptionEngine::apply_changeset_async)
//! (a primary-key-relocating UPDATE, which only a changeset can carry).
//!
//! The uploaded session bytes are built in memory with
//! `DiffSetBuilder::build`, so no SQLite session harness is needed. Ops run
//! against a real Postgres over a `diesel-async` connection on a
//! multi-thread runtime.
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require Docker.
//! Run with:
//!
//! ```sh
//! cargo test --test it apply_diffset_async_pg_e2e:: \
//!     --features apply-patchset-postgres-async -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, QueryableByName, RunQueryDsl};
use diesel_async::{AsyncConnection, AsyncPgConnection};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{
    ChangeSet, ChangesetFormat, DiffOps, Insert, PatchSet, PatchsetFormat, SimpleTable, Update,
    Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::PgAdapter;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT);";
const PG_DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT)";

#[derive(QueryableByName, Debug, PartialEq)]
struct Item {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    label: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    qty: i32,
}

fn load_items(conn: &mut diesel::PgConnection) -> Vec<Item> {
    sql_query("SELECT id, label, qty FROM items ORDER BY id")
        .load(conn)
        .expect("load items")
}

fn items_table() -> SimpleTable {
    SimpleTable::new("items", &["id", "label", "qty"], &[0])
}

fn engine() -> SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
    SubscriptionEngine::new(catalog, PostgreSqlDialect {})
}

/// `apply_diffset_bytes_async` dispatches uploaded **patchset** bytes: parse,
/// reconstruct against the catalog, and apply. Covers the patchset arm plus
/// the byte-parse entry point.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn diffset_bytes_async_applies_patchset_bytes() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    common::multi_thread_rt().block_on(async move {
        let mut verify = common::pg_connect(port);
        sql_query(PG_DDL)
            .execute(&mut verify)
            .expect("create table");
        let mut conn = AsyncPgConnection::establish(&common::pg_url(port))
            .await
            .expect("async pg connect");

        let engine = engine();
        let adapter = PgAdapter::new(engine.database()).expect("the catalog indexes");
        let table = items_table();

        // Two inserts, serialized to SQLite session patchset bytes in memory.
        let insert_bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(
                Insert::from(table.clone())
                    .set(0, 1_i64)
                    .unwrap()
                    .set(1, Value::Text("a".into()))
                    .unwrap()
                    .set(2, 10_i64)
                    .unwrap(),
            )
            .insert(
                Insert::from(table.clone())
                    .set(0, 2_i64)
                    .unwrap()
                    .set(1, Value::Text("b".into()))
                    .unwrap()
                    .set(2, 20_i64)
                    .unwrap(),
            )
            .build();

        let n = engine
            .apply_diffset_bytes_async(&insert_bytes, &mut conn, &adapter)
            .await
            .expect("apply patchset bytes");
        assert_eq!(n, 2, "two rows inserted from patchset bytes");
        assert_eq!(
            load_items(&mut verify),
            vec![
                Item {
                    id: 1,
                    label: "a".into(),
                    qty: 10
                },
                Item {
                    id: 2,
                    label: "b".into(),
                    qty: 20
                },
            ]
        );

        // A non-key update, also carried as patchset bytes.
        let update_bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .update(
                Update::<_, PatchsetFormat, String, Vec<u8>>::from(table)
                    .set(0, 2_i64)
                    .unwrap()
                    .set(1, Value::Text("bb".into()))
                    .unwrap(),
            )
            .build();
        let n = engine
            .apply_diffset_bytes_async(&update_bytes, &mut conn, &adapter)
            .await
            .expect("apply update bytes");
        assert_eq!(n, 1, "one row updated from patchset bytes");
        let row: Item = sql_query("SELECT id, label, qty FROM items WHERE id = 2")
            .get_result(&mut verify)
            .expect("load id=2");
        assert_eq!(
            row,
            Item {
                id: 2,
                label: "bb".into(),
                qty: 20
            }
        );
    });
}

/// `apply_changeset_async` applies a primary-key-relocating UPDATE (id 2 to
/// 20, relabelled, `qty` untouched). A patchset cannot represent this, so it
/// is the distinguishing changeset path. Exercises the direct changeset
/// method (no byte parse).
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_changeset_async_relocates_primary_key() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    common::multi_thread_rt().block_on(async move {
        let mut verify = common::pg_connect(port);
        sql_query(PG_DDL)
            .execute(&mut verify)
            .expect("create table");
        sql_query("INSERT INTO items (id, label, qty) VALUES (1, 'a', 10), (2, 'b', 200)")
            .execute(&mut verify)
            .expect("seed");
        let mut conn = AsyncPgConnection::establish(&common::pg_url(port))
            .await
            .expect("async pg connect");

        let engine = engine();
        let adapter = PgAdapter::new(engine.database()).expect("the catalog indexes");

        let changeset = ChangeSet::<SimpleTable, String, Vec<u8>>::new().update(
            Update::<_, ChangesetFormat, String, Vec<u8>>::from(items_table())
                .set(0, Value::Integer(2), Value::Integer(20))
                .unwrap()
                .set(1, Value::Text("b".into()), Value::Text("moved".into()))
                .unwrap(),
        );

        let n = engine
            .apply_changeset_async(&changeset, &mut conn, &adapter)
            .await
            .expect("apply changeset");
        assert_eq!(n, 1, "one row relocated");
        assert_eq!(
            load_items(&mut verify),
            vec![
                Item {
                    id: 1,
                    label: "a".into(),
                    qty: 10
                },
                // id relocated 2 -> 20, relabelled, qty preserved.
                Item {
                    id: 20,
                    label: "moved".into(),
                    qty: 200
                },
            ]
        );
    });
}

/// `apply_diffset_bytes_async` dispatches uploaded **changeset** bytes: the
/// changeset arm of the byte entry point, carrying the same primary-key
/// relocation. Proves the marker dispatch inside the async byte path.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn diffset_bytes_async_applies_changeset_bytes_pk_change() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    common::multi_thread_rt().block_on(async move {
        let mut verify = common::pg_connect(port);
        sql_query(PG_DDL)
            .execute(&mut verify)
            .expect("create table");
        sql_query("INSERT INTO items (id, label, qty) VALUES (2, 'b', 200)")
            .execute(&mut verify)
            .expect("seed");
        let mut conn = AsyncPgConnection::establish(&common::pg_url(port))
            .await
            .expect("async pg connect");

        let engine = engine();
        let adapter = PgAdapter::new(engine.database()).expect("the catalog indexes");

        let changeset_bytes = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
            .update(
                Update::<_, ChangesetFormat, String, Vec<u8>>::from(items_table())
                    .set(0, Value::Integer(2), Value::Integer(20))
                    .unwrap()
                    .set(1, Value::Text("b".into()), Value::Text("moved".into()))
                    .unwrap(),
            )
            .build();

        let n = engine
            .apply_diffset_bytes_async(&changeset_bytes, &mut conn, &adapter)
            .await
            .expect("apply changeset bytes");
        assert_eq!(n, 1, "one row relocated from changeset bytes");
        assert_eq!(
            load_items(&mut verify),
            vec![Item {
                id: 20,
                label: "moved".into(),
                qty: 200
            }]
        );
    });
}
