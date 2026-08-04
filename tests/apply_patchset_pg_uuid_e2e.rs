//! Docker-backed E2E test for [`subql::patchset::PgAdapter`] UUID
//! dispatch.
//!
//! Exercises both wire flavors clients may pick for local UUID storage:
//!
//! * `Value::Blob(16 bytes)` (compact binary form).
//! * `Value::Text("...")` (hyphenated canonical form).
//!
//! Both must land in a Postgres `UUID` column as the same
//! [`uuid::Uuid`] value with no `CAST` wrapper. Additionally asserts
//! that a `Value::Integer` bound to a `UUID` column is rejected with a
//! [`diesel::result::Error::QueryBuilderError`] at apply time, rolling
//! back the whole batch.
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require
//! Docker. Run with:
//!
//! ```sh
//! cargo test --test apply_patchset_pg_uuid_e2e --features apply-patchset-postgres -- --ignored --nocapture
//! ```

#![cfg(feature = "apply-patchset-postgres")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::result::Error as DieselError;
use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{
    DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::PgAdapter;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};
use uuid::Uuid;

const DDL: &str = "CREATE TABLE things (id UUID PRIMARY KEY, tag TEXT);";
const PG_DDL: &str = "CREATE TABLE things (id UUID PRIMARY KEY, tag TEXT)";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    tag: String,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
#[allow(clippy::too_many_lines)]
fn apply_patchset_uuid_roundtrip_blob_and_text_clients() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut conn).expect("create table");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});

    let things = SimpleTable::new("things", &["id", "tag"], &[0]);
    let adapter = PgAdapter::new(engine.database());

    // Two UUIDs, one delivered by a BLOB-preferring client, one by a
    // TEXT-preferring client. Both must land natively.
    let uuid_blob_client = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
    let uuid_text_client = Uuid::from_u128(0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111);

    // -------------------------------------------------------------------
    // Round 1: INSERT one row with BLOB PK, one row with TEXT PK.
    // -------------------------------------------------------------------
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(things.clone())
                .set(0, uuid_blob_client.as_bytes().to_vec())
                .unwrap()
                .set(1, String::from("from-blob-client"))
                .unwrap(),
        )
        .insert(
            Insert::from(things.clone())
                .set(0, uuid_text_client.hyphenated().to_string())
                .unwrap()
                .set(1, String::from("from-text-client"))
                .unwrap(),
        );

    let n = engine
        .apply_patchset(&inserts, &mut conn, &adapter)
        .expect("apply inserts (BLOB + TEXT UUID flavors)");
    assert_eq!(n, 2, "two rows inserted");

    let rows: Vec<ThingRow> = sql_query("SELECT id, tag FROM things ORDER BY tag")
        .load(&mut conn)
        .expect("load");
    assert_eq!(
        rows,
        vec![
            ThingRow {
                id: uuid_blob_client,
                tag: "from-blob-client".to_string()
            },
            ThingRow {
                id: uuid_text_client,
                tag: "from-text-client".to_string()
            },
        ]
    );

    // -------------------------------------------------------------------
    // Round 2: UPDATE the BLOB-client row's tag via a TEXT PK reference
    // (proving the adapter accepts both flavors even for the same row).
    // -------------------------------------------------------------------
    let updates = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(
        Update::<_, PatchsetFormat, String, Vec<u8>>::from(things.clone())
            .set(0, uuid_blob_client.hyphenated().to_string())
            .unwrap()
            .set(1, String::from("relabeled"))
            .unwrap(),
    );
    let n = engine
        .apply_patchset(&updates, &mut conn, &adapter)
        .expect("apply updates");
    assert_eq!(n, 1, "one row updated");

    let row: ThingRow = sql_query("SELECT id, tag FROM things WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(uuid_blob_client)
        .get_result(&mut conn)
        .expect("load");
    assert_eq!(row.tag, "relabeled");

    // -------------------------------------------------------------------
    // Round 3: DELETE the TEXT-client row via a BLOB PK reference (same
    // adapter accepts either flavor for identifying the same row).
    // -------------------------------------------------------------------
    let deletes = PatchSet::<SimpleTable, String, Vec<u8>>::new().delete(PatchDelete::<
        SimpleTable,
        String,
        Vec<u8>,
    >::new(
        things.clone(),
        vec![Value::Blob(uuid_text_client.as_bytes().to_vec())],
    ));
    let n = engine
        .apply_patchset(&deletes, &mut conn, &adapter)
        .expect("apply deletes");
    assert_eq!(n, 1, "one row deleted");

    let remaining: Vec<ThingRow> = sql_query("SELECT id, tag FROM things ORDER BY id")
        .load(&mut conn)
        .expect("load");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, uuid_blob_client);

    // -------------------------------------------------------------------
    // Round 4 (strict-error path): INTEGER wire value on a UUID column is
    // refused at bind time with `Error::QueryBuilderError`, transaction
    // rolls back.
    // -------------------------------------------------------------------
    let bad = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(
        Insert::from(things)
            .set(0, 42_i64)
            .unwrap()
            .set(1, String::from("this insert must be refused"))
            .unwrap(),
    );
    let err = engine
        .apply_patchset(&bad, &mut conn, &adapter)
        .expect_err("INTEGER on UUID column must error at bind time");
    match err {
        DieselError::QueryBuilderError(inner) => {
            let msg = inner.to_string();
            assert!(
                msg.contains("id")
                    && msg.contains("16-byte BLOB or hyphenated TEXT")
                    && msg.contains("INTEGER"),
                "error message must name the column, expected shape, and observed shape; got: {msg}",
            );
        }
        other => panic!("expected QueryBuilderError, got {other:?}"),
    }

    // Post-rollback: the "refused" row must NOT be present.
    let after_rollback: Vec<ThingRow> = sql_query("SELECT id, tag FROM things ORDER BY id")
        .load(&mut conn)
        .expect("load");
    assert_eq!(
        after_rollback.len(),
        1,
        "batch rollback must leave prior state intact"
    );
    assert_eq!(after_rollback[0].id, uuid_blob_client);
}
