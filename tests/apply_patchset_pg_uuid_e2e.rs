//! Docker-backed E2E test for
//! [`subql::SubscriptionEngine::apply_patchset`] UUID dispatch on
//! Postgres.
//!
//! Spins up a real Postgres, creates a `things (id UUID PK, name TEXT)`
//! table, and drives three scenarios:
//!
//! 1. **BLOB flavor**: client stores UUIDs as 16-byte `Value::Blob` on
//!    the SQLite side. Patchset carries the compact binary form. The
//!    adapter parses `from_slice` and native-binds as
//!    [`diesel::sql_types::Uuid`].
//! 2. **TEXT flavor**: client stores UUIDs as hyphenated `Value::Text`
//!    on the SQLite side. Patchset carries the 36-char string form. The
//!    adapter parses `Uuid::parse_str` and native-binds as
//!    [`diesel::sql_types::Uuid`].
//! 3. **Strict rejection**: a patchset that carries `Value::Integer` in
//!    the UUID column is refused at bind time with a
//!    [`diesel::result::Error::QueryBuilderError`] and the whole
//!    transaction rolls back.
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

const DDL: &str = "CREATE TABLE things (id UUID PRIMARY KEY, name TEXT);";
const PG_DDL: &str = "CREATE TABLE things (id UUID PRIMARY KEY, name TEXT)";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
}

/// Round-trip UUIDs stored as 16-byte BLOB and as hyphenated TEXT
/// through the same adapter, then update via BLOB PK and delete via
/// TEXT PK. Asserts the strict binder path lands both flavors natively.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_uuid_blob_and_text_roundtrip() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut conn).expect("create table");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let adapter = PgAdapter::new(engine.database());

    let things = SimpleTable::new("things", &["id", "name"], &[0]);

    let uuid_blob = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788_u128);
    let uuid_text = Uuid::from_u128(0xdead_beef_cafe_babe_1010_2020_3030_4040_u128);

    // -------------------------------------------------------------------
    // Round 1: INSERT with UUID as 16-byte BLOB.
    // -------------------------------------------------------------------
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(things.clone())
                .set(0, Value::Blob(uuid_blob.as_bytes().to_vec()))
                .unwrap()
                .set(1, "alice".to_owned())
                .unwrap(),
        )
        .insert(
            Insert::from(things.clone())
                .set(0, uuid_text.hyphenated().to_string())
                .unwrap()
                .set(1, "bob".to_owned())
                .unwrap(),
        );

    let n = engine
        .apply_patchset(&inserts, &mut conn, &adapter)
        .expect("apply mixed-flavor inserts");
    assert_eq!(n, 2, "two rows inserted");

    let rows: Vec<ThingRow> = sql_query("SELECT id, name FROM things ORDER BY name")
        .load(&mut conn)
        .expect("load");
    assert_eq!(
        rows,
        vec![
            ThingRow {
                id: uuid_blob,
                name: "alice".to_owned(),
            },
            ThingRow {
                id: uuid_text,
                name: "bob".to_owned(),
            },
        ]
    );

    // -------------------------------------------------------------------
    // Round 2: UPDATE alice's name via BLOB PK.
    // -------------------------------------------------------------------
    let updates = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(
        Update::<_, PatchsetFormat, String, Vec<u8>>::from(things.clone())
            .set(0, Value::Blob(uuid_blob.as_bytes().to_vec()))
            .unwrap()
            .set(1, "alice2".to_owned())
            .unwrap(),
    );
    let n = engine
        .apply_patchset(&updates, &mut conn, &adapter)
        .expect("apply blob-pk update");
    assert_eq!(n, 1);

    let row: ThingRow = sql_query("SELECT id, name FROM things WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(uuid_blob)
        .get_result(&mut conn)
        .expect("load alice2");
    assert_eq!(row.name, "alice2");

    // -------------------------------------------------------------------
    // Round 3: DELETE bob via TEXT PK.
    // -------------------------------------------------------------------
    let deletes = PatchSet::<SimpleTable, String, Vec<u8>>::new().delete(PatchDelete::<
        SimpleTable,
        String,
        Vec<u8>,
    >::new(
        things,
        vec![Value::Text(uuid_text.hyphenated().to_string())],
    ));
    let n = engine
        .apply_patchset(&deletes, &mut conn, &adapter)
        .expect("apply text-pk delete");
    assert_eq!(n, 1);

    let remaining: Vec<ThingRow> = sql_query("SELECT id, name FROM things ORDER BY name")
        .load(&mut conn)
        .expect("load remaining");
    assert_eq!(
        remaining,
        vec![ThingRow {
            id: uuid_blob,
            name: "alice2".to_owned(),
        }]
    );
}

/// A patchset that carries `Value::Integer` for a UUID column must be
/// rejected at bind time with a
/// [`DieselError::QueryBuilderError`], the transaction must roll back,
/// and the target row must not exist post-attempt.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_uuid_integer_wire_is_refused_and_transaction_rolls_back() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut conn = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut conn).expect("create table");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let adapter = PgAdapter::new(engine.database());

    let things = SimpleTable::new("things", &["id", "name"], &[0]);

    // Seed a legitimate row first so we can also verify rollback: the
    // failing batch inserts a second row and then a bogus row. If the
    // adapter refused the bogus row but let the good one land, the count
    // would end at 2 rather than the correct 1.
    let seed_uuid = Uuid::from_u128(0xaaaa_bbbb_cccc_dddd_1111_2222_3333_4444_u128);
    let seed = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(
        Insert::from(things.clone())
            .set(0, Value::Blob(seed_uuid.as_bytes().to_vec()))
            .unwrap()
            .set(1, "seed".to_owned())
            .unwrap(),
    );
    engine
        .apply_patchset(&seed, &mut conn, &adapter)
        .expect("seed insert");

    let bogus_uuid = Uuid::from_u128(0xdead_dead_dead_dead_1010_1010_1010_1010_u128);
    let bogus_batch = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        // First a good row.
        .insert(
            Insert::from(things.clone())
                .set(0, Value::Blob(bogus_uuid.as_bytes().to_vec()))
                .unwrap()
                .set(1, "should_rollback".to_owned())
                .unwrap(),
        )
        // Then a bogus INTEGER on the UUID column.
        .insert(
            Insert::from(things)
                .set(0, Value::Integer(42))
                .unwrap()
                .set(1, "never_lands".to_owned())
                .unwrap(),
        );

    let err = engine
        .apply_patchset(&bogus_batch, &mut conn, &adapter)
        .expect_err("bogus integer on uuid column must error");
    match err {
        DieselError::QueryBuilderError(inner) => {
            let msg = format!("{inner}");
            assert!(
                msg.contains("column `id`") && msg.contains("INTEGER"),
                "expected column + INTEGER in error, got: {msg}"
            );
        }
        other => panic!("expected QueryBuilderError, got {other:?}"),
    }

    // Rollback: only the seed row remains. The "should_rollback" row was
    // never committed because the whole batch was one diesel transaction
    // and the bogus row's binder returned Err.
    let count: RowCount = sql_query("SELECT COUNT(*) AS n FROM things")
        .get_result(&mut conn)
        .expect("count");
    assert_eq!(count.n, 1, "only seed row should survive");
}

#[derive(QueryableByName, Debug)]
struct RowCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}
