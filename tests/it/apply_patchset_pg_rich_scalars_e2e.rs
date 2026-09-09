//! Docker-backed E2E test for the [`PgAdapter`](subql::patchset::PgAdapter)
//! arms that bind a verbatim wire text as a native Postgres scalar.
//!
//! Every column here is one the adapter parses out of `Value::Text` and binds
//! under its own diesel `SqlType`, so a wrong `SqlType` on any arm is a wire
//! type the server refuses.

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{DiffOps, Insert, PatchSet, SimpleTable};
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::PgAdapter;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE rich (id INT PRIMARY KEY, amount NUMERIC(12,3), \
                   at TIMESTAMP, at_tz TIMESTAMPTZ, on_day DATE, at_time TIME, \
                   doc JSON, docb JSONB);";
const PG_DDL: &str = "CREATE TABLE rich (id INT PRIMARY KEY, amount NUMERIC(12,3), \
                      at TIMESTAMP, at_tz TIMESTAMPTZ, on_day DATE, at_time TIME, \
                      doc JSON, docb JSONB)";

const COLUMNS: [&str; 8] = [
    "id", "amount", "at", "at_tz", "on_day", "at_time", "doc", "docb",
];

#[derive(QueryableByName, Debug, PartialEq)]
struct RichRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Numeric)]
    amount: bigdecimal::BigDecimal,
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    at: chrono::NaiveDateTime,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    at_tz: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Date)]
    on_day: chrono::NaiveDate,
    #[diesel(sql_type = diesel::sql_types::Time)]
    at_time: chrono::NaiveTime,
    #[diesel(sql_type = diesel::sql_types::Json)]
    doc: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    docb: serde_json::Value,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_binds_every_rich_scalar_natively() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut conn = db.connect();
    sql_query(PG_DDL).execute(&mut conn).expect("create table");
    // A session zone away from UTC, so a `timestamp` column bound as
    // `timestamptz` would land shifted rather than verbatim.
    sql_query("SET TIME ZONE 'Europe/Rome'")
        .execute(&mut conn)
        .expect("set the session zone");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
    let engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog, PostgreSqlDialect {});

    let rich = SimpleTable::new("rich", &COLUMNS, &[0]);
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new().insert(
        Insert::from(rich)
            .set(0, 1_i64)
            .unwrap()
            .set(1, "12345.678")
            .unwrap()
            .set(2, "2024-03-04 05:06:07.891")
            .unwrap()
            .set(3, "2024-03-04 05:06:07.891+02")
            .unwrap()
            .set(4, "2024-03-04")
            .unwrap()
            .set(5, "05:06:07.891")
            .unwrap()
            .set(6, r#"{"b":1,"a":2}"#)
            .unwrap()
            .set(7, r#"{"b":1,"a":2}"#)
            .unwrap(),
    );

    let adapter = PgAdapter::new(engine.database()).expect("the catalog indexes");
    let n = engine
        .apply_patchset(&inserts, &mut conn, &adapter)
        .expect("apply the insert");
    assert_eq!(n, 1, "one row inserted");

    let row: RichRow = sql_query(
        "SELECT id, amount, at, at_tz, on_day, at_time, doc, docb FROM rich WHERE id = 1",
    )
    .get_result(&mut conn)
    .expect("load");

    assert_eq!(
        row,
        RichRow {
            id: 1,
            amount: "12345.678".parse().unwrap(),
            at: "2024-03-04T05:06:07.891".parse().unwrap(),
            at_tz: "2024-03-04T03:06:07.891Z".parse().unwrap(),
            on_day: "2024-03-04".parse().unwrap(),
            at_time: "05:06:07.891".parse().unwrap(),
            // Both columns store the key order of the adapter's own
            // `serde_json` parse, not the wire order.
            doc: serde_json::json!({"b": 1, "a": 2}),
            docb: serde_json::json!({"a": 2, "b": 1}),
        }
    );
    let doc_text: JsonText = sql_query("SELECT doc::text AS t FROM rich WHERE id = 1")
        .get_result(&mut conn)
        .expect("load the json text");
    assert_eq!(
        doc_text.t, r#"{"a":2,"b":1}"#,
        "a `json` column keeps the bytes it was bound, which is the adapter's parse order"
    );
}

#[derive(QueryableByName, Debug)]
struct JsonText {
    #[diesel(sql_type = diesel::sql_types::Text)]
    t: String,
}
