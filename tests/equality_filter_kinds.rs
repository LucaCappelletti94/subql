//! An equality filter fires whatever kind the column holds.
//!
//! Registration files an equality predicate in a bitmap index keyed by the
//! literal, and dispatch probes that index with a key built from the row
//! cell. The two keys were derived by different code from different inputs,
//! the literal's SQL text on one side and the typed cell on the other, so
//! for every kind whose cell is not a plain bool, integer, float, or string
//! the probe could not reach the entry: the subscription registered and then
//! never fired. Ordered comparisons were unaffected, since those fall to the
//! scan set and reach the VM.
//!
//! One table carries a column per scalar kind, one row satisfies every
//! filter, and every subscriber must hear about it. Runs in process against
//! the parser-backed catalog and the `TestEvent` harness, so no database is
//! required.
#![allow(clippy::unwrap_used)]

use chrono::{NaiveDate, NaiveTime};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

type PgEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;
type SqliteEngine = SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB>;

const PG_DDL: &str = "CREATE TABLE cells (id INT PRIMARY KEY, flag BOOL, tally BIGINT, \
                      ratio DOUBLE PRECISION, label TEXT, payload BYTEA, key UUID, \
                      price NUMERIC(12,4), at_naive TIMESTAMP, at_tz TIMESTAMPTZ, \
                      on_date DATE, at_time TIME, doc JSON, meta JSONB);";

/// One filter per scalar kind, each naming the value the row below carries.
const FILTERS: &[(u64, &str)] = &[
    (1, "flag = true"),
    (2, "tally = 7"),
    (3, "ratio = 2.5"),
    (4, "label = 'row'"),
    (5, "payload = X'DEADBEEF'"),
    (6, "key = '550e8400-e29b-41d4-a716-446655440000'"),
    (7, "price = 1.5"),
    (8, "at_naive = '2026-01-01 00:00:00'"),
    (9, "at_tz = '2026-01-01T00:00:00Z'"),
    (10, "on_date = '2026-01-01'"),
    (11, "at_time = '12:34:56'"),
    (12, "doc = '{\"k\":1}'"),
    (13, "meta = '{\"k\":1}'"),
];

fn pg_engine() -> (PgEngine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("parse DDL");
    let table_id = catalog_helpers::table_id(&db, "cells").expect("table cells exists");
    (SubscriptionEngine::new(db, PostgreSqlDialect {}), table_id)
}

/// A row satisfying every filter above, or, with `shift` set, one whose every
/// filtered cell differs.
fn cells_row(table_id: TableId, id: i64, shift: bool) -> TestEvent<Postgres> {
    let doc = if shift {
        serde_json::json!({"k": 2})
    } else {
        serde_json::json!({"k": 1})
    };
    TestEvent::<Postgres>::insert(
        table_id,
        vec![
            Value::Int(id),
            Value::Bool(!shift),
            Value::Int(if shift { 8 } else { 7 }),
            Value::Float(if shift { 3.5 } else { 2.5 }),
            Value::String(if shift { "other" } else { "row" }.to_owned()),
            Value::Bytes(if shift {
                vec![0x00, 0x11]
            } else {
                vec![0xde, 0xad, 0xbe, 0xef]
            }),
            Value::Uuid(
                if shift {
                    "550e8400-e29b-41d4-a716-44665544ffff"
                } else {
                    "550e8400-e29b-41d4-a716-446655440000"
                }
                .parse()
                .unwrap(),
            ),
            Value::Decimal(if shift { "2.5" } else { "1.5" }.parse().unwrap()),
            Value::Timestamp(
                NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(u32::from(shift), 0, 0)
                    .unwrap(),
            ),
            Value::TimestampTz(
                NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(u32::from(shift), 0, 0)
                    .unwrap()
                    .and_utc(),
            ),
            Value::Date(NaiveDate::from_ymd_opt(2026, 1, 1 + u32::from(shift)).unwrap()),
            Value::Time(NaiveTime::from_hms_opt(12 + u32::from(shift), 34, 56).unwrap()),
            Value::Json(doc.clone()),
            Value::Jsonb(doc),
        ],
    )
    .with_pk_columns([0u16])
}

fn matched(engine: &mut PgEngine, event: &TestEvent<Postgres>) -> Vec<u64> {
    let mut ids = engine
        .consumers(event)
        .expect("dispatch")
        .inserted()
        .to_vec();
    ids.sort_unstable();
    ids
}

/// Every kind's equality filter hears about the row that satisfies it.
#[test]
fn an_equality_filter_fires_for_every_scalar_kind() {
    let (mut engine, table_id) = pg_engine();
    for (consumer, filter) in FILTERS {
        engine
            .register(SubscriptionRequest::new(
                *consumer,
                format!("SELECT * FROM cells WHERE {filter}"),
            ))
            .unwrap_or_else(|e| panic!("{filter} registers: {e}"));
    }

    let expected: Vec<u64> = FILTERS.iter().map(|(consumer, _)| *consumer).collect();
    assert_eq!(
        matched(&mut engine, &cells_row(table_id, 1, false)),
        expected
    );
}

/// The complement: a row whose every filtered cell differs notifies nobody.
/// Without this, "fires for every kind" would also be satisfied by an engine
/// that notifies everyone about everything.
#[test]
fn an_equality_filter_stays_silent_on_a_row_it_does_not_name() {
    let (mut engine, table_id) = pg_engine();
    for (consumer, filter) in FILTERS {
        engine
            .register(SubscriptionRequest::new(
                *consumer,
                format!("SELECT * FROM cells WHERE {filter}"),
            ))
            .unwrap_or_else(|e| panic!("{filter} registers: {e}"));
    }

    assert!(
        matched(&mut engine, &cells_row(table_id, 2, true)).is_empty(),
        "no filter names this row's values"
    );
}

/// SQLite stores a boolean as an integer, so its `Backend::Bool` payload is
/// `i64` while the literal `true` arrives as a SQL boolean. The index key and
/// the probe key have to agree on which of the two representations they use,
/// or a `flag = true` subscriber never hears about its row.
#[test]
fn a_sqlite_bool_equality_filter_fires_although_the_cell_is_an_integer() {
    let db = ParserDB::parse::<SQLiteDialect>(
        "CREATE TABLE flags (id INTEGER PRIMARY KEY, flag BOOLEAN);",
    )
    .expect("parse DDL");
    let table_id = catalog_helpers::table_id(&db, "flags").expect("table flags exists");
    let mut engine: SqliteEngine = SubscriptionEngine::new(db, SQLiteDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM flags WHERE flag = true",
        ))
        .expect("a sqlite bool equality registers");

    let on = TestEvent::<SQLite>::insert(table_id, vec![Value::Int(1), Value::Bool(1)])
        .with_pk_columns([0u16]);
    let off = TestEvent::<SQLite>::insert(table_id, vec![Value::Int(2), Value::Bool(0)])
        .with_pk_columns([0u16]);

    assert_eq!(
        engine.consumers(&on).expect("dispatch").inserted().to_vec(),
        vec![1u64]
    );
    assert!(
        engine
            .consumers(&off)
            .expect("dispatch")
            .inserted()
            .is_empty(),
        "flag = 0 does not satisfy flag = true"
    );
}
