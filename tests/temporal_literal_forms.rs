//! Temporal literals spelled the way a database prints them.
//!
//! A subscription filtering on a `TIMESTAMPTZ` column names an instant in
//! text, and the text a Postgres client prints for that column is
//! `2026-01-01 00:00:00+00`: a space separator and a bare hour offset. That
//! spelling was refused at registration as a type error while the identical
//! text arriving off the WAL decoded fine, so a caller could not subscribe
//! with the value the database had just shown them. Runs in process against
//! the parser-backed catalog and the `TestEvent` harness, so no database is
//! required.
#![allow(clippy::unwrap_used)]

use chrono::{DateTime, NaiveDate, Utc};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

type PgEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn pg_engine() -> (PgEngine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE readings (id INT PRIMARY KEY, taken_at TIMESTAMPTZ, seen_on DATE);",
    )
    .expect("parse DDL");
    let table_id = catalog_helpers::table_id(&db, "readings").expect("table readings exists");
    (SubscriptionEngine::new(db, PostgreSqlDialect {}), table_id)
}

const fn midnight() -> DateTime<Utc> {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

fn reading(table_id: TableId, id: i64, taken_at: DateTime<Utc>) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(
        table_id,
        vec![
            Value::Int(id),
            Value::TimestampTz(taken_at),
            Value::Date(taken_at.date_naive()),
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

/// The spelling a Postgres client prints registers, and the subscription it
/// builds fires on the instant that text names and stays silent on any
/// other. Registration is the regression: the same call returned
/// `RegisterError::TypeError` while the WAL decoder accepted the identical
/// text, so a caller could not subscribe with the value the database had
/// just shown them.
#[test]
fn a_timestamptz_literal_spelled_as_postgres_prints_it_registers_and_matches() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM readings WHERE taken_at = '2026-01-01 00:00:00+00'",
        ))
        .expect("a printed timestamptz registers");

    assert_eq!(
        matched(&mut engine, &reading(table_id, 1, midnight())),
        vec![1u64]
    );
    assert!(
        matched(
            &mut engine,
            &reading(table_id, 2, midnight() + chrono::Duration::seconds(1))
        )
        .is_empty(),
        "one second later is a different instant"
    );
}

/// Every offset spelling of one instant builds the same subscription: all of
/// them fire on the single row at that instant. A parser that accepted a
/// spelling but read its offset wrongly would leave that subscriber out.
#[test]
fn every_offset_spelling_of_one_instant_matches_the_same_row() {
    let (mut engine, table_id) = pg_engine();
    let spellings = [
        "2026-01-01 00:00:00+00",
        "2026-01-01 00:00:00+00:00",
        "2026-01-01 00:00:00+0000",
        "2026-01-01 00:00:00Z",
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00+00:00",
        "2025-12-31 22:00:00-02",
        "2026-01-01 02:30:00+02:30",
    ];
    for (offset, text) in spellings.iter().enumerate() {
        let consumer = u64::try_from(offset).expect("index fits") + 1;
        engine
            .register(SubscriptionRequest::new(
                consumer,
                format!("SELECT * FROM readings WHERE taken_at = '{text}'"),
            ))
            .unwrap_or_else(|e| panic!("{text} registers: {e}"));
    }

    let expected: Vec<u64> = (1..=u64::try_from(spellings.len()).expect("length fits")).collect();
    assert_eq!(
        matched(&mut engine, &reading(table_id, 1, midnight())),
        expected
    );
    assert!(
        matched(
            &mut engine,
            &reading(table_id, 2, midnight() - chrono::Duration::seconds(1))
        )
        .is_empty(),
        "no spelling names the instant one second earlier"
    );
}

/// A `DATE` literal keeps its single accepted spelling and reaches the row
/// carrying that date, while a timestamptz spelling on the same column stays
/// a type error rather than registering a filter that could never match.
#[test]
fn a_date_literal_registers_and_a_timestamp_spelling_on_it_does_not() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM readings WHERE seen_on = '2026-01-01'",
        ))
        .expect("a printed date registers");
    assert_eq!(
        matched(&mut engine, &reading(table_id, 1, midnight())),
        vec![1u64]
    );

    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM readings WHERE seen_on = '2026-01-01 00:00:00+00'",
        ))
        .expect_err("a timestamptz spelling is not a date");
}
