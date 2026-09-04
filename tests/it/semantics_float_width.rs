//! `real` and `double precision` are not the same type, measured.
//!
//! The wire text for a float4 column is the shortest round-trip text of the
//! *float4* value, so `0.1` on a `real` column means the float4 nearest to
//! `0.1`, which as an `f64` is `0.10000000149011612`. Parsing it as `f64`
//! yields `0.1` instead, a different number, and every comparison against a
//! `double precision` column then answers differently from the server.
//!
//! Measured 2026-09-04 on PostgreSQL 16.11, with both columns holding the
//! text the wire carries for float4 `0.1`:
//!
//! ```text
//! case                   postgres   subql before
//! real 0.1 > double 0.1  true       no match
//! real 0.1 = double 0.1  false      notified
//! real 0.7 = double 0.7  false      notified
//! ```
//!
//! The server has already rounded the source value to float4 before the row
//! reaches the wire, which is why a hand-built event cannot show this: the
//! divergence is in what the text means, not in what a caller can write.
#![allow(clippy::unwrap_used, clippy::float_cmp)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind, Value};
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str =
    "CREATE TABLE readings (id INT PRIMARY KEY, single REAL, double DOUBLE PRECISION)";

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses")
}

/// One wal2json insert carrying `text` in both float columns, which is what
/// the server writes when both hold the same source value.
fn event(text: &str) -> impl CdcEvent<Backend = subql::backend::Postgres> {
    let payload = format!(
        r#"{{"action":"I","schema":"public","table":"readings",
             "columns":[{{"name":"id","type":"integer","value":1}},
                        {{"name":"single","type":"real","value":{text}}},
                        {{"name":"double","type":"double precision","value":{text}}}]}}"#
    );
    let mut msgs = subql::wal::parse_wal2json_v2(payload.as_bytes()).expect("the payload parses");
    assert_eq!(msgs.len(), 1);
    msgs.remove(0)
}

/// The cell a `real` column carries is the float4 value, widened, not the
/// f64 the same text parses to.
#[test]
fn a_real_cell_decodes_at_float4_width() {
    let db = catalog();
    let table = catalog_helpers::table_id(&db, "readings").expect("readings is cataloged");
    let single = catalog_helpers::column_id(&db, table, "single").expect("single");
    let double = catalog_helpers::column_id(&db, table, "double").expect("double");
    let ev = event("0.1");

    assert_eq!(
        ev.value_at(&db, RowKind::New, single).unwrap(),
        Value::Float(f64::from(0.1_f32)),
        "a real column's text names a float4 value"
    );
    assert_eq!(
        ev.value_at(&db, RowKind::New, double).unwrap(),
        Value::Float(0.1),
        "and a double precision column's text names an f64"
    );
}

fn notifies(predicate: &str, text: &str) -> bool {
    let mut engine: SubscriptionEngine<_, DefaultIds, ParserDB> =
        SubscriptionEngine::new(catalog(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    !engine
        .consumers(&event(text))
        .expect("dispatch succeeds")
        .inserted()
        .is_empty()
}

/// Measured: the server answers `real 0.1 > double 0.1` true, because the
/// float4 value is above the f64 one.
#[test]
fn pg_real_compared_against_double_matches_the_server() {
    assert!(
        notifies("SELECT * FROM readings WHERE single > double", "0.1"),
        "the float4 value is the larger of the two"
    );
}

/// And equality is false for the same reason, where subql notified.
#[test]
fn pg_real_equality_against_double_matches_the_server() {
    for text in ["0.1", "0.7"] {
        assert!(
            !notifies("SELECT * FROM readings WHERE single = double", text),
            "the two columns hold different numbers, measured: {text}"
        );
    }
}

/// The control: a `double precision` column is untouched, so a filter
/// comparing it against itself still holds.
#[test]
fn pg_double_column_is_unaffected() {
    assert!(
        notifies("SELECT * FROM readings WHERE double = double", "0.1"),
        "an f64 column compares as it always did"
    );
}
