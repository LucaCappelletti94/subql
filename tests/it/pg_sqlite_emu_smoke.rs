//! Smoke test for [`PgSqliteEmuSource`].
//!
//! Replaces the pre-Phase-8.1 `tests/sqlite_cdc_smoke.rs`. Builds a
//! source over an in-memory diesel connection, applies one INSERT,
//! drains one event through the pgoutput wire round trip, and
//! asserts every typed accessor sees the row we wrote. If this fails,
//! the whole emulator pipeline (pg2sqlite -> session extension ->
//! `sqlite_diff_rs::pg_walstream_reverse::op_to_message` ->
//! `pg_walstream::encode_message` -> `PgOutputParser`) is broken.
//!
//! Gated behind the `pg-sqlite-emu` feature.

#![allow(clippy::unwrap_used)]

use subql::backend::{CdcEvent, RowKind, Value};
use subql::{catalog_helpers, EventKind, PgSqliteEmuSource};

const PG_DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

#[test]
fn insert_round_trips_through_the_emulator() {
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");

    let rows = source
        .execute_sql("INSERT INTO orders (id, price, quantity, status) VALUES (7, 9.5, 1, 'paid')")
        .expect("INSERT succeeds");
    assert_eq!(rows, 1);

    let expected_table_id =
        catalog_helpers::table_id(source.pg_catalog(), "orders").expect("orders resolves");

    let event = source
        .poll_next_event()
        .expect("poll succeeds")
        .expect("exactly one event pending");

    assert_eq!(event.kind(), EventKind::Insert);
    assert_eq!(event.table_id(source.pg_catalog()), expected_table_id);
    assert_eq!(event.pk_columns(source.pg_catalog()), &[0u16]);

    // Full row image round-trips through the pgoutput wire.
    assert_eq!(
        event
            .value_at(source.pg_catalog(), RowKind::New, 0)
            .unwrap(),
        Value::Int(7)
    );
    assert_eq!(
        event
            .value_at(source.pg_catalog(), RowKind::New, 1)
            .unwrap(),
        Value::Float(9.5)
    );
    assert_eq!(
        event
            .value_at(source.pg_catalog(), RowKind::New, 2)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        event
            .value_at(source.pg_catalog(), RowKind::New, 3)
            .unwrap(),
        Value::String("paid".to_string()),
    );

    // PK view reads the same integer from the new-image (INSERT has no
    // old image).
    assert_eq!(
        event.value_at(source.pg_catalog(), RowKind::Pk, 0).unwrap(),
        Value::Int(7)
    );

    // Non-PK columns are Missing under RowKind::Pk per the design
    // contract.
    assert_eq!(
        event.value_at(source.pg_catalog(), RowKind::Pk, 1).unwrap(),
        Value::Missing
    );

    assert!(
        source
            .poll_next_event()
            .expect("subsequent poll succeeds")
            .is_none(),
        "exactly one event should have been queued"
    );
}
