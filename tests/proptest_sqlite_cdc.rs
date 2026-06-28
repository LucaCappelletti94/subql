//! Property test: arbitrary DML sequences against a reference model.
//!
//! Generates `(op, row)` sequences over a small primary-key space (so
//! the generator naturally produces INSERT-then-UPDATE-then-DELETE
//! chains and PK conflicts), filters each op against an in-memory
//! [`BTreeMap`] reference model, applies the surviving ops to both the
//! [`SqliteCdcSource`] and the model, drains the event stream, and
//! asserts:
//!
//! 1. The drained event count equals the number of ops that mutated
//!    the model (no events lost, no spurious events).
//! 2. Each event's kind, PK, and row image (and `changed_columns` on
//!    UPDATE) matches the expectation derived from the model.
//!
//! Float generator constraint: SQLite's own `strtod` rounds slightly
//! differently from Rust's `f64::from_str` at the ULP boundary, so a
//! generic `f64` proptest would surface that orthogonal mismatch instead
//! of testing the source. We restrict `price` to integer-valued `f64`s
//! (exactly representable in both parsers and in JSON's IEEE-754 round
//! trip), so `Cell::Float` equality remains a clean check of the
//! source's trigger encoding and not a parser-fidelity test.
//!
//! Gated via `[[test]] required-features = ["sqlite-cdc"]` in
//! `Cargo.toml`.
#![cfg(feature = "sqlite-cdc")]
#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::collections::BTreeMap;
use std::sync::Arc;

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::{Cell, ColumnId, EventKind, SqliteCdcConfig, SqliteCdcSource, WalEvent};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";
const SQLITE_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, price REAL, status TEXT)";

#[derive(Clone, Debug)]
enum Op {
    Insert { id: i64, price: f64, status: String },
    Update { id: i64, price: f64, status: String },
    Delete { id: i64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let id = 1i64..=8;
    // Integer-valued f64 keeps the round trip lossless (every i32 maps
    // to an exact f64; see module-level note on parser fidelity).
    let price = (-10_000i32..=10_000).prop_map(f64::from);
    let status = "[a-z]{1,8}";
    prop_oneof![
        2 => (id.clone(), price.clone(), status).prop_map(|(id, price, status)| Op::Insert {
            id,
            price,
            status,
        }),
        2 => (id.clone(), price, status).prop_map(|(id, price, status)| Op::Update {
            id,
            price,
            status,
        }),
        1 => id.prop_map(|id| Op::Delete { id }),
    ]
}

#[derive(Clone, Debug)]
struct Row {
    id: i64,
    price: f64,
    status: String,
}

#[derive(Clone, Debug)]
enum ExpectedEvent {
    Insert {
        pk: i64,
        new: Row,
    },
    Update {
        pk: i64,
        old: Row,
        new: Row,
        changed: Vec<ColumnId>,
    },
    Delete {
        pk: i64,
        old: Row,
    },
}

fn build_source() -> SqliteCdcSource {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut conn).unwrap();
    SqliteCdcSource::new(conn, catalog, SqliteCdcConfig::default()).unwrap()
}

fn diff(old: &Row, new: &Row) -> Vec<ColumnId> {
    let mut changed = Vec::new();
    if old.id != new.id {
        changed.push(0);
    }
    if old.price != new.price {
        changed.push(1);
    }
    if old.status != new.status {
        changed.push(2);
    }
    changed
}

fn drain_all(source: &mut SqliteCdcSource) -> Vec<WalEvent> {
    let mut events = Vec::new();
    while let Some(event) = source.poll_next_event().unwrap() {
        events.push(event);
    }
    events
}

fn assert_row_matches(actual: &subql::RowImage, expected: &Row) -> Result<(), TestCaseError> {
    prop_assert_eq!(&actual.cells[0], &Cell::Int(expected.id));
    prop_assert_eq!(&actual.cells[1], &Cell::Float(expected.price));
    prop_assert_eq!(
        &actual.cells[2],
        &Cell::String(Arc::from(expected.status.as_str()))
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// For every randomly generated valid DML sequence the source must
    /// emit exactly one event per applied mutation, matching the
    /// reference-model expectation cell-for-cell.
    #[test]
    fn arbitrary_dml_sequence_matches_reference_model(
        ops in prop::collection::vec(op_strategy(), 0..30),
    ) {
        let mut source = build_source();
        let mut model: BTreeMap<i64, Row> = BTreeMap::new();
        let mut expected: Vec<ExpectedEvent> = Vec::new();

        for op in &ops {
            match op {
                Op::Insert { id, price, status } => {
                    if model.contains_key(id) {
                        continue;
                    }
                    let sql = format!(
                        "INSERT INTO orders (id, price, status) VALUES ({id}, {price}, '{status}')"
                    );
                    source.execute(&sql).unwrap();
                    let row = Row {
                        id: *id,
                        price: *price,
                        status: status.clone(),
                    };
                    model.insert(*id, row.clone());
                    expected.push(ExpectedEvent::Insert { pk: *id, new: row });
                }
                Op::Update { id, price, status } => {
                    let Some(old) = model.get(id).cloned() else {
                        continue;
                    };
                    let sql = format!(
                        "UPDATE orders SET price = {price}, status = '{status}' WHERE id = {id}"
                    );
                    source.execute(&sql).unwrap();
                    let new = Row {
                        id: *id,
                        price: *price,
                        status: status.clone(),
                    };
                    let changed = diff(&old, &new);
                    model.insert(*id, new.clone());
                    expected.push(ExpectedEvent::Update {
                        pk: *id,
                        old,
                        new,
                        changed,
                    });
                }
                Op::Delete { id } => {
                    let Some(old) = model.remove(id) else {
                        continue;
                    };
                    let sql = format!("DELETE FROM orders WHERE id = {id}");
                    source.execute(&sql).unwrap();
                    expected.push(ExpectedEvent::Delete { pk: *id, old });
                }
            }
        }

        let actual = drain_all(&mut source);
        prop_assert_eq!(
            actual.len(),
            expected.len(),
            "event count mismatch: drained {} but model recorded {}",
            actual.len(),
            expected.len()
        );

        for (i, (act, exp)) in actual.iter().zip(expected.iter()).enumerate() {
            match exp {
                ExpectedEvent::Insert { pk, new } => {
                    prop_assert_eq!(act.kind(), EventKind::Insert, "event {} kind", i);
                    prop_assert_eq!(act.pk().values(), &[Cell::Int(*pk)], "event {} pk", i);
                    let row = act.new_row().expect("INSERT carries new_row");
                    assert_row_matches(row, new)?;
                }
                ExpectedEvent::Update {
                    pk,
                    old,
                    new,
                    changed,
                } => {
                    prop_assert_eq!(act.kind(), EventKind::Update, "event {} kind", i);
                    prop_assert_eq!(act.pk().values(), &[Cell::Int(*pk)], "event {} pk", i);
                    let old_row = act.old_row().expect("UPDATE carries old_row");
                    let new_row = act.new_row().expect("UPDATE carries new_row");
                    assert_row_matches(old_row, old)?;
                    assert_row_matches(new_row, new)?;
                    prop_assert_eq!(
                        act.changed_columns(),
                        changed.as_slice(),
                        "event {} changed_columns",
                        i
                    );
                }
                ExpectedEvent::Delete { pk, old } => {
                    prop_assert_eq!(act.kind(), EventKind::Delete, "event {} kind", i);
                    prop_assert_eq!(act.pk().values(), &[Cell::Int(*pk)], "event {} pk", i);
                    let row = act.old_row().expect("DELETE carries old_row");
                    assert_row_matches(row, old)?;
                }
            }
        }

        let trailing = source.poll_next_event().unwrap();
        prop_assert!(trailing.is_none(), "shadow log should be empty after drain");
    }
}
