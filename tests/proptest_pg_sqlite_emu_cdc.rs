//! Structural proptest for [`PgSqliteEmuSource`].
//!
//! Replaces the pre-Phase-8.1 `tests/proptest_sqlite_cdc.rs`.
//! Complements `proptest_pg_sqlite_emu_dispatch.rs` (which only
//! asserts that engine notifications match an oracle) by asserting
//! that every drained [`PgOutputEvent`]'s per-column shape matches
//! a reference-model expectation cell-for-cell.
//!
//! Two SQLite session-extension semantics differ from the old
//! trigger-based source and must be mirrored in the model to keep the
//! test sound:
//!
//! * A no-op UPDATE (row values unchanged) leaves the session empty
//!   and produces no event. The model skips those.
//! * A UPDATE that leaves the primary key unchanged records the PK
//!   column as `(None, Some(pk))` in the changeset, not
//!   `(Some(pk), Some(pk))`. The emulator's row-lookup fallback
//!   surfaces the pk on the old side anyway, so the emitted event's
//!   old and new images stay complete.
//!
//! Float generator constraint: `price` is restricted to
//! integer-valued `f64`s so `f64` equality checks are lossless
//! through SQLite's `strtod`, Rust's `f64::from_str`, and JSON's
//! IEEE-754 round trip. A generic `f64` proptest would surface
//! parser-fidelity mismatches unrelated to the emulator.
//!
//! Gated behind the `pg-sqlite-emu` feature.

#![cfg(feature = "pg-sqlite-emu")]
#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use subql::backend::{CdcEvent, RowKind, Value};
use subql::{ChangeEvent, ColumnId, EventKind, PgSqliteEmuSource};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";

#[derive(Clone, Debug)]
enum Op {
    Insert { id: i64, price: f64, status: String },
    Update { id: i64, price: f64, status: String },
    Delete { id: i64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let id = 1i64..=8;
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

#[derive(Clone, Debug, PartialEq)]
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
fn assert_row_matches(
    event: &ChangeEvent,
    db: &ParserDB,
    side: RowKind,
    expected: &Row,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        event.value_at(db, side, 0),
        Value::Int(expected.id),
        "{:?} side id mismatch",
        side,
    );
    prop_assert_eq!(
        event.value_at(db, side, 1),
        Value::Float(expected.price),
        "{:?} side price mismatch",
        side,
    );
    prop_assert_eq!(
        event.value_at(db, side, 2),
        Value::String(expected.status.clone()),
        "{:?} side status mismatch",
        side,
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// For every randomly generated valid DML sequence, the emulator
    /// must emit exactly one event per applied mutation and each
    /// event's typed accessors must match the reference model
    /// cell-for-cell.
    #[test]
    fn arbitrary_dml_sequence_matches_reference_model(
        ops in prop::collection::vec(op_strategy(), 0..30),
    ) {
        let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");
        let mut model: BTreeMap<i64, Row> = BTreeMap::new();
        let mut expected: Vec<ExpectedEvent> = Vec::new();

        // Interleave the drain per DML so we assert the per-event
        // shape immediately after each mutation. Batching DML then
        // draining at the end would rely on SQLite session's
        // changeset iteration order, which groups rows by PK rather
        // than by DML execution time.
        for op in &ops {
            let exp = match op {
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
                    ExpectedEvent::Insert { pk: *id, new: row }
                }
                Op::Update { id, price, status } => {
                    let Some(old) = model.get(id).cloned() else {
                        continue;
                    };
                    let new = Row {
                        id: *id,
                        price: *price,
                        status: status.clone(),
                    };
                    if old == new {
                        // No-op UPDATE. SQLite session drops it.
                        continue;
                    }
                    let sql = format!(
                        "UPDATE orders SET price = {price}, status = '{status}' WHERE id = {id}"
                    );
                    source.execute(&sql).unwrap();
                    let changed = diff(&old, &new);
                    model.insert(*id, new.clone());
                    ExpectedEvent::Update {
                        pk: *id,
                        old,
                        new,
                        changed,
                    }
                }
                Op::Delete { id } => {
                    let Some(old) = model.remove(id) else {
                        continue;
                    };
                    let sql = format!("DELETE FROM orders WHERE id = {id}");
                    source.execute(&sql).unwrap();
                    ExpectedEvent::Delete { pk: *id, old }
                }
            };
            expected.push(exp.clone());

            let act = source
                .poll_next_event()
                .unwrap()
                .expect("op should produce a drained event");
            let i = expected.len() - 1;

            match &exp {
                ExpectedEvent::Insert { pk, new } => {
                    prop_assert_eq!(act.kind(), EventKind::Insert, "event {} kind", i);
                    prop_assert_eq!(
                        act.value_at(source.pg_catalog(), RowKind::Pk, 0),
                        Value::Int(*pk),
                        "event {} pk",
                        i,
                    );
                    assert_row_matches(&act, source.pg_catalog(), RowKind::New, new)?;
                }
                ExpectedEvent::Update {
                    pk,
                    old,
                    new,
                    changed,
                } => {
                    prop_assert_eq!(act.kind(), EventKind::Update, "event {} kind", i);
                    prop_assert_eq!(
                        act.value_at(source.pg_catalog(), RowKind::Pk, 0),
                        Value::Int(*pk),
                        "event {} pk",
                        i,
                    );
                    assert_row_matches(&act, source.pg_catalog(), RowKind::Old, old)?;
                    assert_row_matches(&act, source.pg_catalog(), RowKind::New, new)?;
                    let mut actual_changed = act.changed_columns(source.pg_catalog());
                    actual_changed.sort_unstable();
                    let mut expected_changed = changed.clone();
                    expected_changed.sort_unstable();
                    prop_assert_eq!(
                        &actual_changed,
                        &expected_changed,
                        "event {} changed_columns",
                        i,
                    );
                }
                ExpectedEvent::Delete { pk, old } => {
                    prop_assert_eq!(act.kind(), EventKind::Delete, "event {} kind", i);
                    prop_assert_eq!(
                        act.value_at(source.pg_catalog(), RowKind::Pk, 0),
                        Value::Int(*pk),
                        "event {} pk",
                        i,
                    );
                    assert_row_matches(&act, source.pg_catalog(), RowKind::Old, old)?;
                }
            }
        }

        prop_assert!(
            source.poll_next_event().unwrap().is_none(),
            "emulator queue should be empty after drain",
        );
    }
}
