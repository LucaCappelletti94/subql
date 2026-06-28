//! Property tests for the re-execution wrapper's partial MIN/MAX maintenance.
//!
//! Random INSERT/DELETE/UPDATE sequences are fed to the engine in-process
//! (no database). The test plays the role of the **Subscription Materializer**:
//! it tracks the canonical model itself, services every `ReExecutionTrigger`
//! by computing the new MIN/MAX from the model and calling `engine.install`,
//! and asserts two properties after every op:
//!
//! 1. **Correctness:** the engine's maintained value (initial `install` plus
//!    every emitted `ScalarUpdate` plus every post-trigger `install`) equals
//!    a brute-force minimum/maximum over the model.
//! 2. **Partiality:** the engine emits a `ReExecutionTrigger` **iff** the op
//!    could have removed or displaced the current extreme. Inserts,
//!    non-extreme deletes, and unrelated-column updates emit zero triggers.
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::collections::BTreeMap;
use std::sync::Arc;
use subql::reexec::{ReExecEngine, Registered};
use subql::{
    Cell, ColumnId, DefaultIds, RowImage, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

const PRICE: ColumnId = 1;
const STATUS: ColumnId = 3;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

// orders columns: id=0, price=1, quantity=2, status=3.
fn row(id: i64, price: i64, status: &str) -> RowImage {
    RowImage {
        cells: Arc::from([
            Cell::Int(id),
            Cell::Float(price as f64),
            Cell::Int(1),
            Cell::String(Arc::from(status)),
        ]),
    }
}

fn insert_event(id: i64, price: i64) -> WalEvent {
    WalEvent::builder(0)
        .insert()
        .pk_cell(0, Cell::Int(id))
        .new_row(row(id, price, "x"))
        .build()
        .unwrap()
}

fn delete_event(id: i64, price: i64) -> WalEvent {
    WalEvent::builder(0)
        .delete()
        .pk_cell(0, Cell::Int(id))
        .old_row(row(id, price, "x"))
        .build()
        .unwrap()
}

fn update_event(id: i64, old_price: i64, new_price: i64, changed: &[ColumnId]) -> WalEvent {
    WalEvent::builder(0)
        .update()
        .new_row(row(id, new_price, "x"))
        .pk_cell(0, Cell::Int(id))
        .maybe_old_row(Some(row(id, old_price, "x")))
        .changed_columns(Arc::from(changed))
        .build()
        .unwrap()
}

#[derive(Clone, Debug)]
enum Op {
    Insert { id: i64, price: i64 },
    Delete { id: i64 },
    UpdatePrice { id: i64, price: i64 },
    UpdateStatus { id: i64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let id = 0i64..6;
    let price = 0i64..50;
    prop_oneof![
        (id.clone(), price.clone()).prop_map(|(id, price)| Op::Insert { id, price }),
        id.clone().prop_map(|id| Op::Delete { id }),
        (id.clone(), price).prop_map(|(id, price)| Op::UpdatePrice { id, price }),
        id.prop_map(|id| Op::UpdateStatus { id }),
    ]
}

type Engine = ReExecEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

const fn cell(price: i64) -> Cell {
    Cell::Float(price as f64)
}

fn extremum(model: &BTreeMap<i64, i64>, is_min: bool) -> Cell {
    let v = if is_min {
        model.values().min()
    } else {
        model.values().max()
    };
    v.map_or(Cell::Null, |&p| cell(p))
}

/// Register a `MIN(price)` (or `MAX(price)`) subscription, bootstrapping the
/// initial value from the empty model (= `Cell::Null`).
fn register(engine: &mut Engine, is_min: bool) -> u64 {
    let sql = if is_min {
        "SELECT MIN(price) FROM orders"
    } else {
        "SELECT MAX(price) FROM orders"
    };
    let r = engine
        .register(SubscriptionRequest::new(1u64, sql))
        .unwrap();
    let Registered::ReExec { query_id, .. } = r else {
        panic!("expected ReExec");
    };
    // Bootstrap against the empty model.
    assert!(engine.install(query_id, Cell::Null));
    query_id
}

/// Apply an event, count emitted updates/triggers, and (for any trigger)
/// simulate the materializer by computing the new extreme from the model and
/// calling `install`.
fn dispatch_and_service(
    engine: &mut Engine,
    qid: u64,
    event: &WalEvent,
    model: &BTreeMap<i64, i64>,
    is_min: bool,
    current: &mut Cell,
) -> (usize, usize) {
    let n = engine.consumers(event).unwrap();
    let mut updates = 0;
    let mut triggers = 0;
    for u in n.scalar_updates {
        *current = u.value;
        updates += 1;
    }
    for _ in n.triggers {
        let next = extremum(model, is_min);
        engine.install(qid, next.clone());
        *current = next;
        triggers += 1;
    }
    (updates, triggers)
}

proptest! {
    /// MIN: correctness + triggers fire iff the op removed/displaced the extreme.
    #[test]
    fn min_partial(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            catalog(),
            PostgreSqlDialect {},
        );
        let mut engine = ReExecEngine::new(inner);
        let qid = register(&mut engine, true);
        let mut current = Cell::Null;
        let mut model: BTreeMap<i64, i64> = BTreeMap::new();
        prop_assert_eq!(&current, &extremum(&model, true));

        for op in ops {
            let cur = current.clone();
            let displaces = |p: i64| cur == cell(p);

            match op {
                Op::Insert { id, price } => {
                    if model.contains_key(&id) { continue; }
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &insert_event(id, price), &model, true, &mut current);
                    prop_assert_eq!(t, 0, "insert must not trigger");
                }
                Op::Delete { id } => {
                    let Some(p) = model.remove(&id) else { continue };
                    let expect_trigger = displaces(p);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &delete_event(id, p), &model, true, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger),
                        "delete triggers iff it removes the current extreme");
                }
                Op::UpdatePrice { id, price } => {
                    let Some(old) = model.get(&id).copied() else { continue };
                    let expect_trigger = displaces(old);
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(id, old, price, &[PRICE]),
                        &model, true, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger),
                        "price update triggers iff it displaces the extreme");
                }
                Op::UpdateStatus { id } => {
                    let Some(p) = model.get(&id).copied() else { continue };
                    // changed_columns = [status] disjoint from deps = [price]:
                    // skip optimization must hold (no update, no trigger).
                    let (u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(id, p, p, &[STATUS]),
                        &model, true, &mut current);
                    prop_assert_eq!(u, 0);
                    prop_assert_eq!(t, 0, "unrelated-column update must not trigger");
                }
            }
            prop_assert_eq!(&current, &extremum(&model, true));
        }
    }

    /// MAX mirror.
    #[test]
    fn max_partial(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            catalog(),
            PostgreSqlDialect {},
        );
        let mut engine = ReExecEngine::new(inner);
        let qid = register(&mut engine, false);
        let mut current = Cell::Null;
        let mut model: BTreeMap<i64, i64> = BTreeMap::new();
        prop_assert_eq!(&current, &extremum(&model, false));

        for op in ops {
            let cur = current.clone();
            let displaces = |p: i64| cur == cell(p);

            match op {
                Op::Insert { id, price } => {
                    if model.contains_key(&id) { continue; }
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &insert_event(id, price), &model, false, &mut current);
                    prop_assert_eq!(t, 0);
                }
                Op::Delete { id } => {
                    let Some(p) = model.remove(&id) else { continue };
                    let expect_trigger = displaces(p);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &delete_event(id, p), &model, false, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger));
                }
                Op::UpdatePrice { id, price } => {
                    let Some(old) = model.get(&id).copied() else { continue };
                    let expect_trigger = displaces(old);
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(id, old, price, &[PRICE]),
                        &model, false, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger));
                }
                Op::UpdateStatus { id } => {
                    let Some(p) = model.get(&id).copied() else { continue };
                    let (u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(id, p, p, &[STATUS]),
                        &model, false, &mut current);
                    prop_assert_eq!(u, 0);
                    prop_assert_eq!(t, 0);
                }
            }
            prop_assert_eq!(&current, &extremum(&model, false));
        }
    }
}
