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
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    ColumnId, DefaultIds, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

const PRICE: ColumnId = 1;
const STATUS: ColumnId = 3;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn orders_id(database: &ParserDB) -> TableId {
    subql::catalog_helpers::table_id(database, "orders").expect("orders table")
}

// orders columns: id=0, price=1, quantity=2, status=3.
fn row(id: i64, price: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::Float(price as f64),
        Value::Int(1),
        Value::String("x".into()),
    ]
}

fn insert_event(tid: TableId, id: i64, price: i64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(tid, row(id, price)).with_pk_columns([0u16])
}

fn delete_event(tid: TableId, id: i64, price: i64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::delete(tid, row(id, price)).with_pk_columns([0u16])
}

fn update_event(
    tid: TableId,
    id: i64,
    old_price: i64,
    new_price: i64,
    changed: &[ColumnId],
) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::update(tid, row(id, old_price), row(id, new_price))
        .with_pk_columns([0u16])
        .with_changed_columns(changed.iter().copied())
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

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

const fn value(price: i64) -> Value<Postgres> {
    Value::Float(price as f64)
}

fn extremum(model: &BTreeMap<i64, i64>, is_min: bool) -> Value<Postgres> {
    let v = if is_min {
        model.values().min()
    } else {
        model.values().max()
    };
    v.map_or(Value::Null, |&p| value(p))
}

/// Register a `MIN(price)` (or `MAX(price)`) subscription, bootstrapping the
/// initial value from the empty model (= `Value::Null`).
fn register(engine: &mut Engine, is_min: bool) -> u64 {
    let sql = if is_min {
        "SELECT MIN(price) FROM orders"
    } else {
        "SELECT MAX(price) FROM orders"
    };
    let r = engine
        .register(SubscriptionRequest::new(1u64, sql))
        .unwrap();
    let Registered {
        subscription_id,
        tier: Tier::Scalar { .. },
        ..
    } = r
    else {
        panic!("expected ReExec");
    };
    // Bootstrap against the empty model.
    assert!(subql::Install::install(
        engine,
        subscription_id,
        subql::ScalarInstall {
            value: Value::Null,
            checkpoint: None::<subql::NoCheckpoint>
        }
    )
    .is_ok());
    subscription_id
}

/// Apply an event, count emitted updates/triggers, and (for any trigger)
/// simulate the materializer by computing the new extreme from the model and
/// calling `install`.
fn dispatch_and_service(
    engine: &mut Engine,
    qid: u64,
    event: &TestEvent<Postgres>,
    model: &BTreeMap<i64, i64>,
    is_min: bool,
    current: &mut Value<Postgres>,
) -> (usize, usize) {
    let n = engine.dispatch(event).unwrap();
    let mut updates = 0;
    let mut triggers = 0;
    for u in n.scalar_updates() {
        *current = u.value.clone();
        updates += 1;
    }
    for _ in n.triggers() {
        let next = extremum(model, is_min);
        assert!(subql::Install::install(
            engine,
            qid,
            subql::ScalarInstall {
                value: next.clone(),
                checkpoint: None::<subql::NoCheckpoint>,
            },
        )
        .is_ok());
        *current = next;
        triggers += 1;
    }
    (updates, triggers)
}

proptest! {
    /// MIN: correctness + triggers fire iff the op removed/displaced the extreme.
    #[test]
    fn min_partial(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let database = catalog();
        let tid = orders_id(&database);
        let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            database,
            PostgreSqlDialect {},
        );
        let mut engine = inner;
        let qid = register(&mut engine, true);
        let mut current: Value<Postgres> = Value::Null;
        let mut model: BTreeMap<i64, i64> = BTreeMap::new();
        prop_assert_eq!(&current, &extremum(&model, true));

        for op in ops {
            let cur = current.clone();
            let displaces = |p: i64| cur == value(p);

            match op {
                Op::Insert { id, price } => {
                    if model.contains_key(&id) { continue; }
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &insert_event(tid, id, price), &model, true, &mut current);
                    prop_assert_eq!(t, 0, "insert must not trigger");
                }
                Op::Delete { id } => {
                    let Some(p) = model.remove(&id) else { continue };
                    let expect_trigger = displaces(p);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &delete_event(tid, id, p), &model, true, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger),
                        "delete triggers iff it removes the current extreme");
                }
                Op::UpdatePrice { id, price } => {
                    let Some(old) = model.get(&id).copied() else { continue };
                    let expect_trigger = displaces(old);
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(tid, id, old, price, &[PRICE]),
                        &model, true, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger),
                        "price update triggers iff it displaces the extreme");
                }
                Op::UpdateStatus { id } => {
                    let Some(p) = model.get(&id).copied() else { continue };
                    // changed_columns = [status] disjoint from deps = [price]:
                    // skip optimization must hold (no update, no trigger).
                    let (u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(tid, id, p, p, &[STATUS]),
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
        let database = catalog();
        let tid = orders_id(&database);
        let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            database,
            PostgreSqlDialect {},
        );
        let mut engine = inner;
        let qid = register(&mut engine, false);
        let mut current: Value<Postgres> = Value::Null;
        let mut model: BTreeMap<i64, i64> = BTreeMap::new();
        prop_assert_eq!(&current, &extremum(&model, false));

        for op in ops {
            let cur = current.clone();
            let displaces = |p: i64| cur == value(p);

            match op {
                Op::Insert { id, price } => {
                    if model.contains_key(&id) { continue; }
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &insert_event(tid, id, price), &model, false, &mut current);
                    prop_assert_eq!(t, 0);
                }
                Op::Delete { id } => {
                    let Some(p) = model.remove(&id) else { continue };
                    let expect_trigger = displaces(p);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &delete_event(tid, id, p), &model, false, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger));
                }
                Op::UpdatePrice { id, price } => {
                    let Some(old) = model.get(&id).copied() else { continue };
                    let expect_trigger = displaces(old);
                    model.insert(id, price);
                    let (_u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(tid, id, old, price, &[PRICE]),
                        &model, false, &mut current);
                    prop_assert_eq!(t, usize::from(expect_trigger));
                }
                Op::UpdateStatus { id } => {
                    let Some(p) = model.get(&id).copied() else { continue };
                    let (u, t) = dispatch_and_service(
                        &mut engine, qid, &update_event(tid, id, p, p, &[STATUS]),
                        &model, false, &mut current);
                    prop_assert_eq!(u, 0);
                    prop_assert_eq!(t, 0);
                }
            }
            prop_assert_eq!(&current, &extremum(&model, false));
        }
    }
}
