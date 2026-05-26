//! Property tests for the re-execution wrapper's partial MIN/MAX maintenance.
//!
//! Random INSERT/DELETE/UPDATE sequences are applied to an in-memory SQLite
//! database while the corresponding CDC events are fed to the wrapper. Two
//! things are asserted:
//!
//! 1. **Correctness:** the maintained value (initial value plus every emitted
//!    `ScalarUpdate`) always equals a brute-force minimum/maximum over a model
//!    of the table.
//! 2. **Partiality:** the database is re-queried ONLY when the current extreme
//!    is removed or displaced. A counting connection provider records every
//!    `with_connection` call the engine makes; inserts, non-extreme deletes,
//!    and unrelated-column updates must perform zero of them.
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::cell::Cell as StdCell;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use subql::reexec::{ConnectionProvider, ReExecEngine, Registered};
use subql::{
    Cell, ColumnId, DefaultIds, RowImage, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

/// Shared SQLite connection. The test mutates it directly (uncounted) to keep
/// the live database in sync with the model; the engine reaches it through a
/// counting wrapper.
#[derive(Clone)]
struct Db {
    conn: Rc<std::cell::RefCell<SqliteConnection>>,
}

impl Db {
    fn fresh() -> Self {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, price REAL, \
             quantity INTEGER, status TEXT)",
        )
        .execute(&mut conn)
        .unwrap();
        Self {
            conn: Rc::new(std::cell::RefCell::new(conn)),
        }
    }

    /// Apply setup SQL directly (NOT through the engine, so it is not counted).
    fn exec(&self, sql: &str) {
        sql_query(sql)
            .execute(&mut *self.conn.borrow_mut())
            .expect("setup sql");
    }
}

/// Connection provider given to the engine; counts every connection it takes,
/// which is exactly one per scalar re-execution.
#[derive(Clone)]
struct Counting {
    db: Db,
    calls: Rc<StdCell<usize>>,
}

impl ConnectionProvider for Counting {
    type Connection = SqliteConnection;
    type Error = Infallible;

    fn with_connection<R>(
        &self,
        f: impl FnOnce(&mut SqliteConnection) -> R,
    ) -> Result<R, Self::Error> {
        self.calls.set(self.calls.get() + 1);
        Ok(f(&mut self.db.conn.borrow_mut()))
    }
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

// orders(id=0, price=1, quantity=2, status=3).
const PRICE: ColumnId = 1;
const STATUS: ColumnId = 3;

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

fn insert_event(id: i64, price: i64, status: &str) -> WalEvent {
    WalEvent::builder(0)
        .insert()
        .pk_cell(0, Cell::Int(id))
        .new_row(row(id, price, status))
        .build()
        .unwrap()
}

fn delete_event(id: i64, price: i64, status: &str) -> WalEvent {
    WalEvent::builder(0)
        .delete()
        .pk_cell(0, Cell::Int(id))
        .old_row(row(id, price, status))
        .build()
        .unwrap()
}

fn update_event(id: i64, old: (i64, &str), new: (i64, &str), changed: &[ColumnId]) -> WalEvent {
    WalEvent::builder(0)
        .update()
        .new_row(row(id, new.0, new.1))
        .pk_cell(0, Cell::Int(id))
        .maybe_old_row(Some(row(id, old.0, old.1)))
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

type Engine = ReExecEngine<PostgreSqlDialect, DefaultIds, ParserDB, Counting>;

fn register(sql: &str) -> (Engine, Db, Rc<StdCell<usize>>, Cell) {
    let db = Db::fresh();
    let calls = Rc::new(StdCell::new(0));
    let provider = Counting {
        db: db.clone(),
        calls: Rc::clone(&calls),
    };
    let mut engine = ReExecEngine::new(
        SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            Arc::new(catalog()),
            PostgreSqlDialect {},
        ),
        provider,
    );
    let registered = engine
        .register(SubscriptionRequest::new(1u64, sql))
        .unwrap();
    let initial = match registered {
        Registered::ReExec { initial_value, .. } => initial_value,
        Registered::Engine(_) => panic!("expected reexec capture"),
    };
    (engine, db, calls, initial)
}

const fn cell(price: i64) -> Cell {
    Cell::Float(price as f64)
}

/// Brute-force extreme over the (price-only) model.
fn expected(model: &BTreeMap<i64, i64>, is_min: bool) -> Cell {
    let v = if is_min {
        model.values().min()
    } else {
        model.values().max()
    };
    v.map_or(Cell::Null, |&p| cell(p))
}

proptest! {
    /// MIN with no WHERE: correctness + partiality (DB hit only on min removal).
    #[test]
    fn min_partial(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let (mut e, db, calls, initial) = register("SELECT MIN(price) FROM orders");
        let mut current = initial;
        let mut model: BTreeMap<i64, i64> = BTreeMap::new();
        prop_assert_eq!(&current, &expected(&model, true));

        for op in ops {
            let before = calls.get();
            let cur = current.clone();
            match op {
                Op::Insert { id, price } => {
                    // Pure inserts only (an INSERT for an existing key would be
                    // an update in real CDC).
                    if model.contains_key(&id) {
                        continue;
                    }
                    db.exec(&format!(
                        "INSERT INTO orders (id, price, quantity, status) VALUES ({id}, {price}, 1, 'x')"
                    ));
                    model.insert(id, price);
                    apply(&mut e, &mut current, &insert_event(id, price, "x"));
                    prop_assert_eq!(calls.get(), before, "insert must not re-query");
                }
                Op::Delete { id } => {
                    let Some(p) = model.remove(&id) else { continue };
                    db.exec(&format!("DELETE FROM orders WHERE id = {id}"));
                    let removes_extreme = cur == cell(p);
                    apply(&mut e, &mut current, &delete_event(id, p, "x"));
                    let want = if removes_extreme { before + 1 } else { before };
                    prop_assert_eq!(calls.get(), want, "delete re-query iff extreme removed");
                }
                Op::UpdatePrice { id, price } => {
                    let Some(old) = model.get(&id).copied() else { continue };
                    db.exec(&format!("UPDATE orders SET price = {price} WHERE id = {id}"));
                    model.insert(id, price);
                    let displaces_extreme = cur == cell(old);
                    apply(
                        &mut e,
                        &mut current,
                        &update_event(id, (old, "x"), (price, "x"), &[PRICE]),
                    );
                    let want = if displaces_extreme { before + 1 } else { before };
                    prop_assert_eq!(calls.get(), want, "price update re-query iff extreme displaced");
                }
                Op::UpdateStatus { id } => {
                    let Some(p) = model.get(&id).copied() else { continue };
                    db.exec(&format!("UPDATE orders SET status = 'y' WHERE id = {id}"));
                    apply(
                        &mut e,
                        &mut current,
                        &update_event(id, (p, "x"), (p, "y"), &[STATUS]),
                    );
                    // status is not a dependency of MIN(price): skipped, no re-query.
                    prop_assert_eq!(calls.get(), before, "unrelated-column update must not re-query");
                }
            }
            prop_assert_eq!(&current, &expected(&model, true));
        }
    }

    /// MAX mirror: correctness + partiality.
    #[test]
    fn max_partial(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let (mut e, db, calls, initial) = register("SELECT MAX(price) FROM orders");
        let mut current = initial;
        let mut model: BTreeMap<i64, i64> = BTreeMap::new();
        prop_assert_eq!(&current, &expected(&model, false));

        for op in ops {
            let before = calls.get();
            let cur = current.clone();
            match op {
                Op::Insert { id, price } => {
                    if model.contains_key(&id) {
                        continue;
                    }
                    db.exec(&format!(
                        "INSERT INTO orders (id, price, quantity, status) VALUES ({id}, {price}, 1, 'x')"
                    ));
                    model.insert(id, price);
                    apply(&mut e, &mut current, &insert_event(id, price, "x"));
                    prop_assert_eq!(calls.get(), before);
                }
                Op::Delete { id } => {
                    let Some(p) = model.remove(&id) else { continue };
                    db.exec(&format!("DELETE FROM orders WHERE id = {id}"));
                    let removes_extreme = cur == cell(p);
                    apply(&mut e, &mut current, &delete_event(id, p, "x"));
                    let want = if removes_extreme { before + 1 } else { before };
                    prop_assert_eq!(calls.get(), want);
                }
                Op::UpdatePrice { id, price } => {
                    let Some(old) = model.get(&id).copied() else { continue };
                    db.exec(&format!("UPDATE orders SET price = {price} WHERE id = {id}"));
                    model.insert(id, price);
                    let displaces_extreme = cur == cell(old);
                    apply(
                        &mut e,
                        &mut current,
                        &update_event(id, (old, "x"), (price, "x"), &[PRICE]),
                    );
                    let want = if displaces_extreme { before + 1 } else { before };
                    prop_assert_eq!(calls.get(), want);
                }
                Op::UpdateStatus { id } => {
                    let Some(p) = model.get(&id).copied() else { continue };
                    db.exec(&format!("UPDATE orders SET status = 'y' WHERE id = {id}"));
                    apply(
                        &mut e,
                        &mut current,
                        &update_event(id, (p, "x"), (p, "y"), &[STATUS]),
                    );
                    prop_assert_eq!(calls.get(), before);
                }
            }
            prop_assert_eq!(&current, &expected(&model, false));
        }
    }

    /// MIN with a WHERE filter: correctness under VM membership (status changes
    /// move rows in and out of the filtered set). Accounting is not asserted
    /// here since membership transitions legitimately re-query.
    #[test]
    fn min_where_filtered_correct(ops in prop::collection::vec(op_strategy(), 0..40)) {
        let (mut e, db, _calls, initial) =
            register("SELECT MIN(price) FROM orders WHERE status = 'paid'");
        let mut current = initial;
        // model: id -> (price, status)
        let mut model: BTreeMap<i64, (i64, &'static str)> = BTreeMap::new();

        let filtered_min = |m: &BTreeMap<i64, (i64, &'static str)>| -> Cell {
            m.values()
                .filter(|(_, s)| *s == "paid")
                .map(|(p, _)| *p)
                .min()
                .map_or(Cell::Null, cell)
        };
        prop_assert_eq!(&current, &filtered_min(&model));

        for op in ops {
            // Alternate status by parity of id so both branches occur.
            match op {
                Op::Insert { id, price } => {
                    if model.contains_key(&id) {
                        continue;
                    }
                    let status = if id % 2 == 0 { "paid" } else { "open" };
                    db.exec(&format!(
                        "INSERT INTO orders (id, price, quantity, status) VALUES ({id}, {price}, 1, '{status}')"
                    ));
                    model.insert(id, (price, status));
                    apply(&mut e, &mut current, &insert_event(id, price, status));
                }
                Op::Delete { id } => {
                    let Some((p, s)) = model.remove(&id) else { continue };
                    db.exec(&format!("DELETE FROM orders WHERE id = {id}"));
                    apply(&mut e, &mut current, &delete_event(id, p, s));
                }
                Op::UpdatePrice { id, price } => {
                    let Some((old, s)) = model.get(&id).copied() else { continue };
                    db.exec(&format!("UPDATE orders SET price = {price} WHERE id = {id}"));
                    model.insert(id, (price, s));
                    apply(
                        &mut e,
                        &mut current,
                        &update_event(id, (old, s), (price, s), &[PRICE]),
                    );
                }
                Op::UpdateStatus { id } => {
                    let Some((p, old_s)) = model.get(&id).copied() else { continue };
                    let new_s = if old_s == "paid" { "open" } else { "paid" };
                    db.exec(&format!(
                        "UPDATE orders SET status = '{new_s}' WHERE id = {id}"
                    ));
                    model.insert(id, (p, new_s));
                    apply(
                        &mut e,
                        &mut current,
                        &update_event(id, (p, old_s), (p, new_s), &[STATUS]),
                    );
                }
            }
            prop_assert_eq!(&current, &filtered_min(&model));
        }
    }
}

/// Dispatch an event and fold any emitted scalar update into `current`.
fn apply(engine: &mut Engine, current: &mut Cell, event: &WalEvent) {
    let notifs = engine.consumers(event).unwrap();
    for update in notifs.scalar_updates {
        *current = update.value;
    }
}
