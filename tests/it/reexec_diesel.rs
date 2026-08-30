//! Integration test for [`DieselConnector`]: drives a full `MIN(price)`
//! subscription through the [`AutoResolvingEngine`] against an in-memory
//! SQLite database. Verifies that:
//!
//! 1. Deleting the current extreme triggers a re-execution that goes
//!    through the Diesel connector, decodes the new MIN, installs it, and
//!    surfaces a [`ScalarUpdate`] (no triggers under the auto-resolving
//!    engine).
//! 2. A `MIN` over an empty result set decodes as [`Value::Null`].
//!
//! The lib's unit tests cover the "no DB call on insert above the
//! extreme" optimization with a `MockConnector`. Here we only assert the
//! Diesel side of the round-trip. Each test seeds SQLite with whatever
//! state the connector is expected to read at re-execution time, then
//! dispatches a CDC event that forces the engine to call the connector.
//!
//! Gated behind the `executor-diesel` feature so default `cargo test`
//! does not try to compile it.
#![allow(clippy::unwrap_used)]

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{AutoResolvingEngine, DieselConnector, SyncMode};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

type Engine = AutoResolvingEngine<
    TestEvent<Postgres>,
    DefaultIds,
    ParserDB,
    SyncMode<DieselConnector<SqliteConnection, Postgres>>,
>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn orders_id(database: &ParserDB) -> TableId {
    catalog_helpers::table_id(database, "orders").expect("orders table")
}

fn sqlite_with(rows: &[(i64, f64)]) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query(
        "CREATE TABLE orders (\
            id INTEGER PRIMARY KEY, \
            price REAL, \
            quantity INTEGER, \
            status TEXT)",
    )
    .execute(&mut conn)
    .unwrap();
    for (id, price) in rows {
        sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(&mut conn)
        .unwrap();
    }
    conn
}

fn delete_event(tid: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::delete(
        tid,
        vec![
            Value::Int(id),
            Value::Float(price),
            Value::Int(1),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
}

fn build_engine(conn: SqliteConnection) -> (Engine, TableId) {
    let database = catalog();
    let tid = orders_id(&database);
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        database,
        PostgreSqlDialect {},
    );
    (
        AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(conn))),
        tid,
    )
}

fn register_min(e: &mut Engine, bootstrap: Value<Postgres>) -> u64 {
    let qid = match e
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got Engine, got {other:?}"),
    };
    assert!(subql::Install::install(
        e,
        qid,
        subql::ScalarInstall {
            value: bootstrap,
            checkpoint: None::<subql::NoCheckpoint>
        }
    )
    .is_ok());
    qid
}

#[test]
fn delete_of_extreme_resolves_via_diesel_connector() {
    // SQLite reflects the state AFTER id=1 has been deleted: just id=2 with
    // price=9.0. The model previously had {1: 5.0, 2: 9.0}. We bootstrap
    // with 5.0 to mirror that. The delete event then forces the connector to
    // re-query SQLite, which now contains only id=2 with price=9.0.
    let conn = sqlite_with(&[(2, 9.0)]);
    let (mut e, tid) = build_engine(conn);
    let qid = register_min(&mut e, Value::Float(5.0));

    let n = e.consumers(&delete_event(tid, 1, 5.0)).unwrap();
    assert!(
        n.triggers.is_empty(),
        "AutoResolvingEngine drains triggers internally"
    );
    assert_eq!(n.scalar_updates.len(), 1, "expected MIN to be re-executed");
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(
        n.scalar_updates[0].value,
        Value::Float(9.0),
        "Diesel returned the new MIN"
    );
}

#[test]
fn empty_set_min_decodes_as_null() {
    // SQLite has no rows. The connector re-executes MIN(price) and gets a
    // single SQL-NULL row, which decodes as Value::Null.
    let conn = sqlite_with(&[]);
    let (mut e, tid) = build_engine(conn);
    let qid = register_min(&mut e, Value::Float(5.0));

    let n = e.consumers(&delete_event(tid, 1, 5.0)).unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(
        n.scalar_updates[0].value,
        Value::Null,
        "MIN over empty set -> SQL NULL -> Value::Null"
    );
}
