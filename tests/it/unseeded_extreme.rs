//! An answer nobody has supplied yet is unknown, not empty.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionId, SubscriptionRequest, TableId,
    Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

/// A bare engine holding one uninstalled `MIN(price)` subscription.
fn engine() -> (Engine, TableId, SubscriptionId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "orders").expect("orders resolves");
    let mut engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT MIN(price) FROM orders",
        ))
        .expect("a scalar extreme registers");
    assert!(
        matches!(registered.tier, Tier::Scalar { .. }),
        "the extreme is re-read, so it is the tier under test"
    );
    (engine, table, registered.subscription_id)
}

fn insert(table: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(table, vec![Value::Int(id), Value::Float(price)])
        .with_pk_columns([0u16])
}

/// Nobody has said what the smallest price is, so an insert cannot be the
/// answer: the row it names may not be the smallest one in the table.
#[test]
fn an_insert_before_the_answer_arrives_asks_for_a_read() {
    let (mut engine, table, subscription) = engine();

    let notifications = engine.dispatch(&insert(table, 1, 5.0)).expect("dispatch");

    assert!(
        notifications.scalar_updates().is_empty(),
        "an unknown answer cannot be reported as moved, got {:?}",
        notifications.scalar_updates()
    );
    assert_eq!(
        notifications
            .triggers()
            .iter()
            .map(|t| t.subscription_id)
            .collect::<Vec<_>>(),
        vec![subscription],
        "the only honest answer is to ask the database"
    );
}

/// Once the answer is known, the same insert is decided in process.
#[test]
fn an_insert_after_the_answer_arrives_is_decided_in_process() {
    let (mut engine, table, subscription) = engine();
    assert!(subql::Install::install(
        &mut engine,
        subscription,
        subql::ScalarInstall {
            value: Value::Float(9.0),
            checkpoint: None::<subql::NoCheckpoint>
        }
    )
    .is_ok());

    let notifications = engine.dispatch(&insert(table, 1, 5.0)).expect("dispatch");

    assert!(
        notifications.triggers().is_empty(),
        "a known answer needs no read for an insert"
    );
    assert_eq!(
        notifications
            .scalar_updates()
            .iter()
            .map(|u| (u.subscription_id, u.value.clone()))
            .collect::<Vec<_>>(),
        vec![(subscription, Value::Float(5.0))],
        "5 is smaller than 9, so the answer moved"
    );
}

/// An empty answer is a known answer: `install` of nothing means the filtered
/// set is empty, and then an insert really is the new smallest.
#[test]
fn an_empty_answer_is_known_and_an_insert_wins_it() {
    let (mut engine, table, subscription) = engine();
    assert!(subql::Install::install(
        &mut engine,
        subscription,
        subql::ScalarInstall {
            value: Value::Null,
            checkpoint: None::<subql::NoCheckpoint>
        }
    )
    .is_ok());

    let notifications = engine.dispatch(&insert(table, 1, 5.0)).expect("dispatch");

    assert!(notifications.triggers().is_empty(), "empty is known");
    assert_eq!(
        notifications
            .scalar_updates()
            .iter()
            .map(|u| (u.subscription_id, u.value.clone()))
            .collect::<Vec<_>>(),
        vec![(subscription, Value::Float(5.0))],
        "into an empty set any value is the extreme"
    );
}
