//! What the dispatch trait promises, on every engine that implements it.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggregateSeedInstall, DefaultIds, Install, SubscriptionDispatch,
    SubscriptionEngine, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

/// Dispatching through the trait answers every kind of subscription.
///
/// Two engines implement this method, and one of them meant the whole
/// event while the other meant the half of it the stream can answer, with
/// nothing in the contract distinguishing them. Code written against the
/// trait cannot tell which it holds, so an aggregate a caller registered
/// went unanswered depending on a choice made elsewhere.
#[test]
fn dispatching_through_the_trait_folds_an_aggregate() {
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders");
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    let counted = engine
        .register(SubscriptionRequest::new(
            9u64,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .expect("the aggregate registers");
    Install::install(
        &mut engine,
        counted.subscription_id,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(0)]],
            read_at: None,
        },
    )
    .expect("the starting number lands");

    let event = TestEvent::<Postgres>::insert(
        orders,
        vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
    )
    .with_pk_columns([0u16]);

    let answered = SubscriptionDispatch::consumers(&mut engine, &event).expect("the event applies");
    assert_eq!(
        answered.aggregate_updates().len(),
        1,
        "the aggregate the caller registered is answered, not left to another call"
    );
    assert_eq!(
        answered.notified(),
        vec![9],
        "and its consumer is among those the dispatch reports"
    );
}
