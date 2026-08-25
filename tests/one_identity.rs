//! One identity space: a captured query and a subscription never share a
//! number, whichever order they were registered in.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{
    DefaultIds, Registered, SubscriptionEngine, SubscriptionId, SubscriptionRequest, Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    SubscriptionEngine::new(catalog, PostgreSqlDialect {})
}

/// The core engine serves this one itself, so its id comes from the registry.
fn served(engine: &mut Engine, consumer: u64) -> SubscriptionId {
    let sql = "SELECT * FROM orders WHERE status = 'paid'";
    match engine
        .register(SubscriptionRequest::new(consumer, sql))
        .expect("the core engine serves a plain filter")
    {
        registered @ Registered {
            tier: Tier::InProcess(_),
            ..
        } => registered.subscription_id,
        other => panic!("expected an engine-served registration, got {other:?}"),
    }
}

/// A scalar the core engine refuses, so the wrapper captures it.
fn captured(engine: &mut Engine, consumer: u64) -> SubscriptionId {
    let sql = "SELECT MIN(price) FROM orders";
    match engine
        .register(SubscriptionRequest::new(consumer, sql))
        .expect("a scalar extreme is captured")
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected a scalar capture, got {other:?}"),
    }
}

/// Registering a capture after a served subscription must not reissue its id.
#[test]
fn a_capture_after_a_subscription_gets_its_own_id() {
    let mut engine = engine();
    let first = served(&mut engine, 1);
    let second = captured(&mut engine, 2);
    assert_ne!(
        first, second,
        "a captured query and a subscription name different things, \
         so one number cannot mean both"
    );
}

/// And in the other order, so neither counter can be the shared one by luck.
#[test]
fn a_subscription_after_a_capture_gets_its_own_id() {
    let mut engine = engine();
    let first = captured(&mut engine, 1);
    let second = served(&mut engine, 2);
    assert_ne!(
        first, second,
        "a captured query and a subscription name different things, \
         so one number cannot mean both"
    );
}
