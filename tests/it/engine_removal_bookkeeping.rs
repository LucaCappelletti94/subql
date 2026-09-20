//! What removal leaves behind in the engine's own indexes.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, SubscriptionScope,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";
const PAID: &str = "SELECT * FROM orders WHERE status = 'paid'";
const SHIPPED: &str = "SELECT * FROM orders WHERE status = 'shipped'";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn orders() -> subql::TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders")
}

fn paid_row(id: i64) -> TestEvent<Postgres> {
    TestEvent::insert(
        orders(),
        vec![
            Value::Int(id),
            Value::Float(5.0),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
}

/// Ending one answer of a session leaves its siblings in the session.
///
/// The session's list of answers is pruned by dropping the one that
/// ended. Dropping everything except it instead leaves the session
/// holding an answer that is gone and having forgotten the ones that
/// remain, so ending the session later reaches nothing.
#[test]
fn ending_one_answer_leaves_the_rest_of_its_session() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    let first = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders")
                .scope(SubscriptionScope::Session(7)),
        )
        .expect("the first read answer registers")
        .subscription_id;
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders")
                .scope(SubscriptionScope::Session(7)),
        )
        .expect("the second registers");
    assert_eq!(engine.reread_count(), 2);

    assert!(engine.unregister_reread(first), "one of the two ends");
    assert_eq!(engine.reread_count(), 1, "the other is still live");

    let report = engine.unregister_session(7);
    assert_eq!(
        report.removed_reads, 1,
        "ending the session reaches the answer that stayed"
    );
    assert_eq!(engine.reread_count(), 0, "and it is gone now");
}

/// A consumer keeps its name while it still holds an answer.
///
/// The table names its consumers in a dictionary, and the name is
/// dropped when the last answer holding it ends. Dropping it while
/// another answer still holds it leaves that answer unable to name its
/// consumer, and a subscriber that cannot be named is one that stops
/// being told anything.
#[test]
fn a_consumer_keeps_its_name_while_it_still_holds_an_answer() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    let ending = engine
        .register(SubscriptionRequest::new(1u64, SHIPPED))
        .expect("the answer that will end registers")
        .subscription_id;
    engine
        .register(SubscriptionRequest::new(1u64, PAID))
        .expect("the answer that stays registers");

    assert!(engine.unregister_subscription(ending), "one of them ends");

    let notified = engine
        .consumers(&paid_row(1))
        .expect("the event dispatches");
    assert_eq!(
        notified.inserted(),
        &[1],
        "the consumer still holds an answer, so it still has a name"
    );
}

/// The last answer a consumer holds takes its name with it.
#[test]
fn the_last_answer_takes_the_consumer_name_with_it() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    let only = engine
        .register(SubscriptionRequest::new(1u64, PAID))
        .expect("registers")
        .subscription_id;
    engine
        .register(SubscriptionRequest::new(2u64, PAID))
        .expect("a second consumer on the same predicate registers");

    assert!(engine.unregister_subscription(only), "its only answer ends");

    let notified = engine
        .consumers(&paid_row(1))
        .expect("the event dispatches");
    assert_eq!(
        notified.inserted(),
        &[2],
        "the consumer that ended is not named, and the other still is"
    );
}

/// Ending by statement reports what it removed.
///
/// One statement can hold several answers, and the report is how a
/// caller learns how many went. A count that never rises, or one that
/// counts the wrong set, tells them nothing happened.
#[test]
fn ending_by_statement_reports_what_went() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(1u64, PAID))
        .expect("registers");
    engine
        .register(SubscriptionRequest::new(1u64, SHIPPED))
        .expect("a second statement for the same consumer registers");

    let report = engine
        .unregister_query(1u64, PAID)
        .expect("the statement names a predicate");
    assert_eq!(report.removed_bindings, 1, "one answer held that statement");
    assert_eq!(
        report.removed_predicates, 1,
        "and it was the last one on that predicate"
    );
    assert_eq!(engine.subscription_count(), 1, "the other statement stays");

    let notified = engine
        .consumers(&paid_row(1))
        .expect("the event dispatches");
    assert!(
        notified.inserted().is_empty(),
        "and the ended statement answers nobody, got {:?}",
        notified.inserted()
    );
}
