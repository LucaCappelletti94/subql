//! What an event for a table nobody subscribes to answers, and what a
//! reopened store hands out as the next identity.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DispatchError, SubscriptionEngine, SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE invoices (id INT PRIMARY KEY, state TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn table(name: &str) -> TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), name).expect("the table is in the catalog")
}

fn invoice_row() -> TestEvent<Postgres> {
    TestEvent::insert(
        table("invoices"),
        vec![Value::Int(1), Value::String("open".into())],
    )
    .with_pk_columns([0u16])
}

/// A table in the catalog that nobody watches answers nobody, quietly.
///
/// Erroring there would fail the whole dispatch over a change that
/// concerns no subscription, and every other answer in the batch with
/// it. The checkpoint still comes back, because a caller advancing a
/// cursor needs it whether or not anyone was interested.
#[test]
fn a_watched_nothing_answers_empty_with_its_checkpoint() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("a filter on the other table registers");

    let event = invoice_row();
    let notified = engine.consumers(&event).expect("no subscription, no error");
    assert!(notified.inserted().is_empty());
    assert_eq!(
        notified.checkpoint(),
        event.checkpoint().as_ref(),
        "the position comes back even when nobody is told"
    );

    let dispatched = engine.dispatch(&event).expect("the same through dispatch");
    assert!(dispatched.notifications().inserted().is_empty());
}

/// A table the catalog does not have at all is an error, on both paths.
///
/// That is a caller bug or schema drift rather than an uninteresting
/// change, and answering it with a quiet empty success hides both.
#[test]
fn a_table_outside_the_catalog_is_refused() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("registers");

    let absent = TableId::from(9999u32);
    let event = TestEvent::insert(absent, vec![Value::Int(1)]).with_pk_columns([0u16]);

    assert!(
        matches!(
            engine.consumers(&event),
            Err(DispatchError::UnknownTableId(id)) if id == absent
        ),
        "the id nobody can resolve is named"
    );
    assert!(
        matches!(
            engine.dispatch(&event),
            Err(DispatchError::UnknownTableId(id)) if id == absent
        ),
        "and dispatch refuses it too"
    );
}

/// A table only a read answer depends on does not fail the dispatch.
///
/// Its changes route to no in-process subscription, so the partition
/// does not exist and the ordinary path calls it unknown. The read
/// answers depending on it are the reason it is not.
#[test]
fn a_table_only_a_read_answer_depends_on_is_not_unknown() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT MIN(price) FROM orders",
        ))
        .expect("the read answer registers");
    assert_eq!(engine.reread_count(), 1);
    assert_eq!(
        engine.subscription_count(),
        0,
        "and nothing is maintained in process, so the table has no partition"
    );

    let event = TestEvent::insert(
        table("orders"),
        vec![
            Value::Int(1),
            Value::Float(5.0),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16]);

    let dispatched = engine
        .dispatch(&event)
        .expect("the read answers keep the table known");
    assert!(dispatched.notifications().inserted().is_empty());
    assert_eq!(
        dispatched.notifications().checkpoint(),
        event.checkpoint().as_ref(),
        "and the position still comes back"
    );
}

/// A reopened store never hands out an identity it already gave away.
///
/// One counter serves every answer, and restoring has to move it past
/// the highest identity it brought back. Leaving it where it was hands
/// the next registration an identity a restored answer already holds,
/// and then ending one of them ends the other.
#[test]
fn a_reopened_store_hands_out_a_fresh_identity() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();

    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
    let mut restored_ids = Vec::new();
    for consumer in 1u64..=3 {
        restored_ids.push(
            engine
                .register(SubscriptionRequest::new(
                    consumer,
                    format!("SELECT * FROM orders WHERE status = 'paid{consumer}'"),
                ))
                .expect("registers")
                .subscription_id,
        );
    }
    engine.snapshot_table(table("orders")).expect("snapshot");
    drop(engine);

    let mut reopened = Engine::with_storage(catalog(), PostgreSqlDialect {}, path)
        .expect("reopen")
        .into_parts()
        .0;
    assert_eq!(reopened.subscription_count(), 3, "all three came back");

    let fresh = reopened
        .register(SubscriptionRequest::new(
            9u64,
            "SELECT * FROM orders WHERE status = 'shipped'",
        ))
        .expect("a new answer registers")
        .subscription_id;

    assert!(
        !restored_ids.contains(&fresh),
        "the new identity {fresh} collides with a restored one, {restored_ids:?}"
    );
    assert_eq!(
        reopened.subscription_count(),
        4,
        "and it is a fourth answer rather than a replacement"
    );
}
