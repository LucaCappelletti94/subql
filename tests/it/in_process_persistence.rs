//! Statements of the answers the engine maintains itself, across a restart.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE invoices (id INT PRIMARY KEY, state TEXT);";

use crate::common::store::{StoredEngine, TempStore};

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn table(name: &str) -> subql::TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), name).expect("the table exists")
}

/// A registration that snapshots itself saves its own statement.
///
/// Under `DurabilityMode::Required` the registration snapshots the table
/// before it returns, so the statement has to be kept before that snapshot
/// rather than after the call. Kept after, the snapshot writes every
/// statement except the one that caused it.
#[test]
fn a_registration_that_forces_a_snapshot_saves_its_own_statement() {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    engine.set_durability_mode(subql::DurabilityMode::Required);
    // Rotation is what makes a registration snapshot, and this makes the
    // first one rotate.
    engine.set_rotation_threshold(0);
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("the filter registers");
    // No snapshot by hand: the durability mode is what writes.
    drop(engine);

    let restored =
        StoredEngine::with_storage(catalog(), PostgreSqlDialect {}, store.path()).expect("reopen");
    assert_eq!(
        restored.reads().in_process.len(),
        1,
        "the registration that forced the snapshot is in it"
    );
}

/// A statement whose predicate was never saved does not come back.
///
/// The reads file covers every subscription while a shard covers one
/// table, so snapshotting one table writes statements for answers whose
/// predicates are not on disk. Reporting those would tell a caller an
/// answer came back when nothing answers.
#[test]
fn a_statement_without_its_predicate_is_not_reported() {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    let on_orders = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("the orders filter registers")
        .subscription_id;
    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM invoices WHERE state = 'open'",
        ))
        .expect("the invoices filter registers");

    // Only one table's predicates reach disk.
    engine.snapshot_table(table("orders")).expect("snapshot");
    drop(engine);

    let restored =
        StoredEngine::with_storage(catalog(), PostgreSqlDialect {}, store.path()).expect("reopen");
    let reported: Vec<u64> = restored
        .reads()
        .in_process
        .iter()
        .map(|answer| answer.subscription_id)
        .collect();
    assert_eq!(
        reported,
        vec![on_orders],
        "only the answer whose predicate came back is reported"
    );
}

/// An answer the caller ended does not come back.
///
/// The reads file was written when an answer was captured and never when
/// one was dropped, so a restart revived subscriptions the caller had
/// already been told were gone.
#[test]
fn an_ended_read_does_not_come_back() {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    let answer = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT MIN(price) FROM orders",
        ))
        .expect("the extreme registers")
        .subscription_id;
    assert_eq!(engine.reread_count(), 1);
    assert!(engine.unregister_reread(answer), "the caller ends it");
    drop(engine);

    let restored =
        StoredEngine::with_storage(catalog(), PostgreSqlDialect {}, store.path()).expect("reopen");
    assert!(
        restored.reads().restored.is_empty(),
        "an ended answer stays ended, got {:?}",
        restored.reads().restored
    );
}

/// Ending a maintained answer drops its statement too.
#[test]
fn an_ended_in_process_answer_leaves_no_statement() {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    let answer = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("the filter registers")
        .subscription_id;
    engine.snapshot_table(table("orders")).expect("snapshot");
    assert!(engine.unregister_subscription(answer), "the caller ends it");
    drop(engine);

    let (mut restored, reads) = store.open_reporting(catalog());
    assert!(
        reads.in_process.is_empty(),
        "an ended answer keeps no statement"
    );
    // What a caller feels, rather than what the report says: the shard
    // brings maintained answers back, so a removal left out of it revives
    // one and it notifies a consumer that ended it.
    assert_eq!(
        restored.subscription_count(),
        0,
        "the shard does not bring the ended answer back"
    );
    let notified = restored
        .consumers(&TestEvent::insert(
            table("orders"),
            vec![
                subql::backend::Value::Int(1),
                subql::backend::Value::Float(5.0),
                subql::backend::Value::String("paid".into()),
            ],
        ))
        .expect("the event dispatches");
    assert!(
        notified.inserted().is_empty(),
        "nobody is notified for an answer the caller ended, got {:?}",
        notified.inserted()
    );
}

/// Ending by statement is as final as ending by id.
///
/// `unregister_query` ends every maintained answer sharing a predicate,
/// through the same removal funnel, so it owes the same durability. It is
/// the fourth door onto the same state, and the one added last.
#[test]
fn an_answer_ended_by_statement_does_not_come_back() {
    const FILTER: &str = "SELECT * FROM orders WHERE status = 'paid'";
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("the filter registers");
    engine.snapshot_table(table("orders")).expect("snapshot");
    let report = engine
        .unregister_query(1u64, FILTER)
        .expect("the statement names a predicate");
    assert_eq!(report.removed_bindings, 1, "the answer is ended");
    drop(engine);

    let (mut restored, _reads) = store.open_reporting(catalog());
    assert_eq!(
        restored.subscription_count(),
        0,
        "ending by statement survives the restart"
    );
    let notified = restored
        .consumers(&TestEvent::insert(
            table("orders"),
            vec![
                subql::backend::Value::Int(1),
                subql::backend::Value::Float(5.0),
                subql::backend::Value::String("paid".into()),
            ],
        ))
        .expect("the event dispatches");
    assert!(
        notified.inserted().is_empty(),
        "nobody is notified, got {:?}",
        notified.inserted()
    );
}

/// Ending a session ends its answers on disk too.
///
/// A session's answers go through the same removal as any other, but the
/// tables they were on are collected as the loop runs, and a collection
/// that never fills leaves every shard naming answers the session took
/// with it.
#[test]
fn ending_a_session_stops_its_answers_coming_back() {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'")
                .scope(subql::SubscriptionScope::Session(7)),
        )
        .expect("the session's filter registers");
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT MIN(price) FROM orders")
                .scope(subql::SubscriptionScope::Session(7)),
        )
        .expect("the session's read answer registers");
    engine.snapshot_table(table("orders")).expect("snapshot");

    let report = engine.unregister_session(7);
    assert_eq!(report.removed_bindings, 1, "the session held one answer");
    assert_eq!(report.removed_predicates, 1, "whose predicate went with it");
    assert_eq!(report.removed_consumers, 1, "as did its consumer");
    assert_eq!(report.removed_reads, 1, "and one answer a read served");
    drop(engine);

    let (mut restored, reads) = store.open_reporting(catalog());
    assert_eq!(
        restored.subscription_count(),
        0,
        "the session's maintained answer does not come back"
    );
    assert!(
        reads.restored.is_empty(),
        "nor the one a read served, got {:?}",
        reads.restored
    );
    assert_eq!(
        restored.reread_count(),
        0,
        "and the read registry is empty, not merely unreported"
    );
    let notified = restored
        .consumers(&TestEvent::insert(
            table("orders"),
            vec![
                subql::backend::Value::Int(1),
                subql::backend::Value::Float(5.0),
                subql::backend::Value::String("paid".into()),
            ],
        ))
        .expect("the event dispatches");
    assert!(
        notified.inserted().is_empty(),
        "and nobody is notified for it, got {:?}",
        notified.inserted()
    );
}

/// A session leaving does not disturb the answers that stay.
///
/// Ending a session trims the table's consumer dictionary to those still
/// bound, and a name a table cannot resolve is a subscriber that stops
/// being told anything. This holds the ordinary shape, one session
/// leaving a table another answer stays on. It does not reach the case
/// of one consumer holding both, where the trim has a candidate that is
/// still active, which the removal count in the test above is what
/// catches.
#[test]
fn ending_a_session_keeps_the_consumers_that_remain() {
    const FILTER: &str = "SELECT * FROM orders WHERE status = 'paid'";
    let dir = tempfile::tempdir().expect("temp dir");
    let mut engine =
        Engine::with_storage(catalog(), PostgreSqlDialect {}, dir.path().to_path_buf())
            .expect("open store")
            .into_parts()
            .0;
    engine
        .register(
            SubscriptionRequest::new(1u64, FILTER).scope(subql::SubscriptionScope::Session(7)),
        )
        .expect("the leaving session registers");
    engine
        .register(SubscriptionRequest::new(2u64, FILTER))
        .expect("the durable answer registers");

    engine.unregister_session(7);

    let notified = engine
        .consumers(&TestEvent::insert(
            table("orders"),
            vec![
                subql::backend::Value::Int(1),
                subql::backend::Value::Float(5.0),
                subql::backend::Value::String("paid".into()),
            ],
        ))
        .expect("the event dispatches");
    assert_eq!(
        notified.inserted(),
        &[2],
        "the consumer that stayed is still named and still told"
    );
}

/// Under `Required`, a batch that cannot be written is not registered.
///
/// The mode is a promise that a registration the caller is told
/// succeeded is on disk. When the write fails before anything is
/// committed, keeping the answers in memory breaks that promise in the
/// direction that matters, since the caller holds ids for answers a
/// restart will not bring back.
#[test]
fn a_batch_that_cannot_be_written_is_rolled_back() {
    let store = TempStore::new();
    let path = store.path();
    let mut engine = store.open(catalog());
    engine.set_durability_mode(subql::DurabilityMode::Required);
    engine.set_rotation_threshold(0);

    // The store's directory is gone, so every write into it fails. The
    // premise is asserted rather than assumed, because a fault that
    // silently does not happen leaves a test that proves nothing.
    std::fs::remove_dir_all(&path).expect("remove the store directory");
    assert!(
        engine.snapshot_table(table("orders")).is_err(),
        "the fixture has to make writing fail to test what happens when it does"
    );

    let results = engine.register_batch(vec![
        SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
        SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE status = 'shipped'"),
    ]);

    assert!(
        results.iter().all(std::result::Result::is_err),
        "every answer in the batch is refused, got {results:?}"
    );
    assert_eq!(
        engine.subscription_count(),
        0,
        "and none of them is left registered behind the refusal"
    );
}

/// Under `BestEffort`, the same failure keeps the registrations.
///
/// The mode is the other promise: answers are served from memory and
/// the store is a convenience, so a write that fails costs durability
/// rather than the registration.
#[test]
fn a_batch_that_cannot_be_written_survives_best_effort() {
    let store = TempStore::new();
    let path = store.path();
    let mut engine = store.open(catalog());
    engine.set_durability_mode(subql::DurabilityMode::BestEffort);
    engine.set_rotation_threshold(0);

    std::fs::remove_dir_all(&path).expect("remove the store directory");
    assert!(
        engine.snapshot_table(table("orders")).is_err(),
        "the same fault as the test above"
    );

    let results = engine.register_batch(vec![
        SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
        SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE status = 'shipped'"),
    ]);

    assert!(
        results.iter().all(std::result::Result::is_ok),
        "the answers register despite the store, got {results:?}"
    );
    assert_eq!(engine.subscription_count(), 2, "and both stay registered");
}
