//! The engine reached through its traits, and the rotation boundary.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DurabilityMode, DurableShardMerge, DurableShardStore,
    SubscriptionEngine, SubscriptionRegistration, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";
const FILTER: &str = "SELECT * FROM orders WHERE status = 'paid'";

use crate::common::store::TempStore;

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn orders() -> subql::TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders")
}

/// A store holding one answer that carries `FILTER`.
fn stored_engine_with_one_answer() -> (TempStore, Engine) {
    let store = TempStore::new();
    let mut engine = store.open(catalog());
    engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers");
    (store, engine)
}

/// Ending a subscription through the trait ends it.
///
/// The trait is what a caller holds when the engine behind it is not
/// theirs to name, and a forward that answers without doing the work
/// leaves them holding a subscription they were told was gone.
#[test]
fn the_registration_trait_ends_a_subscription() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    let answer = engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers")
        .subscription_id;

    assert!(
        SubscriptionRegistration::unregister_subscription(&mut engine, answer),
        "the trait ends an answer that exists"
    );
    assert_eq!(engine.subscription_count(), 0, "and it is really gone");
    assert!(
        !SubscriptionRegistration::unregister_subscription(&mut engine, answer),
        "and answers false for one that does not exist"
    );
}

/// Snapshotting through the trait writes the files.
#[test]
fn the_store_trait_writes_a_shard() {
    let (store, engine) = stored_engine_with_one_answer();
    let path = store.path();

    DurableShardStore::snapshot_table(&engine, orders()).expect("the trait writes");

    assert!(
        path.join(format!("table_{}.shard", orders())).exists(),
        "the shard is on disk"
    );
    assert!(
        path.join("reads.shard").exists(),
        "and so are the statements it needs"
    );
}

/// Merging through the trait starts and completes a job.
#[test]
fn the_merge_trait_runs_a_merge() {
    let store = TempStore::new();
    let path = store.path();
    let mut engine = store.open(catalog());
    let answer = engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers")
        .subscription_id;
    engine.snapshot_table(orders()).expect("write the shard");
    let shard = path.join(format!("table_{}.shard", orders()));

    let before = engine.active_merge_jobs();
    let job = DurableShardMerge::merge_shards_background(&mut engine, orders(), &[shard])
        .expect("the trait starts a merge");
    let outstanding = engine.active_merge_jobs();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut report = None;
    while std::time::Instant::now() < deadline {
        report = DurableShardMerge::try_complete_merge(&mut engine, job).expect("the trait polls");
        if report.is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let report = report.expect("the trait completes the merge rather than polling forever");
    assert_eq!(
        (before, outstanding, engine.active_merge_jobs()),
        (0, 1, 0),
        "none before, one while it runs, none once it is swapped in"
    );
    assert_eq!(report.input_shards, 1, "the shard it was given");
    assert_eq!(report.output_predicates, 1, "carrying the one predicate");
    assert_eq!(report.output_bindings, 1, "and the one binding");

    // The swap put a rebuilt partition in place of the live one, so the
    // answer still has to be there and still has to match. A completion
    // that reported success without rebuilding leaves this passing only
    // by accident, which is why the report above is checked as well.
    assert_eq!(
        engine.subscription_count(),
        1,
        "the answer survived the swap"
    );
    let notified = engine
        .consumers(
            &TestEvent::insert(
                orders(),
                vec![
                    subql::backend::Value::Int(1),
                    subql::backend::Value::Float(5.0),
                    subql::backend::Value::String("paid".into()),
                ],
            )
            .with_pk_columns([0u16]),
        )
        .expect("the event dispatches");
    assert_eq!(
        notified.inserted(),
        &[1],
        "and the rebuilt partition still routes to it"
    );
    assert!(
        engine.unregister_subscription(answer),
        "the rebuilt state knows the answer by its own id"
    );
}

/// The durability settings are the ones that were set.
#[test]
fn the_durability_settings_are_read_back() {
    let mut engine = Engine::new(catalog(), PostgreSqlDialect {});
    engine.set_durability_mode(DurabilityMode::BestEffort);
    assert_eq!(engine.durability_mode(), DurabilityMode::BestEffort);
    engine.set_durability_mode(DurabilityMode::Required);
    assert_eq!(
        engine.durability_mode(),
        DurabilityMode::Required,
        "setting it twice keeps the second"
    );

    engine.set_rotation_threshold(4096);
    assert_eq!(engine.rotation_threshold(), 4096);
}

/// Rotation happens past the threshold, not on it.
///
/// A shard is rotated when the partition outweighs the threshold. Sitting
/// exactly on it is not outweighing it, and rotating there writes a file
/// for a store the caller asked to leave alone until it grew.
#[test]
fn a_partition_exactly_on_the_threshold_does_not_rotate() {
    let store = TempStore::new();
    let path = store.path();
    let mut engine = store.open(catalog());
    engine.set_durability_mode(DurabilityMode::Required);
    // One predicate and one binding weigh this much by the engine's own
    // estimate, so the first registration lands exactly on the line.
    engine.set_rotation_threshold(1024 + 128);

    engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers without rotating");

    assert!(
        !path.join(format!("table_{}.shard", orders())).exists(),
        "a partition on the threshold has not passed it"
    );

    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE status = 'shipped'",
        ))
        .expect("registers and rotates");

    assert!(
        path.join(format!("table_{}.shard", orders())).exists(),
        "one past the threshold does"
    );
}

/// The weight counts predicates and bindings, and adds them.
///
/// The estimate is a thousand and twenty four for each predicate plus a
/// hundred and twenty eight for each binding. A threshold between that
/// sum and either part of it alone is what tells the whole estimate from
/// one that dropped the bindings or subtracted them.
#[test]
fn one_answer_outweighs_a_threshold_between_its_parts() {
    let store = TempStore::new();
    let path = store.path();
    let mut engine = store.open(catalog());
    engine.set_durability_mode(DurabilityMode::Required);
    // Above a lone predicate, below a predicate plus its binding.
    engine.set_rotation_threshold(1100);

    engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers and rotates");

    assert!(
        path.join(format!("table_{}.shard", orders())).exists(),
        "one predicate and its binding together outweigh 1100"
    );
}
