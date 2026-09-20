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

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn orders() -> subql::TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders")
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
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
    engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers");

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
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
    engine
        .register(SubscriptionRequest::new(1u64, FILTER))
        .expect("registers");
    engine.snapshot_table(orders()).expect("write the shard");
    let shard = path.join(format!("table_{}.shard", orders()));

    let job = DurableShardMerge::merge_shards_background(&mut engine, orders(), &[shard])
        .expect("the trait starts a merge");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut report = None;
    while std::time::Instant::now() < deadline {
        report = DurableShardMerge::try_complete_merge(&mut engine, job).expect("the trait polls");
        if report.is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        report.is_some(),
        "the trait swaps the merge in rather than reporting nothing forever"
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
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
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
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
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
