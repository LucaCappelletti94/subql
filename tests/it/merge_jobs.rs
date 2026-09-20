//! A background merge is reachable for as long as it is outstanding.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
     CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);";

use crate::common::store::TempStore;

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

/// An engine with one shard file on disk, and the path to it.
fn engine_with_a_shard() -> (TempStore, Engine, std::path::PathBuf) {
    let store = TempStore::new();
    let path = store.path();
    let mut engine = store.open(catalog());
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders");
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE status = 'paid'",
        ))
        .expect("the filter registers");
    engine.snapshot_table(orders).expect("write the shard");
    let shard = path.join(format!("table_{orders}.shard"));
    assert!(shard.exists(), "the snapshot wrote {}", shard.display());
    (store, engine, shard)
}

/// Every merge is drained by name, without the caller holding on to one.
///
/// The id a merge is started with is the only way to reach it, so a caller
/// that loses one leaves the work finished, held in memory, and never
/// applied to the live partition. Draining reaches those by name.
#[test]
fn a_merge_whose_name_was_dropped_is_still_completed() {
    let (_dir, mut engine, shard) = engine_with_a_shard();

    // The caller drops the name, which is the whole footgun.
    let _ = engine
        .merge_shards_background(
            catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders"),
            &[shard],
        )
        .expect("the merge starts");
    assert_eq!(
        engine.pending_merges().len(),
        1,
        "the merge is outstanding and nameable"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut reports = Vec::new();
    while std::time::Instant::now() < deadline {
        reports = engine.complete_ready_merges().expect("the drain runs");
        if !reports.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    assert_eq!(reports.len(), 1, "the dropped merge is swapped in");
    assert!(
        engine.pending_merges().is_empty(),
        "nothing is left outstanding"
    );
}

/// Draining leaves nothing behind when there is nothing to drain.
#[test]
fn draining_without_a_merge_reports_nothing() {
    let (_dir, mut engine, _shard) = engine_with_a_shard();

    assert!(engine.pending_merges().is_empty());
    assert!(engine
        .complete_ready_merges()
        .expect("the drain runs")
        .is_empty());
}

/// A merge that fails does not take the applied ones with it.
///
/// Swapping a merged shard into the live partition cannot be undone, and
/// the job is gone from the manager once it is, so a report lost to a later
/// failure is lost for good.
#[test]
fn a_failed_merge_keeps_the_reports_of_the_merges_already_applied() {
    let (dir, mut engine, shard) = engine_with_a_shard();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders");

    let corrupt = dir.path().join("corrupt.shard");
    std::fs::write(&corrupt, b"not a shard at all").expect("write the corrupt shard");

    let good = engine
        .merge_shards_background(orders, &[shard])
        .expect("the first merge starts");
    let bad = engine
        .merge_shards_background(orders, &[corrupt])
        .expect("the second merge starts");
    assert!(good < bad, "the drain runs in job order");

    // One drain, after both workers have certainly finished. Polling would
    // decide nothing: the corrupt job is the cheaper worker and can finish
    // first, and a poll that catches only the healthy one consumes it, so
    // either way the failing drain would carry nothing through no fault of
    // the code under test.
    std::thread::sleep(std::time::Duration::from_secs(1));
    let error = engine
        .complete_ready_merges()
        .expect_err("both merges are ready by now, and the corrupt one fails");
    assert_eq!(
        error.applied.len(),
        1,
        "the merge applied before the failure is still reported"
    );
    assert!(
        engine.pending_merges().is_empty(),
        "both jobs are done with, one applied and one failed"
    );
}

/// A merge that would drop a live answer is refused, not applied.
///
/// A merge reads a shard from disk in the background while the engine
/// keeps registering. Swapping its payload into the live partition
/// cannot be undone, so a payload that does not carry every answer the
/// partition currently holds would silently unsubscribe whoever
/// registered while the merge was running.
#[test]
fn a_merge_missing_a_live_answer_is_refused() {
    let (_store, mut engine, shard) = engine_with_a_shard();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders");

    let _ = engine
        .merge_shards_background(orders, &[shard])
        .expect("the merge starts");

    // Registered after the merge read its input, so the payload coming
    // back cannot know about it.
    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE status = 'shipped'",
        ))
        .expect("the late answer registers");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let outcome = loop {
        let outcome = engine.complete_ready_merges();
        match outcome {
            Ok(ref reports) if reports.is_empty() => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the merge never finished"
                );
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            other => break other,
        }
    };

    let Err(error) = outcome else {
        panic!("the merge was applied, dropping a live answer: {outcome:?}");
    };
    assert!(
        format!("{error}").contains("missing live subscriptions"),
        "refused for the right reason, got {error}"
    );
    assert_eq!(
        engine.subscription_count(),
        2,
        "and both answers are still registered"
    );
}

/// Applying a merge keeps the other tables' answers on their shards.
///
/// Completing a merge rebuilds the engine's map from answer to table,
/// which every table shares. The map is what tells a later removal which
/// table to write back, so rebuilding it for the merged table has to
/// spare the rows belonging to the others. Losing them costs no answer
/// immediately, and surfaces on the next restart as an answer that was
/// ended coming back.
#[test]
fn a_merge_on_one_table_keeps_the_answers_on_another() {
    let (store, mut engine, shard) = engine_with_a_shard();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders").expect("orders");
    let customers =
        catalog_helpers::table_id::<Postgres, _>(&catalog(), "customers").expect("customers");

    let elsewhere = engine
        .register(SubscriptionRequest::new(
            7u64,
            "SELECT * FROM customers WHERE id = 1",
        ))
        .expect("the answer on the other table registers")
        .subscription_id;
    engine
        .snapshot_table(customers)
        .expect("the other table reaches disk");

    let _ = engine
        .merge_shards_background(orders, &[shard])
        .expect("the merge starts");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let reports = engine.complete_ready_merges().expect("the drain runs");
        if !reports.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the merge never finished"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    assert!(
        engine.unregister_subscription(elsewhere),
        "the answer on the untouched table ends"
    );
    drop(engine);

    let reopened = store.open(catalog());
    assert_eq!(
        reopened.subscription_count(),
        1,
        "only the merged table's answer comes back, not the ended one"
    );
}
