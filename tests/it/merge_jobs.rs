//! A background merge is reachable for as long as it is outstanding.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

/// An engine with one shard file on disk, and the path to it.
fn engine_with_a_shard() -> (tempfile::TempDir, Engine, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
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
    (dir, engine, shard)
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

    // Both workers are given time to finish, so one drain sees a ready
    // merge followed by a failing one.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut error = None;
    while std::time::Instant::now() < deadline {
        match engine.complete_ready_merges() {
            Ok(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => {
                error = Some(e);
                break;
            }
        }
    }

    let error = error.expect("the corrupt shard fails the drain");
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
