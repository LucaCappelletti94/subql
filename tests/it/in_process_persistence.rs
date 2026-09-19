//! Statements of the answers the engine maintains itself, across a restart.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE invoices (id INT PRIMARY KEY, state TEXT);";

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
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();

    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
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

    let restored = Engine::with_storage(catalog(), PostgreSqlDialect {}, path).expect("reopen");
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
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();

    let mut engine = Engine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
        .expect("open store")
        .into_parts()
        .0;
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

    let restored = Engine::with_storage(catalog(), PostgreSqlDialect {}, path).expect("reopen");
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
