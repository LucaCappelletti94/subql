//! Answers that need a database read survive a restart, come back not knowing
//! their value, and are dropped one at a time when their table moves.
#![cfg(feature = "std")]
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DropReason, SubscriptionEngine, SubscriptionRequest, Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE managers (id INT PRIMARY KEY);";

/// The same catalog with one more column on `orders`, which is what a schema
/// change looks like to the loader.
const WIDER_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT, \
                         note TEXT);\
                         CREATE TABLE managers (id INT PRIMARY KEY);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn catalog(ddl: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(ddl).expect("parse DDL")
}

const EXTREME: &str = "SELECT MIN(price) FROM orders";
const KEYED: &str = "SELECT * FROM orders WHERE lower(status) = 'paid'";

/// Register both re-read shapes into a fresh store, then restore from it.
fn save_then_restore(restore_ddl: &str) -> (subql::RestoredReads, Engine) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();

    let (mut engine, first) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    assert!(
        first.restored.is_empty() && first.dropped.is_empty(),
        "an empty store restores nothing"
    );
    engine
        .register(SubscriptionRequest::new(1u64, EXTREME))
        .expect("extreme registers");
    engine
        .register(SubscriptionRequest::new(2u64, KEYED))
        .expect("keyed registers");
    assert_eq!(engine.reread_count(), 2);
    drop(engine);

    let (restored_engine, report) =
        Engine::with_storage(catalog(restore_ddl), PostgreSqlDialect {}, path)
            .expect("reopen store");
    // The directory has to outlive the reopen, so it is dropped here.
    drop(dir);
    (report, restored_engine)
}

/// Both come back, keeping their identities and their tiers.
#[test]
fn saved_answers_come_back_with_their_identities() {
    let (report, engine) = save_then_restore(DDL);

    assert!(
        report.dropped.is_empty(),
        "the schema did not move, so nothing is dropped: {:?}",
        report.dropped
    );
    assert_eq!(engine.reread_count(), 2, "both are live again");

    let mut restored: Vec<(u64, bool)> = report
        .restored
        .iter()
        .map(|r| (r.subscription_id, r.tier_changed))
        .collect();
    restored.sort_unstable();
    assert_eq!(
        restored,
        vec![(1, false), (2, false)],
        "the identities and tiers are the ones that were saved"
    );

    let tiers: Vec<&Tier> = report.restored.iter().map(|r| &r.tier).collect();
    assert!(
        tiers.iter().any(|t| matches!(t, Tier::Scalar { .. }))
            && tiers.iter().any(|t| matches!(t, Tier::KeyedRows { .. })),
        "one of each, got {tiers:?}"
    );
}

/// Restored identities remain taken: the next registration must not reuse one.
#[test]
fn a_new_answer_after_restore_gets_a_new_identity() {
    let (_report, mut engine) = save_then_restore(DDL);
    let next = engine
        .register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM managers WHERE id > 0",
        ))
        .expect("a new subscription registers");
    assert!(
        next.subscription_id > 2,
        "1 and 2 came back from disk, so the counter must pass both, got {}",
        next.subscription_id
    );
}

/// A restored answer knows nothing yet, so its first change asks for a read
/// rather than reporting a value it never had.
#[test]
fn a_restored_answer_asks_before_it_answers() {
    let (report, engine) = save_then_restore(DDL);
    let extreme = report
        .restored
        .iter()
        .find(|r| matches!(r.tier, Tier::Scalar { .. }))
        .expect("the extreme came back");

    let table = catalog_helpers::table_id(&catalog(DDL), "orders").expect("orders resolves");
    let event = TestEvent::<Postgres>::insert(
        table,
        vec![
            subql::backend::Value::Int(1),
            subql::backend::Value::Float(5.0),
            subql::backend::Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16]);

    let mut engine = engine;
    let notifications = engine.dispatch(&event).expect("dispatch");

    assert!(
        notifications.scalar_updates().is_empty(),
        "a restored answer has no value to report, got {:?}",
        notifications.scalar_updates()
    );
    assert!(
        notifications
            .triggers()
            .iter()
            .any(|t| t.subscription_id == extreme.subscription_id),
        "it asks for the read that fills it in"
    );
}

/// A column added to `orders` drops the answers reading it, and says which
/// table moved. Nothing else is disturbed.
#[test]
fn an_answer_whose_table_moved_is_dropped_and_named() {
    let (report, engine) = save_then_restore(WIDER_DDL);

    assert!(
        report.restored.is_empty(),
        "both answers read `orders`, so neither fits the new shape"
    );
    assert_eq!(engine.reread_count(), 0, "and neither is live");

    let table = catalog_helpers::table_id(&catalog(WIDER_DDL), "orders").expect("orders resolves");
    let mut dropped: Vec<u64> = report.dropped.iter().map(|d| d.subscription_id).collect();
    dropped.sort_unstable();
    assert_eq!(dropped, vec![1, 2]);
    for entry in &report.dropped {
        assert_eq!(
            entry.reason,
            DropReason::TableChanged { table_id: table },
            "the reason names the table that moved"
        );
        assert!(
            entry.sql == EXTREME || entry.sql == KEYED,
            "and carries the statement, so a caller can register it again"
        );
    }
}

#[test]
fn a_grouped_scalar_read_restores_under_the_same_identity() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let registered = engine
        .register(SubscriptionRequest::new(
            4u64,
            "SELECT status, MIN(price) FROM orders GROUP BY status",
        ))
        .expect("grouped minimum registers");
    assert!(matches!(registered.tier, Tier::GroupedScalar { .. }));
    drop(engine);

    let (_engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    assert!(report.dropped.is_empty());
    assert_eq!(report.restored.len(), 1);
    assert_eq!(
        report.restored[0].subscription_id,
        registered.subscription_id
    );
    assert!(!report.restored[0].tier_changed);
    assert!(matches!(
        report.restored[0].tier,
        Tier::GroupedScalar { .. }
    ));
}

/// A grouped extreme's `HAVING` survives the restart: the restored plan
/// still installs a failing group silently.
#[test]
fn a_having_extreme_restores_with_its_condition() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let registered = engine
        .register(SubscriptionRequest::new(
            5u64,
            "SELECT status, MIN(price) FROM orders GROUP BY status HAVING MIN(price) < 5",
        ))
        .expect("filtered grouped minimum registers");
    drop(engine);

    let (mut engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    assert_eq!(report.restored.len(), 1);
    assert!(!report.restored[0].tier_changed);
    let Tier::GroupedScalar { ref bootstrap } = report.restored[0].tier else {
        panic!("expected grouped scalar tier, got {:?}", report.restored[0]);
    };
    assert!(
        !bootstrap.sql.to_uppercase().contains("HAVING"),
        "the restored seed still fetches every group: {}",
        bootstrap.sql
    );
    let opening = subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::GroupedScalarSeedInstall {
            rows: vec![vec![
                subql::backend::Value::String("paid".into()),
                subql::backend::Value::Float(9.0),
                subql::backend::Value::Int(1),
            ]],
            read_at: None::<subql::NoCheckpoint>,
        },
    )
    .expect("restored plan accepts its seed");
    assert!(
        opening.updates.is_empty(),
        "minimum 9 fails the restored condition, got {:?}",
        opening.updates
    );
}
