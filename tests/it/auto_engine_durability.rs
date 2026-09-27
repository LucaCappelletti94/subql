//! Durability through `AutoResolvingEngine`, across restarts.
//!
//! A restart hands back an engine at the default rotation threshold, and
//! `adopt` wraps it, so the resolving engine has to be where a caller writes
//! its shards again, by a threshold or on demand.
#![allow(clippy::unwrap_used)]

use diesel::{Connection as _, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::SQLite;
use subql::reexec::{AutoResolvingEngine, DieselConnector, SyncMode};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionId, SubscriptionRequest};

type Engine = SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB>;
type Auto = AutoResolvingEngine<
    TestEvent<SQLite>,
    DefaultIds,
    ParserDB,
    SyncMode<DieselConnector<SqliteConnection, SQLite>>,
>;

const DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)";

/// A store directory removed when the test ends.
struct Store(tempfile::TempDir);

impl Store {
    fn new() -> Self {
        Self(tempfile::tempdir().expect("temp dir"))
    }

    fn open(&self) -> subql::Restored<TestEvent<SQLite>, DefaultIds, ParserDB> {
        Engine::with_storage(
            ParserDB::parse::<SQLiteDialect>(DDL).unwrap(),
            SQLiteDialect {},
            self.0.path().to_path_buf(),
        )
        .unwrap()
    }

    /// Reopen and wrap, and name the subscriptions the shards brought back.
    fn restart(&self) -> (Auto, Vec<SubscriptionId>) {
        let restored = self.open();
        let back = restored
            .reads()
            .in_process
            .iter()
            .map(|read| read.subscription_id)
            .collect();
        let engine = AutoResolvingEngine::adopt(restored, connector(), |_| (), |_| ());
        (engine, back)
    }
}

fn connector() -> SyncMode<DieselConnector<SqliteConnection, SQLite>> {
    SyncMode(DieselConnector::new(
        SqliteConnection::establish(":memory:").unwrap(),
    ))
}

fn register(engine: &mut Auto, sql: &str) -> SubscriptionId {
    engine
        .register(SubscriptionRequest::new(1u64, sql), ())
        .unwrap()
        .subscription_id
}

#[test]
fn a_threshold_set_after_a_restart_writes_every_later_registration() {
    let store = Store::new();
    let (mut engine, _) = store.restart();
    engine.set_rotation_threshold(0);
    let first = register(&mut engine, "SELECT * FROM t WHERE a > 1");
    drop(engine);

    let (mut engine, back) = store.restart();
    assert_eq!(back, vec![first]);
    engine.set_rotation_threshold(0);
    let second = register(&mut engine, "SELECT * FROM t WHERE a > 2");
    drop(engine);

    let (_, back) = store.restart();
    assert_eq!(back, vec![first, second]);
}

#[test]
fn a_table_snapshot_writes_registrations_below_the_threshold() {
    let store = Store::new();
    let (mut engine, _) = store.restart();
    let registered = register(&mut engine, "SELECT * FROM t WHERE a > 1");
    let database = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id::<SQLite, _>(&database, "t").unwrap();
    engine.snapshot_table(table).unwrap();
    drop(engine);

    let (_, back) = store.restart();
    assert_eq!(back, vec![registered]);
}
