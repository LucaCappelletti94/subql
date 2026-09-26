//! Keyed re-reads through the sync SQLite connector.
//!
//! A keyed read asks the database only about the keys that changed, so an
//! update from key 4 to key 6 has to ask about both: key 6 now holds the row
//! and key 4 no longer does. Asking about key 6 alone left a subscriber
//! holding the row under key 4 forever.
//!
//! A change with no key, a truncate among them, moves a keyed read to a whole
//! re-read, and the store has to follow it there, or a restart drops it.
#![allow(clippy::unwrap_used)]

use diesel::{Connection as _, RunQueryDsl as _, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{SQLite, Value};
use subql::reexec::{AutoResolvingEngine, DieselConnector, SyncMode};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, Tier};

type Engine = SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB>;

const DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)";

#[test]
fn a_moved_key_is_reported_gone_under_its_old_key() {
    let url = "file:keyed_reads_key_change?mode=memory&cache=shared";
    let mut model = SqliteConnection::establish(url).unwrap();
    // DDL and the model's writes, which the test drives as raw statements
    // so the event below mirrors them exactly.
    diesel::sql_query(DDL).execute(&mut model).unwrap();
    let database = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id::<SQLite, _>(&database, "t").unwrap();
    let mut engine = AutoResolvingEngine::new(
        SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
            database,
            SQLiteDialect {},
        ),
        SyncMode(DieselConnector::<SqliteConnection, SQLite>::new(
            SqliteConnection::establish(url).unwrap(),
        )),
    );
    // `2 LIKE 0` is not served in process, so the filter is a keyed read.
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM t WHERE NOT (2 LIKE 0)"),
            (),
        )
        .unwrap();
    assert!(matches!(registered.tier, Tier::KeyedRows { .. }));

    diesel::sql_query("INSERT INTO t VALUES (4, 1)")
        .execute(&mut model)
        .unwrap();
    diesel::sql_query("UPDATE t SET id = 6 WHERE id = 4")
        .execute(&mut model)
        .unwrap();
    let settled = engine
        .apply(
            &TestEvent::update(
                table,
                vec![Value::Int(4), Value::Int(1)],
                vec![Value::Int(6), Value::Int(1)],
            )
            .with_pk_columns([0u16])
            .with_changed_columns([0u16]),
        )
        .unwrap()
        .resolve_collect();
    let mut deltas: Vec<(Vec<Value<SQLite>>, bool)> = settled
        .reads
        .unwrap()
        .row_deltas
        .into_iter()
        .map(|delta| (delta.key, delta.row.is_some()))
        .collect();
    deltas.sort_by_key(|(key, _)| format!("{key:?}"));
    assert_eq!(
        deltas,
        vec![(vec![Value::Int(4)], false), (vec![Value::Int(6)], true)],
        "key 4 left the answer and key 6 joined it"
    );
}

/// A keyed read a truncate moved to a whole re-read comes back from the store.
#[test]
fn a_read_a_truncate_moved_to_a_whole_reread_survives_a_restart() {
    let url = "file:keyed_reads_truncate_restart?mode=memory&cache=shared";
    let mut model = SqliteConnection::establish(url).unwrap();
    diesel::sql_query(DDL).execute(&mut model).unwrap();
    let store = tempfile::tempdir().expect("temp dir");
    let database = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id::<SQLite, _>(&database, "t").unwrap();
    let (inner, _) = Engine::with_storage(database, SQLiteDialect {}, store.path().to_path_buf())
        .unwrap()
        .into_parts();
    let mut engine = AutoResolvingEngine::new(
        inner,
        SyncMode(DieselConnector::<SqliteConnection, SQLite>::new(
            SqliteConnection::establish(url).unwrap(),
        )),
    );
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM t WHERE NOT (2 LIKE 0)"),
            (),
        )
        .unwrap();
    assert!(matches!(registered.tier, Tier::KeyedRows { .. }));
    let settled = engine
        .apply(&TestEvent::truncate(table))
        .unwrap()
        .resolve_collect();
    assert_eq!(
        settled.dispatched.transitions.len(),
        1,
        "the truncate moves the read"
    );
    drop(engine);

    let restored = Engine::with_storage(
        ParserDB::parse::<SQLiteDialect>(DDL).unwrap(),
        SQLiteDialect {},
        store.path().to_path_buf(),
    )
    .unwrap();
    let back: Vec<_> = restored
        .reads()
        .restored
        .iter()
        .map(|read| read.subscription_id)
        .collect();
    assert_eq!(
        back,
        vec![registered.subscription_id],
        "the read comes back"
    );
}
