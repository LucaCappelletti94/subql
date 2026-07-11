//! Regression test: after `SubscriptionEngine::with_storage` reloads a
//! shard, dispatching a matching event on the restored table must
//! surface the restored subscription.
//!
//! The bug: `load_shard` restored the partition + consumer dictionary,
//! but skipped `ensure_column_kinds_cached`, which normally fires on
//! `register()`. Without the per-table `ScalarKind` cache the dispatch
//! path took `arity = 0`, evaluated only the fallback bitmap, and
//! silently dropped every predicate whose `IndexableAtom`s were
//! equality or range (i.e. anything the prefilter could actually
//! index). Fixed by populating the cache from `load_shard`.
#![cfg(feature = "std")]
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn insert_event(table_id: subql::TableId, id: i64, amount: i64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(
        table_id,
        vec![
            Value::Int(id),
            Value::Int(amount),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
}

#[test]
fn restore_populates_column_kinds_cache() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let orders = catalog_helpers::table_id(&catalog(), "orders").unwrap();

    // Register a range predicate; the prefilter emits a `Range` atom for
    // `amount > 100`, so `arity` matters at dispatch time.
    {
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(catalog(), PostgreSqlDialect {}, path.clone())
                .expect("with_storage");
        engine
            .register(SubscriptionRequest::<DefaultIds, Postgres>::new(
                7u64,
                "SELECT * FROM orders WHERE amount > 100",
            ))
            .unwrap();
        engine.snapshot_table(orders).expect("snapshot");
    }

    let mut restored: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::with_storage(catalog(), PostgreSqlDialect {}, path)
            .expect("restore with_storage");

    let notifs = restored.consumers(&insert_event(orders, 1, 250)).unwrap();
    assert_eq!(
        notifs.inserted(),
        vec![7u64],
        "restored range-indexed predicate must still match at dispatch time"
    );
}
