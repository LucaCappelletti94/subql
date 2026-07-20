//! In-memory SQLite integration test for the diesel-typed follow-insert path.
//!
//! Unlike MySQL, diesel's SQLite backend supports `RETURNING`, so the typed
//! `register_follow_insert` path works here too, and SQLite runs in-process, so
//! this is a plain `#[test]` (no Docker). Proves `FollowRowDecode for Sqlite`:
//! executing `INSERT ... RETURNING id`, decoding the minted `INTEGER PRIMARY KEY`
//! from the returned `SqliteValue`, and registering the follow.
#![cfg(feature = "diesel-typed-sqlite")]
#![allow(clippy::unwrap_used)]

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{SQLite, Value};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine};

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[test]
fn register_follow_insert_sqlite_decodes_minted_pk() {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .execute(&mut conn)
        .expect("CREATE TABLE users");

    let catalog =
        ParserDB::parse::<SQLiteDialect>("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
            .expect("catalog");
    let mut engine =
        SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, _>::new(catalog, SQLiteDialect {});

    // Execute INSERT ... RETURNING id on SQLite; the DB mints id=1; subql reads
    // the returned SqliteValue, decodes the key, and follows it.
    let ann = engine
        .register_follow_insert(
            1,
            diesel::insert_into(users::table).values(users::name.eq("ann")),
            &mut conn,
        )
        .expect("follow insert ann");
    assert_eq!(ann.len(), 1);
    let ann_pk = engine.follow_row(1, "users", vec![Value::Int(1)]).unwrap();
    assert_eq!(ann[0].subscription_id, ann_pk.subscription_id);

    let bob = engine
        .register_follow_insert(
            1,
            diesel::insert_into(users::table).values(users::name.eq("bob")),
            &mut conn,
        )
        .expect("follow insert bob");
    assert_eq!(bob.len(), 1);
    let bob_pk = engine.follow_row(1, "users", vec![Value::Int(2)]).unwrap();
    assert_eq!(bob[0].subscription_id, bob_pk.subscription_id);

    assert_ne!(ann[0].subscription_id, bob[0].subscription_id);
}
