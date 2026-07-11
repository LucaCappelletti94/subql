//! Docker-backed integration test for the MySQL follow-on-insert story.
//!
//! MySQL has no `RETURNING`, so `SubscriptionEngine::register_follow_insert`
//! (which appends `RETURNING <pk>`) does not apply. The MySQL path executes the
//! insert with diesel's `InsertStatement::execute_returning_id`, which reads the
//! DB-minted `AUTO_INCREMENT` key from the client library (`mysql_insert_id`)
//! with no extra round trip, then follows that row with `follow_row`.
//!
//! Requires Docker. `#[ignore]`d so default `cargo test` does not spin up a
//! container. Run with:
//!
//! ```sh
//! cargo test --test follow_insert_mysql -- --ignored --nocapture
//! ```
#![cfg(feature = "diesel-typed-mysql")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, Value};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine};

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[test]
#[ignore = "requires Docker"]
fn follow_inserted_row_via_execute_returning_id() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let mut conn = common::mysql_connect(common::mysql_port(&container));

    // A genuine auto-generated primary key: the id is minted by the DB.
    diesel::sql_query("CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)")
        .execute(&mut conn)
        .expect("CREATE TABLE users");

    let catalog =
        ParserDB::parse::<MySqlDialect>("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
            .expect("catalog");
    let mut engine =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, _>::new(catalog, MySqlDialect {});

    // MySQL has no RETURNING; `execute_returning_id` runs the insert and reads the
    // minted AUTO_INCREMENT key straight from the client library, no extra query.
    let minted: u64 = diesel::insert_into(users::table)
        .values(users::name.eq("ann"))
        .execute_returning_id(&mut conn)
        .expect("execute_returning_id");
    assert_eq!(minted, 1);

    // Follow the row the DB just minted; it dedups with an explicit follow on the
    // same id (same predicate -> same subscription), proving the key threaded
    // through correctly.
    let follow = engine
        .follow_row(1, "users", vec![Value::Int(i64::try_from(minted).unwrap())])
        .expect("follow row");
    let explicit = engine.follow_row(1, "users", vec![Value::Int(1)]).unwrap();
    assert_eq!(follow.subscription_id, explicit.subscription_id);
}
