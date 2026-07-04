//! Docker-backed integration test for the MySQL follow-on-insert story.
//!
//! MySQL has no `RETURNING`, so `SubscriptionEngine::register_follow_insert`
//! (RETURNING-based, Pg-only) does not apply. The documented MySQL path is:
//! execute the insert, read the DB-minted auto-increment key from
//! `LAST_INSERT_ID()`, then `follow_row`. This test exercises exactly that.
//!
//! It reads `LAST_INSERT_ID()` with **raw** SQL because diesel exposes no sugar
//! for it on MySQL (contrast SQLite's `last_insert_rowid()`). See MILESTONES.md:
//! once diesel grows that sugar, swap the raw query for the typed call.
//!
//! Requires Docker. `#[ignore]`d so default `cargo test` does not spin up a
//! container. Run with:
//!
//! ```sh
//! cargo test --test follow_insert_mysql -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used)]

mod common;

use diesel::prelude::*;
use diesel::sql_types::BigInt;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{Cell, DefaultIds, SubscriptionEngine};

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

/// Row type for `SELECT ... AS id`. `CAST(... AS SIGNED)` avoids the
/// `BIGINT UNSIGNED` that `LAST_INSERT_ID()` returns natively.
#[derive(diesel::QueryableByName)]
struct LastId {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

#[test]
#[ignore = "requires Docker"]
fn follow_inserted_row_via_last_insert_id() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let mut conn = common::mysql_connect(common::mysql_port(&container));

    // A genuine auto-generated primary key: the id is minted by the DB.
    diesel::sql_query("CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)")
        .execute(&mut conn)
        .expect("CREATE TABLE users");

    let catalog =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
            .expect("catalog");
    let mut engine = SubscriptionEngine::<_, DefaultIds, _>::new(catalog, PostgreSqlDialect {});

    // Typed diesel insert: executes and returns affected rows, not the id.
    diesel::insert_into(users::table)
        .values(users::name.eq("ann"))
        .execute(&mut conn)
        .expect("insert ann");

    // MySQL has no RETURNING; read the minted key from LAST_INSERT_ID() (raw SQL
    // for now, no diesel sugar - see MILESTONES.md).
    let minted: i64 = diesel::sql_query("SELECT CAST(LAST_INSERT_ID() AS SIGNED) AS id")
        .get_result::<LastId>(&mut conn)
        .expect("last_insert_id")
        .id;
    assert_eq!(minted, 1);

    // Follow the row the DB just minted; it dedups with an explicit follow on the
    // same id (same predicate -> same subscription), proving the key threaded
    // through correctly.
    let follow = engine
        .follow_row(1, "users", vec![Cell::Int(minted)])
        .expect("follow row");
    let explicit = engine.follow_row(1, "users", vec![Cell::Int(1)]).unwrap();
    assert_eq!(follow.subscription_id, explicit.subscription_id);
}
