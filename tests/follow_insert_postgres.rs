//! Docker-backed integration test for the diesel-typed follow-insert path
//! (`SubscriptionEngine::register_follow_insert`) against real Postgres.
//!
//! Proves the one path the unit tests cannot: executing an `INSERT ... RETURNING`
//! on a live database, decoding the DB-minted auto-increment primary key from the
//! returned row (the `Row`/`Field`/`PgValue` walk), and registering a follow on it.
//!
//! Requires Docker. `#[ignore]`d so default `cargo test` does not spin up a
//! container. Run with:
//!
//! ```sh
//! cargo test --test follow_insert_postgres --features diesel-typed \
//!     -- --ignored --nocapture
//! ```
#![cfg(feature = "diesel-typed")]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{Cell, DefaultIds, SubscriptionEngine};

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[test]
#[ignore = "requires Docker"]
fn register_follow_insert_decodes_minted_pk() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let mut conn = common::pg_connect(common::pg_port(&container));

    // A genuine auto-generated (SERIAL) primary key: the id is minted by the DB.
    diesel::sql_query("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
        .execute(&mut conn)
        .expect("CREATE TABLE users");

    // subql's catalog mirrors the schema (declared PK on `id`).
    let catalog =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
            .expect("catalog");
    let mut engine = SubscriptionEngine::<_, DefaultIds, _>::new(catalog, PostgreSqlDialect {});

    // Execute `INSERT INTO users (name) VALUES ('ann') RETURNING id` on real PG.
    // The DB mints id=1; subql reads the RETURNING row, decodes the minted key,
    // and follows it.
    let ann = engine
        .register_follow_insert(
            1,
            "users",
            diesel::insert_into(users::table)
                .values(users::name.eq("ann"))
                .returning(users::id),
            &mut conn,
        )
        .expect("follow insert ann");
    assert_eq!(ann.len(), 1);
    // The minted key was decoded as 1: this follow dedups with an explicit
    // follow on id = 1 (same predicate -> same subscription).
    let ann_pk = engine.follow_row(1, "users", vec![Cell::Int(1)]).unwrap();
    assert_eq!(ann[0].subscription_id, ann_pk.subscription_id);

    // Second insert mints id=2 and follows a distinct row.
    let bob = engine
        .register_follow_insert(
            1,
            "users",
            diesel::insert_into(users::table)
                .values(users::name.eq("bob"))
                .returning(users::id),
            &mut conn,
        )
        .expect("follow insert bob");
    assert_eq!(bob.len(), 1);
    let bob_pk = engine.follow_row(1, "users", vec![Cell::Int(2)]).unwrap();
    assert_eq!(bob[0].subscription_id, bob_pk.subscription_id);

    assert_ne!(ann[0].subscription_id, bob[0].subscription_id);
}
