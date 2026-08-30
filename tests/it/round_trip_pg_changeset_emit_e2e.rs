//! Outbound changeset emit round trip: a source-side primary-key change
//! propagates to the SQLite replica through a changeset.
//!
//! The existing round trips emit patchsets, which cannot carry a
//! primary-key change. This one drives the changeset emit path instead. A
//! Postgres source with `REPLICA IDENTITY FULL` (so an update's old-row
//! image carries the old key) is mutated with a non-key update, a
//! primary-key relocation, and a delete. subql drains wal2json, folds the
//! events into a changeset with [`wal2json_changeset_builder`], and applies
//! it to a SQLite replica through [`SubscriptionEngine::apply_changeset`]
//! with a [`SqliteAdapter`]. The replica must end up with the row
//! relocated to the new key, which a patchset emit could not achieve.
//!
//! This is the outbound half only (source to replica). Every column rides
//! `sqlite_diff_rs::DefaultBinder`. No SQL casts.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test it round_trip_pg_changeset_emit_e2e:: \
//!     --features "apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" \
//!     -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl, SqliteConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::SQLite as SqliteBackend;
use subql::emit::wal2json_changeset_builder;
use subql::patchset::SqliteAdapter;
use subql::testing::TestEvent;
use subql::{parse_wal2json_v2, DefaultIds, MessageV2, SubscriptionEngine};

const SLOT: &str = "rt_changeset_emit_slot";

// `REPLICA IDENTITY FULL` so an update's old-row image (wal2json `identity`)
// carries the old key that the changeset WHERE needs.
const PG_DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT)";
const SUBQL_PG_DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT);";
const SQLITE_DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, qty INTEGER);";

#[derive(QueryableByName, Debug, PartialEq)]
struct Item {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    label: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    qty: i64,
}

fn load_sqlite(conn: &mut SqliteConnection) -> Vec<Item> {
    sql_query("SELECT id, label, qty FROM items ORDER BY id")
        .load(conn)
        .unwrap()
}

const SEED: &[(i32, &str, i32)] = &[(1, "a", 10), (2, "b", 20), (3, "c", 30)];

fn seed_rows() -> Vec<Item> {
    vec![
        Item {
            id: 1,
            label: "a".to_owned(),
            qty: 10,
        },
        Item {
            id: 2,
            label: "b".to_owned(),
            qty: 20,
        },
        Item {
            id: 3,
            label: "c".to_owned(),
            qty: 30,
        },
    ]
}

// Net replica state after the mutate: id 1 qty updated, id 2 relocated to
// id 20 (label changed, qty preserved), id 3 deleted.
fn final_rows() -> Vec<Item> {
    vec![
        Item {
            id: 1,
            label: "a".to_owned(),
            qty: 99,
        },
        Item {
            id: 20,
            label: "moved".to_owned(),
            qty: 20,
        },
    ]
}

/// Drain every pending wal2json v2 change and parse it to row events.
fn drain(pg: &mut PgConnection) -> Vec<MessageV2> {
    let mut events = Vec::new();
    for line in &common::drain_slot(pg, SLOT) {
        events.extend(parse_wal2json_v2(line.as_bytes()).unwrap());
    }
    events
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn changeset_emit_propagates_pk_change_to_replica() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut pg = common::pg_connect(port);

    sql_query(PG_DDL).execute(&mut pg).unwrap();
    sql_query("ALTER TABLE items REPLICA IDENTITY FULL")
        .execute(&mut pg)
        .unwrap();
    common::create_slot(&mut pg, SLOT);
    let catalog = ParserDB::parse::<PostgreSqlDialect>(SUBQL_PG_DDL).unwrap();

    // Replica engine and adapter over the SQLite catalog.
    let mut sqlite = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut sqlite).unwrap();
    let sqlite_engine: SubscriptionEngine<TestEvent<SqliteBackend>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(
            ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).unwrap(),
            SQLiteDialect {},
        );
    let sqlite_adapter = SqliteAdapter::new(sqlite_engine.database());

    // ---- Seed phase: source inserts emitted as a changeset ----
    for (id, label, qty) in SEED {
        sql_query(format!(
            "INSERT INTO items (id, label, qty) VALUES ({id}, '{label}', {qty})"
        ))
        .execute(&mut pg)
        .unwrap();
    }
    let seed_events = drain(&mut pg);
    assert!(!seed_events.is_empty(), "seed drain yielded no row events");
    let seed_changeset = wal2json_changeset_builder(&catalog, &seed_events).unwrap();
    sqlite_engine
        .apply_changeset(&seed_changeset, &mut sqlite, &sqlite_adapter)
        .unwrap();
    assert_eq!(
        load_sqlite(&mut sqlite),
        seed_rows(),
        "replica after seed changeset"
    );

    // ---- Mutate phase: a non-key update, a primary-key relocation, and a
    // delete, all emitted as one changeset ----
    sql_query("UPDATE items SET qty = 99 WHERE id = 1")
        .execute(&mut pg)
        .unwrap();
    sql_query("UPDATE items SET id = 20, label = 'moved' WHERE id = 2")
        .execute(&mut pg)
        .unwrap();
    sql_query("DELETE FROM items WHERE id = 3")
        .execute(&mut pg)
        .unwrap();
    let mutate_events = drain(&mut pg);
    assert!(!mutate_events.is_empty(), "mutate drain yielded no events");
    let mutate_changeset = wal2json_changeset_builder(&catalog, &mutate_events).unwrap();
    sqlite_engine
        .apply_changeset(&mutate_changeset, &mut sqlite, &sqlite_adapter)
        .unwrap();
    assert_eq!(
        load_sqlite(&mut sqlite),
        final_rows(),
        "replica after mutate changeset, row relocated to the new primary key"
    );
}
