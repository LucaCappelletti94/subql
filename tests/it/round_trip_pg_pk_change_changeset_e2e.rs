//! Inbound round trip for a primary-key-changing UPDATE carried as a
//! SQLite session changeset (the client-to-server half of the loop).
//!
//! A patchset cannot represent a primary-key change, so this exercises the
//! changeset path instead. A SQLite replica holds the server's rows, the
//! client relocates a row's primary key with `UPDATE ... SET id = new
//! WHERE id = old` (and changes a non-key column too), the SQLite session
//! extension records the mutation as a changeset, and subql applies it to
//! Postgres through [`SubscriptionEngine::apply_diffset_bytes`] with a
//! plain [`PgAdapter`]. The server row must end up relocated to the new
//! key, which proves the real `session.changeset()` bytes carry the old
//! key and the diesel apply renders `SET id = new WHERE id = old` with no
//! SQL cast.
//!
//! This is the inbound half only, so it seeds both sides directly rather
//! than draining CDC. Every column rides `sqlite_diff_rs::DefaultBinder`.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test it round_trip_pg_pk_change_changeset_e2e:: \
//!     --features "apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" \
//!     -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl, SqliteConnection};
use diesel_sqlite_session::SqliteSessionExt;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::PgAdapter;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};

const PG_DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT)";
const SUBQL_PG_DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, qty INT);";
const SQLITE_DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, qty INTEGER);";

#[derive(QueryableByName, Debug, PartialEq)]
struct Item {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    label: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    qty: i32,
}

fn load_pg(conn: &mut PgConnection) -> Vec<Item> {
    sql_query("SELECT id, label, qty FROM items ORDER BY id")
        .load(conn)
        .unwrap()
}

// Seed rows shared by both sides.
const SEED: &[(i32, &str, i32)] = &[(1, "a", 10), (2, "b", 200), (3, "c", 30)];

// Net state after the client relocates id 2 to id 20 and relabels it. The
// qty is untouched, so a correct changeset apply preserves it while moving
// the key.
fn final_rows() -> Vec<Item> {
    vec![
        Item {
            id: 1,
            label: "a".to_owned(),
            qty: 10,
        },
        Item {
            id: 3,
            label: "c".to_owned(),
            qty: 30,
        },
        Item {
            id: 20,
            label: "moved".to_owned(),
            qty: 200,
        },
    ]
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn round_trip_pk_change_via_changeset() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut pg = common::pg_connect(port);

    // Server state: create and seed the Postgres table
    sql_query(PG_DDL).execute(&mut pg).unwrap();
    for (id, label, qty) in SEED {
        sql_query(format!(
            "INSERT INTO items (id, label, qty) VALUES ({id}, '{label}', {qty})"
        ))
        .execute(&mut pg)
        .unwrap();
    }

    // Client replica: same rows in SQLite
    let mut sqlite = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut sqlite).unwrap();
    for (id, label, qty) in SEED {
        sql_query(format!(
            "INSERT INTO items (id, label, qty) VALUES ({id}, '{label}', {qty})"
        ))
        .execute(&mut sqlite)
        .unwrap();
    }

    // Client mutation: relocate a primary key, recorded in a session
    let mut session = sqlite.create_session().unwrap();
    session.attach_all().unwrap();
    sql_query("UPDATE items SET id = 20, label = 'moved' WHERE id = 2")
        .execute(&mut sqlite)
        .unwrap();
    let changeset = session.changeset().unwrap();
    assert!(!changeset.is_empty(), "session recorded no changes");

    // Apply the client changeset to the server
    let pg_engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> = SubscriptionEngine::new(
        ParserDB::parse::<PostgreSqlDialect>(SUBQL_PG_DDL).unwrap(),
        PostgreSqlDialect {},
    );
    let pg_adapter = PgAdapter::new(pg_engine.database()).expect("the catalog indexes");
    pg_engine
        .apply_diffset_bytes(&changeset, &mut pg, &pg_adapter)
        .unwrap();

    assert_eq!(
        load_pg(&mut pg),
        final_rows(),
        "server row relocated to the new primary key with its non-key value preserved"
    );
}
