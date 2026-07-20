//! Full CDC round trip for a composite primary key under default replica
//! identity (Phase 5 of the loop-hardening plan), on the wal2json vehicle.
//!
//! Two structural cases the other round trips do not cover:
//!
//! * The `regions` table has a composite primary key `(country, code)`.
//!   The seed rows are chosen so one shares `country` with another and one
//!   shares `code` with another, so a WHERE that matched only one key
//!   column would touch the wrong row. Update and delete must match on
//!   both columns.
//! * The table keeps the default replica identity (no `REPLICA IDENTITY
//!   FULL`), so the old-row image an update or delete carries is key-only.
//!   The patchset update digest builds its WHERE from the primary key in
//!   the new image and never reads the old image, and the delete needs
//!   only the key, so the loop does not require full row images.
//!
//! Every column rides `sqlite_diff_rs::DefaultBinder` (text, int, bigint),
//! so the plain [`PgAdapter`] applies the inbound patchset. No SQL casts.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test round_trip_pg_composite_default_ri_e2e \
//!     --features "apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" \
//!     -- --ignored --nocapture
//! ```

#![cfg(all(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
#![allow(clippy::unwrap_used)]

mod common;

use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl, SqliteConnection};
use diesel_sqlite_session::SqliteSessionExt;
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::PatchSet;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::SQLite as SqliteBackend;
use subql::emit::{wal2json_patchset_builder, WireTable};
use subql::patchset::{PgAdapter, SqliteAdapter};
use subql::testing::TestEvent;
use subql::{parse_wal2json_v2, ChangeEvent, DefaultIds, MessageV2, SubscriptionEngine};

const SLOT: &str = "rt_composite_slot";

// No `REPLICA IDENTITY FULL`: the table keeps its default replica
// identity, which is the composite primary key.
const PG_DDL: &str = "CREATE TABLE regions (country TEXT, code INT, name TEXT, population BIGINT, PRIMARY KEY (country, code))";
const SUBQL_PG_DDL: &str = "CREATE TABLE regions (country TEXT, code INT, name TEXT, population BIGINT, PRIMARY KEY (country, code));";
const SQLITE_DDL: &str = "CREATE TABLE regions (country TEXT, code INTEGER, name TEXT, population INTEGER, PRIMARY KEY (country, code));";

#[derive(QueryableByName, Debug, PartialEq)]
struct PgRegion {
    #[diesel(sql_type = diesel::sql_types::Text)]
    country: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    code: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    population: i64,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct SqliteRegion {
    #[diesel(sql_type = diesel::sql_types::Text)]
    country: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    code: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    population: i64,
}

fn load_pg(conn: &mut PgConnection) -> Vec<PgRegion> {
    sql_query("SELECT country, code, name, population FROM regions ORDER BY country, code")
        .load(conn)
        .unwrap()
}

fn load_sqlite(conn: &mut SqliteConnection) -> Vec<SqliteRegion> {
    sql_query("SELECT country, code, name, population FROM regions ORDER BY country, code")
        .load(conn)
        .unwrap()
}

// Seed rows: (US, 1) and (US, 2) share a country, (US, 1) and (CA, 1)
// share a code. Matching on one key column alone would be ambiguous.
fn seed_pg_rows() -> Vec<PgRegion> {
    vec![
        PgRegion {
            country: "CA".to_owned(),
            code: 1,
            name: "Ontario".to_owned(),
            population: 15_000_000,
        },
        PgRegion {
            country: "US".to_owned(),
            code: 1,
            name: "California".to_owned(),
            population: 39_000_000,
        },
        PgRegion {
            country: "US".to_owned(),
            code: 2,
            name: "Texas".to_owned(),
            population: 30_000_000,
        },
    ]
}

// Net state after the mutate: (US, 1) updated, (CA, 1) deleted, (US, 2)
// untouched. That (US, 1) survives while (CA, 1) with the same code is
// deleted and (US, 2) with the same country is left alone proves the
// composite WHERE matched on both key columns.
fn final_pg_rows() -> Vec<PgRegion> {
    vec![
        PgRegion {
            country: "US".to_owned(),
            code: 1,
            name: "California (updated)".to_owned(),
            population: 40_000_000,
        },
        PgRegion {
            country: "US".to_owned(),
            code: 2,
            name: "Texas".to_owned(),
            population: 30_000_000,
        },
    ]
}

fn sqlite_view(row: &PgRegion) -> SqliteRegion {
    SqliteRegion {
        country: row.country.clone(),
        code: i64::from(row.code),
        name: row.name.clone(),
        population: row.population,
    }
}

fn seed_sqlite_rows() -> Vec<SqliteRegion> {
    seed_pg_rows().iter().map(sqlite_view).collect()
}

fn final_sqlite_rows() -> Vec<SqliteRegion> {
    final_pg_rows().iter().map(sqlite_view).collect()
}

fn subql_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(SUBQL_PG_DDL).unwrap()
}

fn create_schema(pg: &mut PgConnection) {
    sql_query(PG_DDL).execute(pg).unwrap();
}

fn seed_dml(pg: &mut PgConnection) {
    for row in seed_pg_rows() {
        sql_query(format!(
            "INSERT INTO regions (country, code, name, population) VALUES ('{}', {}, '{}', {})",
            row.country, row.code, row.name, row.population
        ))
        .execute(pg)
        .unwrap();
    }
}

fn mutate_dml(pg: &mut PgConnection) {
    sql_query(
        "UPDATE regions SET name = 'California (updated)', population = 40000000 WHERE country = 'US' AND code = 1",
    )
    .execute(pg)
    .unwrap();
    sql_query("DELETE FROM regions WHERE country = 'CA' AND code = 1")
        .execute(pg)
        .unwrap();
}

/// Drain every pending wal2json v2 change and parse it to row events.
fn drain(pg: &mut PgConnection) -> Vec<MessageV2> {
    let mut events = Vec::new();
    for line in &common::drain_slot(pg, SLOT) {
        events.extend(parse_wal2json_v2(line.as_bytes()).unwrap());
    }
    events
}

fn finish_loop(
    pg: &mut PgConnection,
    seed: &PatchSet<WireTable, String, Vec<u8>>,
    mutate: &PatchSet<WireTable, String, Vec<u8>>,
) {
    let source_net = load_pg(pg);
    assert_eq!(source_net, final_pg_rows(), "source net state");

    // ---- Outgoing: seed then mutate the SQLite replica ----
    let mut sqlite = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut sqlite).unwrap();
    let sqlite_catalog = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).unwrap();
    let sqlite_engine: SubscriptionEngine<TestEvent<SqliteBackend>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(sqlite_catalog, SQLiteDialect {});
    let sqlite_adapter = SqliteAdapter::new(sqlite_engine.database());

    sqlite_engine
        .apply_patchset(seed, &mut sqlite, &sqlite_adapter)
        .unwrap();
    assert_eq!(
        load_sqlite(&mut sqlite),
        seed_sqlite_rows(),
        "SQLite replica after seed"
    );

    let mut session = sqlite.create_session().unwrap();
    session.attach_all().unwrap();

    sqlite_engine
        .apply_patchset(mutate, &mut sqlite, &sqlite_adapter)
        .unwrap();
    assert_eq!(
        load_sqlite(&mut sqlite),
        final_sqlite_rows(),
        "SQLite replica after mutate (composite update and delete applied)"
    );

    // ---- Incoming: capture the session patchset and re-apply to PG ----
    let session_patchset = session.patchset().unwrap();
    assert!(!session_patchset.is_empty(), "session recorded no changes");

    sql_query("TRUNCATE regions").execute(pg).unwrap();

    let pg_engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(subql_catalog(), PostgreSqlDialect {});
    let pg_adapter = PgAdapter::new(pg_engine.database());

    pg_engine.apply_patchset(seed, pg, &pg_adapter).unwrap();
    assert_eq!(load_pg(pg), seed_pg_rows(), "Postgres after re-seed");

    pg_engine
        .apply_diffset_bytes(&session_patchset, pg, &pg_adapter)
        .unwrap();
    assert_eq!(
        load_pg(pg),
        source_net,
        "round-tripped Postgres rows equal the source net state, composite WHERE matched both key columns"
    );
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn round_trip_composite_pk_under_default_replica_identity() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut pg = common::pg_connect(port);

    create_schema(&mut pg);
    common::create_slot(&mut pg, SLOT);
    let catalog = subql_catalog();

    // Seed phase: inserts.
    seed_dml(&mut pg);
    let seed_events = drain(&mut pg);
    assert!(!seed_events.is_empty(), "seed drain yielded no row events");
    let seed_builder = wal2json_patchset_builder(&catalog, &seed_events).unwrap();

    // Mutate phase: composite-key update and delete under default RI.
    mutate_dml(&mut pg);
    let mutate_events = drain(&mut pg);
    assert!(!mutate_events.is_empty(), "mutate drain yielded no events");
    let mutate_builder = wal2json_patchset_builder(&catalog, &mutate_events).unwrap();

    finish_loop(&mut pg, &seed_builder, &mutate_builder);
}
