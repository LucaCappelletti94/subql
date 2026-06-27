//! Phase 5 — Schema-size sweep.
//!
//! Tests whether schema complexity changes the polling-vs-push verdict.
//! pgoutput emits a `Relation` message on first contact with each
//! table; wide rows pay a per-event parse cost proportional to column
//! count; mixed access patterns exercise both edges at once.
//!
//! Three workloads, four transports:
//!
//! - S5.1 — Wide rows. A single 50-column table mixing INT / TEXT /
//!   BIGINT / DOUBLE PRECISION / TIMESTAMPTZ / JSONB. 200 INSERTs at
//!   10 ms gap.
//! - S5.2 — Pagila (real DVD-rental schema, 15 base tables). The full
//!   official Pagila schema is applied to PG via `batch_execute` from
//!   `examples/fixtures/pagila-schema.sql`. The schema includes
//!   custom DOMAINs, ENUMs, `tsvector` columns, `text[]` arrays, and
//!   per-table triggers. Triggers are disabled session-wide via
//!   `SET session_replication_role = 'replica'` so the workload's
//!   events are the ONLY events the transports see. The subql
//!   catalog uses a hand-rolled simplified DDL matching Pagila's
//!   actual column counts / order / PK (sqlparser's PostgreSQL
//!   dialect does not handle Pagila's full DDL surface). 200 INSERTs
//!   rotating across the 14 non-partitioned base tables (`actor`,
//!   `category`, `language`, `country`, `city`, `address`,
//!   `customer`, `store`, `inventory`, `staff`, `film`, `film_actor`,
//!   `film_category`, `rental`). Pagila's 15th base table `payment`
//!   is `PARTITION BY RANGE (payment_date)`; under pgoutput
//!   `proto_version = 1`, partition-child events are not re-tagged
//!   to the parent's relation (that requires `proto_version >= 2`
//!   plus `publish_via_partition_root = true`), so we skip it here.
//! - S5.3 — Append-only audit + UPDATE-heavy lookup. Two tables:
//!   `audit_log` (INSERT-only) and `lookup_state` (pre-seeded, then
//!   UPDATEd). 4:1 INSERT:UPDATE mix, 250 events at 10 ms gap.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example phase5_schema_sweep --features pg-streaming
//! ```

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::format_push_string,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::duration_suboptimal_units,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]

#[path = "cdc_bench_common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::connection::SimpleConnection;
use diesel::{sql_query, PgConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig, PollingPgCdcSource,
};

use common::{
    assert_docker_available, collect_full_latencies, create_pgoutput_slot, markdown_table_header,
    pg_connect, pg_port, pg_replication_url, pg_url, pg_with_wal2json, spawn_full_event_receiver,
    EventKey, LatencyStats,
};

const DRAIN_GRACE: Duration = Duration::from_secs(15);
const POLL_INTERVALS_MS: &[u64] = &[10, 100, 1_000];
const BUFFER_CAPACITY: usize = 4_096;

/// Real Pagila schema, vendored from
/// <https://github.com/devrimgunduz/pagila/blob/master/pagila-schema.sql>.
/// Applied to PG verbatim via `SimpleConnection::batch_execute`.
const PAGILA_SCHEMA_SQL: &str = include_str!("./fixtures/pagila-schema.sql");

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

#[derive(Debug, Clone, Copy)]
enum Transport {
    Push,
    Poll { interval_ms: u64 },
}

impl Transport {
    fn slot_suffix(self) -> String {
        match self {
            Self::Push => "push".to_string(),
            Self::Poll { interval_ms } => format!("poll_{interval_ms}"),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Push => "push (PgStreamingCdcSource)".to_string(),
            Self::Poll { interval_ms } => format!("poll @ {interval_ms}ms interval"),
        }
    }
}

// ----------------------------------------------------------------------
// Schemas
// ----------------------------------------------------------------------

/// S5.1 + S5.3 schema. Applied alongside Pagila so all three workloads
/// share a single PG container.
fn build_s5_1_and_s5_3_pg_ddl() -> Vec<String> {
    let mut stmts: Vec<String> = Vec::new();

    let mut wide = String::from("CREATE TABLE wide_rows (id BIGINT PRIMARY KEY");
    for i in 1..=10 {
        wide.push_str(&format!(", i{i:02} INT"));
    }
    for i in 1..=10 {
        wide.push_str(&format!(", t{i:02} TEXT"));
    }
    for i in 1..=10 {
        wide.push_str(&format!(", n{i:02} BIGINT"));
    }
    for i in 1..=10 {
        wide.push_str(&format!(", f{i:02} DOUBLE PRECISION"));
    }
    for i in 1..=5 {
        wide.push_str(&format!(", ts{i} TIMESTAMPTZ"));
    }
    for i in 1..=5 {
        wide.push_str(&format!(", j{i} JSONB"));
    }
    wide.push(')');
    stmts.push(wide);

    stmts.push(
        "CREATE TABLE audit_log (\
            id BIGINT PRIMARY KEY,\
            event_type TEXT NOT NULL,\
            actor_id BIGINT NOT NULL,\
            payload TEXT NOT NULL,\
            ts TIMESTAMPTZ NOT NULL\
         )"
        .into(),
    );
    stmts.push(
        "CREATE TABLE lookup_state (\
            id BIGINT PRIMARY KEY,\
            name TEXT NOT NULL,\
            value TEXT NOT NULL,\
            last_updated_at TIMESTAMPTZ NOT NULL\
         )"
        .into(),
    );

    stmts.push("ALTER TABLE wide_rows REPLICA IDENTITY FULL".into());
    stmts.push("ALTER TABLE audit_log REPLICA IDENTITY FULL".into());
    stmts.push("ALTER TABLE lookup_state REPLICA IDENTITY FULL".into());

    stmts
}

/// The 14 Pagila base tables we publish and run the workload against.
///
/// Pagila has a 15th base table, `payment`, which is `PARTITION BY
/// RANGE (payment_date)` with 7 monthly partitions for 2022. Under
/// pgoutput `proto_version = 1` (what subql's parser handles),
/// INSERTs into the partitioned parent are emitted with the
/// partition CHILD's relation OID rather than the parent's. The
/// `publish_via_partition_root` publication option that fixes this
/// requires `proto_version >= 2`. Rather than churn the wire-protocol
/// parser for this benchmark, we skip `payment` and run S5.2 against
/// the 14 non-partitioned base tables — still a meaningful
/// "many-tables" relation-cache stress.
const PAGILA_BASE_TABLES: &[&str] = &[
    "actor",
    "category",
    "language",
    "country",
    "city",
    "address",
    "customer",
    "store",
    "inventory",
    "staff",
    "film",
    "film_actor",
    "film_category",
    "rental",
];

const NUM_PAGILA_TABLES: usize = 14;

/// Hand-rolled simplified DDL for the 15 Pagila base tables. Column
/// COUNT, ORDER, and PK must match PG's actual Pagila schema so the
/// subql parser correctly resolves event rows. Column TYPES are
/// simplified (custom DOMAINs / ENUMs / tsvector / arrays become
/// plain INT / TEXT etc.) because sqlparser cannot handle Pagila's
/// full type surface. subql doesn't need precise type info for pgoutput
/// parsing; it needs column identity and PK position.
const PAGILA_PARSER_DDL: &str = "\
CREATE TABLE actor (\
    actor_id INTEGER PRIMARY KEY,\
    first_name TEXT NOT NULL,\
    last_name TEXT NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE category (\
    category_id INTEGER PRIMARY KEY,\
    name TEXT NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE language (\
    language_id INTEGER PRIMARY KEY,\
    name TEXT NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE country (\
    country_id INTEGER PRIMARY KEY,\
    country TEXT NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE city (\
    city_id INTEGER PRIMARY KEY,\
    city TEXT NOT NULL,\
    country_id INTEGER NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE address (\
    address_id INTEGER PRIMARY KEY,\
    address TEXT NOT NULL,\
    address2 TEXT,\
    district TEXT NOT NULL,\
    city_id INTEGER NOT NULL,\
    postal_code TEXT,\
    phone TEXT NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE customer (\
    customer_id INTEGER PRIMARY KEY,\
    store_id INTEGER NOT NULL,\
    first_name TEXT NOT NULL,\
    last_name TEXT NOT NULL,\
    email TEXT,\
    address_id INTEGER NOT NULL,\
    activebool BOOLEAN NOT NULL,\
    create_date DATE NOT NULL,\
    last_update TIMESTAMP,\
    active INTEGER\
);\
CREATE TABLE store (\
    store_id INTEGER PRIMARY KEY,\
    manager_staff_id INTEGER NOT NULL,\
    address_id INTEGER NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE inventory (\
    inventory_id INTEGER PRIMARY KEY,\
    film_id INTEGER NOT NULL,\
    store_id INTEGER NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
CREATE TABLE staff (\
    staff_id INTEGER PRIMARY KEY,\
    first_name TEXT NOT NULL,\
    last_name TEXT NOT NULL,\
    address_id INTEGER NOT NULL,\
    email TEXT,\
    store_id INTEGER NOT NULL,\
    active BOOLEAN NOT NULL,\
    username TEXT NOT NULL,\
    password TEXT,\
    last_update TIMESTAMP NOT NULL,\
    picture TEXT\
);\
CREATE TABLE film (\
    film_id INTEGER PRIMARY KEY,\
    title TEXT NOT NULL,\
    description TEXT,\
    release_year INTEGER,\
    language_id INTEGER NOT NULL,\
    original_language_id INTEGER,\
    rental_duration INTEGER NOT NULL,\
    rental_rate DECIMAL NOT NULL,\
    length INTEGER,\
    replacement_cost DECIMAL NOT NULL,\
    rating TEXT,\
    last_update TIMESTAMP NOT NULL,\
    special_features TEXT,\
    fulltext TEXT NOT NULL\
);\
CREATE TABLE film_actor (\
    actor_id INTEGER NOT NULL,\
    film_id INTEGER NOT NULL,\
    last_update TIMESTAMP NOT NULL,\
    PRIMARY KEY (actor_id, film_id)\
);\
CREATE TABLE film_category (\
    film_id INTEGER NOT NULL,\
    category_id INTEGER NOT NULL,\
    last_update TIMESTAMP NOT NULL,\
    PRIMARY KEY (film_id, category_id)\
);\
CREATE TABLE rental (\
    rental_id INTEGER PRIMARY KEY,\
    rental_date TIMESTAMP NOT NULL,\
    inventory_id INTEGER NOT NULL,\
    customer_id INTEGER NOT NULL,\
    return_date TIMESTAMP,\
    staff_id INTEGER NOT NULL,\
    last_update TIMESTAMP NOT NULL\
);\
";

fn apply_pagila_schema(conn: &mut PgConnection) {
    // Pagila's dump has ~60 `ALTER ... OWNER TO postgres` statements.
    // Our test container's superuser is `subql_test`, not `postgres`.
    // Create a `postgres` role at the same level so those statements
    // succeed; cheaper than rewriting the dump.
    conn.batch_execute(
        "DO $$ BEGIN \
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN \
                CREATE ROLE postgres SUPERUSER; \
            END IF; \
         END $$",
    )
    .expect("create postgres role");
    // PG handles all the complexity: DOMAINs, ENUMs, FUNCTIONs,
    // TRIGGERs, ALTER TABLE constraints, partitioning. We just feed
    // the entire dump in.
    conn.batch_execute(PAGILA_SCHEMA_SQL)
        .expect("apply Pagila schema");
    // Pagila's dump resets `search_path` to empty so it can use
    // schema-qualified names throughout. Restore it before any of
    // OUR queries try to use unqualified table names.
    conn.batch_execute("SET search_path TO public")
        .expect("restore search_path");
    // REPLICA IDENTITY FULL on each base table so UPDATE events
    // carry the full old-row image (matching the rest of the
    // benchmarks).
    for table in PAGILA_BASE_TABLES {
        sql_query(format!("ALTER TABLE public.{table} REPLICA IDENTITY FULL"))
            .execute(conn)
            .unwrap_or_else(|e| panic!("REPLICA IDENTITY FULL {table}: {e}"));
    }
}

fn apply_phase5_extra_ddl(conn: &mut PgConnection) {
    for stmt in build_s5_1_and_s5_3_pg_ddl() {
        sql_query(stmt.as_str())
            .execute(conn)
            .unwrap_or_else(|e| panic!("apply DDL `{stmt}`: {e}"));
    }
}

fn create_publication(conn: &mut PgConnection, publication: &str) {
    let mut tables: Vec<String> = vec![
        "wide_rows".to_string(),
        "audit_log".to_string(),
        "lookup_state".to_string(),
    ];
    for t in PAGILA_BASE_TABLES {
        tables.push((*t).to_string());
    }
    // `publish_via_partition_root = true` is the load-bearing option:
    // Pagila's `payment` table is range-partitioned across
    // `payment_p2022_01..07`. Without this flag, pgoutput tags each
    // INSERT with the partition CHILD's relation OID (e.g.
    // `payment_p2022_03`), which our parser DB does not know about.
    // The source would then return `UnknownTable` and die mid-workload.
    // With the flag, every event for any partition child is re-tagged
    // to the parent's relation, which the parser DB does know.
    sql_query(format!(
        "CREATE PUBLICATION {publication} FOR TABLE {} WITH (publish_via_partition_root = true)",
        tables.join(", ")
    ))
    .execute(conn)
    .expect("create publication");
}

#[derive(Debug, Clone, Copy)]
struct Phase5TableIds {
    wide_rows: u32,
    audit_log: u32,
    lookup_state: u32,
    // Pagila base tables, in `PAGILA_BASE_TABLES` order.
    pagila: [u32; NUM_PAGILA_TABLES],
}

fn resolve_phase5_table_ids(catalog: &CombinedCatalog) -> Phase5TableIds {
    let pagila_db: &ParserDB = &catalog.pagila;
    let extras_db: &ParserDB = &catalog.extras;
    let mut pagila = [0u32; NUM_PAGILA_TABLES];
    for (i, name) in PAGILA_BASE_TABLES.iter().enumerate() {
        pagila[i] = subql::catalog_helpers::table_id(pagila_db, name)
            .unwrap_or_else(|| panic!("pagila table id for `{name}`"));
    }
    Phase5TableIds {
        wide_rows: subql::catalog_helpers::table_id(extras_db, "wide_rows")
            .expect("wide_rows table id"),
        audit_log: subql::catalog_helpers::table_id(extras_db, "audit_log")
            .expect("audit_log table id"),
        lookup_state: subql::catalog_helpers::table_id(extras_db, "lookup_state")
            .expect("lookup_state table id"),
        pagila,
    }
}

/// We need TWO catalogs: one for S5.1+S5.3 (`wide_rows`, `audit_log`,
/// `lookup_state`) and one for S5.2 (Pagila). Each workload picks the
/// right one when constructing its CDC source. They cannot be merged
/// because a single `ParserDB::parse` call assigns table_ids in DDL
/// order, and the COMBINED order would shift the Pagila ids; keeping
/// them separate lets each catalog match the publication-time order
/// PG assigns to its relations.
struct CombinedCatalog {
    extras: Arc<ParserDB>,
    pagila: Arc<ParserDB>,
}

fn build_catalogs() -> CombinedCatalog {
    let extras_ddl = build_s5_1_and_s5_3_parser_ddl();
    CombinedCatalog {
        extras: Arc::new(
            ParserDB::parse::<PostgreSqlDialect>(&extras_ddl).expect("extras ParserDB"),
        ),
        pagila: Arc::new(
            ParserDB::parse::<PostgreSqlDialect>(PAGILA_PARSER_DDL).expect("pagila ParserDB"),
        ),
    }
}

fn build_s5_1_and_s5_3_parser_ddl() -> String {
    let mut ddl = String::new();
    ddl.push_str("CREATE TABLE wide_rows (id BIGINT PRIMARY KEY");
    for i in 1..=10 {
        ddl.push_str(&format!(", i{i:02} INT"));
    }
    for i in 1..=10 {
        ddl.push_str(&format!(", t{i:02} TEXT"));
    }
    for i in 1..=10 {
        ddl.push_str(&format!(", n{i:02} BIGINT"));
    }
    for i in 1..=10 {
        ddl.push_str(&format!(", f{i:02} DOUBLE PRECISION"));
    }
    for i in 1..=5 {
        ddl.push_str(&format!(", ts{i} TIMESTAMP"));
    }
    for i in 1..=5 {
        ddl.push_str(&format!(", j{i} JSONB"));
    }
    ddl.push_str(");");
    ddl.push_str(
        "CREATE TABLE audit_log (id BIGINT PRIMARY KEY, event_type TEXT, actor_id BIGINT, payload TEXT, ts TIMESTAMP);",
    );
    ddl.push_str(
        "CREATE TABLE lookup_state (id BIGINT PRIMARY KEY, name TEXT, value TEXT, last_updated_at TIMESTAMP);",
    );
    ddl
}

// ----------------------------------------------------------------------
// S5.1 — Wide rows
// ----------------------------------------------------------------------

async fn drive_s5_1(
    conn: &mut PgConnection,
    id_base: i64,
    ids: Phase5TableIds,
) -> HashMap<EventKey, Instant> {
    const COUNT: i64 = 200;
    const GAP: Duration = Duration::from_millis(10);
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    for c in 0..COUNT {
        if c > 0 {
            tokio::time::sleep(GAP).await;
        }
        let id = id_base + c;
        let mut sql = String::from("INSERT INTO wide_rows (id");
        for i in 1..=10 {
            sql.push_str(&format!(", i{i:02}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", t{i:02}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", n{i:02}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", f{i:02}"));
        }
        for i in 1..=5 {
            sql.push_str(&format!(", ts{i}"));
        }
        for i in 1..=5 {
            sql.push_str(&format!(", j{i}"));
        }
        sql.push_str(&format!(") VALUES ({id}"));
        for i in 1..=10 {
            let v = id + i64::from(i);
            sql.push_str(&format!(", {v}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", 't{i:02}_row_{id}'"));
        }
        for i in 1..=10 {
            let v = id * 1000 + i64::from(i);
            sql.push_str(&format!(", {v}"));
        }
        for i in 1..=10 {
            sql.push_str(&format!(", {id}.{i:02}"));
        }
        for _ in 1..=5 {
            sql.push_str(", NOW()");
        }
        for i in 1..=5 {
            sql.push_str(&format!(", '{{\"k\":{i},\"id\":{id}}}'::jsonb"));
        }
        sql.push(')');
        let commit_at = Instant::now();
        sql_query(sql.as_str())
            .execute(conn)
            .unwrap_or_else(|e| panic!("wide insert id={id}: {e}"));
        commits.insert((ids.wide_rows, id, EventKind::Insert), commit_at);
    }
    commits
}

// ----------------------------------------------------------------------
// S5.2 — Pagila
// ----------------------------------------------------------------------

/// S5.2: 200 INSERTs rotating across the 15 Pagila base tables at 10
/// ms gap. Each table receives ~13 events. The FIRST event per table
/// triggers a pgoutput `Relation` message (cold cache). Triggers are
/// disabled session-wide so workload events are the only events the
/// transports see.
///
/// FK columns are filled with synthetic values that do NOT need to
/// satisfy any real Pagila row — `SET session_replication_role =
/// 'replica'` disables FK enforcement alongside triggers. The
/// workload's purpose is CDC stress, not data realism.
async fn drive_s5_2(
    conn: &mut PgConnection,
    id_base: i64,
    ids: Phase5TableIds,
) -> HashMap<EventKey, Instant> {
    const COUNT: i64 = 200;
    const GAP: Duration = Duration::from_millis(10);
    conn.batch_execute("SET session_replication_role = 'replica'")
        .expect("disable triggers and FK enforcement");
    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    for c in 0..COUNT {
        if c > 0 {
            tokio::time::sleep(GAP).await;
        }
        let table_index = (c as usize) % NUM_PAGILA_TABLES;
        let id = (id_base + c) as i32; // Pagila uses integer PKs
        let commit_at = Instant::now();
        let sql = pagila_insert_sql(table_index, id);
        sql_query(sql.as_str())
            .execute(conn)
            .unwrap_or_else(|e| panic!("pagila insert table={table_index} id={id}: {e}"));
        commits.insert(
            (ids.pagila[table_index], i64::from(id), EventKind::Insert),
            commit_at,
        );
    }
    commits
}

#[allow(clippy::too_many_lines)]
fn pagila_insert_sql(table_index: usize, id: i32) -> String {
    match table_index {
        0 => format!(
            // actor
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
             VALUES ({id}, 'F{id}', 'L{id}', NOW())"
        ),
        1 => format!(
            // category
            "INSERT INTO category (category_id, name, last_update) \
             VALUES ({id}, 'cat_{id}', NOW())"
        ),
        2 => format!(
            // language
            "INSERT INTO language (language_id, name, last_update) \
             VALUES ({id}, 'lang_{id}', NOW())"
        ),
        3 => format!(
            // country
            "INSERT INTO country (country_id, country, last_update) \
             VALUES ({id}, 'country_{id}', NOW())"
        ),
        4 => format!(
            // city
            "INSERT INTO city (city_id, city, country_id, last_update) \
             VALUES ({id}, 'city_{id}', {id}, NOW())"
        ),
        5 => format!(
            // address
            "INSERT INTO address (address_id, address, address2, district, city_id, \
                                  postal_code, phone, last_update) \
             VALUES ({id}, '{id} Main St', NULL, 'district_{id}', {id}, '00000', '555-0000', NOW())"
        ),
        6 => format!(
            // customer
            "INSERT INTO customer (customer_id, store_id, first_name, last_name, email, \
                                   address_id, activebool, create_date, last_update, active) \
             VALUES ({id}, 1, 'C{id}', 'CL{id}', 'c{id}@example.invalid', {id}, TRUE, \
                     CURRENT_DATE, NOW(), 1)"
        ),
        7 => format!(
            // store
            "INSERT INTO store (store_id, manager_staff_id, address_id, last_update) \
             VALUES ({id}, {id}, {id}, NOW())"
        ),
        8 => format!(
            // inventory
            "INSERT INTO inventory (inventory_id, film_id, store_id, last_update) \
             VALUES ({id}, {id}, 1, NOW())"
        ),
        9 => format!(
            // staff
            "INSERT INTO staff (staff_id, first_name, last_name, address_id, email, store_id, \
                                active, username, password, last_update, picture) \
             VALUES ({id}, 'S{id}', 'SL{id}', {id}, 's{id}@example.invalid', 1, TRUE, \
                     'u{id}', 'p{id}', NOW(), NULL)"
        ),
        10 => format!(
            // film
            "INSERT INTO film (film_id, title, description, release_year, language_id, \
                               original_language_id, rental_duration, rental_rate, length, \
                               replacement_cost, rating, last_update, special_features, fulltext) \
             VALUES ({id}, 'Film {id}', 'Desc {id}', 2024, 1, NULL, 3, 4.99, 90, 19.99, 'PG', \
                     NOW(), ARRAY['trailer']::text[], to_tsvector('english', 'film {id}'))"
        ),
        11 => format!(
            // film_actor
            "INSERT INTO film_actor (actor_id, film_id, last_update) \
             VALUES ({id}, {id}, NOW())"
        ),
        12 => format!(
            // film_category
            "INSERT INTO film_category (film_id, category_id, last_update) \
             VALUES ({id}, {id}, NOW())"
        ),
        13 => format!(
            // rental
            "INSERT INTO rental (rental_id, rental_date, inventory_id, customer_id, return_date, \
                                 staff_id, last_update) \
             VALUES ({id}, NOW(), {id}, {id}, NULL, 1, NOW())"
        ),
        _ => unreachable!("table_index 0..14 only"),
    }
}

// ----------------------------------------------------------------------
// S5.3 — audit + lookup
// ----------------------------------------------------------------------

async fn drive_s5_3(
    conn: &mut PgConnection,
    id_base: i64,
    ids: Phase5TableIds,
) -> HashMap<EventKey, Instant> {
    const SEED: i64 = 50;
    const COUNT: i64 = 250;
    const GAP: Duration = Duration::from_millis(10);
    for i in 0..SEED {
        let id = id_base + i;
        sql_query(format!(
            "INSERT INTO lookup_state (id, name, value, last_updated_at) \
             VALUES ({id}, 'name_{id}', 'v0', NOW())"
        ))
        .execute(conn)
        .unwrap_or_else(|e| panic!("seed lookup_state id={id}: {e}"));
    }

    let mut commits: HashMap<EventKey, Instant> = HashMap::with_capacity(COUNT as usize);
    let mut audit_offset: i64 = 0;
    let mut update_offset: i64 = 0;
    for c in 0..COUNT {
        if c > 0 {
            tokio::time::sleep(GAP).await;
        }
        let do_audit = (c % 5) != 0;
        if do_audit {
            let id = id_base + 100_000 + audit_offset;
            audit_offset += 1;
            let commit_at = Instant::now();
            sql_query(format!(
                "INSERT INTO audit_log (id, event_type, actor_id, payload, ts) \
                 VALUES ({id}, 'click', 1, 'p_{id}', NOW())"
            ))
            .execute(conn)
            .unwrap_or_else(|e| panic!("audit insert id={id}: {e}"));
            commits.insert((ids.audit_log, id, EventKind::Insert), commit_at);
        } else {
            let id = id_base + (update_offset % SEED);
            update_offset += 1;
            let commit_at = Instant::now();
            sql_query(format!(
                "UPDATE lookup_state SET value = 'v_{c}', last_updated_at = NOW() WHERE id = {id}"
            ))
            .execute(conn)
            .unwrap_or_else(|e| panic!("lookup update id={id}: {e}"));
            commits
                .entry((ids.lookup_state, id, EventKind::Update))
                .or_insert(commit_at);
        }
    }
    commits
}

#[derive(Debug, Clone, Copy)]
struct WorkloadDesc {
    name: &'static str,
    description: &'static str,
    id_base: i64,
}

const S5_1: WorkloadDesc = WorkloadDesc {
    name: "S5.1",
    description: "wide rows (50-column table, 200 INSERTs at 10ms gap)",
    id_base: 900_000,
};
const S5_2: WorkloadDesc = WorkloadDesc {
    name: "S5.2",
    description:
        "Pagila (200 INSERTs rotating across 14 non-partitioned base tables, triggers/FKs off)",
    id_base: 10_000,
};
const S5_3: WorkloadDesc = WorkloadDesc {
    name: "S5.3",
    description: "audit log + lookup (4:1 INSERT:UPDATE mix, 250 events at 10ms gap)",
    id_base: 1_000_000,
};

fn slot_name(workload: &WorkloadDesc, transport: Transport) -> String {
    format!(
        "phase5_{}_{}",
        workload.name.replace('.', "_").to_lowercase(),
        transport.slot_suffix()
    )
}

fn catalog_for(w: &WorkloadDesc, c: &CombinedCatalog) -> Arc<ParserDB> {
    match w.name {
        "S5.2" => Arc::clone(&c.pagila),
        _ => Arc::clone(&c.extras),
    }
}

async fn drive_for_workload(
    w: &WorkloadDesc,
    ids: Phase5TableIds,
    dml: &mut PgConnection,
) -> HashMap<EventKey, Instant> {
    match w.name {
        "S5.1" => drive_s5_1(dml, w.id_base, ids).await,
        "S5.2" => drive_s5_2(dml, w.id_base, ids).await,
        "S5.3" => drive_s5_3(dml, w.id_base, ids).await,
        other => panic!("unknown workload {other}"),
    }
}

async fn measure_push(
    pg_replication_url: String,
    slot: String,
    publication: String,
    catalog: Arc<ParserDB>,
    label: String,
    workload: WorkloadDesc,
    ids: Phase5TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PgStreamingConfig::new(pg_replication_url, slot, publication)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PgStreamingCdcSource::connect(config, catalog)
        .await
        .expect("connect push source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let commit_times = drive_for_workload(&workload, ids, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

async fn measure_poll(
    pg_url: String,
    slot: String,
    publication: String,
    catalog: Arc<ParserDB>,
    interval: Duration,
    label: String,
    workload: WorkloadDesc,
    ids: Phase5TableIds,
    dml: &mut PgConnection,
) -> LatencyStats {
    let config = PollingPgCdcConfig::new(pg_url, slot, publication)
        .poll_interval(interval)
        .buffer_capacity(BUFFER_CAPACITY);
    let source = PollingPgCdcSource::connect(config, catalog)
        .await
        .expect("connect polling source");
    let (rx, task) = spawn_full_event_receiver(source);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let commit_times = drive_for_workload(&workload, ids, dml).await;
    let deadline = Instant::now() + DRAIN_GRACE + interval * 4;
    let samples = collect_full_latencies(&commit_times, rx, deadline).await;
    task.abort();
    LatencyStats::new(label, samples)
}

fn main() {
    assert_docker_available();
    println!("Phase 5 — Schema-size sweep");
    println!("Wide rows, real Pagila (15-table DVD-rental), and append-only-plus-lookup.");
    println!();

    let container = pg_with_wal2json();
    let port = pg_port(&container);
    let mut setup = pg_connect(port);
    let mut dml = pg_connect(port);

    // Apply Pagila FIRST so its sequences and types exist when later
    // ALTER TABLE statements run.
    apply_pagila_schema(&mut setup);
    apply_phase5_extra_ddl(&mut setup);

    let publication = "phase5_pub";
    create_publication(&mut setup, publication);

    let catalogs = build_catalogs();
    let table_ids = resolve_phase5_table_ids(&catalogs);
    let pg_repl_url = pg_replication_url(port);
    let pg_sql_url = pg_url(port);

    let workloads = [S5_1, S5_2, S5_3];
    let transports = {
        let mut v = Vec::with_capacity(1 + POLL_INTERVALS_MS.len());
        v.push(Transport::Push);
        for &ms in POLL_INTERVALS_MS {
            v.push(Transport::Poll { interval_ms: ms });
        }
        v
    };

    let rt = current_thread_rt();
    let mut all_stats: Vec<(WorkloadDesc, Vec<LatencyStats>)> = Vec::new();

    for w in workloads {
        println!("=== {} — {} ===", w.name, w.description);
        let mut row: Vec<LatencyStats> = Vec::with_capacity(transports.len());
        for (ti, &t) in transports.iter().enumerate() {
            let slot = slot_name(&w, t);
            create_pgoutput_slot(&mut setup, &slot);
            let label = t.label();
            let workload_for_run = WorkloadDesc {
                id_base: w.id_base + (ti as i64) * 10_000,
                ..w
            };
            let catalog = catalog_for(&w, &catalogs);
            let stats = rt.block_on(async {
                match t {
                    Transport::Push => {
                        measure_push(
                            pg_repl_url.clone(),
                            slot,
                            publication.to_string(),
                            catalog,
                            label,
                            workload_for_run,
                            table_ids,
                            &mut dml,
                        )
                        .await
                    }
                    Transport::Poll { interval_ms } => {
                        measure_poll(
                            pg_sql_url.clone(),
                            slot,
                            publication.to_string(),
                            catalog,
                            Duration::from_millis(interval_ms),
                            label,
                            workload_for_run,
                            table_ids,
                            &mut dml,
                        )
                        .await
                    }
                }
            });
            println!("{}", stats.text_summary());
            row.push(stats);
        }
        println!();
        all_stats.push((w, row));
    }

    println!("---");
    println!();
    for (w, row) in &all_stats {
        println!("### {} — {}", w.name, w.description);
        println!();
        println!("{}", markdown_table_header());
        for stats in row {
            println!("{}", stats.markdown_row());
        }
        println!();
    }

    for (w, row) in &all_stats {
        let push_median = row[0].median();
        let poll_100_median = row[2].median();
        let poll_1000_median = row[3].median();
        assert!(
            push_median < poll_100_median,
            "{} push median {push_median:?} must beat poll@100ms median {poll_100_median:?}",
            w.name
        );
        assert!(
            push_median < poll_1000_median,
            "{} push median {push_median:?} must beat poll@1000ms median {poll_1000_median:?}",
            w.name
        );
    }
    println!("Phase 5 architectural-claim check: PASS");
    println!(
        "  schema complexity (wide rows, real Pagila, mixed access) does not invert the verdict."
    );
}
