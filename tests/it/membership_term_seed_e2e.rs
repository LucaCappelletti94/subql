//! The seed a membership term needs, read from a real Postgres with the query
//! subql itself handed over.
//!
//! Two things only a database can settle. First, that
//! [`TermDescription::seed_sql`] is runnable at all: it is emitted from the
//! parsed filter, so nothing inside subql proves Postgres accepts it. Second,
//! that reading the membership table is what makes a seed complete. A seed
//! derived from the snapshot rows omits every value whose rows do not exist yet,
//! and no later membership event repairs that, because the membership did not
//! change, so rows inserted under such a value are silently never delivered.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test it membership_term_seed_e2e:: --features membership-term -- --ignored
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::prelude::*;
use diesel::sql_types::Integer;
use diesel::{sql_query, PgConnection, QueryableByName};
use rls2fga::translator::{Translator, TranslatorBuilder};
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::term::{MembershipTermDescription, TermDescription};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

/// One string for the catalog and for the database, so a name the engine
/// resolves is a name Postgres has.
const SCHEMA: &str = "
CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT);
";

/// The filter under test: documents belonging to a project the caller is a
/// member of.
const TERM: &str = "SELECT * FROM docs WHERE project_id IN \
     (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";

const CALLER: &str = "alice";

diesel::table! {
    projects (id) {
        id -> Integer,
        name -> Text,
    }
}

diesel::table! {
    project_members (project_id, user_id) {
        project_id -> Integer,
        user_id -> Text,
    }
}

diesel::table! {
    docs (id) {
        id -> Integer,
        project_id -> Integer,
        title -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(docs, project_members);

/// One value of the seed read. Bound by column name, which the description
/// states as `member_key`, and typed as it states in `key_kind`.
#[derive(QueryableByName)]
struct SeedValue {
    #[diesel(sql_type = Integer)]
    project_id: i32,
}

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn translator() -> Translator {
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
}

fn engine() -> (Engine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(SCHEMA).unwrap();
    let docs = catalog_helpers::table_id(&db, "docs").unwrap();
    (
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator()),
        docs,
    )
}

/// DDL, which the query DSL does not express.
fn create_schema(conn: &mut PgConnection) {
    for statement in SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
        sql_query(statement).execute(conn).expect(statement);
    }
}

/// Two projects the caller belongs to, and a document in the first only. The
/// second is the childless parent: nothing in the caller's snapshot names it.
fn seed_data(conn: &mut PgConnection) {
    diesel::insert_into(projects::table)
        .values([
            (projects::id.eq(1), projects::name.eq("first")),
            (projects::id.eq(2), projects::name.eq("second")),
        ])
        .execute(conn)
        .unwrap();
    diesel::insert_into(project_members::table)
        .values([
            (
                project_members::project_id.eq(1),
                project_members::user_id.eq(CALLER),
            ),
            (
                project_members::project_id.eq(2),
                project_members::user_id.eq(CALLER),
            ),
        ])
        .execute(conn)
        .unwrap();
    diesel::insert_into(docs::table)
        .values((
            docs::id.eq(10),
            docs::project_id.eq(1),
            docs::title.eq("spec"),
        ))
        .execute(conn)
        .unwrap();
}

/// Bind the caller for the session, which is how `current_setting` resolves.
/// `SET` is a session command the query DSL does not express.
fn become_caller(conn: &mut PgConnection) {
    sql_query(format!("SET app.user_id = '{CALLER}'"))
        .execute(conn)
        .unwrap();
}

/// Run the seed read subql handed over. Raw because the statement is text the
/// engine produced at run time, so no typed schema describes it.
fn run_seed(conn: &mut PgConnection, description: &MembershipTermDescription) -> Vec<i64> {
    let mut values: Vec<i64> = sql_query(&description.seed_sql)
        .load::<SeedValue>(conn)
        .unwrap_or_else(|error| panic!("seed read failed: {error}\n{}", description.seed_sql))
        .into_iter()
        .map(|row| i64::from(row.project_id))
        .collect();
    values.sort_unstable();
    values
}

/// What the wrong advice yields: the compared column of the rows the caller's
/// snapshot returns. Typed, because this one is the client's own query.
fn snapshot_seed(conn: &mut PgConnection) -> Vec<i64> {
    let mut values: Vec<i64> = docs::table
        .filter(
            docs::project_id.eq_any(
                project_members::table
                    .select(project_members::project_id)
                    .filter(project_members::user_id.eq(CALLER)),
            ),
        )
        .select(docs::project_id)
        .distinct()
        .load::<i32>(conn)
        .unwrap()
        .into_iter()
        .map(i64::from)
        .collect();
    values.sort_unstable();
    values
}

/// Register `TERM` for `CALLER` with `values`, then report whether a document
/// inserted under `project` reaches it.
fn delivers(values: &[i64], project: i64) -> bool {
    let (mut engine, docs_id) = engine();
    let mut request = SubscriptionRequest::new(1u64, TERM).subscriber(Value::String(CALLER.into()));
    let TermDescription::Membership(description) =
        engine.describe_terms(&request).unwrap().remove(0)
    else {
        panic!("a membership subquery is described with its seed read");
    };
    request = request.term_values(
        description
            .pairs
            .iter()
            .map(|pair| pair.column.clone())
            .collect(),
        values
            .iter()
            .map(|&value| vec![Value::Int(value)])
            .collect(),
    );
    engine.register(request).unwrap();

    !engine
        .consumers(&TestEvent::insert(
            docs_id,
            vec![
                Value::Int(99),
                Value::Int(project),
                Value::String("new".into()),
            ],
        ))
        .unwrap()
        .inserted()
        .is_empty()
}

#[test]
#[ignore = "requires Docker"]
fn the_described_seed_read_runs_and_admits_a_parent_with_no_rows_yet() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let mut pg = common::pg_connect(common::pg_port(&container));

    create_schema(&mut pg);
    seed_data(&mut pg);
    become_caller(&mut pg);

    let (engine, _) = engine();
    let described = engine
        .describe_terms(&SubscriptionRequest::new(1u64, TERM))
        .unwrap();
    let [TermDescription::Membership(description)] = described.as_slice() else {
        panic!("one membership subquery, got {described:?}");
    };
    assert_eq!(
        description.pairs[0].member_key, "project_id",
        "the bound column name"
    );
    assert_eq!(
        description.pairs[0].kind,
        BuiltinKind::Int,
        "the decode kind"
    );

    // Runnable, and it reads the membership table rather than the snapshot.
    let from_membership = run_seed(&mut pg, description);
    assert_eq!(
        from_membership,
        [1, 2],
        "the caller belongs to both projects, whether or not either has documents"
    );

    let from_snapshot = snapshot_seed(&mut pg);
    assert_eq!(
        from_snapshot,
        [1],
        "project 2 has no documents, so the snapshot never names it"
    );

    // The two seeds differ, and the difference is a subscriber that stops
    // receiving rows under a project it is a member of.
    assert!(
        delivers(&from_membership, 2),
        "the described seed must deliver a document inserted under project 2"
    );
    assert!(
        !delivers(&from_snapshot, 2),
        "the snapshot-derived seed must be shown to lose it, or this proves nothing"
    );
    assert!(
        delivers(&from_snapshot, 1),
        "and it loses only the value it omitted, not the filter altogether"
    );
}
