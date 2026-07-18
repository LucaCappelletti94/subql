//! Shared machinery for the CDC round-trip E2E tests, one per source
//! vehicle (wal2json and pgoutput). Both drive the same two-phase DML on
//! a Postgres source, emit patchsets, apply them to a SQLite replica
//! through [`SqliteAdapter`], capture the SQLite session patchset, and
//! re-apply it to a fresh Postgres through a dispatch-aware [`PgAdapter`].
//!
//! # Two phases, so update and delete are real patchset ops
//!
//! A fresh replica coalesces insert + update + delete into net-state
//! inserts, which would never exercise the adapters' update or delete
//! paths. So the flow is split: a seed phase inserts rows, then a mutate
//! phase updates one row and deletes another on top of the already
//! present rows. The mutate patchset carries a genuine `UPDATE` and
//! `DELETE`, applied through the adapters in both directions.
//!
//! # Primary key is a UUID
//!
//! `id UUID PRIMARY KEY` puts a UUID in the WHERE clause of every update
//! and delete. It travels as a 16-byte blob through SQLite and is
//! re-bound to a native `UUID` by [`PgAdapter`], with no SQL cast.
//!
//! # Dispatched columns
//!
//! `active BOOLEAN` (integer affinity to native `Bool`), `token UUID` (a
//! non-key UUID, 16-byte blob to native `UUID`), `code sku` (a `DOMAIN`
//! over text), and `feeling mood` (an `ENUM`). The domain and enum bind
//! through diesel's own `SqlType`/`ToSql` resolved by OID name.
//!
//! Only compiled for test crates that enable the full apply stack.

#![allow(dead_code)]

use std::io::Write;

use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgValue};
use diesel::query_builder::AstPass;
use diesel::result::{Error as DieselError, QueryResult};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl, SqliteConnection};
use diesel_sqlite_session::SqliteSessionExt;
use sql_traits::prelude::DatabaseLike;
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, PatchSet, Value};
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::SQLite as SqliteBackend;
use subql::emit::WireTable;
use subql::patchset::{PgAdapter, SqliteAdapter};
use subql::testing::TestEvent;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};
use uuid::Uuid;

// ============================================================================
// Postgres user-defined types bound through diesel's type system, so the
// round trip binds them natively (by the type's OID) with no SQL CAST.
// ============================================================================

#[derive(diesel::sql_types::SqlType)]
#[diesel(postgres_type(name = "mood"))]
struct MoodType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mood {
    Happy,
    Sad,
    Neutral,
}

impl Mood {
    const fn label(self) -> &'static str {
        match self {
            Self::Happy => "happy",
            Self::Sad => "sad",
            Self::Neutral => "neutral",
        }
    }

    fn from_label(label: &str) -> Option<Self> {
        match label {
            "happy" => Some(Self::Happy),
            "sad" => Some(Self::Sad),
            "neutral" => Some(Self::Neutral),
            _ => None,
        }
    }
}

impl ToSql<MoodType, Pg> for Mood {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(self.label().as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<MoodType, Pg> for Mood {
    fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
        core::str::from_utf8(value.as_bytes())
            .ok()
            .and_then(Self::from_label)
            .ok_or_else(|| "unrecognized mood label".into())
    }
}

#[derive(diesel::sql_types::SqlType)]
#[diesel(postgres_type(name = "sku"))]
struct SkuType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sku(pub String);

impl ToSql<SkuType, Pg> for Sku {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(self.0.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<SkuType, Pg> for Sku {
    fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
        Ok(Self(core::str::from_utf8(value.as_bytes())?.to_owned()))
    }
}

struct MoodBinder(Mood);

impl Binder<Pg> for MoodBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<MoodType, Mood>(&self.0)
    }
}

struct SkuBinder(Sku);

impl Binder<Pg> for SkuBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<SkuType, Sku>(&self.0)
    }
}

/// [`PgAdapter`] wrapped with dispatch for the `feeling` (enum) and
/// `code` (domain) columns, delegating everything else (including the
/// bool and both UUID columns) to the inner adapter.
struct DomainAwarePgAdapter<'db, DB: DatabaseLike> {
    inner: PgAdapter<'db, DB>,
}

fn query_error(message: impl Into<String>) -> DieselError {
    DieselError::QueryBuilderError(message.into().into())
}

impl<DB, S, B> Adapter<Pg, S, B> for DomainAwarePgAdapter<'_, DB>
where
    DB: DatabaseLike,
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    fn column_name(&self, table_name: &str, column_index: usize) -> &str {
        <PgAdapter<'_, DB> as Adapter<Pg, S, B>>::column_name(&self.inner, table_name, column_index)
    }

    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError> {
        match <PgAdapter<'_, DB> as Adapter<Pg, S, B>>::column_name(
            &self.inner,
            table_name,
            column_index,
        ) {
            "feeling" => match value {
                Value::Text(s) => Mood::from_label(s.as_ref())
                    .map(|m| -> Box<dyn Binder<Pg> + Send + 'a> { Box::new(MoodBinder(m)) })
                    .ok_or_else(|| query_error(format!("unknown mood label {:?}", s.as_ref()))),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                _ => Err(query_error("mood column expects TEXT or NULL")),
            },
            "code" => match value {
                Value::Text(s) => Ok(Box::new(SkuBinder(Sku(s.as_ref().to_owned())))),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                _ => Err(query_error("sku column expects TEXT or NULL")),
            },
            _ => self.inner.bind(table_name, column_index, value),
        }
    }
}

// ============================================================================
// Schema, DML, and fixtures
// ============================================================================

const PG_CREATE_MOOD: &str = "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')";
const PG_CREATE_SKU: &str = "CREATE DOMAIN sku AS TEXT";
const PG_DDL: &str = "CREATE TABLE orders (id UUID PRIMARY KEY, amount INT, status TEXT, active BOOLEAN, token UUID, code sku, feeling mood)";
/// The subql catalog only needs the table shape. `sku` and `mood` are
/// unknown scalars, so subql treats their columns as text on the wire.
const SUBQL_PG_DDL: &str = "CREATE TABLE orders (id UUID PRIMARY KEY, amount INT, status TEXT, active BOOLEAN, token UUID, code sku, feeling mood);";
const SQLITE_DDL: &str = "CREATE TABLE orders (id BLOB PRIMARY KEY, amount INTEGER, status TEXT, active INTEGER, token BLOB, code TEXT, feeling TEXT);";

const ID_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ID_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const ID_C: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const TOKEN_A: &str = "11111111-1111-1111-1111-111111111111";
const TOKEN_B: &str = "22222222-2222-2222-2222-222222222222";
const TOKEN_C: &str = "33333333-3333-3333-3333-333333333333";

#[derive(QueryableByName, Debug, PartialEq)]
struct PgOrder {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    amount: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    active: bool,
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    token: Uuid,
    #[diesel(sql_type = SkuType)]
    code: Sku,
    #[diesel(sql_type = MoodType)]
    feeling: Mood,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct SqliteOrder {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    id: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    amount: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    active: i64,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    token: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    code: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    feeling: String,
}

fn uuid(text: &str) -> Uuid {
    Uuid::parse_str(text).unwrap()
}

fn load_pg(conn: &mut PgConnection) -> Vec<PgOrder> {
    sql_query("SELECT id, amount, status, active, token, code, feeling FROM orders ORDER BY id")
        .load(conn)
        .unwrap()
}

fn load_sqlite(conn: &mut SqliteConnection) -> Vec<SqliteOrder> {
    sql_query("SELECT id, amount, status, active, token, code, feeling FROM orders ORDER BY id")
        .load(conn)
        .unwrap()
}

fn seed_pg_rows() -> Vec<PgOrder> {
    vec![
        PgOrder {
            id: uuid(ID_A),
            amount: 100,
            status: "new".to_owned(),
            active: true,
            token: uuid(TOKEN_A),
            code: Sku("SKU-1".to_owned()),
            feeling: Mood::Happy,
        },
        PgOrder {
            id: uuid(ID_B),
            amount: 200,
            status: "new".to_owned(),
            active: false,
            token: uuid(TOKEN_B),
            code: Sku("SKU-2".to_owned()),
            feeling: Mood::Sad,
        },
        PgOrder {
            id: uuid(ID_C),
            amount: 300,
            status: "new".to_owned(),
            active: true,
            token: uuid(TOKEN_C),
            code: Sku("SKU-3".to_owned()),
            feeling: Mood::Neutral,
        },
    ]
}

// Net state after the mutate: id_b updated (amount, status, active,
// feeling; token and code unchanged), id_c deleted.
fn final_pg_rows() -> Vec<PgOrder> {
    vec![
        PgOrder {
            id: uuid(ID_A),
            amount: 100,
            status: "new".to_owned(),
            active: true,
            token: uuid(TOKEN_A),
            code: Sku("SKU-1".to_owned()),
            feeling: Mood::Happy,
        },
        PgOrder {
            id: uuid(ID_B),
            amount: 250,
            status: "shipped".to_owned(),
            active: true,
            token: uuid(TOKEN_B),
            code: Sku("SKU-2".to_owned()),
            feeling: Mood::Neutral,
        },
    ]
}

fn seed_sqlite_rows() -> Vec<SqliteOrder> {
    seed_pg_rows().into_iter().map(sqlite_view).collect()
}

fn final_sqlite_rows() -> Vec<SqliteOrder> {
    final_pg_rows().into_iter().map(sqlite_view).collect()
}

fn sqlite_view(row: PgOrder) -> SqliteOrder {
    SqliteOrder {
        id: row.id.as_bytes().to_vec(),
        amount: i64::from(row.amount),
        status: row.status,
        active: i64::from(row.active),
        token: row.token.as_bytes().to_vec(),
        code: row.code.0,
        feeling: row.feeling.label().to_owned(),
    }
}

/// The subql catalog mirroring the Postgres table shape.
#[must_use]
pub fn subql_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(SUBQL_PG_DDL).unwrap()
}

/// Create the enum, domain, and table on the source, with `REPLICA
/// IDENTITY FULL` so update and delete carry full old-row images.
pub fn create_schema(pg: &mut PgConnection) {
    sql_query(PG_CREATE_MOOD).execute(pg).unwrap();
    sql_query(PG_CREATE_SKU).execute(pg).unwrap();
    sql_query(PG_DDL).execute(pg).unwrap();
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(pg)
        .unwrap();
}

/// Seed phase: insert three rows with UUID primary keys.
pub fn seed_dml(pg: &mut PgConnection) {
    for stmt in [
        format!("INSERT INTO orders (id, amount, status, active, token, code, feeling) VALUES ('{ID_A}', 100, 'new', true, '{TOKEN_A}', 'SKU-1', 'happy')"),
        format!("INSERT INTO orders (id, amount, status, active, token, code, feeling) VALUES ('{ID_B}', 200, 'new', false, '{TOKEN_B}', 'SKU-2', 'sad')"),
        format!("INSERT INTO orders (id, amount, status, active, token, code, feeling) VALUES ('{ID_C}', 300, 'new', true, '{TOKEN_C}', 'SKU-3', 'neutral')"),
    ] {
        sql_query(&stmt).execute(pg).unwrap();
    }
}

/// Mutate phase: update one row (flipping the bool and enum among other
/// columns) and delete another, both matched on the UUID primary key.
pub fn mutate_dml(pg: &mut PgConnection) {
    sql_query(format!(
        "UPDATE orders SET amount = 250, status = 'shipped', active = true, feeling = 'neutral' WHERE id = '{ID_B}'"
    ))
    .execute(pg)
    .unwrap();
    sql_query(format!("DELETE FROM orders WHERE id = '{ID_C}'"))
        .execute(pg)
        .unwrap();
}

/// Complete the round trip from the seed and mutate patchsets emitted by
/// the source CDC.
///
/// Applies the seed patchset to a fresh SQLite replica, then (with a
/// session tracking) the mutate patchset, so the session records a real
/// update and delete. That session patchset is re-applied to a
/// re-seeded Postgres through the dispatch-aware [`PgAdapter`], and row
/// parity is asserted at every step. Update and delete match on the UUID
/// primary key, bound natively.
pub fn finish_loop(
    pg: &mut PgConnection,
    seed: &PatchSet<WireTable, String, Vec<u8>>,
    mutate: &PatchSet<WireTable, String, Vec<u8>>,
) {
    // Postgres net state after the mutate, captured before the round trip.
    let source_net = load_pg(pg);
    assert_eq!(source_net, final_pg_rows(), "source net state");

    // ---- Outgoing: seed then mutate the SQLite replica ----
    let mut sqlite = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut sqlite).unwrap();
    let sqlite_catalog = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).unwrap();
    let sqlite_engine: SubscriptionEngine<TestEvent<SqliteBackend>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(sqlite_catalog, SQLiteDialect {});
    let sqlite_adapter = SqliteAdapter::new(sqlite_engine.database());

    // Seed the replica before the session starts, so the session only
    // records the mutate (a genuine update and delete).
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
        "SQLite replica after mutate (update and delete applied)"
    );

    // ---- Incoming: capture the session patchset and re-apply to PG ----
    let session_patchset = session.patchset().unwrap();
    assert!(!session_patchset.is_empty(), "session recorded no changes");

    // Re-seed a fresh Postgres, then re-apply the captured session
    // patchset (a genuine update and delete) through the production
    // inbound API, which reconstructs the ops from the raw bytes.
    sql_query("TRUNCATE orders").execute(pg).unwrap();

    let pg_engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(subql_catalog(), PostgreSqlDialect {});
    let pg_adapter = DomainAwarePgAdapter {
        inner: PgAdapter::new(pg_engine.database()),
    };

    pg_engine.apply_patchset(seed, pg, &pg_adapter).unwrap();
    assert_eq!(load_pg(pg), seed_pg_rows(), "Postgres after re-seed");

    pg_engine
        .apply_patchset_bytes(&session_patchset, pg, &pg_adapter)
        .unwrap();
    assert_eq!(
        load_pg(pg),
        source_net,
        "round-tripped Postgres rows equal the source net state"
    );
}
