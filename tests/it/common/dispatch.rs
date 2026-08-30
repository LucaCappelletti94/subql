//! Shared machinery for the CDC round-trip E2E tests, one per source
//! vehicle (wal2json and pgoutput). Both drive the same two-phase DML on
//! a Postgres source, emit patchsets, apply them to a SQLite replica
//! through [`SqliteAdapter`], capture the SQLite session patchset, and
//! re-apply it to a fresh Postgres through the shipped [`CustomTypePgAdapter`].
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
//! `price NUMERIC(12,4)` binds natively as `Numeric`, and `ts TIMESTAMP`,
//! `tstz TIMESTAMPTZ`, `d DATE`, and `t TIME` bind natively as their
//! matching diesel temporal types, all through [`PgAdapter`].
//!
//! Only compiled for test crates that enable the full apply stack.

#![allow(dead_code)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::unwrap_used)]

use std::io::Write;

use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgValue};
use diesel::result::{Error as DieselError, QueryResult};
use diesel::serialize::{self, IsNull, Output, ToSql};
use sqlite_diff_rs::{Binder, Value};
use subql::patchset::{bind_as, PgCustomBinder};

// Postgres user-defined types bound through diesel's type system, so the
// round trip binds them natively (by the type's OID) with no SQL CAST.

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

struct MoodBinding;

impl PgCustomBinder<String, Vec<u8>> for MoodBinding {
    fn bind<'a>(
        &self,
        value: &'a Value<String, Vec<u8>>,
    ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
        match value {
            Value::Text(s) => Mood::from_label(s)
                .map(|m| -> Box<dyn Binder<Pg> + Send + 'a> { bind_as::<MoodType, Mood>(m) })
                .ok_or_else(|| query_error(format!("unknown mood label {s:?}"))),
            _ => Err(query_error("mood column expects TEXT or NULL")),
        }
    }
}

struct SkuBinding;

impl PgCustomBinder<String, Vec<u8>> for SkuBinding {
    fn bind<'a>(
        &self,
        value: &'a Value<String, Vec<u8>>,
    ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
        match value {
            Value::Text(s) => Ok(bind_as::<SkuType, Sku>(Sku(s.clone()))),
            _ => Err(query_error("sku column expects TEXT or NULL")),
        }
    }
}

fn query_error(message: impl Into<String>) -> DieselError {
    DieselError::QueryBuilderError(message.into().into())
}

pub use schema_dml_fixtures::{create_schema, finish_loop, mutate_dml, seed_dml, subql_catalog};

mod schema_dml_fixtures {
    use super::{Mood, MoodBinding, MoodType, Sku, SkuBinding, SkuType};
    use bigdecimal::BigDecimal;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use core::str::FromStr;
    use diesel::{
        sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl, SqliteConnection,
    };
    use diesel_sqlite_session::SqliteSessionExt;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::PatchSet;
    use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
    use subql::backend::SQLite as SqliteBackend;
    use subql::emit::WireTable;
    use subql::patchset::{CustomTypePgAdapter, SqliteAdapter};
    use subql::testing::TestEvent;
    use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};
    use uuid::Uuid;

    const PG_CREATE_MOOD: &str = "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')";
    const PG_CREATE_SKU: &str = "CREATE DOMAIN sku AS TEXT";
    const PG_DDL: &str = "CREATE TABLE orders (id UUID PRIMARY KEY, amount INT, status TEXT, active BOOLEAN, token UUID, code sku, feeling mood, note TEXT, price NUMERIC(12,4), ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, t TIME, js JSON, jb JSONB, dp DOUBLE PRECISION, bin BYTEA)";
    /// The subql catalog only needs the table shape. `sku` and `mood` are
    /// unknown scalars, so subql treats their columns as text on the wire.
    const SUBQL_PG_DDL: &str = "CREATE TABLE orders (id UUID PRIMARY KEY, amount INT, status TEXT, active BOOLEAN, token UUID, code sku, feeling mood, note TEXT, price NUMERIC(12,4), ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, t TIME, js JSON, jb JSONB, dp DOUBLE PRECISION, bin BYTEA);";
    const SQLITE_DDL: &str = "CREATE TABLE orders (id BLOB PRIMARY KEY, amount INTEGER, status TEXT, active INTEGER, token BLOB, code TEXT, feeling TEXT, note TEXT, price TEXT, ts TEXT, tstz TEXT, d TEXT, t TEXT, js TEXT, jb TEXT, dp REAL, bin BLOB);";

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
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        note: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Numeric>)]
        price: Option<BigDecimal>,
        #[diesel(sql_type = diesel::sql_types::Timestamp)]
        ts: NaiveDateTime,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        tstz: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Date)]
        d: NaiveDate,
        #[diesel(sql_type = diesel::sql_types::Time)]
        t: NaiveTime,
        #[diesel(sql_type = diesel::sql_types::Json)]
        js: serde_json::Value,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        jb: serde_json::Value,
        #[diesel(sql_type = diesel::sql_types::Double)]
        dp: f64,
        #[diesel(sql_type = diesel::sql_types::Binary)]
        bin: Vec<u8>,
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
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        note: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        price: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Text)]
        ts: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        tstz: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        d: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        t: String,
        #[diesel(sql_type = diesel::sql_types::Double)]
        dp: f64,
        #[diesel(sql_type = diesel::sql_types::Binary)]
        bin: Vec<u8>,
    }

    fn uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).unwrap()
    }

    const fn ndt(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, us: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_micro_opt(h, mi, s, us)
            .unwrap()
    }

    const fn nd(y: i32, mo: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, mo, d).unwrap()
    }

    const fn nt(h: u32, mi: u32, s: u32, us: u32) -> NaiveTime {
        NaiveTime::from_hms_micro_opt(h, mi, s, us).unwrap()
    }

    // JSON documents for the `js` (JSON) and `jb` (JSONB) columns. Compared
    // structurally as `serde_json::Value`, so key order and whitespace
    // normalization on either side does not matter.
    const JS_A: &str = r#"{"k":"a"}"#;
    const JB_A: &str = "[1,2,3]";
    const JS_B: &str = r#"{"k":"b"}"#;
    const JB_B: &str = "[4,5]";
    const JS_C: &str = r#"{"k":"c"}"#;
    const JB_C: &str = "[6]";
    const JS_B2: &str = r#"{"k":"b2"}"#;
    const JB_B2: &str = "[7,8,9]";

    fn load_pg(conn: &mut PgConnection) -> Vec<PgOrder> {
        sql_query(
        "SELECT id, amount, status, active, token, code, feeling, note, price, ts, tstz, d, t, js, jb, dp, bin FROM orders ORDER BY id",
    )
    .load(conn)
    .unwrap()
    }

    fn load_sqlite(conn: &mut SqliteConnection) -> Vec<SqliteOrder> {
        sql_query(
        "SELECT id, amount, status, active, token, code, feeling, note, price, ts, tstz, d, t, dp, bin FROM orders ORDER BY id",
    )
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
                note: None,
                price: Some(BigDecimal::from_str("1234.5678").unwrap()),
                ts: ndt(2024, 1, 15, 8, 30, 0, 123456),
                tstz: ndt(2024, 1, 15, 8, 30, 0, 123456).and_utc(),
                d: nd(2024, 1, 15),
                t: nt(8, 30, 0, 123456),
                js: serde_json::from_str(JS_A).unwrap(),
                jb: serde_json::from_str(JB_A).unwrap(),
                dp: 1.5,
                bin: vec![0x00, 0x01, 0xde, 0xad, 0xff],
            },
            PgOrder {
                id: uuid(ID_B),
                amount: 200,
                status: "new".to_owned(),
                active: false,
                token: uuid(TOKEN_B),
                code: Sku("SKU-2".to_owned()),
                feeling: Mood::Sad,
                note: Some("packed".to_owned()),
                price: Some(BigDecimal::from_str("8765.4321").unwrap()),
                ts: ndt(2023, 6, 20, 22, 10, 5, 654321),
                tstz: ndt(2023, 6, 20, 22, 10, 5, 654321).and_utc(),
                d: nd(2023, 6, 20),
                t: nt(22, 10, 5, 654321),
                js: serde_json::from_str(JS_B).unwrap(),
                jb: serde_json::from_str(JB_B).unwrap(),
                dp: 2.25,
                bin: vec![0x01, 0x02, 0x03],
            },
            PgOrder {
                id: uuid(ID_C),
                amount: 300,
                status: "new".to_owned(),
                active: true,
                token: uuid(TOKEN_C),
                code: Sku("SKU-3".to_owned()),
                feeling: Mood::Neutral,
                note: Some("void".to_owned()),
                price: None,
                ts: ndt(2022, 12, 31, 23, 59, 59, 999999),
                tstz: ndt(2022, 12, 31, 23, 59, 59, 999999).and_utc(),
                d: nd(2022, 12, 31),
                t: nt(23, 59, 59, 999999),
                js: serde_json::from_str(JS_C).unwrap(),
                jb: serde_json::from_str(JB_C).unwrap(),
                dp: 3.75,
                bin: vec![0xff, 0xfe],
            },
        ]
    }

    // Net state after the mutate: id_b updated (amount, status, active,
    // feeling, and note cleared to NULL; token and code unchanged), id_c
    // deleted. id_a keeps its NULL note from the seed, so a NULL survives an
    // insert and an update across the whole loop.
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
                note: None,
                price: Some(BigDecimal::from_str("1234.5678").unwrap()),
                ts: ndt(2024, 1, 15, 8, 30, 0, 123456),
                tstz: ndt(2024, 1, 15, 8, 30, 0, 123456).and_utc(),
                d: nd(2024, 1, 15),
                t: nt(8, 30, 0, 123456),
                js: serde_json::from_str(JS_A).unwrap(),
                jb: serde_json::from_str(JB_A).unwrap(),
                dp: 1.5,
                bin: vec![0x00, 0x01, 0xde, 0xad, 0xff],
            },
            PgOrder {
                id: uuid(ID_B),
                amount: 250,
                status: "shipped".to_owned(),
                active: true,
                token: uuid(TOKEN_B),
                code: Sku("SKU-2".to_owned()),
                feeling: Mood::Neutral,
                note: None,
                price: Some(BigDecimal::from_str("2468.1357").unwrap()),
                ts: ndt(2025, 3, 3, 3, 3, 3, 30303),
                tstz: ndt(2025, 3, 3, 3, 3, 3, 30303).and_utc(),
                d: nd(2025, 3, 3),
                t: nt(3, 3, 3, 30303),
                js: serde_json::from_str(JS_B2).unwrap(),
                jb: serde_json::from_str(JB_B2).unwrap(),
                dp: 4.125,
                bin: vec![0x0a, 0x14, 0x1e],
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
            note: row.note,
            price: row.price.as_ref().map(ToString::to_string),
            ts: row.ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            tstz: row.tstz.format("%Y-%m-%d %H:%M:%S%.6f+00").to_string(),
            d: row.d.format("%Y-%m-%d").to_string(),
            t: row.t.format("%H:%M:%S%.6f").to_string(),
            dp: row.dp,
            bin: row.bin,
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
        sql_query("SET TIME ZONE 'UTC'").execute(pg).unwrap();
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
        format!("INSERT INTO orders (id, amount, status, active, token, code, feeling, note, price, ts, tstz, d, t, js, jb, dp, bin) VALUES ('{ID_A}', 100, 'new', true, '{TOKEN_A}', 'SKU-1', 'happy', NULL, 1234.5678, '2024-01-15 08:30:00.123456', '2024-01-15 08:30:00.123456+00', '2024-01-15', '08:30:00.123456', '{JS_A}', '{JB_A}', 1.5, '\\x0001deadff')"),
        format!("INSERT INTO orders (id, amount, status, active, token, code, feeling, note, price, ts, tstz, d, t, js, jb, dp, bin) VALUES ('{ID_B}', 200, 'new', false, '{TOKEN_B}', 'SKU-2', 'sad', 'packed', 8765.4321, '2023-06-20 22:10:05.654321', '2023-06-20 22:10:05.654321+00', '2023-06-20', '22:10:05.654321', '{JS_B}', '{JB_B}', 2.25, '\\x010203')"),
        format!("INSERT INTO orders (id, amount, status, active, token, code, feeling, note, price, ts, tstz, d, t, js, jb, dp, bin) VALUES ('{ID_C}', 300, 'new', true, '{TOKEN_C}', 'SKU-3', 'neutral', 'void', NULL, '2022-12-31 23:59:59.999999', '2022-12-31 23:59:59.999999+00', '2022-12-31', '23:59:59.999999', '{JS_C}', '{JB_C}', 3.75, '\\xfffe')"),
    ] {
        sql_query(&stmt).execute(pg).unwrap();
    }
    }

    /// Mutate phase: update one row (flipping the bool and enum among other
    /// columns) and delete another, both matched on the UUID primary key.
    pub fn mutate_dml(pg: &mut PgConnection) {
        sql_query(format!(
        "UPDATE orders SET amount = 250, status = 'shipped', active = true, feeling = 'neutral', note = NULL, price = 2468.1357, ts = '2025-03-03 03:03:03.030303', tstz = '2025-03-03 03:03:03.030303+00', d = '2025-03-03', t = '03:03:03.030303', js = '{JS_B2}', jb = '{JB_B2}', dp = 4.125, bin = '\\x0a141e' WHERE id = '{ID_B}'"
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
    /// re-seeded Postgres through the shipped [`CustomTypePgAdapter`], and row
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

        // Outgoing: seed then mutate the SQLite replica
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

        // Incoming: capture the session patchset and re-apply to PG
        let session_patchset = session.patchset().unwrap();
        assert!(!session_patchset.is_empty(), "session recorded no changes");

        // Re-seed a fresh Postgres, then re-apply the captured session
        // patchset (a genuine update and delete) through the production
        // inbound API, which reconstructs the ops from the raw bytes.
        sql_query("TRUNCATE orders").execute(pg).unwrap();

        let pg_engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
            SubscriptionEngine::new(subql_catalog(), PostgreSqlDialect {});
        let pg_adapter = CustomTypePgAdapter::new(pg_engine.database())
            .register("mood", MoodBinding)
            .register("sku", SkuBinding);

        pg_engine.apply_patchset(seed, pg, &pg_adapter).unwrap();
        assert_eq!(load_pg(pg), seed_pg_rows(), "Postgres after re-seed");

        pg_engine
            .apply_diffset_bytes(&session_patchset, pg, &pg_adapter)
            .unwrap();
        let round_tripped = load_pg(pg);
        assert_eq!(
            round_tripped, source_net,
            "round-tripped Postgres rows equal the source net state"
        );
        // Fidelity: NUMERIC(12,4) preserves the exact scale (four fractional
        // digits) across the whole loop, not merely the numeric value.
        let price_a = round_tripped
            .iter()
            .find(|row| row.id == uuid(ID_A))
            .unwrap()
            .price
            .as_ref()
            .unwrap();
        assert_eq!(
            price_a.to_string(),
            "1234.5678",
            "decimal scale preserved through the round trip"
        );
    }
}
