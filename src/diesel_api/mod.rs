//! Diesel-typed subscription API (Postgres).
//!
//! Renders a typed diesel query to placeholder SQL plus a list of resolved bind
//! [`Cell`]s, so callers get compile-time-checked queries from their diesel
//! `table!` schema while the engine keeps consuming SQL + binds (see
//! [`crate::SubscriptionRequest::binds`]). Diesel fixes the bind collector per
//! backend (`Pg::BindCollector = RawBytesBindCollector<Pg>`), so values come out
//! only as serialized wire bytes; we byte-decode Postgres's binary format into
//! `Cell`. Postgres-first; other backends would need their own decoders.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt::Write as _;

use diesel::backend::Backend;
use diesel::pg::{Pg, PgMetadataLookup, PgTypeMetadata};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{QueryBuilder, QueryFragment};
use sql_traits::prelude::DatabaseLike;
use sqlparser::dialect::Dialect;

use crate::{
    Cell, IdTypes, RegisterError, RegisterResult, SubscriptionEngine, SubscriptionRequest,
};

/// Postgres built-in scalar type OIDs (stable constants).
mod oid {
    pub const BOOL: u32 = 16;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const UUID: u32 = 2950;
}

/// A metadata lookup that resolves nothing. Built-in scalar Postgres types
/// report their OID statically without consulting the lookup, so this suffices
/// for scalar binds without a database connection. A custom type (enum, domain,
/// composite) would consult it; we return sentinel OID `0`, which
/// [`decode_pg_bind`] then rejects as an unsupported bind type.
struct NoLookup;

impl PgMetadataLookup for NoLookup {
    fn lookup_type(&mut self, _type_name: &str, _schema: Option<&str>) -> PgTypeMetadata {
        PgTypeMetadata::new(0, 0)
    }
}

/// Render a typed diesel Postgres query to placeholder SQL (`$1..`) plus its
/// bind values decoded to [`Cell`]s, in placeholder order.
///
/// # Errors
/// Returns [`RegisterError::UnsupportedSql`] if diesel fails to render the query
/// or a bind uses a type outside the supported scalar set (bool, integer, float,
/// text, uuid).
pub fn render_typed<Q>(query: &Q) -> Result<(String, Vec<Cell>), RegisterError>
where
    Q: QueryFragment<Pg>,
{
    let backend = Pg;

    // 1. SQL skeleton with `$N` placeholders (needs only the backend type).
    let mut qb = <Pg as Backend>::QueryBuilder::default();
    query.to_sql(&mut qb, &backend).map_err(|e| {
        RegisterError::UnsupportedSql(format!("diesel query rendering failed: {e}"))
    })?;
    let sql = qb.finish();

    // 2. Binds as serialized wire bytes + type OIDs.
    let mut collector = RawBytesBindCollector::<Pg>::new();
    let mut lookup = NoLookup;
    query
        .collect_binds(&mut collector, &mut lookup, &backend)
        .map_err(|e| {
            RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
        })?;

    // 3. Decode each bind to a Cell (placeholder order).
    let mut cells = Vec::with_capacity(collector.binds.len());
    for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
        let type_oid = meta.oid().map_err(|_| {
            RegisterError::UnsupportedSql("diesel bind has an unresolved type OID".to_string())
        })?;
        cells.push(decode_pg_bind(bytes.as_deref(), type_oid)?);
    }

    Ok((sql, cells))
}

/// Decode one Postgres binary-format bind value into a [`Cell`]. `None` bytes
/// are a SQL NULL.
fn decode_pg_bind(bytes: Option<&[u8]>, type_oid: u32) -> Result<Cell, RegisterError> {
    let Some(b) = bytes else {
        return Ok(Cell::Null);
    };
    let cell = match type_oid {
        oid::BOOL => Cell::Bool(*b.first().ok_or_else(|| bad_len("bool", b.len()))? != 0),
        oid::INT2 => Cell::Int(i64::from(i16::from_be_bytes(fixed(b, "int2")?))),
        oid::INT4 => Cell::Int(i64::from(i32::from_be_bytes(fixed(b, "int4")?))),
        oid::INT8 => Cell::Int(i64::from_be_bytes(fixed(b, "int8")?)),
        oid::FLOAT4 => Cell::Float(f64::from(f32::from_be_bytes(fixed(b, "float4")?))),
        oid::FLOAT8 => Cell::Float(f64::from_be_bytes(fixed(b, "float8")?)),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => {
            let s = core::str::from_utf8(b).map_err(|_| {
                RegisterError::UnsupportedSql("diesel text bind is not valid UTF-8".to_string())
            })?;
            Cell::String(s.into())
        }
        oid::UUID => Cell::String(format_uuid(fixed(b, "uuid")?).into()),
        other => {
            return Err(RegisterError::UnsupportedSql(format!(
                "unsupported diesel bind type (Postgres OID {other}); only bool, integer, float, text and uuid are supported"
            )))
        }
    };
    Ok(cell)
}

fn bad_len(ty: &str, got: usize) -> RegisterError {
    RegisterError::UnsupportedSql(format!("diesel {ty} bind has unexpected byte length {got}"))
}

fn fixed<const N: usize>(b: &[u8], ty: &str) -> Result<[u8; N], RegisterError> {
    b.try_into().map_err(|_| bad_len(ty, b.len()))
}

/// Canonical lowercase-hyphenated uuid string, matching how CDC ingests uuids
/// (`Cell::String`), so bind-side and CDC-side uuid values compare equal.
fn format_uuid(b: [u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, byte) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Diesel-typed registration methods on the engine.
impl<D, I, DB> SubscriptionEngine<D, I, DB>
where
    D: Dialect,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Register a subscription from a typed diesel Postgres query.
    ///
    /// The query is rendered to SQL plus bind values (see [`render_typed`]) and
    /// registered like any other subscription, so it is checked against your
    /// diesel `table!` schema at compile time. Projection rules match
    /// [`SubscriptionEngine::register_select`].
    ///
    /// # Errors
    /// Propagates rendering errors from [`render_typed`] (e.g. an unsupported
    /// bind type) and registration errors from [`SubscriptionEngine::register`].
    pub fn register_select_typed<Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        query: &Q,
    ) -> Result<RegisterResult, RegisterError>
    where
        Q: QueryFragment<Pg>,
    {
        let (sql, binds) = render_typed(query)?;
        self.register(SubscriptionRequest::new(consumer_id, sql).binds(binds))
    }

    /// Register a follow subscription from a typed diesel Postgres UPDATE.
    ///
    /// The UPDATE is rendered to SQL + binds (see [`render_typed`]) and its
    /// target rows become a standing `SELECT * ... WHERE <the UPDATE's WHERE>`
    /// subscription. Nothing is executed. See
    /// [`SubscriptionEngine::register_follow_update`].
    ///
    /// # Errors
    /// Propagates rendering errors from [`render_typed`] and the follow-shape /
    /// registration errors from
    /// [`SubscriptionEngine::register_follow_update_with_binds`].
    pub fn register_follow_update_typed<Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        update: &Q,
    ) -> Result<RegisterResult, RegisterError>
    where
        Q: QueryFragment<Pg>,
    {
        let (sql, binds) = render_typed(update)?;
        self.register_follow_update_with_binds(consumer_id, sql, binds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_scalars() {
        assert_eq!(decode_pg_bind(None, oid::INT4).unwrap(), Cell::Null);
        assert_eq!(
            decode_pg_bind(Some(&5i32.to_be_bytes()), oid::INT4).unwrap(),
            Cell::Int(5)
        );
        assert_eq!(
            decode_pg_bind(Some(&(-7i64).to_be_bytes()), oid::INT8).unwrap(),
            Cell::Int(-7)
        );
        assert_eq!(
            decode_pg_bind(Some(&3.5f64.to_be_bytes()), oid::FLOAT8).unwrap(),
            Cell::Float(3.5)
        );
        assert_eq!(
            decode_pg_bind(Some(&[1]), oid::BOOL).unwrap(),
            Cell::Bool(true)
        );
        assert_eq!(
            decode_pg_bind(Some(b"hello"), oid::TEXT).unwrap(),
            Cell::String("hello".into())
        );
    }

    #[test]
    fn decode_uuid_is_canonical() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            decode_pg_bind(Some(&bytes), oid::UUID).unwrap(),
            Cell::String("550e8400-e29b-41d4-a716-446655440000".into())
        );
    }

    #[test]
    fn decode_unsupported_oid_errors() {
        // 1700 = numeric
        assert!(matches!(
            decode_pg_bind(Some(&[0, 0]), 1700),
            Err(RegisterError::UnsupportedSql(_))
        ));
    }
}

#[cfg(test)]
mod render_tests {
    use super::{render_typed, Cell};
    use diesel::prelude::*;

    diesel::table! {
        users (id) {
            id -> Integer,
            name -> Text,
            active -> Bool,
        }
    }

    #[test]
    fn render_select_with_binds() {
        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed(&query).expect("render");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert_eq!(
            binds,
            alloc::vec![Cell::Int(5), Cell::String("ann".into()), Cell::Bool(true)]
        );
    }

    #[test]
    fn register_typed_row_query_via_engine() {
        use crate::SubscriptionEngine;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::PostgreSqlDialect;

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine =
            SubscriptionEngine::<_, crate::DefaultIds, _>::new(catalog, PostgreSqlDialect {});

        // Diesel renders a row query as a fully-qualified all-columns list plus a
        // parameterized WHERE; the typed path must accept it (complete column
        // list == SELECT *) and resolve the bind.
        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed(1, &query)
            .expect("typed register");
        // Deterministic: registering the same typed query dedups to the same id.
        let b = engine
            .register_select_typed(1, &query)
            .expect("typed register again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[test]
    fn register_typed_update_follow_via_engine() {
        use crate::SubscriptionEngine;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::PostgreSqlDialect;

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine =
            SubscriptionEngine::<_, crate::DefaultIds, _>::new(catalog, PostgreSqlDialect {});

        // A typed diesel UPDATE registers as a follow on its target rows.
        let update = diesel::update(users::table.filter(users::id.eq(5))).set(users::name.eq("x"));
        let a = engine
            .register_follow_update_typed(1, &update)
            .expect("typed update follow");
        // Deterministic: the same typed UPDATE dedups to the same subscription.
        let b = engine
            .register_follow_update_typed(1, &update)
            .expect("typed update follow again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }
}
