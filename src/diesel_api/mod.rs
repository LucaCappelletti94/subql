#![allow(clippy::type_complexity)]

//! Diesel-typed subscription API.
//!
//! Renders a typed diesel query to placeholder SQL plus a list of resolved bind
//! [`Value<B>`](crate::backend::Value)s, so callers get compile-time-checked
//! queries from their diesel `table!` schema while the engine keeps consuming
//! SQL + binds (see [`crate::SubscriptionRequest::binds`]).
//!
//! # Backends
//!
//! Diesel fixes the bind collector per backend, so binds come out in a
//! backend-specific form that each [`BindDecode`] impl turns into
//! [`Value<B>`](crate::backend::Value)s:
//!
//! - Postgres (`diesel-typed`): serialized binary wire bytes plus a type OID,
//!   decoded big-endian by OID. Pure Rust (`postgres_backend`, no libpq).
//! - SQLite (`diesel-typed-sqlite`): already-typed bind values, read directly.
//! - MySQL (`diesel-typed-mysql`): native-endian client buffers tagged with a
//!   `MysqlType`, read by their byte length. Pure Rust (`mysql_backend`, no
//!   libmysqlclient).
//!
//! Typed SELECT and UPDATE-follow work on all three. Follow-insert
//! ([`SubscriptionEngine::register_follow_insert`](crate::SubscriptionEngine::register_follow_insert))
//! covers Postgres and SQLite through `RETURNING`. MySQL has no `RETURNING`,
//! so it takes the DB-minted key from diesel's `execute_returning_id` and
//! follows it with
//! [`SubscriptionEngine::follow_row`](crate::SubscriptionEngine::follow_row).
//!
//! # Caveats
//!
//! - The rendered SQL is the backend's own flavor: `$N` vs `?` placeholders,
//!   and double-quoted vs backtick-quoted identifiers. So the engine's
//!   sqlparser `Dialect` must match the backend the query was rendered for.
//! - SQLite and MySQL have no boolean storage class, so a `bool` bind decodes
//!   to [`Value::Int`] `(0)` or [`Value::Int`] `(1)`, not
//!   [`Value::Bool`] (Postgres yields [`Value::Bool`]).
//!
//! [`Value::Int`]: crate::backend::Value::Int
//! [`Value::Bool`]: crate::backend::Value::Bool
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use diesel::backend::Backend;
use diesel::connection::{DefaultLoadingMode, LoadConnection};
use diesel::expression::QueryMetadata;
use diesel::pg::{Pg, PgMetadataLookup, PgTypeMetadata, PgValue};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::returning::ReturningClause;
use diesel::query_builder::{InsertStatement, Query, QueryBuilder, QueryFragment, QueryId};
use diesel::row::{Field, Row};
use diesel::Table;

/// A follow-insert query: the caller's insert with a `RETURNING <primary key>`
/// clause that we derive from the table type, so the returned columns are always
/// the table's primary key, in its order (never restated by the caller).
type PkReturningInsert<T, U, Op> =
    InsertStatement<T, U, Op, ReturningClause<<T as Table>::PrimaryKey>>;
use sql_traits::prelude::DatabaseLike;

use crate::backend::{CdcEvent, Postgres, Value};
use crate::compiler::literals::SqlLiteralParse;
use crate::{IdTypes, RegisterError, RegisterResult, SubscriptionEngine, SubscriptionRequest};

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

/// Render a typed diesel query to placeholder SQL plus its bind values.
///
/// The binds are decoded to [`Value<B>`](crate::backend::Value)s, in
/// placeholder order, where `B` is the subql
/// [`crate::backend::Backend`] the diesel backend corresponds to.
///
/// Generic over the diesel backend `D` that decodes its own binds
/// ([`BindDecode`]): `Pg` yields `$N` placeholders and decodes the
/// Postgres binary wire format by type OID, `Sqlite` yields `?`
/// placeholders and reads its typed bind values directly.
///
/// # Errors
/// Returns [`RegisterError::UnsupportedSql`] if diesel fails to render
/// the query or a bind uses a type outside the supported scalar set
/// (bool, integer, float, text, uuid).
pub fn render_typed<D, Q>(query: &Q) -> Result<(String, Vec<Value<D::SubqlBackend>>), RegisterError>
where
    D: BindDecode,
    Q: QueryFragment<D>,
{
    D::render_sql_and_binds(query)
}

/// Render only the placeholder SQL skeleton for a backend, no binds. Needs just
/// the backend type, no connection.
fn render_sql<B, Q>(query: &Q) -> Result<String, RegisterError>
where
    B: Backend + Default,
    <B as Backend>::QueryBuilder: Default,
    Q: QueryFragment<B>,
{
    let mut qb = <B as Backend>::QueryBuilder::default();
    query.to_sql(&mut qb, &B::default()).map_err(|e| {
        RegisterError::UnsupportedSql(format!("diesel query rendering failed: {e}"))
    })?;
    Ok(qb.finish())
}

/// Render a typed diesel query to placeholder SQL plus its bind
/// [`Value<B>`](crate::backend::Value)s.
///
/// One diesel backend `D` whose associated
/// [`SubqlBackend`](BindDecode::SubqlBackend) picks the subql
/// [`crate::backend::Backend`] to type the values against. The bind
/// side is backend-specific: Postgres decodes the binary wire format
/// by OID, SQLite reads its typed bind values. This is the input-side
/// counterpart to [`FollowRowDecode`], which decodes the values an
/// executed query hands back.
pub trait BindDecode: Backend {
    /// The subql [`crate::backend::Backend`] whose [`Value`] shape this
    /// diesel backend's binds decode to.
    type SubqlBackend: crate::backend::Backend;

    /// Render `query` to `(placeholder SQL, ordered bind values)`.
    ///
    /// # Errors
    /// [`RegisterError::UnsupportedSql`] if rendering fails or a bind
    /// uses a type outside the supported scalar set.
    fn render_sql_and_binds<Q>(
        query: &Q,
    ) -> Result<(String, Vec<Value<Self::SubqlBackend>>), RegisterError>
    where
        Q: QueryFragment<Self>;
}

impl BindDecode for Pg {
    type SubqlBackend = Postgres;

    fn render_sql_and_binds<Q>(query: &Q) -> Result<(String, Vec<Value<Postgres>>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        let sql = render_sql::<Self, _>(query)?;

        // Binds as serialized wire bytes + type OIDs, decoded per OID.
        let mut collector = RawBytesBindCollector::<Self>::new();
        let mut lookup = NoLookup;
        query
            .collect_binds(&mut collector, &mut lookup, &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let mut values = Vec::with_capacity(collector.binds.len());
        for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
            let type_oid = meta.oid().map_err(|_| {
                RegisterError::UnsupportedSql("diesel bind has an unresolved type OID".to_string())
            })?;
            values.push(decode_pg_bind(bytes.as_deref(), type_oid)?);
        }
        Ok((sql, values))
    }
}

#[cfg(feature = "diesel-typed-sqlite")]
impl BindDecode for diesel::sqlite::Sqlite {
    type SubqlBackend = crate::backend::SQLite;

    fn render_sql_and_binds<Q>(
        query: &Q,
    ) -> Result<(String, Vec<Value<crate::backend::SQLite>>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        use diesel::query_builder::MoveableBindCollector;
        use diesel::sqlite::SqliteBindCollector;

        let sql = render_sql::<Self, _>(query)?;

        let mut collector = SqliteBindCollector::default();
        query
            .collect_binds(&mut collector, &mut (), &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let data = collector.moveable();
        let mut values = Vec::with_capacity(data.binds().len());
        for (value, _ty) in data.binds() {
            values.push(owned_sqlite_to_value(value));
        }
        Ok((sql, values))
    }
}

/// Map an owned SQLite bind value to a `Value<SQLite>`. SQLite has no
/// boolean or uuid storage class, so those arrive as integer or text,
/// and a BLOB maps to [`Value::Bytes`]. The mapping is total over
/// `OwnedSqliteBindValue`.
#[cfg(feature = "diesel-typed-sqlite")]
fn owned_sqlite_to_value(
    value: &diesel::sqlite::OwnedSqliteBindValue,
) -> Value<crate::backend::SQLite> {
    use diesel::sqlite::OwnedSqliteBindValue as V;
    match value {
        V::I32(i) => Value::Int(i64::from(*i)),
        V::I64(i) => Value::Int(*i),
        V::F64(f) => Value::Float(*f),
        V::String(s) => Value::String(s.as_ref().to_string()),
        V::Binary(b) => Value::Bytes(b.to_vec()),
        V::Null => Value::Null,
    }
}

#[cfg(feature = "diesel-typed-mysql")]
impl BindDecode for diesel::mysql::Mysql {
    type SubqlBackend = crate::backend::MySql;

    fn render_sql_and_binds<Q>(
        query: &Q,
    ) -> Result<(String, Vec<Value<crate::backend::MySql>>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        let sql = render_sql::<Self, _>(query)?;

        let mut collector = RawBytesBindCollector::<Self>::new();
        query
            .collect_binds(&mut collector, &mut (), &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let mut values = Vec::with_capacity(collector.binds.len());
        for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
            values.push(decode_mysql_bind(bytes.as_deref(), *meta)?);
        }
        Ok((sql, values))
    }
}

/// Decode one MySQL bind (native-endian client buffer) into a
/// `Value<MySql>` by its `MysqlType`. `None` bytes are a SQL NULL.
/// Integers read by their actual byte length, which also handles `bool`
/// (serialized as a 4-byte `i32`).
#[cfg(feature = "diesel-typed-mysql")]
fn decode_mysql_bind(
    bytes: Option<&[u8]>,
    ty: diesel::mysql::MysqlType,
) -> Result<Value<crate::backend::MySql>, RegisterError> {
    use diesel::mysql::MysqlType as T;
    let Some(b) = bytes else {
        return Ok(Value::Null);
    };
    let value = match ty {
        T::Tiny | T::Short | T::Long | T::LongLong => Value::Int(mysql_int_ne(b, true)?),
        T::UnsignedTiny | T::UnsignedShort | T::UnsignedLong | T::UnsignedLongLong => {
            Value::Int(mysql_int_ne(b, false)?)
        }
        T::Float => Value::Float(f64::from(f32::from_ne_bytes(fixed(b, "mysql float")?))),
        T::Double => Value::Float(f64::from_ne_bytes(fixed(b, "mysql double")?)),
        T::String | T::Enum | T::Set => {
            let s = core::str::from_utf8(b).map_err(|_| {
                RegisterError::UnsupportedSql(
                    "diesel MySQL text bind is not valid UTF-8".to_string(),
                )
            })?;
            Value::String(s.to_string())
        }
        other => {
            return Err(RegisterError::UnsupportedSql(format!(
                "unsupported diesel MySQL bind type ({other:?}); only integer, float, text and enum/set are supported"
            )))
        }
    };
    Ok(value)
}

/// Read a native-endian MySQL integer bind of 1/2/4/8 bytes into an `i64`,
/// interpreting the bytes as signed or unsigned per the `MysqlType`.
#[cfg(feature = "diesel-typed-mysql")]
fn mysql_int_ne(b: &[u8], signed: bool) -> Result<i64, RegisterError> {
    Ok(match (b.len(), signed) {
        (1, true) => i64::from(i8::from_ne_bytes(fixed(b, "mysql tiny")?)),
        (1, false) => i64::from(u8::from_ne_bytes(fixed(b, "mysql utiny")?)),
        (2, true) => i64::from(i16::from_ne_bytes(fixed(b, "mysql short")?)),
        (2, false) => i64::from(u16::from_ne_bytes(fixed(b, "mysql ushort")?)),
        (4, true) => i64::from(i32::from_ne_bytes(fixed(b, "mysql long")?)),
        (4, false) => i64::from(u32::from_ne_bytes(fixed(b, "mysql ulong")?)),
        (8, true) => i64::from_ne_bytes(fixed(b, "mysql longlong")?),
        (8, false) => {
            i64::try_from(u64::from_ne_bytes(fixed(b, "mysql ulonglong")?)).map_err(|_| {
                RegisterError::UnsupportedSql(
                    "diesel MySQL unsigned bind exceeds i64 range".to_string(),
                )
            })?
        }
        (n, _) => return Err(bad_len("mysql integer", n)),
    })
}

/// Decode one Postgres binary-format bind value into a `Value<Postgres>`.
/// `None` bytes are a SQL NULL.
fn decode_pg_bind(bytes: Option<&[u8]>, type_oid: u32) -> Result<Value<Postgres>, RegisterError> {
    let Some(b) = bytes else {
        return Ok(Value::Null);
    };
    let value = match type_oid {
        oid::BOOL => Value::Bool(*b.first().ok_or_else(|| bad_len("bool", b.len()))? != 0),
        oid::INT2 => Value::Int(i64::from(i16::from_be_bytes(fixed(b, "int2")?))),
        oid::INT4 => Value::Int(i64::from(i32::from_be_bytes(fixed(b, "int4")?))),
        oid::INT8 => Value::Int(i64::from_be_bytes(fixed(b, "int8")?)),
        oid::FLOAT4 => Value::Float(f64::from(f32::from_be_bytes(fixed(b, "float4")?))),
        oid::FLOAT8 => Value::Float(f64::from_be_bytes(fixed(b, "float8")?)),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => {
            let s = core::str::from_utf8(b).map_err(|_| {
                RegisterError::UnsupportedSql("diesel text bind is not valid UTF-8".to_string())
            })?;
            Value::String(s.to_string())
        }
        oid::UUID => Value::Uuid(uuid::Uuid::from_bytes(fixed(b, "uuid")?)),
        other => {
            return Err(RegisterError::UnsupportedSql(format!(
                "unsupported diesel bind type (Postgres OID {other}); only bool, integer, float, text and uuid are supported"
            )))
        }
    };
    Ok(value)
}

/// Recover the bare table name an insert targets straight from its diesel table
/// type, so a follow never restates (and so cannot mistype) the table it writes.
///
/// Renders the table's identifier with the backend's own quoting (`"t"` on
/// Postgres/SQLite, `` `t` `` on MySQL), strips that quoting, and takes the final
/// dotted segment so a schema-qualified table (`"public"."users"`) resolves to
/// its bare catalog name (`users`).
fn insert_target_table_name<T, DB>() -> Result<String, RegisterError>
where
    T: Table + QueryFragment<DB> + Default,
    DB: Backend + Default,
    <DB as Backend>::QueryBuilder: Default,
{
    let mut qb = <DB as Backend>::QueryBuilder::default();
    QueryFragment::<DB>::to_sql(&T::default(), &mut qb, &DB::default()).map_err(|e| {
        RegisterError::UnsupportedSql(format!("diesel table rendering failed: {e}"))
    })?;
    let rendered = qb.finish();
    let bare = rendered
        .rsplit('.')
        .next()
        .unwrap_or(rendered.as_str())
        .trim_matches(|c| c == '"' || c == '`')
        .to_string();
    if bare.is_empty() {
        return Err(RegisterError::UnsupportedSql(format!(
            "could not determine the target table name from the diesel insert (rendered {rendered:?})"
        )));
    }
    Ok(bare)
}

fn bad_len(ty: &str, got: usize) -> RegisterError {
    RegisterError::UnsupportedSql(format!("diesel {ty} bind has unexpected byte length {got}"))
}

fn fixed<const N: usize>(b: &[u8], ty: &str) -> Result<[u8; N], RegisterError> {
    b.try_into().map_err(|_| bad_len(ty, b.len()))
}

/// Decode a column value returned by an executed query into a
/// `Value<B>`, per backend.
///
/// Lets an `INSERT ... RETURNING` row be read without a compile-time row
/// struct.
pub trait FollowRowDecode: Backend {
    /// The subql [`crate::backend::Backend`] whose [`Value`] shape this
    /// diesel backend's returned fields decode to.
    type SubqlBackend: crate::backend::Backend;

    /// Decode one field's raw value (or SQL NULL) into a
    /// `Value<Self::SubqlBackend>`.
    ///
    /// # Errors
    /// [`RegisterError::UnsupportedSql`] for a column type outside the
    /// supported scalar set.
    fn field_to_value(
        value: Option<Self::RawValue<'_>>,
    ) -> Result<Value<Self::SubqlBackend>, RegisterError>;
}

impl FollowRowDecode for Pg {
    type SubqlBackend = Postgres;

    fn field_to_value(value: Option<PgValue<'_>>) -> Result<Value<Postgres>, RegisterError> {
        value.map_or(Ok(Value::Null), |v| {
            decode_pg_bind(Some(v.as_bytes()), v.get_oid().get())
        })
    }
}

#[cfg(feature = "diesel-typed-sqlite")]
impl FollowRowDecode for diesel::sqlite::Sqlite {
    type SubqlBackend = crate::backend::SQLite;

    fn field_to_value(
        value: Option<Self::RawValue<'_>>,
    ) -> Result<Value<crate::backend::SQLite>, RegisterError> {
        use diesel::sqlite::SqliteType;
        let Some(mut v) = value else {
            return Ok(Value::Null);
        };
        Ok(match v.value_type() {
            None => Value::Null,
            Some(SqliteType::SmallInt | SqliteType::Integer | SqliteType::Long) => {
                Value::Int(v.read_long())
            }
            Some(SqliteType::Float | SqliteType::Double) => Value::Float(v.read_double()),
            Some(SqliteType::Text) => Value::String(v.read_text().to_string()),
            Some(SqliteType::Binary) => {
                return Err(RegisterError::UnsupportedSql(
                    "unsupported SQLite BLOB in a followed row".to_string(),
                ))
            }
        })
    }
}

/// Error from [`SubscriptionEngine::register_follow_insert`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FollowInsertError {
    /// Executing or reading the `INSERT ... RETURNING` failed.
    #[error("insert execution failed: {0}")]
    Load(#[from] diesel::result::Error),
    /// Building or registering the follow failed (unknown table, no primary key,
    /// key-arity mismatch, or an unsupported returned column type).
    #[error("follow registration failed: {0}")]
    Register(#[from] RegisterError),
}

/// Diesel-typed registration methods on the engine.
impl<E, I, DB> SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Register a subscription from a typed diesel query.
    pub fn register_select_typed<D, Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        query: &Q,
    ) -> Result<RegisterResult, RegisterError>
    where
        D: BindDecode<SubqlBackend = E::Backend>,
        Q: QueryFragment<D>,
    {
        let (sql, binds) = render_typed::<D, _>(query)?;
        self.register(SubscriptionRequest::new(consumer_id, sql).binds(binds))
    }

    /// Register a follow subscription from a typed diesel UPDATE.
    pub fn register_follow_update_typed<D, Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        update: &Q,
    ) -> Result<RegisterResult, RegisterError>
    where
        D: BindDecode<SubqlBackend = E::Backend>,
        Q: QueryFragment<D>,
    {
        let (sql, binds) = render_typed::<D, _>(update)?;
        // Diesel emits binds in placeholder order (SET first, then WHERE).
        // The follow SELECT drops the SET clause, so its `?` positional
        // placeholders bind against the wrong end of the collected list
        // and its `$N` numbered placeholders reference indices that no
        // longer exist after trimming. Ask the derive helper to also report
        // how many SET binds it discarded and to renumber surviving `$N`s
        // so the caller's slice + engine round-trip line up.
        let (select_sql, set_bind_count) =
            crate::compiler::derive_update_follow_select_with_set_binds(&sql, self.dialect())?;
        let where_binds: Vec<Value<E::Backend>> = binds.into_iter().skip(set_bind_count).collect();
        self.register(SubscriptionRequest::new(consumer_id, select_sql).binds(where_binds))
    }

    /// Execute a typed diesel `INSERT ... RETURNING <pk>` and follow the
    /// inserted row(s) by their (possibly DB-minted) primary key.
    pub fn register_follow_insert<T, U, Op, C>(
        &mut self,
        consumer_id: I::ConsumerId,
        insert: InsertStatement<T, U, Op>,
        conn: &mut C,
    ) -> Result<alloc::vec::Vec<RegisterResult>, FollowInsertError>
    where
        C: LoadConnection<DefaultLoadingMode>,
        C::Backend: FollowRowDecode<SubqlBackend = E::Backend>
            + Default
            + QueryMetadata<<PkReturningInsert<T, U, Op> as Query>::SqlType>,
        <C::Backend as Backend>::QueryBuilder: Default,
        T: Table + QueryFragment<C::Backend> + Default,
        PkReturningInsert<T, U, Op>: Query + QueryFragment<C::Backend> + QueryId,
    {
        let table =
            insert_target_table_name::<T, C::Backend>().map_err(FollowInsertError::Register)?;

        let insert = insert.returning(T::default().primary_key());

        let rows: alloc::vec::Vec<alloc::vec::Vec<Value<E::Backend>>> = {
            let cursor = conn.load(insert)?;
            let mut out = alloc::vec::Vec::new();
            for row in cursor {
                let row = row?;
                let n = row.field_count();
                let mut values = alloc::vec::Vec::with_capacity(n);
                for i in 0..n {
                    let value = match row.get(i) {
                        Some(field) => {
                            <C::Backend as FollowRowDecode>::field_to_value(field.value())
                                .map_err(FollowInsertError::Register)?
                        }
                        None => Value::Null,
                    };
                    values.push(value);
                }
                out.push(values);
            }
            out
        };

        let mut results = alloc::vec::Vec::with_capacity(rows.len());
        for pk in rows {
            results.push(
                self.follow_row(consumer_id, &table, pk)
                    .map_err(FollowInsertError::Register)?,
            );
        }
        Ok(results)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn decode_scalars() {
        assert_eq!(decode_pg_bind(None, oid::INT4).unwrap(), Value::Null);
        assert_eq!(
            decode_pg_bind(Some(&5i32.to_be_bytes()), oid::INT4).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            decode_pg_bind(Some(&(-7i64).to_be_bytes()), oid::INT8).unwrap(),
            Value::Int(-7)
        );
        assert_eq!(
            decode_pg_bind(Some(&3.5f64.to_be_bytes()), oid::FLOAT8).unwrap(),
            Value::Float(3.5)
        );
        assert_eq!(
            decode_pg_bind(Some(&[1]), oid::BOOL).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            decode_pg_bind(Some(b"hello"), oid::TEXT).unwrap(),
            Value::String("hello".into())
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
            Value::Uuid(uuid::Uuid::from_bytes(bytes))
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
#[allow(clippy::unwrap_used)]
mod render_tests {
    use super::render_typed;
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use diesel::pg::Pg;
    use diesel::prelude::*;

    diesel::table! {
        users (id) {
            id -> Integer,
            name -> Text,
            active -> Bool,
        }
    }

    // Table with a `Binary` column, used by the SQLite blob-bind tests.
    #[cfg(feature = "diesel-typed-sqlite")]
    diesel::table! {
        blobs (id) {
            id -> Integer,
            name -> Text,
            payload -> Binary,
        }
    }

    #[test]
    fn render_select_with_binds() {
        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed::<Pg, _>(&query).expect("render");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert_eq!(
            binds,
            alloc::vec![
                Value::Int(5),
                Value::String("ann".into()),
                Value::Bool(true)
            ]
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
        let mut engine = SubscriptionEngine::<TestEvent<Postgres>, crate::DefaultIds, _>::new(
            catalog,
            PostgreSqlDialect {},
        );

        // Diesel renders a row query as a fully-qualified all-columns list plus a
        // parameterized WHERE. The typed path must accept it (complete column
        // list == SELECT *) and resolve the bind.
        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed::<Pg, _>(1, &query)
            .expect("typed register");
        // Deterministic: registering the same typed query dedups to the same id.
        let b = engine
            .register_select_typed::<Pg, _>(1, &query)
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
        let mut engine = SubscriptionEngine::<TestEvent<Postgres>, crate::DefaultIds, _>::new(
            catalog,
            PostgreSqlDialect {},
        );

        // A typed diesel UPDATE registers as a follow on its target rows.
        let update = diesel::update(users::table.filter(users::id.eq(5))).set(users::name.eq("x"));
        let a = engine
            .register_follow_update_typed::<Pg, _>(1, &update)
            .expect("typed update follow");
        // Deterministic: the same typed UPDATE dedups to the same subscription.
        let b = engine
            .register_follow_update_typed::<Pg, _>(1, &update)
            .expect("typed update follow again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn render_select_with_binds_sqlite() {
        use diesel::sqlite::Sqlite;

        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed::<Sqlite, _>(&query).expect("render sqlite");
        // SQLite renders positional `?` placeholders, not `$N`.
        assert!(sql.contains('?'), "sql: {sql}");
        // SQLite has no boolean storage class: `true` binds as an integer, so it
        // decodes to `Value::Int(1)` rather than `Value::Bool(true)` (contrast Pg).
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::SQLite>::Int(5),
                Value::String("ann".into()),
                Value::Int(1)
            ]
        );
    }

    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_select_sqlite_via_engine() {
        use crate::backend::SQLite;
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::SQLiteDialect;

        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        // The rendered SQL is SQLite-flavored (`?` placeholders, `"..."` idents),
        // so the engine parses it with the matching dialect.
        let mut engine = SubscriptionEngine::<TestEvent<SQLite>, crate::DefaultIds, _>::new(
            catalog,
            SQLiteDialect {},
        );

        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed::<Sqlite, _>(1, &query)
            .expect("typed register sqlite");
        let b = engine
            .register_select_typed::<Sqlite, _>(1, &query)
            .expect("typed register sqlite again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn render_select_with_binds_mysql() {
        use diesel::mysql::Mysql;

        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed::<Mysql, _>(&query).expect("render mysql");
        // MySQL renders positional `?` placeholders.
        assert!(sql.contains('?'), "sql: {sql}");
        // Like SQLite, MySQL has no boolean type: `true` binds as an integer.
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::MySql>::Int(5),
                Value::String("ann".into()),
                Value::Int(1)
            ]
        );
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn register_typed_select_mysql_via_engine() {
        use crate::backend::MySql;
        use crate::SubscriptionEngine;
        use diesel::mysql::Mysql;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::MySqlDialect;

        let catalog = ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        // MySQL renders `?` placeholders and backtick idents, so parse with the
        // matching dialect.
        let mut engine = SubscriptionEngine::<TestEvent<MySql>, crate::DefaultIds, _>::new(
            catalog,
            MySqlDialect {},
        );

        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed::<Mysql, _>(1, &query)
            .expect("typed register mysql");
        let b = engine
            .register_select_typed::<Mysql, _>(1, &query)
            .expect("typed register mysql again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn register_typed_update_follow_mysql_via_engine() {
        use crate::backend::MySql;
        use crate::SubscriptionEngine;
        use diesel::mysql::Mysql;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::MySqlDialect;

        let catalog = ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine = SubscriptionEngine::<TestEvent<MySql>, crate::DefaultIds, _>::new(
            catalog,
            MySqlDialect {},
        );

        // A typed diesel UPDATE follows its target rows: the rendered MySQL UPDATE
        // (backtick idents, `?` binds) must parse under MySqlDialect and derive the
        // follow SELECT.
        let update = diesel::update(users::table.filter(users::id.eq(5))).set(users::name.eq("x"));
        let a = engine
            .register_follow_update_typed::<Mysql, _>(1, &update)
            .expect("mysql update follow");
        let b = engine
            .register_follow_update_typed::<Mysql, _>(1, &update)
            .expect("mysql update follow again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    /// A blob bind renders to a `?` placeholder plus a `Value::Bytes`, not
    /// an inlined literal. Empty blobs round-trip too.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn render_blob_bind_sqlite() {
        use diesel::sqlite::Sqlite;

        let query = blobs::table.filter(blobs::payload.eq(alloc::vec![1u8, 2, 3]));
        let (sql, binds) = render_typed::<Sqlite, _>(&query).expect("render blob");
        assert!(sql.contains('?'), "sql: {sql}");
        // The value rides as a bind, never inlined as a hex literal.
        assert!(!sql.contains("X'"), "blob must not be inlined: {sql}");
        assert_eq!(
            binds,
            alloc::vec![Value::<crate::backend::SQLite>::Bytes(alloc::vec![1, 2, 3])]
        );

        let empty = blobs::table.filter(blobs::payload.eq(alloc::vec::Vec::<u8>::new()));
        let (_, binds) = render_typed::<Sqlite, _>(&empty).expect("render empty blob");
        assert_eq!(
            binds,
            alloc::vec![Value::<crate::backend::SQLite>::Bytes(alloc::vec![])]
        );
    }

    /// Bind order follows placeholder order: text, then blob, then integer
    /// come back in exactly that sequence, so a `?` at position N pairs with
    /// bind N.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn render_blob_bind_order_sqlite() {
        use diesel::sqlite::Sqlite;

        let query = blobs::table
            .filter(blobs::name.eq("ann"))
            .filter(blobs::payload.eq(alloc::vec![0xaau8, 0xbb]))
            .filter(blobs::id.eq(7));
        let (_, binds) = render_typed::<Sqlite, _>(&query).expect("render ordered");
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::SQLite>::String("ann".into()),
                Value::Bytes(alloc::vec![0xaa, 0xbb]),
                Value::Int(7),
            ]
        );
    }

    /// Regression guard on the full `OwnedSqliteBindValue` mapping: every
    /// variant decodes as before, and `Binary` now maps to `Value::Bytes`
    /// (empty included) rather than being rejected.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn owned_sqlite_to_value_covers_all_variants() {
        use super::owned_sqlite_to_value;
        use crate::backend::SQLite;
        use diesel::sqlite::OwnedSqliteBindValue as V;

        let dec = owned_sqlite_to_value;
        assert_eq!(dec(&V::Null), Value::<SQLite>::Null);
        assert_eq!(dec(&V::I32(5)), Value::Int(5));
        assert_eq!(dec(&V::I64(9)), Value::Int(9));
        assert_eq!(dec(&V::F64(1.5)), Value::Float(1.5));
        assert_eq!(dec(&V::String("hi".into())), Value::String("hi".into()));
        assert_eq!(
            dec(&V::Binary(alloc::vec![1u8, 2, 3].into())),
            Value::Bytes(alloc::vec![1, 2, 3])
        );
        assert_eq!(
            dec(&V::Binary(alloc::vec::Vec::<u8>::new().into())),
            Value::Bytes(alloc::vec![])
        );
    }

    /// End to end: a typed diesel query filtering a blob column registers
    /// through `render_typed` plus placeholder resolution and matches a CDC
    /// row carrying the same bytes, while a row with different bytes does
    /// not. Depends on the `Value::Bytes` placeholder-resolution path.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_blob_matches_sqlite() {
        use crate::backend::SQLite;
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::SQLiteDialect;

        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE blobs (id INTEGER PRIMARY KEY, name TEXT, payload BLOB);",
        )
        .expect("catalog");
        let table_id = crate::catalog_helpers::table_id(&catalog, "blobs").expect("blobs table");
        let mut engine = SubscriptionEngine::<TestEvent<SQLite>, crate::DefaultIds, _>::new(
            catalog,
            SQLiteDialect {},
        );

        let bytes = alloc::vec![0xde, 0xad, 0xbe, 0xef];
        let query = blobs::table.filter(blobs::payload.eq(bytes.clone()));
        engine
            .register_select_typed::<Sqlite, _>(1, &query)
            .expect("register typed blob");

        let hit = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![Value::Int(1), Value::Null, Value::Bytes(bytes)],
        )
        .with_pk_columns([0u16]);
        assert_eq!(
            engine.consumers(&hit).expect("dispatch hit").inserted(),
            &[1u64]
        );

        let miss = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![Value::Int(2), Value::Null, Value::Bytes(alloc::vec![0x00])],
        )
        .with_pk_columns([0u16]);
        assert!(
            engine
                .consumers(&miss)
                .expect("dispatch miss")
                .inserted()
                .is_empty(),
            "wrong payload bytes notified no consumer"
        );
    }

    /// A typed SQLite UPDATE with a blob predicate follows its target rows.
    /// The SET bind (`name = 'x'`) is trimmed from the front while the blob
    /// WHERE bind survives `skip(set_bind_count)` and placeholder
    /// resolution, so the derived follow SELECT matches a row carrying those
    /// bytes and rejects one that does not. Also the only SQLite
    /// update-follow coverage.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_update_follow_blob_sqlite() {
        use crate::backend::SQLite;
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::SQLiteDialect;

        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE blobs (id INTEGER PRIMARY KEY, name TEXT, payload BLOB);",
        )
        .expect("catalog");
        let table_id = crate::catalog_helpers::table_id(&catalog, "blobs").expect("blobs table");
        let mut engine = SubscriptionEngine::<TestEvent<SQLite>, crate::DefaultIds, _>::new(
            catalog,
            SQLiteDialect {},
        );

        let bytes = alloc::vec![0xcau8, 0xfe];
        // UPDATE blobs SET name = 'x' WHERE payload = ? derives
        // SELECT * FROM blobs WHERE payload = ?, keeping the blob bind after
        // the SET text bind is trimmed.
        let update = diesel::update(blobs::table.filter(blobs::payload.eq(bytes.clone())))
            .set(blobs::name.eq("x"));
        engine
            .register_follow_update_typed::<Sqlite, _>(1, &update)
            .expect("register update follow");

        let hit = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![
                Value::Int(1),
                Value::String("y".into()),
                Value::Bytes(bytes)
            ],
        )
        .with_pk_columns([0u16]);
        assert_eq!(
            engine.consumers(&hit).expect("dispatch hit").inserted(),
            &[1u64]
        );

        let miss = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![
                Value::Int(2),
                Value::String("y".into()),
                Value::Bytes(alloc::vec![0x00])
            ],
        )
        .with_pk_columns([0u16]);
        assert!(
            engine
                .consumers(&miss)
                .expect("dispatch miss")
                .inserted()
                .is_empty(),
            "wrong payload bytes notified no consumer"
        );
    }
}
