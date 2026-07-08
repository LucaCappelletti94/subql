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

/// Render a typed diesel query to placeholder SQL plus its bind values decoded
/// to [`Cell`]s, in placeholder order.
///
/// Generic over the backend that decodes its own binds ([`BindDecode`]):
/// `Pg` yields `$N` placeholders and decodes the Postgres binary wire format by
/// type OID, `Sqlite` yields `?` placeholders and reads its typed bind values
/// directly.
///
/// # Errors
/// Returns [`RegisterError::UnsupportedSql`] if diesel fails to render the query
/// or a bind uses a type outside the supported scalar set (bool, integer, float,
/// text, uuid).
pub fn render_typed<B, Q>(query: &Q) -> Result<(String, Vec<Cell>), RegisterError>
where
    B: BindDecode,
    Q: QueryFragment<B>,
{
    B::render_sql_and_binds(query)
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

/// Render a typed diesel query to placeholder SQL plus its bind [`Cell`]s, for
/// one backend.
///
/// The bind side is backend-specific: Postgres decodes the binary wire format by
/// OID, SQLite reads its typed bind values. This is the input-side counterpart to
/// [`FollowRowDecode`], which decodes the values an executed query hands *back*.
pub trait BindDecode: Backend {
    /// Render `query` to `(placeholder SQL, ordered bind cells)`.
    ///
    /// # Errors
    /// [`RegisterError::UnsupportedSql`] if rendering fails or a bind uses a type
    /// outside the supported scalar set.
    fn render_sql_and_binds<Q>(query: &Q) -> Result<(String, Vec<Cell>), RegisterError>
    where
        Q: QueryFragment<Self>;
}

impl BindDecode for Pg {
    fn render_sql_and_binds<Q>(query: &Q) -> Result<(String, Vec<Cell>), RegisterError>
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
        let mut cells = Vec::with_capacity(collector.binds.len());
        for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
            let type_oid = meta.oid().map_err(|_| {
                RegisterError::UnsupportedSql("diesel bind has an unresolved type OID".to_string())
            })?;
            cells.push(decode_pg_bind(bytes.as_deref(), type_oid)?);
        }
        Ok((sql, cells))
    }
}

#[cfg(feature = "diesel-typed-sqlite")]
impl BindDecode for diesel::sqlite::Sqlite {
    fn render_sql_and_binds<Q>(query: &Q) -> Result<(String, Vec<Cell>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        use diesel::query_builder::MoveableBindCollector;
        use diesel::sqlite::SqliteBindCollector;

        let sql = render_sql::<Self, _>(query)?;

        // SQLite keeps binds as typed values, not wire bytes. Collect, then move
        // the borrowed values out to owned ones we can read (`moveable`).
        let mut collector = SqliteBindCollector::default();
        query
            .collect_binds(&mut collector, &mut (), &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let data = collector.moveable();
        let mut cells = Vec::with_capacity(data.binds.len());
        for (value, _ty) in &data.binds {
            cells.push(owned_sqlite_to_cell(value)?);
        }
        Ok((sql, cells))
    }
}

/// Map an owned SQLite bind value to a [`Cell`]. SQLite has no boolean or uuid
/// storage class (they arrive as integer or text), and a BLOB has no `Cell`
/// representation, so it is rejected.
#[cfg(feature = "diesel-typed-sqlite")]
fn owned_sqlite_to_cell(
    value: &diesel::sqlite::OwnedSqliteBindValue,
) -> Result<Cell, RegisterError> {
    use diesel::sqlite::OwnedSqliteBindValue as V;
    Ok(match value {
        V::I32(i) => Cell::Int(i64::from(*i)),
        V::I64(i) => Cell::Int(*i),
        V::F64(f) => Cell::Float(*f),
        V::String(s) => Cell::String(s.as_ref().into()),
        V::Null => Cell::Null,
        V::Binary(_) => {
            return Err(RegisterError::UnsupportedSql(
                "unsupported SQLite BLOB bind (only integer, float and text are supported)"
                    .to_string(),
            ))
        }
    })
}

#[cfg(feature = "diesel-typed-mysql")]
impl BindDecode for diesel::mysql::Mysql {
    fn render_sql_and_binds<Q>(query: &Q) -> Result<(String, Vec<Cell>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        let sql = render_sql::<Self, _>(query)?;

        // MySQL binds are serialized wire bytes (host-native endian, these are
        // libmysqlclient bind buffers) tagged with a `MysqlType`.
        let mut collector = RawBytesBindCollector::<Self>::new();
        query
            .collect_binds(&mut collector, &mut (), &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let mut cells = Vec::with_capacity(collector.binds.len());
        for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
            cells.push(decode_mysql_bind(bytes.as_deref(), *meta)?);
        }
        Ok((sql, cells))
    }
}

/// Decode one MySQL bind (native-endian client buffer) into a [`Cell`] by its
/// `MysqlType`. `None` bytes are a SQL NULL. Integers read by their actual byte
/// length, which also handles `bool` (serialized as a 4-byte `i32`).
#[cfg(feature = "diesel-typed-mysql")]
fn decode_mysql_bind(
    bytes: Option<&[u8]>,
    ty: diesel::mysql::MysqlType,
) -> Result<Cell, RegisterError> {
    use diesel::mysql::MysqlType as T;
    let Some(b) = bytes else {
        return Ok(Cell::Null);
    };
    let cell = match ty {
        T::Tiny | T::Short | T::Long | T::LongLong => Cell::Int(mysql_int_ne(b, true)?),
        T::UnsignedTiny | T::UnsignedShort | T::UnsignedLong | T::UnsignedLongLong => {
            Cell::Int(mysql_int_ne(b, false)?)
        }
        T::Float => Cell::Float(f64::from(f32::from_ne_bytes(fixed(b, "mysql float")?))),
        T::Double => Cell::Float(f64::from_ne_bytes(fixed(b, "mysql double")?)),
        T::String | T::Enum | T::Set => {
            let s = core::str::from_utf8(b).map_err(|_| {
                RegisterError::UnsupportedSql("diesel MySQL text bind is not valid UTF-8".to_string())
            })?;
            Cell::String(s.into())
        }
        other => {
            return Err(RegisterError::UnsupportedSql(format!(
                "unsupported diesel MySQL bind type ({other:?}); only integer, float, text and enum/set are supported"
            )))
        }
    };
    Ok(cell)
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

/// Decode a column value returned by an executed query into a [`Cell`], per
/// backend.
///
/// Lets an `INSERT ... RETURNING` row be read without a compile-time row struct.
/// Implemented for `Pg` now; other backends slot in later without changing
/// [`SubscriptionEngine::register_follow_insert`]'s signature.
pub trait FollowRowDecode: Backend {
    /// Decode one field's raw value (or SQL NULL) into a `Cell`.
    ///
    /// # Errors
    /// [`RegisterError::UnsupportedSql`] for a column type outside the supported
    /// scalar set.
    fn field_to_cell(value: Option<Self::RawValue<'_>>) -> Result<Cell, RegisterError>;
}

impl FollowRowDecode for Pg {
    fn field_to_cell(value: Option<PgValue<'_>>) -> Result<Cell, RegisterError> {
        value.map_or(Ok(Cell::Null), |v| {
            decode_pg_bind(Some(v.as_bytes()), v.get_oid().get())
        })
    }
}

#[cfg(feature = "diesel-typed-sqlite")]
impl FollowRowDecode for diesel::sqlite::Sqlite {
    fn field_to_cell(value: Option<Self::RawValue<'_>>) -> Result<Cell, RegisterError> {
        use diesel::sqlite::SqliteType;
        // SQLite hands back typed values (`read_*` take `&mut self`), so no wire
        // decode is needed.
        let Some(mut v) = value else {
            return Ok(Cell::Null);
        };
        Ok(match v.value_type() {
            None => Cell::Null,
            Some(SqliteType::SmallInt | SqliteType::Integer | SqliteType::Long) => {
                Cell::Int(v.read_long())
            }
            Some(SqliteType::Float | SqliteType::Double) => Cell::Float(v.read_double()),
            Some(SqliteType::Text) => Cell::String(v.read_text().into()),
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
impl<D, I, DB> SubscriptionEngine<D, I, DB>
where
    D: Dialect,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Register a subscription from a typed diesel query.
    ///
    /// The query is rendered to SQL plus bind values (see [`render_typed`]) and
    /// registered like any other subscription, so it is checked against your
    /// diesel `table!` schema at compile time. Projection rules match
    /// [`SubscriptionEngine::register_select`].
    ///
    /// Generic over the backend `B` (`Pg`, or `Sqlite` under
    /// `diesel-typed-sqlite`); the rendered SQL is that backend's flavor, so the
    /// engine's `Dialect` must match it.
    ///
    /// # Errors
    /// Propagates rendering errors from [`render_typed`] (e.g. an unsupported
    /// bind type) and registration errors from [`SubscriptionEngine::register`].
    pub fn register_select_typed<B, Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        query: &Q,
    ) -> Result<RegisterResult, RegisterError>
    where
        B: BindDecode,
        Q: QueryFragment<B>,
    {
        let (sql, binds) = render_typed::<B, _>(query)?;
        self.register(SubscriptionRequest::new(consumer_id, sql).binds(binds))
    }

    /// Register a follow subscription from a typed diesel UPDATE.
    ///
    /// The UPDATE is rendered to SQL + binds (see [`render_typed`]) and its
    /// target rows become a standing `SELECT * ... WHERE <the UPDATE's WHERE>`
    /// subscription. Nothing is executed. See
    /// [`SubscriptionEngine::register_follow_update`].
    ///
    /// Generic over the backend `B` (`Pg`, or `Sqlite` under
    /// `diesel-typed-sqlite`); the rendered SQL is that backend's flavor, so the
    /// engine's `Dialect` must match it.
    ///
    /// # Errors
    /// Propagates rendering errors from [`render_typed`] and the follow-shape /
    /// registration errors from
    /// [`SubscriptionEngine::register_follow_update_with_binds`].
    pub fn register_follow_update_typed<B, Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        update: &Q,
    ) -> Result<RegisterResult, RegisterError>
    where
        B: BindDecode,
        Q: QueryFragment<B>,
    {
        let (sql, binds) = render_typed::<B, _>(update)?;
        self.register_follow_update_with_binds(consumer_id, sql, binds)
    }

    /// Execute a typed diesel `INSERT ... RETURNING <pk>` and follow the inserted
    /// row(s) by their (possibly DB-minted) primary key.
    ///
    /// This is the one method that writes to the database (via the caller's
    /// connection). It holds `&mut self` across both executing the insert and
    /// registering the follow, so the insert's own CDC event cannot be dispatched
    /// until the follow exists: the follow is guaranteed to observe it (no loss),
    /// by the engine's single-threaded `&mut self` ordering. Nothing is
    /// fabricated, so there is nothing to de-duplicate.
    ///
    /// Pass the insert **without** a `RETURNING` clause: this method appends
    /// `RETURNING <primary key>` derived from the table type, so only the key is
    /// read and it always matches the table's primary key, in order. A multi-row
    /// insert yields one follow per returned row.
    ///
    /// Both the followed table and the returned key columns are taken from the
    /// insert's own diesel table type, so neither can disagree with the statement
    /// being executed.
    ///
    /// `RETURNING` is Postgres/SQLite/MariaDB; on stock MySQL (no `RETURNING`)
    /// fetch the id yourself and use [`SubscriptionEngine::follow_row`].
    ///
    /// # Errors
    /// [`FollowInsertError::Load`] if the insert fails to execute or its rows fail
    /// to read; [`FollowInsertError::Register`] if a follow cannot be built or
    /// registered.
    pub fn register_follow_insert<T, U, Op, C>(
        &mut self,
        consumer_id: I::ConsumerId,
        insert: InsertStatement<T, U, Op>,
        conn: &mut C,
    ) -> Result<alloc::vec::Vec<RegisterResult>, FollowInsertError>
    where
        C: LoadConnection<DefaultLoadingMode>,
        C::Backend: FollowRowDecode
            + Default
            + QueryMetadata<<PkReturningInsert<T, U, Op> as Query>::SqlType>,
        <C::Backend as Backend>::QueryBuilder: Default,
        T: Table + QueryFragment<C::Backend> + Default,
        PkReturningInsert<T, U, Op>: Query + QueryFragment<C::Backend> + QueryId,
    {
        // The table comes from the insert's diesel type, never a restated string.
        let table =
            insert_target_table_name::<T, C::Backend>().map_err(FollowInsertError::Register)?;

        // Return the table's primary key, derived from the type: the caller does
        // not (and cannot) restate which columns identify the row.
        let insert = insert.returning(T::default().primary_key());

        // Execute the insert and read each RETURNING row's columns as Cells.
        let rows: alloc::vec::Vec<alloc::vec::Vec<Cell>> = {
            let cursor = conn.load(insert)?;
            let mut out = alloc::vec::Vec::new();
            for row in cursor {
                let row = row?;
                let n = row.field_count();
                let mut cells = alloc::vec::Vec::with_capacity(n);
                for i in 0..n {
                    let cell = match row.get(i) {
                        Some(field) => {
                            <C::Backend as FollowRowDecode>::field_to_cell(field.value())
                                .map_err(FollowInsertError::Register)?
                        }
                        None => Cell::Null,
                    };
                    cells.push(cell);
                }
                out.push(cells);
            }
            out
        };

        // Register one follow per returned row (still within this `&mut self`).
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
#[allow(clippy::unwrap_used)]
mod render_tests {
    use super::{render_typed, Cell};
    use diesel::pg::Pg;
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
        let (sql, binds) = render_typed::<Pg, _>(&query).expect("render");
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
        let mut engine =
            SubscriptionEngine::<_, crate::DefaultIds, _>::new(catalog, PostgreSqlDialect {});

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
        // decodes to `Cell::Int(1)` rather than `Cell::Bool(true)` (contrast Pg).
        assert_eq!(
            binds,
            alloc::vec![Cell::Int(5), Cell::String("ann".into()), Cell::Int(1)]
        );
    }

    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_select_sqlite_via_engine() {
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        // The rendered SQL is SQLite-flavored (`?` placeholders, `"..."` idents),
        // so the engine must parse it with the matching dialect.
        let mut engine =
            SubscriptionEngine::<_, crate::DefaultIds, _>::new(catalog, SQLiteDialect {});

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
            alloc::vec![Cell::Int(5), Cell::String("ann".into()), Cell::Int(1)]
        );
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn register_typed_select_mysql_via_engine() {
        use crate::SubscriptionEngine;
        use diesel::mysql::Mysql;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        // MySQL renders `?` placeholders and backtick idents, so parse with the
        // matching dialect.
        let mut engine =
            SubscriptionEngine::<_, crate::DefaultIds, _>::new(catalog, MySqlDialect {});

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
        use crate::SubscriptionEngine;
        use diesel::mysql::Mysql;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine =
            SubscriptionEngine::<_, crate::DefaultIds, _>::new(catalog, MySqlDialect {});

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
}
