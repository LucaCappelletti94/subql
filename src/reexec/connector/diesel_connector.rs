#![allow(clippy::type_complexity)]
//! Generic sync [`Connector`] backed by a single diesel connection.

#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
use super::diesel_backend::boxed_postgres_read_query;
use super::diesel_backend::{boxed_read_query, DieselBackend};
use super::{
    run_setup_statements, Connector, ReadQuery, RowPage, ScalarRowError, SessionSetup, Snapshot,
};
use crate::backend::{BuiltinKind, ScalarKind, Value};
use alloc::string::String;
use core::cell::RefCell;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::query_dsl::LoadQuery;
use diesel::sql_types::{
    BigInt, Binary, Bool, Date, Double, Json, Nullable, Numeric, Text, Time, Timestamp,
};
use diesel::{Connection, QueryResult, RunQueryDsl};

/// Returned when the aggregate seed row has the wrong column count.
#[cfg(feature = "executor-diesel")]
#[derive(Debug, thiserror::Error)]
#[error("read returned {got} columns, expected {expected}")]
struct ReadShapeError {
    expected: usize,
    got: usize,
}

/// Sync [`Connector`] backed by a single diesel [`Connection`].
///
/// Holds the connection in a [`RefCell`] for the interior-mutability the
/// trait's `&self` requires. Not `Send`/`Sync` (diesel connections are
/// not `Send`). For multi-threaded use, either keep the connector
/// thread-local or implement [`Connector`] yourself over a connection
/// pool (`r2d2::Pool<ConnectionManager<C>>`, `deadpool`, `bb8`).
///
/// Type parameters:
/// * `C: Connection` - any diesel connection.
/// * `B: DieselBackend` - the subql [`crate::backend::Backend`] whose [`Value`] shape the
///   connector produces. Pick to match the diesel connection's backend
///   ([`crate::backend::Postgres`] for `PgConnection`,
///   [`crate::backend::SQLite`] for `SqliteConnection`,
///   [`crate::backend::MySql`] for `MysqlConnection`).
///
/// Bounds on the [`Connector`] impl: an HRTB on [`diesel::sql_query`] so the
/// nullable scalar rows (`Nullable<BigInt>`, `Nullable<Double>`,
/// `Nullable<Text>`) decode. PostgreSQL, MySQL, and SQLite all satisfy
/// these.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure
/// (network drop, statement error, decoding mismatch).
#[cfg(feature = "executor-diesel")]
pub struct DieselConnector<C: Connection, B: DieselBackend, S = ()> {
    conn: RefCell<C>,
    _backend: core::marker::PhantomData<fn() -> B>,
    _setup: core::marker::PhantomData<fn() -> S>,
}

#[cfg(feature = "executor-diesel")]
impl<C: Connection, B: DieselBackend> DieselConnector<C, B> {
    /// Wrap an owned connection with no session setup. The connector takes
    /// exclusive ownership and serializes access through interior mutability.
    pub const fn new(conn: C) -> Self {
        Self {
            conn: RefCell::new(conn),
            _backend: core::marker::PhantomData,
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel")]
impl<C: Connection, B: DieselBackend, S: SessionSetup> DieselConnector<C, B, S> {
    /// Wrap an owned connection whose reads run the setup statements carried by
    /// the per-read [`SessionSetup`] value `S`. `S` is named by the return
    /// type, since the value itself arrives per read rather than being stored.
    pub const fn with_session_setup(conn: C) -> Self {
        Self {
            conn: RefCell::new(conn),
            _backend: core::marker::PhantomData,
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
pub struct IntRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    pub v: Option<i64>,
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
pub struct FloatRow {
    #[diesel(sql_type = Nullable<Double>)]
    pub v: Option<f64>,
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
pub struct TextRow {
    #[diesel(sql_type = Nullable<Text>)]
    pub v: Option<String>,
}

/// Route the projected column through the `Nullable<BigInt|Double|Text>`
/// row shape that matches `kind`, then lift into [`Value<B>`].
///
/// Aggregate-only column kinds ([`BuiltinKind::Int`] / [`BuiltinKind::Float`])
/// map to the numeric rows; every other kind reads through the `Text`
/// row. Decimals are carried as text through this path so precision is
/// not lost through `f64`.
#[cfg(feature = "executor-diesel")]
pub(super) fn load_scalar<C, B>(
    conn: &mut C,
    query: &ReadQuery<'_, B>,
    kind: BuiltinKind,
) -> QueryResult<Value<B>>
where
    C: Connection,
    C::Backend: diesel::backend::DieselReserveSpecialization
        + diesel::sql_types::HasSqlType<Bool>
        + diesel::sql_types::HasSqlType<BigInt>
        + diesel::sql_types::HasSqlType<Double>
        + diesel::sql_types::HasSqlType<Text>
        + diesel::sql_types::HasSqlType<Binary>
        + diesel::sql_types::HasSqlType<Timestamp>
        + diesel::sql_types::HasSqlType<Date>
        + diesel::sql_types::HasSqlType<Time>
        + diesel::sql_types::HasSqlType<Numeric>
        + diesel::sql_types::HasSqlType<Json>,
    B: DieselBackend,
    bool: diesel::serialize::ToSql<Bool, C::Backend>,
    i64: diesel::serialize::ToSql<BigInt, C::Backend>,
    f64: diesel::serialize::ToSql<Double, C::Backend>,
    for<'b> &'b str: diesel::serialize::ToSql<Text, C::Backend>,
    for<'b> &'b [u8]: diesel::serialize::ToSql<Binary, C::Backend>,
    for<'b> &'b chrono::NaiveDateTime: diesel::serialize::ToSql<Timestamp, C::Backend>,
    for<'b> &'b chrono::NaiveDate: diesel::serialize::ToSql<Date, C::Backend>,
    for<'b> &'b chrono::NaiveTime: diesel::serialize::ToSql<Time, C::Backend>,
    for<'b> &'b bigdecimal::BigDecimal: diesel::serialize::ToSql<Numeric, C::Backend>,
    for<'b> &'b serde_json::Value: diesel::serialize::ToSql<Json, C::Backend>,
    for<'q> BoxedSqlQuery<'q, C::Backend, SqlQuery>:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    let value = match kind {
        BuiltinKind::Int => {
            let widened = alloc::format!(
                "SELECT CAST(({}) AS {cast}) AS v",
                query.sql(),
                cast = B::int_cast_type()
            );
            let widened = ReadQuery::borrowed(&widened, query.binds());
            let value = boxed_read_query::<C::Backend, B>(&widened)?
                .get_result::<IntRow>(conn)?
                .v
                .map_or(Value::Null, B::value_from_i64);
            value
        }
        BuiltinKind::Float => boxed_read_query::<C::Backend, B>(query)?
            .get_result::<FloatRow>(conn)?
            .v
            .map_or(Value::Null, B::value_from_f64),
        BuiltinKind::Bool
        | BuiltinKind::String
        | BuiltinKind::Bytes
        | BuiltinKind::Uuid
        | BuiltinKind::Timestamp
        | BuiltinKind::TimestampTz
        | BuiltinKind::Date
        | BuiltinKind::Time
        | BuiltinKind::Decimal
        | BuiltinKind::Json
        | BuiltinKind::Jsonb => boxed_read_query::<C::Backend, B>(query)?
            .get_result::<TextRow>(conn)?
            .v
            .map_or(Value::Null, B::value_from_string),
    };
    Ok(B::decode_group_value(ScalarKind::from(kind), value).unwrap_or(Value::Missing))
}

/// Decodes one aggregate seed row using its runtime database types.
#[cfg(feature = "executor-diesel")]
pub(super) fn load_scalar_row<C, B>(
    conn: &mut C,
    query: &ReadQuery<'_, B>,
    kinds: &[BuiltinKind],
) -> QueryResult<alloc::vec::Vec<Value<B>>>
where
    C: Connection,
    C::Backend: crate::diesel_decode::RowFieldDecode
        + diesel::backend::DieselReserveSpecialization
        + diesel::sql_types::HasSqlType<Bool>
        + diesel::sql_types::HasSqlType<BigInt>
        + diesel::sql_types::HasSqlType<Double>
        + diesel::sql_types::HasSqlType<Text>
        + diesel::sql_types::HasSqlType<Binary>
        + diesel::sql_types::HasSqlType<Timestamp>
        + diesel::sql_types::HasSqlType<Date>
        + diesel::sql_types::HasSqlType<Time>
        + diesel::sql_types::HasSqlType<Numeric>
        + diesel::sql_types::HasSqlType<Json>,
    B: DieselBackend + crate::diesel_decode::SpellCanonical,
    bool: diesel::serialize::ToSql<Bool, C::Backend>,
    i64: diesel::serialize::ToSql<BigInt, C::Backend>,
    f64: diesel::serialize::ToSql<Double, C::Backend>,
    for<'b> &'b str: diesel::serialize::ToSql<Text, C::Backend>,
    for<'b> &'b [u8]: diesel::serialize::ToSql<Binary, C::Backend>,
    for<'b> &'b chrono::NaiveDateTime: diesel::serialize::ToSql<Timestamp, C::Backend>,
    for<'b> &'b chrono::NaiveDate: diesel::serialize::ToSql<Date, C::Backend>,
    for<'b> &'b chrono::NaiveTime: diesel::serialize::ToSql<Time, C::Backend>,
    for<'b> &'b bigdecimal::BigDecimal: diesel::serialize::ToSql<Numeric, C::Backend>,
    for<'b> &'b serde_json::Value: diesel::serialize::ToSql<Json, C::Backend>,
    for<'q> BoxedSqlQuery<'q, C::Backend, SqlQuery>:
        LoadQuery<'q, C, crate::diesel_decode::DynamicRow<B>>,
{
    let row = boxed_read_query::<C::Backend, B>(query)?
        .get_result::<crate::diesel_decode::DynamicRow<B>>(conn)?;
    if row.values.len() != kinds.len() {
        return Err(diesel::result::Error::DeserializationError(Box::new(
            ReadShapeError {
                expected: kinds.len(),
                got: row.values.len(),
            },
        )));
    }
    Ok(row
        .values
        .into_iter()
        .zip(kinds)
        .map(|(value, kind)| {
            B::decode_group_value(ScalarKind::from(*kind), value).unwrap_or(Value::Missing)
        })
        .collect())
}

#[cfg(feature = "executor-diesel")]
/// Reading a row needs a decoder for the connection's backend, so this impl
/// asks for one. Every backend subql speaks has it behind that backend's own
/// feature (`diesel-typed` for Postgres, `diesel-typed-sqlite`,
/// `diesel-typed-mysql`, `executor-diesel-postgres`, `executor-diesel-mysql`),
/// and a connector that cannot decode a row cannot honestly claim to read one.
impl<C, B, S> Connector for DieselConnector<C, B, S>
where
    C: Connection + diesel::connection::LoadConnection<diesel::connection::DefaultLoadingMode>,
    C::Backend: crate::diesel_decode::RowFieldDecode
        + diesel::backend::DieselReserveSpecialization
        + diesel::sql_types::HasSqlType<Bool>
        + diesel::sql_types::HasSqlType<BigInt>
        + diesel::sql_types::HasSqlType<Double>
        + diesel::sql_types::HasSqlType<Text>
        + diesel::sql_types::HasSqlType<Binary>
        + diesel::sql_types::HasSqlType<Timestamp>
        + diesel::sql_types::HasSqlType<Date>
        + diesel::sql_types::HasSqlType<Time>
        + diesel::sql_types::HasSqlType<Numeric>
        + diesel::sql_types::HasSqlType<Json>,
    B: DieselBackend + crate::diesel_decode::SpellCanonical,
    S: SessionSetup,
    bool: diesel::serialize::ToSql<Bool, C::Backend>,
    i64: diesel::serialize::ToSql<BigInt, C::Backend>,
    f64: diesel::serialize::ToSql<Double, C::Backend>,
    for<'b> &'b str: diesel::serialize::ToSql<Text, C::Backend>,
    for<'b> &'b [u8]: diesel::serialize::ToSql<Binary, C::Backend>,
    for<'b> &'b chrono::NaiveDateTime: diesel::serialize::ToSql<Timestamp, C::Backend>,
    for<'b> &'b chrono::NaiveDate: diesel::serialize::ToSql<Date, C::Backend>,
    for<'b> &'b chrono::NaiveTime: diesel::serialize::ToSql<Time, C::Backend>,
    for<'b> &'b bigdecimal::BigDecimal: diesel::serialize::ToSql<Numeric, C::Backend>,
    for<'b> &'b serde_json::Value: diesel::serialize::ToSql<Json, C::Backend>,
    for<'q> SqlQuery: LoadQuery<'q, C, IntRow>
        + LoadQuery<'q, C, FloatRow>
        + LoadQuery<'q, C, TextRow>
        + diesel::query_dsl::methods::ExecuteDsl<C, C::Backend>,
    for<'q> BoxedSqlQuery<'q, C::Backend, SqlQuery>: LoadQuery<'q, C, IntRow>
        + LoadQuery<'q, C, FloatRow>
        + LoadQuery<'q, C, TextRow>
        + LoadQuery<'q, C, crate::diesel_decode::DynamicRow<B>>,
{
    type AuthContext = S;
    type Error = diesel::result::Error;
    /// Backend-agnostic v1 default: this connector does not read the
    /// underlying source's position. PG-aware variants
    /// (`PgDieselConnector`) override this to `PgLsn` and read
    /// `pg_current_wal_lsn()` before the snapshot transaction.
    type Checkpoint = crate::NoCheckpoint;
    type Backend = B;

    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, B>,
        kind: BuiltinKind,
        auth: &S,
    ) -> Result<(Value<B>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        let setup = auth.setup_statements();
        // Decision 5: this path opens no transaction today, so a caller
        // supplying nothing gets that behaviour byte for byte, and one
        // supplying statements gets a real transaction so the setup takes hold.
        let value = if setup.is_empty() {
            load_scalar::<_, B>(&mut *conn, query, kind)?
        } else {
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                run_setup_statements(conn, setup)?;
                load_scalar::<_, B>(conn, query, kind)
            })?
        };
        Ok((value, None))
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, B>,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<B>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.conn.borrow_mut();
        let setup = auth.setup_statements();
        // Decision 5, as in `execute_scalar`: a transaction only when the
        // caller supplied statements.
        let value = if setup.is_empty() {
            load_page::<_, B>(&mut *conn, query, max_bytes)?
        } else {
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                run_setup_statements(conn, setup)?;
                load_page::<_, B>(conn, query, max_bytes)
            })?
        };
        Ok(Snapshot {
            value,
            checkpoint: None,
        })
    }

    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, B>,
        kinds: &[BuiltinKind],
        auth: &S,
    ) -> Result<(alloc::vec::Vec<Value<B>>, Option<Self::Checkpoint>), ScalarRowError<Self::Error>>
    {
        let mut conn = self.conn.borrow_mut();
        // Read every component in one transaction so a variance seed's sum,
        // sum-of-squares, and count come from a single snapshot. This basic
        // connector uses the connection's default isolation; the LSN-aware
        // `PgDieselConnector` additionally pins REPEATABLE READ.
        let values = diesel::connection::Connection::transaction(&mut *conn, |conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            load_scalar_row::<_, B>(conn, query, kinds)
        })
        .map_err(ScalarRowError::Connector)?;
        Ok((values, None))
    }
}

/// Read one bounded page through a typed query.
#[cfg(feature = "executor-diesel")]
pub(super) fn load_page<C, B>(
    conn: &mut C,
    query: &ReadQuery<'_, B>,
    max_bytes: usize,
) -> QueryResult<RowPage<B>>
where
    C: diesel::connection::LoadConnection<diesel::connection::DefaultLoadingMode>,
    C::Backend: crate::diesel_decode::RowFieldDecode
        + diesel::backend::DieselReserveSpecialization
        + diesel::sql_types::HasSqlType<Bool>
        + diesel::sql_types::HasSqlType<BigInt>
        + diesel::sql_types::HasSqlType<Double>
        + diesel::sql_types::HasSqlType<Text>
        + diesel::sql_types::HasSqlType<Binary>
        + diesel::sql_types::HasSqlType<Timestamp>
        + diesel::sql_types::HasSqlType<Date>
        + diesel::sql_types::HasSqlType<Time>
        + diesel::sql_types::HasSqlType<Numeric>
        + diesel::sql_types::HasSqlType<Json>,
    B: crate::diesel_decode::SpellCanonical + DieselBackend,
    bool: diesel::serialize::ToSql<Bool, C::Backend>,
    i64: diesel::serialize::ToSql<BigInt, C::Backend>,
    f64: diesel::serialize::ToSql<Double, C::Backend>,
    for<'b> &'b str: diesel::serialize::ToSql<Text, C::Backend>,
    for<'b> &'b [u8]: diesel::serialize::ToSql<Binary, C::Backend>,
    for<'b> &'b chrono::NaiveDateTime: diesel::serialize::ToSql<Timestamp, C::Backend>,
    for<'b> &'b chrono::NaiveDate: diesel::serialize::ToSql<Date, C::Backend>,
    for<'b> &'b chrono::NaiveTime: diesel::serialize::ToSql<Time, C::Backend>,
    for<'b> &'b bigdecimal::BigDecimal: diesel::serialize::ToSql<Numeric, C::Backend>,
    for<'b> &'b serde_json::Value: diesel::serialize::ToSql<Json, C::Backend>,
    for<'q> BoxedSqlQuery<'q, C::Backend, SqlQuery>:
        LoadQuery<'q, C, crate::diesel_decode::DynamicRow<B>>,
{
    collect_page(conn, boxed_read_query::<C::Backend, B>(query)?, max_bytes)
}

#[cfg(feature = "executor-diesel")]
fn collect_page<'conn, C, B, Q>(
    conn: &'conn mut C,
    query: Q,
    max_bytes: usize,
) -> QueryResult<RowPage<B>>
where
    C: diesel::connection::LoadConnection<diesel::connection::DefaultLoadingMode>,
    B: crate::diesel_decode::SpellCanonical,
    Q: LoadQuery<'conn, C, crate::diesel_decode::DynamicRow<B>>,
{
    let mut columns = alloc::vec::Vec::new();
    let mut rows: alloc::vec::Vec<alloc::vec::Vec<Value<B>>> = alloc::vec::Vec::new();
    let mut spent = 0_usize;
    let mut more = false;
    let iter = diesel::query_dsl::LoadQuery::<'conn, C, crate::diesel_decode::DynamicRow<B>>::internal_load(
        query,
        conn,
    )?;
    for row in iter {
        let row = row?;
        if columns.is_empty() {
            columns = row.columns;
        }
        let cost = RowPage::<B>::row_bytes_of(&row.values);
        if !rows.is_empty() && spent + cost > max_bytes {
            more = true;
            break;
        }
        spent += cost;
        rows.push(row.values);
    }
    Ok(RowPage {
        columns,
        rows,
        more,
    })
}

/// Read one bounded page using the Postgres-specific boxed query builder,
/// which supports Postgres-only bind types (UUID, JSONB, timestamptz).
#[cfg(feature = "executor-diesel-postgres")]
pub(super) fn load_page_postgres<C>(
    conn: &mut C,
    query: &ReadQuery<'_, crate::backend::Postgres>,
    max_bytes: usize,
) -> QueryResult<RowPage<crate::backend::Postgres>>
where
    C: Connection<Backend = diesel::pg::Pg>
        + diesel::connection::LoadConnection<diesel::connection::DefaultLoadingMode>,
    for<'q> BoxedSqlQuery<'q, diesel::pg::Pg, SqlQuery>:
        LoadQuery<'q, C, crate::diesel_decode::DynamicRow<crate::backend::Postgres>>,
{
    collect_page(conn, boxed_postgres_read_query(query)?, max_bytes)
}
