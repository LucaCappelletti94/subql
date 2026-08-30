#![allow(clippy::type_complexity)]
//! [`DieselBackend`] trait and its impls for the three shipped backends,
//! plus the boxed query helpers shared by the diesel connector impls.

use super::ReadQuery;
use crate::backend::Value;
use alloc::string::String;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::sql_query;
use diesel::sql_types::{BigInt, Binary, Bool, Date, Double, Json, Numeric, Text, Time, Timestamp};
use diesel::QueryResult;

/// This error is for unsupported bind types in the generic boxed query builder.
/// `sql_query` with typed binds is necessary here: the re-execution queries
/// are user-supplied SQL that the typed DSL cannot express.
#[cfg(feature = "executor-diesel")]
#[derive(Debug, thiserror::Error)]
#[error("this diesel connector does not support typed read binds")]
struct UnsupportedReadBinds;

/// Bridge trait: names the subql [`crate::backend::Backend`] a diesel-backed connector
/// produces [`Value`]s for, and constructs those [`Value`]s from the
/// scalar wire shapes diesel's `sql_query` hands back.
///
/// Implemented for the three shipped backends ([`crate::backend::Postgres`],
/// [`crate::backend::MySql`], [`crate::backend::SQLite`]), each of which
/// spells [`crate::backend::Backend::Int`] / [`crate::backend::Backend::Float`] / [`crate::backend::Backend::String`] as
/// `i64` / `f64` / `String` respectively; the constructors are trivial and
/// let the generic [`DieselConnector<C, B>`](super::DieselConnector) stay backend-agnostic at the
/// type level while producing correctly typed [`Value<B>`]s.
#[cfg(feature = "executor-diesel")]
pub trait DieselBackend: crate::backend::Backend + Sized {
    /// Wrap an `i64` decoded via `Nullable<BigInt>` as [`Value::Int`].
    fn value_from_i64(x: i64) -> Value<Self>;
    /// Wrap an `f64` decoded via `Nullable<Double>` as [`Value::Float`].
    fn value_from_f64(x: f64) -> Value<Self>;
    /// Wrap a `String` decoded via `Nullable<Text>` as [`Value::String`].
    fn value_from_string(s: String) -> Value<Self>;
    /// Converts a subql value to the shared diesel bind vocabulary.
    fn read_bind(value: &Value<Self>) -> Option<DieselReadBind<'_>>;
    /// SQL type name to cast a `SUM` component to double precision in this
    /// backend's dialect, so `SUM`'s promoted integer type decodes as `f64`
    /// for the accumulator. Defaults to `DOUBLE PRECISION` (PostgreSQL, and
    /// SQLite via `REAL` affinity); MySQL overrides to `DOUBLE`.
    #[must_use]
    fn double_cast_type() -> &'static str {
        "DOUBLE PRECISION"
    }
    /// SQL type name to cast an integer scalar read to eight bytes, so an
    /// aggregate over a column narrower than `bigint` decodes through
    /// `Nullable<BigInt>`. Defaults to `BIGINT` (PostgreSQL, and SQLite via
    /// `INTEGER` affinity); MySQL overrides to `SIGNED`, its `CAST` spelling.
    #[must_use]
    fn int_cast_type() -> &'static str {
        "BIGINT"
    }
}

#[cfg(feature = "executor-diesel")]
#[doc(hidden)]
pub enum DieselReadBind<'a> {
    Bool(&'a bool),
    Int(&'a i64),
    Float(&'a f64),
    Text(&'a str),
    Bytes(&'a [u8]),
    Timestamp(&'a chrono::NaiveDateTime),
    Date(&'a chrono::NaiveDate),
    Time(&'a chrono::NaiveTime),
    Decimal(&'a bigdecimal::BigDecimal),
    Json(&'a serde_json::Value),
}

#[cfg(feature = "executor-diesel")]
impl DieselBackend for crate::backend::Postgres {
    fn value_from_i64(x: i64) -> Value<Self> {
        Value::Int(x)
    }
    fn value_from_f64(x: f64) -> Value<Self> {
        Value::Float(x)
    }
    fn value_from_string(s: String) -> Value<Self> {
        Value::String(s)
    }
    fn read_bind(value: &Value<Self>) -> Option<DieselReadBind<'_>> {
        Some(match value {
            Value::Bool(value) => DieselReadBind::Bool(value),
            Value::Int(value) => DieselReadBind::Int(value),
            Value::Float(value) => DieselReadBind::Float(value),
            Value::String(value) => DieselReadBind::Text(value),
            Value::Bytes(value) => DieselReadBind::Bytes(value),
            Value::Timestamp(value) => DieselReadBind::Timestamp(value),
            Value::Date(value) => DieselReadBind::Date(value),
            Value::Time(value) => DieselReadBind::Time(value),
            Value::Decimal(value) => DieselReadBind::Decimal(value),
            Value::Json(value) => DieselReadBind::Json(value),
            Value::Missing
            | Value::Null
            | Value::Uuid(_)
            | Value::TimestampTz(_)
            | Value::Jsonb(_)
            | Value::Custom(_) => return None,
        })
    }
}

#[cfg(feature = "executor-diesel")]
impl DieselBackend for crate::backend::MySql {
    fn value_from_i64(x: i64) -> Value<Self> {
        Value::Int(x)
    }
    fn value_from_f64(x: f64) -> Value<Self> {
        Value::Float(x)
    }
    fn value_from_string(s: String) -> Value<Self> {
        Value::String(s)
    }
    fn read_bind(value: &Value<Self>) -> Option<DieselReadBind<'_>> {
        Some(match value {
            Value::Bool(value) => DieselReadBind::Bool(value),
            Value::Int(value) => DieselReadBind::Int(value),
            Value::Float(value) => DieselReadBind::Float(value),
            Value::String(value) | Value::Uuid(value) => DieselReadBind::Text(value),
            Value::Bytes(value) => DieselReadBind::Bytes(value),
            Value::Timestamp(value) => DieselReadBind::Timestamp(value),
            Value::Date(value) => DieselReadBind::Date(value),
            Value::Time(value) => DieselReadBind::Time(value),
            Value::Decimal(value) => DieselReadBind::Decimal(value),
            Value::Json(value) | Value::Jsonb(value) => DieselReadBind::Json(value),
            Value::Missing | Value::Null | Value::TimestampTz(_) | Value::Custom(_) => return None,
        })
    }
    fn double_cast_type() -> &'static str {
        "DOUBLE"
    }
    fn int_cast_type() -> &'static str {
        "SIGNED"
    }
}

#[cfg(feature = "executor-diesel")]
impl DieselBackend for crate::backend::SQLite {
    fn value_from_i64(x: i64) -> Value<Self> {
        Value::Int(x)
    }
    fn value_from_f64(x: f64) -> Value<Self> {
        Value::Float(x)
    }
    fn value_from_string(s: String) -> Value<Self> {
        Value::String(s)
    }
    fn read_bind(value: &Value<Self>) -> Option<DieselReadBind<'_>> {
        Some(match value {
            Value::Bool(value) | Value::Int(value) => DieselReadBind::Int(value),
            Value::Float(value) => DieselReadBind::Float(value),
            Value::String(value) | Value::Uuid(value) => DieselReadBind::Text(value),
            Value::Bytes(value) => DieselReadBind::Bytes(value),
            Value::Timestamp(value) => DieselReadBind::Timestamp(value),
            Value::Date(value) => DieselReadBind::Date(value),
            Value::Time(value) => DieselReadBind::Time(value),
            Value::Decimal(value) => DieselReadBind::Decimal(value),
            Value::Json(value) | Value::Jsonb(value) => match value.storage() {
                crate::backend::SqliteJsonStorage::Text(value) => DieselReadBind::Text(value),
                crate::backend::SqliteJsonStorage::Integer(value) => DieselReadBind::Int(value),
                crate::backend::SqliteJsonStorage::Real(value) => DieselReadBind::Float(value),
                crate::backend::SqliteJsonStorage::Blob(value) => DieselReadBind::Bytes(value),
            },
            Value::Missing | Value::Null | Value::TimestampTz(_) | Value::Custom(_) => return None,
        })
    }
}

/// Build a boxed, bind-populated query for any diesel backend.
///
/// The re-execution SQL is user-supplied and cannot be expressed through the
/// typed DSL, so `sql_query` with typed binds is the correct tool here.
#[cfg(feature = "executor-diesel")]
pub(super) fn boxed_read_query<'a, DB, B>(
    query: &'a ReadQuery<'_, B>,
) -> QueryResult<BoxedSqlQuery<'a, DB, SqlQuery>>
where
    DB: diesel::backend::Backend
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
    B: DieselBackend,
    bool: diesel::serialize::ToSql<Bool, DB>,
    i64: diesel::serialize::ToSql<BigInt, DB>,
    f64: diesel::serialize::ToSql<Double, DB>,
    for<'b> &'b str: diesel::serialize::ToSql<Text, DB>,
    for<'b> &'b [u8]: diesel::serialize::ToSql<Binary, DB>,
    for<'b> &'b chrono::NaiveDateTime: diesel::serialize::ToSql<Timestamp, DB>,
    for<'b> &'b chrono::NaiveDate: diesel::serialize::ToSql<Date, DB>,
    for<'b> &'b chrono::NaiveTime: diesel::serialize::ToSql<Time, DB>,
    for<'b> &'b bigdecimal::BigDecimal: diesel::serialize::ToSql<Numeric, DB>,
    for<'b> &'b serde_json::Value: diesel::serialize::ToSql<Json, DB>,
{
    let mut boxed = sql_query(query.sql()).into_boxed::<DB>();
    for value in query.binds() {
        boxed = match B::read_bind(value) {
            Some(DieselReadBind::Bool(value)) => boxed.bind::<Bool, _>(*value),
            Some(DieselReadBind::Int(value)) => boxed.bind::<BigInt, _>(*value),
            Some(DieselReadBind::Float(value)) => boxed.bind::<Double, _>(*value),
            Some(DieselReadBind::Text(value)) => boxed.bind::<Text, _>(value),
            Some(DieselReadBind::Bytes(value)) => boxed.bind::<Binary, _>(value),
            Some(DieselReadBind::Timestamp(value)) => boxed.bind::<Timestamp, _>(value),
            Some(DieselReadBind::Date(value)) => boxed.bind::<Date, _>(value),
            Some(DieselReadBind::Time(value)) => boxed.bind::<Time, _>(value),
            Some(DieselReadBind::Decimal(value)) => boxed.bind::<Numeric, _>(value),
            Some(DieselReadBind::Json(value)) => boxed.bind::<Json, _>(value),
            None => {
                return Err(diesel::result::Error::QueryBuilderError(Box::new(
                    UnsupportedReadBinds,
                )));
            }
        };
    }
    Ok(boxed)
}

/// Build a Postgres-specific boxed query with borrowed binds.
///
/// Uses `sql_query` because the re-execution query is user-supplied SQL
/// that the typed DSL cannot express.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
pub(super) fn boxed_postgres_read_query<'a>(
    query: &'a ReadQuery<'_, crate::backend::Postgres>,
) -> QueryResult<BoxedSqlQuery<'a, diesel::pg::Pg, SqlQuery>> {
    let mut boxed = sql_query(query.sql()).into_boxed::<diesel::pg::Pg>();
    for value in query.binds() {
        boxed = match value {
            Value::Bool(value) => boxed.bind::<Bool, _>(*value),
            Value::Int(value) => boxed.bind::<BigInt, _>(*value),
            Value::Float(value) => boxed.bind::<Double, _>(*value),
            Value::String(value) => boxed.bind::<Text, _>(value.as_str()),
            Value::Bytes(value) => boxed.bind::<Binary, _>(value.as_slice()),
            Value::Uuid(value) => boxed.bind::<diesel::sql_types::Uuid, _>(value),
            Value::Timestamp(value) => boxed.bind::<Timestamp, _>(value),
            Value::TimestampTz(value) => boxed.bind::<diesel::sql_types::Timestamptz, _>(value),
            Value::Date(value) => boxed.bind::<Date, _>(value),
            Value::Time(value) => boxed.bind::<Time, _>(value),
            Value::Decimal(value) => boxed.bind::<Numeric, _>(value),
            Value::Json(value) => boxed.bind::<Json, _>(value),
            Value::Jsonb(value) => boxed.bind::<diesel::sql_types::Jsonb, _>(value),
            Value::Missing | Value::Null | Value::Custom(_) => {
                return Err(diesel::result::Error::QueryBuilderError(Box::new(
                    UnsupportedReadBinds,
                )));
            }
        };
    }
    Ok(boxed)
}

/// Build a Postgres-specific boxed query with owned (static-lifetime) binds
/// for use by the async connector, which must hold the query across awaits.
///
/// Uses `sql_query` because the re-execution query is user-supplied SQL.
#[cfg(feature = "executor-diesel-async-postgres")]
pub fn boxed_postgres_read_query_owned(
    query: &ReadQuery<'_, crate::backend::Postgres>,
) -> QueryResult<BoxedSqlQuery<'static, diesel::pg::Pg, SqlQuery>> {
    let mut boxed = sql_query(query.sql()).into_boxed::<diesel::pg::Pg>();
    for value in query.binds() {
        boxed = match value {
            Value::Bool(value) => boxed.bind::<Bool, _>(*value),
            Value::Int(value) => boxed.bind::<BigInt, _>(*value),
            Value::Float(value) => boxed.bind::<Double, _>(*value),
            Value::String(value) => boxed.bind::<Text, _>(value.clone()),
            Value::Bytes(value) => boxed.bind::<Binary, _>(value.clone()),
            Value::Uuid(value) => boxed.bind::<diesel::sql_types::Uuid, _>(*value),
            Value::Timestamp(value) => boxed.bind::<Timestamp, _>(*value),
            Value::TimestampTz(value) => boxed.bind::<diesel::sql_types::Timestamptz, _>(*value),
            Value::Date(value) => boxed.bind::<Date, _>(*value),
            Value::Time(value) => boxed.bind::<Time, _>(*value),
            Value::Decimal(value) => boxed.bind::<Numeric, _>(value.clone()),
            Value::Json(value) => boxed.bind::<Json, _>(value.clone()),
            Value::Jsonb(value) => boxed.bind::<diesel::sql_types::Jsonb, _>(value.clone()),
            Value::Missing | Value::Null | Value::Custom(_) => {
                return Err(diesel::result::Error::QueryBuilderError(Box::new(
                    UnsupportedReadBinds,
                )));
            }
        };
    }
    Ok(boxed)
}

/// Build a MySQL-specific boxed query with owned binds for async use.
///
/// Uses `sql_query` because the re-execution query is user-supplied SQL.
#[cfg(feature = "executor-diesel-async-mysql")]
pub fn boxed_mysql_read_query_owned(
    query: &ReadQuery<'_, crate::backend::MySql>,
) -> QueryResult<BoxedSqlQuery<'static, diesel::mysql::Mysql, SqlQuery>> {
    let mut boxed = sql_query(query.sql()).into_boxed::<diesel::mysql::Mysql>();
    for value in query.binds() {
        boxed = match value {
            Value::Bool(value) => boxed.bind::<Bool, _>(*value),
            Value::Int(value) => boxed.bind::<BigInt, _>(*value),
            Value::Float(value) => boxed.bind::<Double, _>(*value),
            Value::String(value) | Value::Uuid(value) => boxed.bind::<Text, _>(value.clone()),
            Value::Bytes(value) => boxed.bind::<Binary, _>(value.clone()),
            Value::Timestamp(value) => boxed.bind::<Timestamp, _>(*value),
            Value::Date(value) => boxed.bind::<Date, _>(*value),
            Value::Time(value) => boxed.bind::<Time, _>(*value),
            Value::Decimal(value) => boxed.bind::<Numeric, _>(value.clone()),
            Value::Json(value) | Value::Jsonb(value) => boxed.bind::<Json, _>(value.clone()),
            Value::Missing | Value::Null | Value::TimestampTz(_) | Value::Custom(_) => {
                return Err(diesel::result::Error::QueryBuilderError(Box::new(
                    UnsupportedReadBinds,
                )));
            }
        };
    }
    Ok(boxed)
}
