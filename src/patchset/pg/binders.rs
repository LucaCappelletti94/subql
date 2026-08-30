//! Native diesel [`Binder`] implementations for each Postgres scalar family.

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::sql_types::{
    Bool, Date, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid as UuidSqlType,
};
use sqlite_diff_rs::Binder;

/// Binder that pushes a boolean value onto the AST as a native
/// [`Bool`] bind.
pub(super) struct BoolBinder(pub(super) bool);

impl Binder<Pg> for BoolBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Bool, bool>(&self.0)
    }
}

/// Binder that pushes a UUID onto the AST as a native [`UuidSqlType`]
/// bind. Constructed by [`super::PgAdapter`] for either 16-byte BLOB or
/// hyphenated TEXT wire input on a Postgres `UUID` column.
pub(super) struct UuidBinder(pub(super) uuid::Uuid);

impl Binder<Pg> for UuidBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<UuidSqlType, uuid::Uuid>(&self.0)
    }
}

/// Binder that pushes a decimal onto the AST as a native [`Numeric`]
/// bind. Constructed by [`super::PgAdapter`] for verbatim decimal TEXT on a
/// Postgres `NUMERIC` / `DECIMAL` column.
pub(super) struct DecimalBinder(pub(super) BigDecimal);

impl Binder<Pg> for DecimalBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Numeric, BigDecimal>(&self.0)
    }
}

/// Binder that pushes a naive datetime onto the AST as a native
/// [`Timestamp`] bind. Constructed by [`super::PgAdapter`] for verbatim
/// timestamp TEXT on a Postgres `TIMESTAMP` column.
pub(super) struct TimestampBinder(pub(super) NaiveDateTime);

impl Binder<Pg> for TimestampBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Timestamp, NaiveDateTime>(&self.0)
    }
}

/// Binder that pushes a UTC instant onto the AST as a native
/// [`Timestamptz`] bind. Constructed by [`super::PgAdapter`] for verbatim
/// timestamptz TEXT on a Postgres `TIMESTAMPTZ` column, normalized to UTC.
pub(super) struct TimestampTzBinder(pub(super) DateTime<Utc>);

impl Binder<Pg> for TimestampTzBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Timestamptz, DateTime<Utc>>(&self.0)
    }
}

/// Binder that pushes a date onto the AST as a native [`Date`] bind.
/// Constructed by [`super::PgAdapter`] for verbatim date TEXT on a Postgres
/// `DATE` column.
pub(super) struct DateBinder(pub(super) NaiveDate);

impl Binder<Pg> for DateBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Date, NaiveDate>(&self.0)
    }
}

/// Binder that pushes a time-of-day onto the AST as a native [`Time`]
/// bind. Constructed by [`super::PgAdapter`] for verbatim time TEXT on a
/// Postgres `TIME` column.
pub(super) struct TimeBinder(pub(super) NaiveTime);

impl Binder<Pg> for TimeBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Time, NaiveTime>(&self.0)
    }
}

/// Binder that pushes a JSON document onto the AST as a native [`Json`]
/// bind. Constructed by [`super::PgAdapter`] for JSON `Value::Text` on a
/// Postgres `JSON` column.
pub(super) struct JsonBinder(pub(super) serde_json::Value);

impl Binder<Pg> for JsonBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Json, serde_json::Value>(&self.0)
    }
}

/// Binder that pushes a JSON document onto the AST as a native [`Jsonb`]
/// bind. Constructed by [`super::PgAdapter`] for JSON `Value::Text` on a
/// Postgres `JSONB` column.
pub(super) struct JsonbBinder(pub(super) serde_json::Value);

impl Binder<Pg> for JsonbBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Jsonb, serde_json::Value>(&self.0)
    }
}
