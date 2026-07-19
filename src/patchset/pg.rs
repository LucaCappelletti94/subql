//! Postgres adapter for [`sqlite_diff_rs`] patchset application.
//!
//! Dispatches columns to native diesel binds based on the target
//! column's Postgres type, resolved from the subql catalog.
//!
//! # Dispatched types
//!
//! * `BOOLEAN` gets a native `Bool` bind when the wire carries an SQLite
//!   `Value::Integer`. Any other wire shape on a `BOOLEAN` column is
//!   rejected with a [`diesel::result::Error::QueryBuilderError`] before
//!   the query executes.
//! * `UUID` gets a native `Uuid` bind when the wire carries either a
//!   16-byte `Value::Blob` (compact binary UUID) or a `Value::Text` that
//!   parses through [`uuid::Uuid::parse_str`] (hyphenated form). Both
//!   flavors are accepted transparently, allowing clients that prefer
//!   BLOB storage and clients that prefer TEXT storage to coexist against
//!   the same server without changing the adapter. Any other wire shape
//!   on a `UUID` column is rejected.
//! * `NUMERIC` / `DECIMAL` gets a native `Numeric` bind when the wire
//!   carries a `Value::Text` holding the verbatim decimal digits, parsed
//!   through [`bigdecimal::BigDecimal`]. The target column is classified
//!   through the catalog's [`ScalarKind`], since Postgres has no implicit
//!   assignment cast from text to `numeric`. Any other wire shape on a
//!   decimal column is rejected.
//! * `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, and `TIME` get native temporal
//!   binds when the wire carries a `Value::Text` holding the verbatim
//!   Postgres text form, parsed through the shared `chrono` parsers in
//!   `crate::wal::pg_type`. `TIMESTAMPTZ` normalizes to a UTC instant.
//!   Each column is classified through the catalog's [`ScalarKind`],
//!   since Postgres has no implicit assignment cast from text to these
//!   types. Any other wire shape on such a column is rejected.
//!
//! Every other column falls through to [`sqlite_diff_rs::DefaultBinder`]
//! which handles the trivial SQLite-to-diesel type mappings (`Integer ->
//! BigInt`, `Real -> Double`, `Text -> Text`, `Blob -> Binary`, `Null ->
//! literal NULL`).

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use core::fmt;
use core::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::result::{Error as DieselError, QueryResult};
use diesel::sql_types::{Bool, Date, Numeric, Time, Timestamp, Timestamptz, Uuid as UuidSqlType};
use sql_traits::prelude::{ColumnLike, DatabaseLike, DialectLike, TableLike, TypeMatchLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use crate::backend::ScalarKind;
use crate::catalog_helpers;
use crate::types::ColumnId;
use crate::wal::pg_type::{parse_pg_date, parse_pg_time, parse_pg_timestamp, parse_pg_timestamptz};

/// Adapter that resolves column names and native diesel binders for a
/// Postgres target from a subql catalog.
///
/// See the module docs for the full dispatch table.
#[derive(Debug)]
pub struct PgAdapter<'db, DB: DatabaseLike> {
    catalog: &'db DB,
}

impl<'db, DB: DatabaseLike> PgAdapter<'db, DB> {
    /// Build a new [`PgAdapter`] borrowing the given catalog.
    #[must_use]
    pub const fn new(catalog: &'db DB) -> Self {
        Self { catalog }
    }

    fn column_at(&self, table_name: &str, index: usize) -> Option<&DB::Column> {
        let table = self
            .catalog
            .tables()
            .find(|t| t.table_name() == table_name)?;
        table.columns(self.catalog).nth(index)
    }

    /// Classify the target column through the catalog's [`ScalarKind`],
    /// the dispatch key for families Postgres will not assignment-cast
    /// from a text bind. Returns `None` for an unknown table or column,
    /// or a declared type that maps to no supported scalar.
    fn scalar_kind_at(&self, table_name: &str, column_index: usize) -> Option<ScalarKind> {
        let table_id = catalog_helpers::table_id(self.catalog, table_name)?;
        let column_id = ColumnId::try_from(column_index).ok()?;
        catalog_helpers::column_scalar_kind(self.catalog, table_id, column_id)
    }
}

impl<DB, S, B> Adapter<Pg, S, B> for PgAdapter<'_, DB>
where
    DB: DatabaseLike,
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    fn column_name(&self, table_name: &str, column_index: usize) -> &str {
        self.column_at(table_name, column_index)
            .map_or("", ColumnLike::column_name)
    }

    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError> {
        let Some(col) = self.column_at(table_name, column_index) else {
            return Ok(Box::new(DefaultBinder::from(value)));
        };
        let dialect = self.catalog.dialect();
        let col_name = col.column_name();

        // BOOLEAN: strict Integer -> Bool. Null is legitimate NULL.
        // Other wire shapes are refused with a rollback-inducing error
        // so the mismatch surfaces at the adapter, not two frames deep
        // inside PG's error text.
        if dialect.is_bool(self.catalog, col).is_yes() {
            return match value {
                Value::Integer(i) => Ok(Box::new(BoolBinder(*i != 0))),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => Err(bind_error(col_name, "INTEGER or NULL", shape_of(other))),
            };
        }

        // UUID: accept both 16-byte Blob and hyphenated Text. Null is
        // legitimate NULL. Other wire shapes are refused.
        if dialect.is_uuid(self.catalog, col).is_yes() {
            return match value {
                Value::Blob(b) => uuid::Uuid::from_slice(b.as_ref())
                    .map(|u| -> Box<dyn Binder<Pg> + Send + 'a> { Box::new(UuidBinder(u)) })
                    .map_err(|_| {
                        bind_error(
                            col_name,
                            "16-byte BLOB or hyphenated TEXT",
                            &format!("BLOB of length {}", b.as_ref().len()),
                        )
                    }),
                Value::Text(s) => uuid::Uuid::parse_str(s.as_ref())
                    .map(|u| -> Box<dyn Binder<Pg> + Send + 'a> { Box::new(UuidBinder(u)) })
                    .map_err(|_| {
                        bind_error(
                            col_name,
                            "16-byte BLOB or hyphenated TEXT",
                            "unparseable TEXT",
                        )
                    }),
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => Err(bind_error(
                    col_name,
                    "16-byte BLOB or hyphenated TEXT",
                    shape_of(other),
                )),
            };
        }

        // Rich scalars Postgres will not assignment-cast from a text
        // bind: decimal and the temporals. Each parses the verbatim wire
        // text into its diesel type and binds it natively. Bool and UUID
        // are handled above, and everything else falls to DefaultBinder.
        match self.scalar_kind_at(table_name, column_index) {
            Some(ScalarKind::Decimal) => {
                text_scalar_bind(col_name, value, "decimal TEXT or NULL", |s| {
                    Some(Box::new(DecimalBinder(BigDecimal::from_str(s).ok()?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(ScalarKind::Timestamp) => {
                text_scalar_bind(col_name, value, "timestamp TEXT or NULL", |s| {
                    Some(Box::new(TimestampBinder(parse_pg_timestamp(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(ScalarKind::TimestampTz) => {
                text_scalar_bind(col_name, value, "timestamptz TEXT or NULL", |s| {
                    Some(Box::new(TimestampTzBinder(parse_pg_timestamptz(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(ScalarKind::Date) => text_scalar_bind(col_name, value, "date TEXT or NULL", |s| {
                Some(Box::new(DateBinder(parse_pg_date(s)?)) as Box<dyn Binder<Pg> + Send + 'a>)
            }),
            Some(ScalarKind::Time) => text_scalar_bind(col_name, value, "time TEXT or NULL", |s| {
                Some(Box::new(TimeBinder(parse_pg_time(s)?)) as Box<dyn Binder<Pg> + Send + 'a>)
            }),
            _ => Ok(Box::new(DefaultBinder::from(value))),
        }
    }
}

/// Bind a rich scalar carried as verbatim `Value::Text`. `parse` turns
/// the text into a native binder. `Value::Null` passes through as a
/// literal NULL, and any other wire shape (or a parse failure) is
/// refused with a rollback-inducing error naming the column and the
/// expected shape.
fn text_scalar_bind<'a, S, B>(
    col_name: &str,
    value: &'a Value<S, B>,
    expected: &str,
    parse: impl FnOnce(&str) -> Option<Box<dyn Binder<Pg> + Send + 'a>>,
) -> Result<Box<dyn Binder<Pg> + Send + 'a>, DieselError>
where
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    match value {
        Value::Text(s) => {
            parse(s.as_ref()).ok_or_else(|| bind_error(col_name, expected, "unparseable TEXT"))
        }
        Value::Null => Ok(Box::new(DefaultBinder::from(value))),
        other => Err(bind_error(col_name, expected, shape_of(other))),
    }
}

// ============================================================================
// Binders
// ============================================================================

/// Binder that pushes a boolean value onto the AST as a native
/// [`Bool`] bind.
struct BoolBinder(bool);

impl Binder<Pg> for BoolBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Bool, bool>(&self.0)
    }
}

/// Binder that pushes a UUID onto the AST as a native [`UuidSqlType`]
/// bind. Constructed by [`PgAdapter`] for either 16-byte BLOB or
/// hyphenated TEXT wire input on a Postgres `UUID` column.
struct UuidBinder(uuid::Uuid);

impl Binder<Pg> for UuidBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<UuidSqlType, uuid::Uuid>(&self.0)
    }
}

/// Binder that pushes a decimal onto the AST as a native [`Numeric`]
/// bind. Constructed by [`PgAdapter`] for verbatim decimal TEXT on a
/// Postgres `NUMERIC` / `DECIMAL` column.
struct DecimalBinder(BigDecimal);

impl Binder<Pg> for DecimalBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Numeric, BigDecimal>(&self.0)
    }
}

/// Binder that pushes a naive datetime onto the AST as a native
/// [`Timestamp`] bind. Constructed by [`PgAdapter`] for verbatim
/// timestamp TEXT on a Postgres `TIMESTAMP` column.
struct TimestampBinder(NaiveDateTime);

impl Binder<Pg> for TimestampBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Timestamp, NaiveDateTime>(&self.0)
    }
}

/// Binder that pushes a UTC instant onto the AST as a native
/// [`Timestamptz`] bind. Constructed by [`PgAdapter`] for verbatim
/// timestamptz TEXT on a Postgres `TIMESTAMPTZ` column, normalized to UTC.
struct TimestampTzBinder(DateTime<Utc>);

impl Binder<Pg> for TimestampTzBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Timestamptz, DateTime<Utc>>(&self.0)
    }
}

/// Binder that pushes a date onto the AST as a native [`Date`] bind.
/// Constructed by [`PgAdapter`] for verbatim date TEXT on a Postgres
/// `DATE` column.
struct DateBinder(NaiveDate);

impl Binder<Pg> for DateBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Date, NaiveDate>(&self.0)
    }
}

/// Binder that pushes a time-of-day onto the AST as a native [`Time`]
/// bind. Constructed by [`PgAdapter`] for verbatim time TEXT on a
/// Postgres `TIME` column.
struct TimeBinder(NaiveTime);

impl Binder<Pg> for TimeBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Time, NaiveTime>(&self.0)
    }
}

// ============================================================================
// Bind-time error helpers
// ============================================================================

const fn shape_of<S, B>(value: &Value<S, B>) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    }
}

fn bind_error(column: &str, expected: &str, got: &str) -> DieselError {
    DieselError::QueryBuilderError(Box::new(BindTypeMismatch {
        message: format!("column `{column}` expects {expected}, got {got}"),
    }))
}

/// Bind-time type mismatch: the wire value carried a shape the adapter
/// refuses to interpret as the target column's type.
#[derive(Debug, Clone)]
struct BindTypeMismatch {
    message: String,
}

impl fmt::Display for BindTypeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BindTypeMismatch {}
