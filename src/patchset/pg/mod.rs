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
//!   through the catalog's [`crate::backend::ScalarKind`], since Postgres has no implicit
//!   assignment cast from text to `numeric`. Any other wire shape on a
//!   decimal column is rejected.
//! * `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, and `TIME` get native temporal
//!   binds when the wire carries a `Value::Text` holding the verbatim
//!   Postgres text form, parsed through `sql_scalar_text`. `TIMESTAMPTZ` normalizes to a UTC instant.
//!   Each column is classified through the catalog's [`crate::backend::ScalarKind`],
//!   since Postgres has no implicit assignment cast from text to these
//!   types. Any other wire shape on such a column is rejected.
//! * `JSON` and `JSONB` get native binds when the wire carries a
//!   `Value::Text` holding JSON text, parsed through `serde_json`. A
//!   `JSONB` column normalizes key order and whitespace on store, so a
//!   round trip preserves the value, not the exact input bytes. Each is
//!   classified through the catalog's [`crate::backend::ScalarKind`]. Any other wire
//!   shape on such a column is rejected.
//!
//! Every other column falls through to [`sqlite_diff_rs::DefaultBinder`]
//! which handles the trivial SQLite-to-diesel type mappings (`Integer ->
//! BigInt`, `Real -> Double`, `Text -> Text`, `Blob -> Binary`, `Null ->
//! literal NULL`).
//!
//! A Postgres `ENUM`, `DOMAIN`, or other user-defined type has no
//! built-in arm here, since a native bind needs a diesel `SqlType` and a
//! [`ToSql`](diesel::serialize::ToSql) the caller owns. Wrap [`PgAdapter`] in a
//! [`CustomTypePgAdapter`] and register a [`PgCustomBinder`] per such
//! type to bind those columns natively, still with no SQL cast.

use alloc::borrow::Cow;
use alloc::boxed::Box;

use diesel::pg::Pg;
use diesel::result::Error as DieselError;
use sql_scalar_text::{parse_date, parse_time, parse_timestamp, parse_timestamp_tz};
use sql_traits::prelude::{ColumnLike, DatabaseLike, DialectLike, TableLike, TypeMatchLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use crate::backend::{BuiltinKind, ScalarKindOf};
use crate::catalog_helpers;
use crate::types::ColumnId;

pub(crate) mod binders;
pub(crate) mod custom_type;
pub(crate) mod errors;

use binders::{
    BoolBinder, DateBinder, DecimalBinder, JsonBinder, JsonbBinder, TimeBinder, TimestampBinder,
    TimestampTzBinder, UuidBinder,
};
use errors::{bind_error, shape_of};

pub use custom_type::{bind_as, CustomTypePgAdapter, PgCustomBinder};

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
        table.columns(self.catalog).ok()?.nth(index)
    }

    /// Classify the target column through the catalog's [`crate::backend::ScalarKind`],
    /// the dispatch key for families Postgres will not assignment-cast
    /// from a text bind. Returns `None` for an unknown table or column,
    /// or a declared type that maps to no supported scalar.
    fn scalar_kind_at(
        &self,
        table_name: &str,
        column_index: usize,
    ) -> Option<ScalarKindOf<crate::backend::Postgres>> {
        let table_id = catalog_helpers::table_id(self.catalog, table_name)?;
        let column_id = ColumnId::try_from(column_index).ok()?;
        catalog_helpers::column_scalar_kind::<crate::backend::Postgres, _>(
            self.catalog,
            table_id,
            column_id,
        )
    }

    /// The declared SQL type name of the target column, verbatim from the
    /// catalog. For a Postgres user-defined type such as an `ENUM` or a
    /// `DOMAIN`, this is the type's own name (for example `mood` or
    /// `sku`). [`CustomTypePgAdapter`] uses it to route a column to a
    /// caller-registered native bind. Returns `None` for an unknown table
    /// or column.
    #[must_use]
    pub fn column_type_name(&self, table_name: &str, column_index: usize) -> Option<Cow<'_, str>> {
        self.column_at(table_name, column_index)
            .map(|col| col.data_type(self.catalog))
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
                            &alloc::format!("BLOB of length {}", b.as_ref().len()),
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
        match self
            .scalar_kind_at(table_name, column_index)
            .and_then(|kind| kind.as_builtin())
        {
            Some(BuiltinKind::Decimal) => {
                text_scalar_bind(col_name, value, "decimal TEXT or NULL", |s| {
                    Some(Box::new(DecimalBinder(sql_scalar_text::parse_decimal(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(BuiltinKind::Timestamp) => {
                text_scalar_bind(col_name, value, "timestamp TEXT or NULL", |s| {
                    Some(Box::new(TimestampBinder(parse_timestamp(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(BuiltinKind::TimestampTz) => {
                text_scalar_bind(col_name, value, "timestamptz TEXT or NULL", |s| {
                    Some(Box::new(TimestampTzBinder(parse_timestamp_tz(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(BuiltinKind::Date) => {
                text_scalar_bind(col_name, value, "date TEXT or NULL", |s| {
                    Some(Box::new(DateBinder(parse_date(s)?)) as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(BuiltinKind::Time) => {
                text_scalar_bind(col_name, value, "time TEXT or NULL", |s| {
                    Some(Box::new(TimeBinder(parse_time(s)?)) as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(BuiltinKind::Json) => {
                text_scalar_bind(col_name, value, "json TEXT or NULL", |s| {
                    Some(Box::new(JsonBinder(serde_json::from_str(s).ok()?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(BuiltinKind::Jsonb) => {
                text_scalar_bind(col_name, value, "jsonb TEXT or NULL", |s| {
                    Some(Box::new(JsonbBinder(serde_json::from_str(s).ok()?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
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
