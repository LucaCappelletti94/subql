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
//!   `crate::temporal`. `TIMESTAMPTZ` normalizes to a UTC instant.
//!   Each column is classified through the catalog's [`ScalarKind`],
//!   since Postgres has no implicit assignment cast from text to these
//!   types. Any other wire shape on such a column is rejected.
//! * `JSON` and `JSONB` get native binds when the wire carries a
//!   `Value::Text` holding JSON text, parsed through `serde_json`. A
//!   `JSONB` column normalizes key order and whitespace on store, so a
//!   round trip preserves the value, not the exact input bytes. Each is
//!   classified through the catalog's [`ScalarKind`]. Any other wire
//!   shape on such a column is rejected.
//!
//! Every other column falls through to [`sqlite_diff_rs::DefaultBinder`]
//! which handles the trivial SQLite-to-diesel type mappings (`Integer ->
//! BigInt`, `Real -> Double`, `Text -> Text`, `Blob -> Binary`, `Null ->
//! literal NULL`).
//!
//! A Postgres `ENUM`, `DOMAIN`, or other user-defined type has no
//! built-in arm here, since a native bind needs a diesel `SqlType` and a
//! [`ToSql`] the caller owns. Wrap [`PgAdapter`] in a
//! [`CustomTypePgAdapter`] and register a [`PgCustomBinder`] per such
//! type to bind those columns natively, still with no SQL cast.

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;
use core::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::result::{Error as DieselError, QueryResult};
use diesel::serialize::ToSql;
use diesel::sql_types::{
    Bool, Date, HasSqlType, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid as UuidSqlType,
};
use sql_traits::prelude::{ColumnLike, DatabaseLike, DialectLike, TableLike, TypeMatchLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use crate::backend::{ScalarKind, ScalarKindOf};
use crate::catalog_helpers;
use crate::temporal::{parse_date, parse_time, parse_timestamp, parse_timestamp_tz};
use crate::types::ColumnId;

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

    /// Classify the target column through the catalog's [`ScalarKind`],
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
                    Some(Box::new(TimestampBinder(parse_timestamp(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(ScalarKind::TimestampTz) => {
                text_scalar_bind(col_name, value, "timestamptz TEXT or NULL", |s| {
                    Some(Box::new(TimestampTzBinder(parse_timestamp_tz(s)?))
                        as Box<dyn Binder<Pg> + Send + 'a>)
                })
            }
            Some(ScalarKind::Date) => text_scalar_bind(col_name, value, "date TEXT or NULL", |s| {
                Some(Box::new(DateBinder(parse_date(s)?)) as Box<dyn Binder<Pg> + Send + 'a>)
            }),
            Some(ScalarKind::Time) => text_scalar_bind(col_name, value, "time TEXT or NULL", |s| {
                Some(Box::new(TimeBinder(parse_time(s)?)) as Box<dyn Binder<Pg> + Send + 'a>)
            }),
            Some(ScalarKind::Json) => text_scalar_bind(col_name, value, "json TEXT or NULL", |s| {
                Some(Box::new(JsonBinder(serde_json::from_str(s).ok()?))
                    as Box<dyn Binder<Pg> + Send + 'a>)
            }),
            Some(ScalarKind::Jsonb) => {
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

/// Binder that pushes a JSON document onto the AST as a native [`Json`]
/// bind. Constructed by [`PgAdapter`] for JSON `Value::Text` on a
/// Postgres `JSON` column.
struct JsonBinder(serde_json::Value);

impl Binder<Pg> for JsonBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Json, serde_json::Value>(&self.0)
    }
}

/// Binder that pushes a JSON document onto the AST as a native [`Jsonb`]
/// bind. Constructed by [`PgAdapter`] for JSON `Value::Text` on a
/// Postgres `JSONB` column.
struct JsonbBinder(serde_json::Value);

impl Binder<Pg> for JsonbBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Jsonb, serde_json::Value>(&self.0)
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

// ============================================================================
// Custom user-defined type dispatch
// ============================================================================

/// A caller-supplied native bind for one Postgres user-defined type,
/// such as an `ENUM` or a `DOMAIN`, that [`PgAdapter`] cannot bind on its
/// own (a text bind to a custom type fails without a SQL cast).
///
/// Register an implementation on a [`CustomTypePgAdapter`] under the
/// type's declared name. When a column of that type is applied, the
/// wrapper calls [`bind`](PgCustomBinder::bind) with the carried wire
/// value. `Value::Null` never reaches here, since the wrapper binds it as
/// a literal NULL before dispatching. The returned [`Binder`] pushes a
/// native bind carrying the type's OID, which diesel resolves from a
/// `SqlType` marked `#[diesel(postgres_type(name = "..."))]`. No SQL cast
/// is emitted.
///
/// Use [`bind_as`] to build the returned binder without hand-writing a
/// [`Binder`] impl.
pub trait PgCustomBinder<S, B>: Send + Sync {
    /// Bind a carried (non-null) wire value as this Postgres type.
    ///
    /// # Errors
    ///
    /// Return an error when the wire value cannot represent the target
    /// type, for example an unknown enum label or a non-text wire shape.
    /// The error rolls the apply transaction back.
    fn bind<'a>(&self, value: &'a Value<S, B>) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>>;
}

/// Box a native diesel bind of `value` as the Postgres `SqlType` `ST`,
/// with no SQL cast.
///
/// The building block for a [`PgCustomBinder`] implementation: given a
/// diesel `SqlType` `ST` (marked `#[diesel(postgres_type(name = "..."))]`)
/// and a value that serializes to it through [`ToSql`], this pushes a
/// bind parameter carrying the type's OID, which diesel resolves from the
/// type name at execute time.
#[must_use]
pub fn bind_as<ST, U>(value: U) -> Box<dyn Binder<Pg> + Send>
where
    ST: 'static,
    U: ToSql<ST, Pg> + Send + 'static,
    Pg: HasSqlType<ST>,
{
    Box::new(CustomBinder::<ST, U> {
        value,
        _marker: PhantomData,
    })
}

/// Binder produced by [`bind_as`]: pushes `value` as the `SqlType` `ST`.
struct CustomBinder<ST, U> {
    value: U,
    _marker: PhantomData<fn() -> ST>,
}

impl<ST, U> Binder<Pg> for CustomBinder<ST, U>
where
    U: ToSql<ST, Pg>,
    Pg: HasSqlType<ST>,
{
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<ST, U>(&self.value)
    }
}

/// [`PgAdapter`] wrapped with dispatch for Postgres user-defined types
/// (`ENUM`s, `DOMAIN`s, and other custom types) that need a
/// caller-supplied native bind.
///
/// [`PgAdapter`] natively binds the built-in scalar families (bool, uuid,
/// numeric, temporal, json), but it cannot bind a custom type, since a
/// native bind needs a diesel `SqlType` and [`ToSql`] the caller owns.
/// This wrapper resolves each column's declared type name from the
/// catalog and, when a [`PgCustomBinder`] is registered under that name
/// (matched case-insensitively, mirroring Postgres unquoted-identifier
/// folding), dispatches the column to it. Every other column, and
/// `Value::Null` on a registered column, delegates to the inner
/// [`PgAdapter`].
///
/// ```
/// use diesel::pg::Pg;
/// use diesel::result::QueryResult;
/// use diesel::serialize::{self, IsNull, Output, ToSql};
/// use std::io::Write;
/// use sql_traits::structs::ParserDB;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use sqlite_diff_rs::{Adapter, Binder, Value};
/// use subql::patchset::{bind_as, CustomTypePgAdapter, PgCustomBinder};
///
/// // A diesel SqlType naming the Postgres enum, so diesel resolves its
/// // OID and binds natively.
/// #[derive(diesel::sql_types::SqlType)]
/// #[diesel(postgres_type(name = "mood"))]
/// struct MoodType;
///
/// // A local value type carrying the enum label, serialized as the raw
/// // bytes Postgres expects for the enum.
/// #[derive(Debug)]
/// struct Mood(String);
/// impl ToSql<MoodType, Pg> for Mood {
///     fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
///         out.write_all(self.0.as_bytes())?;
///         Ok(IsNull::No)
///     }
/// }
///
/// // The registered rule: a mood column's wire text becomes a native bind.
/// struct MoodBinder;
/// impl PgCustomBinder<String, Vec<u8>> for MoodBinder {
///     fn bind<'a>(
///         &self,
///         value: &'a Value<String, Vec<u8>>,
///     ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
///         match value {
///             Value::Text(label) => Ok(bind_as::<MoodType, Mood>(Mood(label.clone()))),
///             _ => Err(diesel::result::Error::QueryBuilderError(
///                 "mood column expects TEXT".into(),
///             )),
///         }
///     }
/// }
///
/// let catalog = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE t (id INT PRIMARY KEY, feeling mood);",
/// )
/// .expect("valid DDL");
/// let adapter = CustomTypePgAdapter::new(&catalog).register("mood", MoodBinder);
///
/// // The `feeling` column (index 1) routes to the registered binder.
/// assert!(adapter.bind("t", 1, &Value::Text("happy".to_owned())).is_ok());
/// // A non-text wire shape on the enum column is refused.
/// assert!(adapter
///     .bind("t", 1, &Value::<String, Vec<u8>>::Integer(1))
///     .is_err());
/// ```
pub struct CustomTypePgAdapter<'db, DB: DatabaseLike, S, B> {
    inner: PgAdapter<'db, DB>,
    binders: Vec<(String, Box<dyn PgCustomBinder<S, B>>)>,
}

impl<'db, DB: DatabaseLike, S, B> CustomTypePgAdapter<'db, DB, S, B> {
    /// Build a wrapper around a fresh [`PgAdapter`] borrowing `catalog`,
    /// with no custom types registered yet.
    #[must_use]
    pub const fn new(catalog: &'db DB) -> Self {
        Self {
            inner: PgAdapter::new(catalog),
            binders: Vec::new(),
        }
    }

    /// Register `binder` for every column whose declared catalog type
    /// name equals `type_name` (matched case-insensitively). Chainable.
    #[must_use]
    pub fn register(
        mut self,
        type_name: impl Into<String>,
        binder: impl PgCustomBinder<S, B> + 'static,
    ) -> Self {
        self.binders.push((type_name.into(), Box::new(binder)));
        self
    }
}

impl<DB, S, B> Adapter<Pg, S, B> for CustomTypePgAdapter<'_, DB, S, B>
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
    ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
        let custom = self
            .inner
            .column_type_name(table_name, column_index)
            .and_then(|type_name| {
                self.binders
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(type_name.as_ref()))
            });
        match custom {
            Some((_, binder)) => match value {
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => binder.bind(other),
            },
            None => self.inner.bind(table_name, column_index, value),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{bind_as, CustomTypePgAdapter, PgCustomBinder};
    use alloc::borrow::ToOwned;
    use alloc::boxed::Box;
    use alloc::string::String;
    use alloc::vec::Vec;
    use diesel::pg::Pg;
    use diesel::result::{Error as DieselError, QueryResult};
    use diesel::serialize::{self, IsNull, Output, ToSql};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{Adapter, Binder, Value};
    use sqlparser::dialect::PostgreSqlDialect;
    use std::io::Write;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "mood"))]
    struct MoodType;

    #[derive(Debug)]
    struct Mood(String);

    impl ToSql<MoodType, Pg> for Mood {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
            out.write_all(self.0.as_bytes())?;
            Ok(IsNull::No)
        }
    }

    // Only "happy" is a valid label, so any other text is refused, which
    // proves the wrapper routes the column here rather than to the inner
    // adapter's text fall-through.
    struct MoodBinder;

    impl PgCustomBinder<String, Vec<u8>> for MoodBinder {
        fn bind<'a>(
            &self,
            value: &'a Value<String, Vec<u8>>,
        ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
            match value {
                Value::Text(s) if s == "happy" => Ok(bind_as::<MoodType, Mood>(Mood(s.clone()))),
                Value::Text(_) => Err(DieselError::QueryBuilderError("unknown mood label".into())),
                _ => Err(DieselError::QueryBuilderError(
                    "feeling expects TEXT".into(),
                )),
            }
        }
    }

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, active BOOLEAN, feeling mood);",
        )
        .unwrap()
    }

    fn adapter(db: &ParserDB) -> CustomTypePgAdapter<'_, ParserDB, String, Vec<u8>> {
        CustomTypePgAdapter::new(db).register("mood", MoodBinder)
    }

    #[test]
    fn registered_type_routes_to_custom_binder() {
        let db = catalog();
        let a = adapter(&db);
        // feeling is column index 2, declared type `mood`.
        assert!(a
            .bind("orders", 2, &Value::Text("happy".to_owned()))
            .is_ok());
    }

    #[test]
    fn unknown_label_is_refused_by_the_custom_binder() {
        let db = catalog();
        let a = adapter(&db);
        assert!(a
            .bind("orders", 2, &Value::Text("elated".to_owned()))
            .is_err());
    }

    #[test]
    fn null_on_registered_column_binds_as_literal_null() {
        let db = catalog();
        let a = adapter(&db);
        assert!(a.bind("orders", 2, &Value::<String, Vec<u8>>::Null).is_ok());
    }

    #[test]
    fn type_name_match_is_case_insensitive() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, feeling MOOD);",
        )
        .unwrap();
        let a = adapter(&db);
        // feeling is column index 1 here, declared type `MOOD`.
        assert!(a
            .bind("orders", 1, &Value::Text("happy".to_owned()))
            .is_ok());
        assert!(a
            .bind("orders", 1, &Value::Text("elated".to_owned()))
            .is_err());
    }

    #[test]
    fn unregistered_column_delegates_to_inner_adapter() {
        let db = catalog();
        let a = adapter(&db);
        // `active` is BOOLEAN: the inner adapter accepts Integer and
        // refuses TEXT, so delegation is observable.
        assert!(a.bind("orders", 1, &Value::Integer(1)).is_ok());
        assert!(a.bind("orders", 1, &Value::Text("x".to_owned())).is_err());
    }
}
