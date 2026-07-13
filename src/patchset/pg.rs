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
//!
//! Every other column falls through to [`sqlite_diff_rs::DefaultBinder`]
//! which handles the trivial SQLite-to-diesel type mappings (`Integer ->
//! BigInt`, `Real -> Double`, `Text -> Text`, `Blob -> Binary`, `Null ->
//! literal NULL`).

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use core::fmt;

use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::result::{Error as DieselError, QueryResult};
use diesel::sql_types::{Bool, Uuid as UuidSqlType};
use sql_traits::prelude::{ColumnLike, DatabaseLike, DialectLike, TableLike, TypeMatchLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

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

        Ok(Box::new(DefaultBinder::from(value)))
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
