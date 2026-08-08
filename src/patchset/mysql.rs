//! MySQL adapter for [`sqlite_diff_rs`] patchset application.
//!
//! Dispatches MySQL `TINYINT(1)` / `BOOLEAN` columns to native `bool`
//! binds when the wire carries an SQLite `Value::Integer`. All other
//! columns fall through to [`sqlite_diff_rs::DefaultBinder`].

use alloc::boxed::Box;

use diesel::mysql::Mysql;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::sql_types::Bool;
use sql_traits::prelude::{ColumnLike, DatabaseLike, DialectLike, TableLike, TypeMatchLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

/// Adapter that resolves column names and native diesel binders for a
/// MySQL target from a subql catalog.
///
/// Dispatches MySQL `TINYINT(1)` / `BOOLEAN` columns to native `bool`
/// binds when the wire carries an SQLite `Value::Integer`. All other
/// columns fall through to [`DefaultBinder`].
#[derive(Debug)]
pub struct MysqlAdapter<'db, DB: DatabaseLike> {
    catalog: &'db DB,
}

impl<'db, DB: DatabaseLike> MysqlAdapter<'db, DB> {
    /// Build a new [`MysqlAdapter`] borrowing the given catalog.
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
}

impl<DB, S, B> Adapter<Mysql, S, B> for MysqlAdapter<'_, DB>
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
    ) -> Result<Box<dyn Binder<Mysql> + Send + 'a>, diesel::result::Error> {
        // MySQL `TINYINT(1)` (aka `BOOLEAN`) columns arrive as
        // `Value::Integer` on the wire (SQLite has no native bool).
        // Native-bind those as `Bool`. Null is legitimate NULL. Any
        // other wire shape on a boolean column is refused with a
        // rollback-inducing error.
        if let Some(col) = self.column_at(table_name, column_index) {
            if self.catalog.dialect().is_bool(self.catalog, col).is_yes() {
                return match value {
                    Value::Integer(i) => Ok(Box::new(BoolBinder(*i != 0))),
                    Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                    other => Err(bind_error(
                        col.column_name(),
                        "INTEGER or NULL",
                        shape_of(other),
                    )),
                };
            }
        }
        Ok(Box::new(DefaultBinder::from(value)))
    }
}

const fn shape_of<S, B>(value: &Value<S, B>) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    }
}

fn bind_error(column: &str, expected: &str, got: &str) -> diesel::result::Error {
    diesel::result::Error::QueryBuilderError(Box::new(BindTypeMismatch {
        message: alloc::format!("column `{column}` expects {expected}, got {got}"),
    }))
}

#[derive(Debug, Clone)]
struct BindTypeMismatch {
    message: alloc::string::String,
}

impl core::fmt::Display for BindTypeMismatch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BindTypeMismatch {}

/// Binder that pushes a boolean value onto the AST as a native
/// [`Bool`] bind. Constructed by [`MysqlAdapter`] when a column resolved
/// to MySQL `TINYINT(1)` / `BOOLEAN` receives an SQLite `Value::Integer`
/// on the wire.
struct BoolBinder(bool);

impl Binder<Mysql> for BoolBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Mysql>) -> QueryResult<()> {
        out.push_bind_param::<Bool, bool>(&self.0)
    }
}
