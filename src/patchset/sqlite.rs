//! SQLite adapter for [`sqlite_diff_rs`] patchset application.
//!
//! The trivial case of the patchset apply pipeline: source (SQLite
//! session-extension bytes) and target (SQLite database) speak the same
//! affinity-based type system, so no per-column type dispatch is needed.
//! Every value flows through [`sqlite_diff_rs::DefaultBinder`]:
//! `Integer -> BigInt`, `Real -> Double`, `Text -> Text`, `Blob -> Binary`,
//! `Null -> literal NULL`.
//!
//! The adapter still exists for one reason: [`sqlite_diff_rs::Adapter`]
//! requires the caller to resolve `(table_name, column_index) ->
//! column_name` via [`Adapter::column_name`], which sqlite-diff-rs does
//! not know how to do on its own. [`SqliteAdapter`] provides that
//! resolution from the subql catalog.

use alloc::boxed::Box;

use diesel::result::Error as DieselError;
use diesel::sqlite::Sqlite;
use sql_traits::prelude::{ColumnLike, DatabaseLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use super::columns::{unknown_column_error, ColumnIndex};

/// Adapter that resolves column names for a SQLite target from a subql
/// catalog and delegates every bind to
/// [`sqlite_diff_rs::DefaultBinder`].
///
/// Source and target speak the same affinity-based SQLite type system,
/// so no per-column dispatch is needed. See the module docs.
#[derive(Debug)]
pub struct SqliteAdapter<'db, DB: DatabaseLike> {
    columns: ColumnIndex<'db, DB>,
}

impl<'db, DB: DatabaseLike> SqliteAdapter<'db, DB> {
    /// Index the catalog once and build the adapter over the index.
    ///
    /// After construction the adapter holds no catalog handle at all, so
    /// no lookup can walk the catalog again.
    ///
    /// # Errors
    /// [`CatalogError`](crate::CatalogError) when the catalog fails to
    /// yield a table's columns.
    pub fn new(catalog: &'db DB) -> Result<Self, crate::CatalogError> {
        Ok(Self {
            columns: ColumnIndex::new(catalog)?,
        })
    }
}

impl<DB, S, B> Adapter<Sqlite, S, B> for SqliteAdapter<'_, DB>
where
    DB: DatabaseLike,
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    fn column_name(&self, table_name: &str, column_index: usize) -> &str {
        self.columns
            .column_at(table_name, column_index)
            .map_or("", ColumnLike::column_name)
    }

    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> Result<Box<dyn Binder<Sqlite> + Send + 'a>, DieselError> {
        if self.columns.column_at(table_name, column_index).is_none() {
            return Err(unknown_column_error(table_name, column_index));
        }
        Ok(Box::new(DefaultBinder::from(value)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    use diesel::sqlite::Sqlite;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{Adapter, Value};
    use sqlparser::dialect::SQLiteDialect;

    use super::SqliteAdapter;

    fn catalog() -> ParserDB {
        ParserDB::parse::<SQLiteDialect>("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap()
    }

    /// A column or table the catalog does not know is refused at bind time
    /// with an error naming the lookup, never silently bound with a default.
    #[test]
    fn an_unknown_column_is_refused_at_bind() {
        let db = catalog();
        let adapter = SqliteAdapter::new(&db).expect("the catalog indexes");

        let err =
            Adapter::<Sqlite, String, Vec<u8>>::bind(&adapter, "items", 9, &Value::Integer(1))
                .map(drop)
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("items") && err.contains('9'),
            "the refusal names the table and the column index, got {err:?}"
        );

        let err =
            Adapter::<Sqlite, String, Vec<u8>>::bind(&adapter, "ghosts", 0, &Value::Integer(1))
                .map(drop)
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("ghosts"),
            "the refusal names the unknown table, got {err:?}"
        );
    }
}
