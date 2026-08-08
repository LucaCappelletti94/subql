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
use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

/// Adapter that resolves column names for a SQLite target from a subql
/// catalog and delegates every bind to
/// [`sqlite_diff_rs::DefaultBinder`].
///
/// Source and target speak the same affinity-based SQLite type system,
/// so no per-column dispatch is needed. See the module docs.
#[derive(Debug)]
pub struct SqliteAdapter<'db, DB: DatabaseLike> {
    catalog: &'db DB,
}

impl<'db, DB: DatabaseLike> SqliteAdapter<'db, DB> {
    /// Build a new [`SqliteAdapter`] borrowing the given catalog.
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

impl<DB, S, B> Adapter<Sqlite, S, B> for SqliteAdapter<'_, DB>
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
        _table_name: &str,
        _column_index: usize,
        value: &'a Value<S, B>,
    ) -> Result<Box<dyn Binder<Sqlite> + Send + 'a>, DieselError> {
        Ok(Box::new(DefaultBinder::from(value)))
    }
}
