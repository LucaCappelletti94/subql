//! Postgres adapter for [`sqlite_diff_rs`] patchset application.
//!
//! Dispatches Postgres `BOOLEAN` columns to native `bool` binds when the
//! wire carries an SQLite `Value::Integer`. All other columns fall
//! through to [`sqlite_diff_rs::DefaultBinder`].

use alloc::boxed::Box;

use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::sql_types::Bool;
use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

/// Adapter that resolves column names and native diesel binders for a
/// Postgres target from a subql catalog.
///
/// Dispatches Postgres `BOOLEAN` columns to native `bool` binds when the
/// wire carries an SQLite `Value::Integer`. All other columns fall
/// through to [`DefaultBinder`].
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
    ) -> Box<dyn Binder<Pg> + Send + 'a> {
        // Postgres `BOOLEAN` columns arrive as `Value::Integer` on the
        // wire (SQLite has no native bool). Native-bind those as `Bool`;
        // everything else defers to `DefaultBinder`.
        if let Value::Integer(i) = value {
            let is_bool = self
                .column_at(table_name, column_index)
                .is_some_and(|col| col.is_bool(self.catalog));
            if is_bool {
                return Box::new(BoolBinder(*i != 0));
            }
        }
        Box::new(DefaultBinder::from(value))
    }
}

/// Binder that pushes a boolean value onto the AST as a native
/// [`Bool`] bind. Constructed by [`PgAdapter`] when a column resolved
/// to Postgres `BOOLEAN` receives an SQLite `Value::Integer` on the
/// wire.
struct BoolBinder(bool);

impl Binder<Pg> for BoolBinder {
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<Bool, bool>(&self.0)
    }
}
