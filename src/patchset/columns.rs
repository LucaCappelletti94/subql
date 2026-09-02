//! Catalog metadata indexed once, shared by the patchset adapters.
//!
//! [`sqlite_diff_rs::Adapter`] resolves `(table_name, column_index)` for
//! every adapted cell. Walking the catalog for each cell scales with the
//! whole schema, so each adapter builds this index at construction and
//! answers every lookup with one hash lookup and one slice index.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use diesel::result::Error as DieselError;
use sql_traits::prelude::{DatabaseLike, TableLike};

/// The columns of every catalog table, keyed by the table's bare stored
/// name.
///
/// When two schemas store the same bare name, the first table the catalog
/// yields wins, matching the linear scan this index replaced.
#[derive(Debug)]
pub struct ColumnIndex<'db, DB: DatabaseLike> {
    tables: hashbrown::HashMap<&'db str, Vec<&'db DB::Column>>,
}

impl<'db, DB: DatabaseLike> ColumnIndex<'db, DB> {
    /// Walk the catalog once and index it.
    ///
    /// # Errors
    /// [`CatalogError::Lookup`](crate::CatalogError::Lookup) when the
    /// catalog fails to yield a table's columns.
    pub fn new(catalog: &'db DB) -> Result<Self, crate::CatalogError> {
        let mut tables: hashbrown::HashMap<&'db str, Vec<&'db DB::Column>> =
            hashbrown::HashMap::new();
        for table in catalog.tables() {
            let columns = table
                .columns(catalog)
                .map_err(|error| crate::CatalogError::Lookup {
                    table_id: catalog
                        .table_id(table)
                        .and_then(|id| u32::try_from(id).ok())
                        .unwrap_or(u32::MAX),
                    error,
                })?
                .collect();
            tables.entry(table.table_name()).or_insert(columns);
        }
        Ok(Self { tables })
    }

    /// The column at `index` of `table_name`, or `None` when the catalog
    /// knows no such table or the index is out of range.
    pub fn column_at(&self, table_name: &str, index: usize) -> Option<&'db DB::Column> {
        self.tables.get(table_name)?.get(index).copied()
    }
}

/// The refusal for a bind against a column the catalog does not know.
///
/// [`sqlite_diff_rs::Adapter::column_name`] has no failure channel, so the
/// same missed lookup there can only answer an empty string. The bind path
/// carries a `Result` and refuses explicitly instead of binding a default
/// into a statement that is already wrong.
pub fn unknown_column_error(table_name: &str, column_index: usize) -> DieselError {
    DieselError::QueryBuilderError(Box::new(UnknownColumn {
        message: alloc::format!(
            "column {column_index} of table {table_name} is not in the catalog"
        ),
    }))
}

#[derive(Debug, Clone)]
struct UnknownColumn {
    message: String,
}

impl core::fmt::Display for UnknownColumn {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl core::error::Error for UnknownColumn {}
