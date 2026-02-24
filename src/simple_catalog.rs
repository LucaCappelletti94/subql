//! A builder-style [`SchemaCatalog`] for tests and examples.
//!
//! # Example
//! ```
//! use subql::SimpleCatalog;
//!
//! let catalog = SimpleCatalog::new()
//!     .add_table("orders", 1, 3)
//!     .add_column(1, "id", 0)
//!     .add_column(1, "amount", 1)
//!     .add_column(1, "status", 2);
//! ```

use crate::{ColumnId, SchemaCatalog, TableId};
use std::collections::HashMap;

/// A simple builder-style catalog for use in tests and examples.
///
/// Tables and columns are registered via the builder methods
/// [`add_table`](SimpleCatalog::add_table) and [`add_column`](SimpleCatalog::add_column).
#[derive(Debug, Default, Clone)]
pub struct SimpleCatalog {
    /// name → (table_id, arity)
    tables: HashMap<String, (TableId, usize)>,
    /// (table_id, column_name) → column_id
    columns: HashMap<(TableId, String), ColumnId>,
}

impl SimpleCatalog {
    /// Create an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table. Returns `self` for chaining.
    ///
    /// - `name`: the SQL table name
    /// - `id`: the numeric table identifier used in [`WalEvent`](crate::WalEvent)
    /// - `arity`: number of columns in the table
    #[must_use]
    pub fn add_table(mut self, name: &str, id: TableId, arity: usize) -> Self {
        self.tables.insert(name.to_string(), (id, arity));
        self
    }

    /// Register a column. Returns `self` for chaining.
    ///
    /// - `table_id`: the table this column belongs to
    /// - `name`: the SQL column name
    /// - `id`: the zero-based column index in a [`RowImage`](crate::RowImage)
    #[must_use]
    pub fn add_column(mut self, table_id: TableId, name: &str, id: ColumnId) -> Self {
        self.columns.insert((table_id, name.to_string()), id);
        self
    }
}

impl SchemaCatalog for SimpleCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.tables.get(table_name).map(|(id, _)| *id)
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId> {
        self.columns
            .get(&(table_id, column_name.to_string()))
            .copied()
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.tables
            .values()
            .find(|(id, _)| *id == table_id)
            .map(|(_, arity)| *arity)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaCatalog;

    #[test]
    fn test_simple_catalog_table_lookup() {
        let catalog = SimpleCatalog::new()
            .add_table("orders", 1, 3)
            .add_column(1, "id", 0)
            .add_column(1, "amount", 1)
            .add_column(1, "status", 2);

        assert_eq!(catalog.table_id("orders"), Some(1));
        assert_eq!(catalog.table_id("unknown"), None);
        assert_eq!(catalog.table_arity(1), Some(3));
        assert_eq!(catalog.table_arity(99), None);
    }

    #[test]
    fn test_simple_catalog_column_lookup() {
        let catalog = SimpleCatalog::new()
            .add_table("orders", 1, 3)
            .add_column(1, "id", 0)
            .add_column(1, "amount", 1);

        assert_eq!(catalog.column_id(1, "id"), Some(0));
        assert_eq!(catalog.column_id(1, "amount"), Some(1));
        assert_eq!(catalog.column_id(1, "missing_col"), None);
        assert_eq!(catalog.column_id(99, "id"), None);
    }

    #[test]
    fn test_simple_catalog_multiple_tables() {
        let catalog = SimpleCatalog::new()
            .add_table("users", 1, 2)
            .add_column(1, "id", 0)
            .add_column(1, "name", 1)
            .add_table("orders", 2, 3)
            .add_column(2, "id", 0)
            .add_column(2, "user_id", 1)
            .add_column(2, "amount", 2);

        assert_eq!(catalog.table_id("users"), Some(1));
        assert_eq!(catalog.table_id("orders"), Some(2));
        assert_eq!(catalog.column_id(1, "name"), Some(1));
        assert_eq!(catalog.column_id(2, "amount"), Some(2));
        // Columns don't bleed across tables
        assert_eq!(catalog.column_id(1, "amount"), None);
    }
}
