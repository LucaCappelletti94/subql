use crate::{ColumnId, SchemaCatalog, TableId};
use std::collections::HashMap;

pub(super) struct TestCatalog {
    pub(super) tables: HashMap<String, (TableId, usize)>,
    pub(super) columns: HashMap<(TableId, String), ColumnId>,
    pub(super) primary_keys: HashMap<TableId, Vec<ColumnId>>,
}

impl SchemaCatalog for TestCatalog {
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
        Some(0)
    }

    fn primary_key_columns(&self, table_id: TableId) -> Option<&[ColumnId]> {
        self.primary_keys.get(&table_id).map(Vec::as_slice)
    }
}
