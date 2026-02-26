use crate::{SchemaCatalog, TableId};
use std::collections::HashMap;

pub(crate) struct MockCatalog {
    pub(crate) fingerprints: HashMap<TableId, u64>,
}

impl SchemaCatalog for MockCatalog {
    fn table_id(&self, _table_name: &str) -> Option<TableId> {
        Some(1)
    }

    fn column_id(&self, _table_id: TableId, _column_name: &str) -> Option<u16> {
        Some(0)
    }

    fn table_arity(&self, _table_id: TableId) -> Option<usize> {
        Some(5)
    }

    fn schema_fingerprint(&self, table_id: TableId) -> Option<u64> {
        self.fingerprints.get(&table_id).copied()
    }
}

pub(crate) fn make_catalog() -> MockCatalog {
    let mut fingerprints = HashMap::new();
    fingerprints.insert(1, 0x1234_5678_90AB_CDEF);
    MockCatalog { fingerprints }
}
