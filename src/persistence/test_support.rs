use crate::{DefaultIds, SchemaCatalog, TableId};
use std::collections::HashMap;

use super::shard::{ConsumerDictData, ShardPayload};

pub struct MockCatalog {
    pub fingerprints: HashMap<TableId, u64>,
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

pub fn make_catalog() -> MockCatalog {
    let mut fingerprints = HashMap::new();
    fingerprints.insert(1, 0x1234_5678_90AB_CDEF);
    MockCatalog { fingerprints }
}

/// Empty shard payload (no predicates, no bindings, no consumers).
pub fn empty_shard_payload(created_at_unix_ms: u64) -> ShardPayload<DefaultIds> {
    shard_payload_with_consumers(vec![], created_at_unix_ms)
}

/// Shard payload with specific consumer ordinals (no predicates or bindings).
pub fn shard_payload_with_consumers(
    consumers: Vec<u64>,
    created_at_unix_ms: u64,
) -> ShardPayload<DefaultIds> {
    ShardPayload {
        predicates: vec![],
        bindings: vec![],
        consumer_dict: ConsumerDictData {
            ordinal_to_consumer: consumers,
        },
        created_at_unix_ms,
    }
}
