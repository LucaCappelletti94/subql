//! Shard format with header and validation

use super::codec;
use crate::{IdTypes, SchemaCatalog, StorageError, TableId};
use serde::{Deserialize, Serialize};

/// Shard format version
const SHARD_VERSION: u16 = 1;

/// Magic bytes for shard identification
const MAGIC: &[u8; 5] = b"SUBQL";

/// Shard header (32 bytes, fixed size)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardHeader {
    /// Magic bytes: "SUBQL"
    pub magic: [u8; 5],
    /// Format version
    pub version: u16,
    /// Padding for alignment
    pub padding: u8,
    /// Table ID this shard belongs to
    pub table_id: TableId,
    /// Schema fingerprint (for compatibility checking)
    pub schema_fingerprint: u64,
    /// Uncompressed payload size
    pub uncompressed_size: u64,
    /// Compressed payload size
    pub compressed_size: u64,
}

impl ShardHeader {
    /// Create new shard header
    #[must_use]
    pub const fn new(
        table_id: TableId,
        schema_fingerprint: u64,
        uncompressed_size: u64,
        compressed_size: u64,
    ) -> Self {
        Self {
            magic: *MAGIC,
            version: SHARD_VERSION,
            padding: 0,
            table_id,
            schema_fingerprint,
            uncompressed_size,
            compressed_size,
        }
    }

    /// Validate header
    pub fn validate(&self, catalog: &dyn SchemaCatalog) -> Result<(), StorageError> {
        // Check magic
        if &self.magic != MAGIC {
            return Err(StorageError::Corrupt(format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                MAGIC, self.magic
            )));
        }

        // Check version
        if self.version != SHARD_VERSION {
            return Err(StorageError::VersionMismatch {
                expected: SHARD_VERSION,
                got: self.version,
            });
        }

        // Check schema fingerprint
        if let Some(expected_fp) = catalog.schema_fingerprint(self.table_id) {
            if expected_fp != self.schema_fingerprint {
                return Err(StorageError::SchemaMismatch {
                    table_id: self.table_id,
                    expected: expected_fp,
                    got: self.schema_fingerprint,
                });
            }
        }

        Ok(())
    }
}

/// Shard payload (compressed)
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ShardPayload<I: IdTypes> {
    /// Predicates in this shard
    pub predicates: Vec<PredicateData>,
    /// Bindings in this shard
    pub bindings: Vec<BindingData<I>>,
    /// User dictionary
    pub user_dict: UserDictData<I>,
    /// Shard creation timestamp (milliseconds since Unix epoch)
    pub created_at_unix_ms: u64,
}

impl<I: IdTypes> Clone for ShardPayload<I> {
    fn clone(&self) -> Self {
        Self {
            predicates: self.predicates.clone(),
            bindings: self.bindings.clone(),
            user_dict: self.user_dict.clone(),
            created_at_unix_ms: self.created_at_unix_ms,
        }
    }
}

/// Serializable predicate data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateData {
    pub hash: u128,
    pub normalized_sql: String,
    pub bytecode_instructions: Vec<u8>, // Serialized bytecode
    pub dependency_columns: Vec<u16>,
    pub refcount: u32,
    pub updated_at_unix_ms: u64,
}

/// Serializable binding data
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct BindingData<I: IdTypes> {
    pub subscription_id: I::SubscriptionId,
    pub predicate_hash: u128, // Link to predicate
    pub user_id: I::UserId,
    pub session_id: Option<I::SessionId>,
    pub updated_at_unix_ms: u64,
}

impl<I: IdTypes> Clone for BindingData<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: IdTypes> Copy for BindingData<I> {}

/// Serializable user dictionary
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct UserDictData<I: IdTypes> {
    pub ordinal_to_user: Vec<I::UserId>,
}

impl<I: IdTypes> Clone for UserDictData<I> {
    fn clone(&self) -> Self {
        Self {
            ordinal_to_user: self.ordinal_to_user.clone(),
        }
    }
}

/// Serialize shard to bytes
///
/// Returns full shard (header + compressed payload).
pub fn serialize_shard<I: IdTypes>(
    table_id: TableId,
    payload: &ShardPayload<I>,
    catalog: &dyn SchemaCatalog,
) -> Result<Vec<u8>, StorageError> {
    // Serialize payload
    let uncompressed = bincode::serialize(payload)
        .map_err(|e| StorageError::Codec(format!("Payload serialize error: {e}")))?;

    // Compress payload (reuse the already serialized buffer)
    let compressed = codec::encode_serialized(&uncompressed)?;

    // Get schema fingerprint
    let schema_fingerprint = catalog.schema_fingerprint(table_id).ok_or_else(|| {
        StorageError::Corrupt(format!("No schema fingerprint for table {table_id}"))
    })?;

    // Create header
    let header = ShardHeader::new(
        table_id,
        schema_fingerprint,
        uncompressed.len() as u64,
        compressed.len() as u64,
    );

    // Serialize header
    let header_bytes = bincode::serialize(&header)
        .map_err(|e| StorageError::Codec(format!("Header serialize error: {e}")))?;

    // Concatenate header + compressed payload
    let mut result = header_bytes;
    result.extend_from_slice(&compressed);

    Ok(result)
}

/// Deserialize shard from bytes
///
/// Returns (header, payload).
pub fn deserialize_shard<I: IdTypes>(
    bytes: &[u8],
    catalog: &dyn SchemaCatalog,
) -> Result<(ShardHeader, ShardPayload<I>), StorageError> {
    // Deserialize header
    let header: ShardHeader = bincode::deserialize(bytes)
        .map_err(|e| StorageError::Codec(format!("Header deserialize error: {e}")))?;

    // Validate header
    header.validate(catalog)?;

    // Extract payload bytes (skip header)
    let header_size = bincode::serialized_size(&header)
        .map_err(|e| StorageError::Codec(format!("Header size error: {e}")))?;

    #[allow(clippy::cast_possible_truncation)] // header_size is always small enough for usize
    let payload_bytes = bytes
        .get(header_size as usize..)
        .ok_or_else(|| StorageError::Corrupt("Truncated shard".to_string()))?;

    // Decompress and deserialize payload
    let payload: ShardPayload<I> = codec::decode(payload_bytes)?;

    Ok((header, payload))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::unreadable_literal)]
mod tests {
    use super::*;
    use crate::DefaultIds;
    use std::collections::HashMap;

    struct MockCatalog {
        fingerprints: HashMap<TableId, u64>,
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

    fn make_catalog() -> MockCatalog {
        let mut fingerprints = HashMap::new();
        fingerprints.insert(1, 0x1234_5678_90AB_CDEF);
        MockCatalog { fingerprints }
    }

    #[test]
    fn test_shard_roundtrip() {
        let catalog = make_catalog();

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![10, 20, 30],
            },
            created_at_unix_ms: 1234567890,
        };

        let bytes = serialize_shard(1, &payload, &catalog).unwrap();
        let (header, decoded_payload) = deserialize_shard::<DefaultIds>(&bytes, &catalog).unwrap();

        assert_eq!(header.table_id, 1);
        assert_eq!(header.schema_fingerprint, 0x1234_5678_90AB_CDEF);
        assert_eq!(decoded_payload.user_dict.ordinal_to_user, vec![10, 20, 30]);
    }

    #[test]
    fn test_invalid_magic() {
        let catalog = make_catalog();

        let mut header = ShardHeader::new(1, 0x1234, 100, 80);
        header.magic = *b"WRONG";

        let result = header.validate(&catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_version_mismatch() {
        let catalog = make_catalog();

        let mut header = ShardHeader::new(1, 0x1234_5678_90AB_CDEF, 100, 80);
        header.version = 999;

        let result = header.validate(&catalog);
        assert!(matches!(result, Err(StorageError::VersionMismatch { .. })));
    }

    #[test]
    fn test_schema_mismatch() {
        let catalog = make_catalog();

        let header = ShardHeader::new(1, 0xDEADBEEF, 100, 80);

        let result = header.validate(&catalog);
        assert!(matches!(result, Err(StorageError::SchemaMismatch { .. })));
    }

    #[test]
    fn test_serialize_missing_fingerprint() {
        // Catalog that returns None for schema fingerprint
        let catalog = MockCatalog {
            fingerprints: HashMap::new(), // Empty - no fingerprints
        };

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let result = serialize_shard(1, &payload, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }
}
