//! Shard format with header and validation

use super::codec;
use crate::{compiler::sql_shape::QueryProjection, IdTypes, SchemaCatalog, StorageError, TableId};
use serde::{Deserialize, Serialize};

/// Shard format version
const SHARD_VERSION: u16 = 6; // v6: AggSpec::CountColumn, AggSpec::Avg added

/// Hard cap for decompressed shard payload size (defense in depth).
///
/// Intentionally separate from [`super::codec::MAX_DECODE_UNCOMPRESSED`] — each
/// layer enforces its own limit independently.
const MAX_SHARD_UNCOMPRESSED_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB

/// Magic bytes for shard identification
const MAGIC: &[u8; 5] = b"SUBQL";

/// Shard header (36 bytes, fixed size)
const SHARD_HEADER_SIZE: usize = 36;

/// Shard header metadata.
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

        // Check schema fingerprint.
        // If the shard has a recorded fingerprint but the catalog returns None
        // (unknown table), reject the shard to prevent silent bypass.
        match catalog.schema_fingerprint(self.table_id) {
            Some(expected_fp) => {
                if expected_fp != self.schema_fingerprint {
                    return Err(StorageError::SchemaMismatch {
                        table_id: self.table_id,
                        expected: expected_fp,
                        got: self.schema_fingerprint,
                    });
                }
            }
            None => {
                if self.schema_fingerprint != 0 {
                    return Err(StorageError::SchemaMismatch {
                        table_id: self.table_id,
                        expected: 0,
                        got: self.schema_fingerprint,
                    });
                }
            }
        }

        Ok(())
    }
}

fn encode_header(header: &ShardHeader) -> [u8; SHARD_HEADER_SIZE] {
    let mut bytes = [0_u8; SHARD_HEADER_SIZE];
    bytes[0..5].copy_from_slice(&header.magic);
    bytes[5..7].copy_from_slice(&header.version.to_le_bytes());
    bytes[7] = header.padding;
    bytes[8..12].copy_from_slice(&header.table_id.to_le_bytes());
    bytes[12..20].copy_from_slice(&header.schema_fingerprint.to_le_bytes());
    bytes[20..28].copy_from_slice(&header.uncompressed_size.to_le_bytes());
    bytes[28..36].copy_from_slice(&header.compressed_size.to_le_bytes());
    bytes
}

fn decode_header(bytes: &[u8]) -> Result<ShardHeader, StorageError> {
    if bytes.len() < SHARD_HEADER_SIZE {
        return Err(StorageError::Corrupt(format!(
            "Truncated shard header: expected at least {SHARD_HEADER_SIZE} bytes, got {}",
            bytes.len()
        )));
    }

    let mut magic = [0_u8; 5];
    magic.copy_from_slice(&bytes[0..5]);

    Ok(ShardHeader {
        magic,
        version: u16::from_le_bytes([bytes[5], bytes[6]]),
        padding: bytes[7],
        table_id: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        schema_fingerprint: u64::from_le_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ]),
        uncompressed_size: u64::from_le_bytes([
            bytes[20], bytes[21], bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27],
        ]),
        compressed_size: u64::from_le_bytes([
            bytes[28], bytes[29], bytes[30], bytes[31], bytes[32], bytes[33], bytes[34], bytes[35],
        ]),
    })
}

/// Shard payload (compressed)
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ShardPayload<I: IdTypes> {
    /// Predicates in this shard
    pub predicates: Vec<PredicateData>,
    /// Bindings in this shard
    pub bindings: Vec<BindingData<I>>,
    /// Consumer dictionary
    pub consumer_dict: ConsumerDictData<I>,
    /// Shard creation timestamp (milliseconds since Unix epoch)
    pub created_at_unix_ms: u64,
}

impl<I: IdTypes> Clone for ShardPayload<I> {
    fn clone(&self) -> Self {
        Self {
            predicates: self.predicates.clone(),
            bindings: self.bindings.clone(),
            consumer_dict: self.consumer_dict.clone(),
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
    pub prefilter_plan: Vec<u8>,        // Serialized prefilter plan
    pub dependency_columns: Vec<u16>,
    pub projection: QueryProjection,
    pub refcount: u32,
    pub updated_at_unix_ms: u64,
}

/// Serializable binding data
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct BindingData<I: IdTypes> {
    pub subscription_id: crate::SubscriptionId,
    pub predicate_hash: u128, // Link to predicate
    pub consumer_id: I::ConsumerId,
    pub scope: crate::SubscriptionScope<I>,
    pub updated_at_unix_ms: u64,
}

impl<I: IdTypes> Clone for BindingData<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: IdTypes> Copy for BindingData<I> {}

/// Serializable consumer dictionary
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ConsumerDictData<I: IdTypes> {
    pub ordinal_to_consumer: Vec<I::ConsumerId>,
}

impl<I: IdTypes> Clone for ConsumerDictData<I> {
    fn clone(&self) -> Self {
        Self {
            ordinal_to_consumer: self.ordinal_to_consumer.clone(),
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
    let uncompressed = codec::serialize(payload)?;

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

    let header_bytes = encode_header(&header);

    // Concatenate fixed-size header + compressed payload
    let mut result = Vec::with_capacity(SHARD_HEADER_SIZE + compressed.len());
    result.extend_from_slice(&header_bytes);
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
    let header = decode_header(bytes)?;

    // Validate header
    header.validate(catalog)?;

    if header.uncompressed_size > MAX_SHARD_UNCOMPRESSED_SIZE {
        return Err(StorageError::Corrupt(format!(
            "Uncompressed payload too large: {} > {}",
            header.uncompressed_size, MAX_SHARD_UNCOMPRESSED_SIZE
        )));
    }

    // Extract payload bytes (skip fixed-size header).
    let payload_bytes = bytes
        .get(SHARD_HEADER_SIZE..)
        .ok_or_else(|| StorageError::Corrupt("Truncated shard".to_string()))?;

    let expected_compressed = usize::try_from(header.compressed_size)
        .map_err(|_| StorageError::Corrupt("Compressed size does not fit usize".to_string()))?;
    if payload_bytes.len() != expected_compressed {
        return Err(StorageError::Corrupt(format!(
            "Compressed payload size mismatch: header {}, actual {}",
            expected_compressed,
            payload_bytes.len()
        )));
    }

    let expected_uncompressed = usize::try_from(header.uncompressed_size)
        .map_err(|_| StorageError::Corrupt("Uncompressed size does not fit usize".to_string()))?;
    let decompressed = codec::decompress_with_limit(payload_bytes, expected_uncompressed)?;
    if decompressed.len() != expected_uncompressed {
        return Err(StorageError::Corrupt(format!(
            "Uncompressed payload size mismatch: header {}, actual {}",
            expected_uncompressed,
            decompressed.len()
        )));
    }

    // Deserialize payload
    let payload: ShardPayload<I> = codec::deserialize(&decompressed)?;

    Ok((header, payload))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::unreadable_literal)]
mod tests {
    use super::super::test_support::{
        empty_shard_payload, make_catalog, shard_payload_with_consumers, MockCatalog,
    };
    use super::*;
    use crate::DefaultIds;
    use std::collections::HashMap;

    /// Serialize a shard, decode+mutate its header, and re-encode the tampered
    /// bytes.  The payload bytes are kept unchanged.
    fn tamper_shard_header(bytes: &[u8], mutate: impl FnOnce(&mut ShardHeader)) -> Vec<u8> {
        let mut hdr = decode_header(bytes).unwrap();
        mutate(&mut hdr);
        let mut tampered = encode_header(&hdr).to_vec();
        tampered.extend_from_slice(&bytes[SHARD_HEADER_SIZE..]);
        tampered
    }

    #[test]
    fn test_shard_roundtrip() {
        let catalog = make_catalog();
        let payload = shard_payload_with_consumers(vec![10, 20, 30], 1234567890);

        let bytes = serialize_shard(1, &payload, &catalog).unwrap();
        let (header, decoded_payload) = deserialize_shard::<DefaultIds>(&bytes, &catalog).unwrap();

        assert_eq!(header.table_id, 1);
        assert_eq!(header.schema_fingerprint, 0x1234_5678_90AB_CDEF);
        assert_eq!(
            decoded_payload.consumer_dict.ordinal_to_consumer,
            vec![10, 20, 30]
        );
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
        let catalog = MockCatalog {
            fingerprints: HashMap::new(),
        };
        let payload = empty_shard_payload(1000);

        let result = serialize_shard(1, &payload, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_deserialize_rejects_compressed_size_mismatch() {
        let catalog = make_catalog();
        let payload = shard_payload_with_consumers(vec![1, 2, 3], 1);

        let bytes = serialize_shard(1, &payload, &catalog).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.compressed_size = hdr.compressed_size.saturating_add(1);
        });

        let result = deserialize_shard::<DefaultIds>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_deserialize_rejects_uncompressed_size_mismatch() {
        let catalog = make_catalog();
        let payload = shard_payload_with_consumers(vec![7, 8], 2);

        let bytes = serialize_shard(1, &payload, &catalog).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.uncompressed_size = hdr.uncompressed_size.saturating_add(1);
        });

        let result = deserialize_shard::<DefaultIds>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_deserialize_rejects_oversized_uncompressed_header() {
        let catalog = make_catalog();
        let payload = shard_payload_with_consumers(vec![42], 3);

        let bytes = serialize_shard(1, &payload, &catalog).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.uncompressed_size = u64::MAX;
        });

        let result = deserialize_shard::<DefaultIds>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    // =========================================================================
    // D5 — Schema fingerprint bypass must be blocked
    // =========================================================================

    #[test]
    fn test_fingerprint_bypass_blocked_when_catalog_returns_none() {
        let catalog_with_fingerprint = make_catalog();
        let payload = empty_shard_payload(1000);
        let bytes = serialize_shard(1, &payload, &catalog_with_fingerprint).unwrap();

        let catalog_without_fingerprint = MockCatalog {
            fingerprints: HashMap::new(),
        };

        let result = deserialize_shard::<DefaultIds>(&bytes, &catalog_without_fingerprint);
        assert!(
            matches!(result, Err(StorageError::SchemaMismatch { .. })),
            "Must reject shard with fingerprint when catalog returns None"
        );
    }

    #[test]
    fn test_fingerprint_zero_shard_accepted_when_catalog_returns_none() {
        let catalog_with_fingerprint = make_catalog();
        let payload = empty_shard_payload(1000);

        let bytes = serialize_shard(1, &payload, &catalog_with_fingerprint).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.schema_fingerprint = 0;
        });

        let catalog_without_fingerprint = MockCatalog {
            fingerprints: HashMap::new(),
        };

        let result = deserialize_shard::<DefaultIds>(&tampered, &catalog_without_fingerprint);
        assert!(
            result.is_ok(),
            "Zero fingerprint with None catalog should succeed"
        );
    }

    #[test]
    fn decompression_caps_are_consistent() {
        assert!(
            MAX_SHARD_UNCOMPRESSED_SIZE <= super::codec::MAX_DECODE_UNCOMPRESSED as u64,
            "Shard cap must not exceed codec cap"
        );
    }
}
