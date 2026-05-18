//! Shard format with header and validation
//!
//! # v7 header (49 bytes, fixed size)
//!
//! ```text
//!  0..5    magic                    "SUBQL"
//!  5..7    version (LE u16)          7
//!  7       padding                   0
//!  8..12   table_id (LE u32)
//! 12       algorithm_id              1 = sha2-256
//! 13..15   canonicalization_version (BE u16)
//! 15..17   profile_id               (BE u16)
//! 17..33   fingerprint128           (first 16 bytes of fp256)
//! 33..41   uncompressed_size (LE u64)
//! 41..49   compressed_size   (LE u64)
//! ```
//!
//! v7 replaces v6's bare 8-byte `u64` fingerprint with the full
//! `SchemaFingerprint` envelope (algorithm + canonicalization version +
//! profile + 128-bit digest), as required by FINGERPRINT_SPEC §10–§12.
//! Older v6 shards refuse to load — this is a clean break, no compat path.

use super::codec;
use crate::{
    catalog_helpers, compiler::sql_shape::QueryProjection, IdTypes, StorageError, TableId,
};
use serde::{Deserialize, Serialize};
use sql_traits::{
    prelude::DatabaseLike,
    structs::{AlgorithmId, SchemaFingerprint},
};

/// Shard format version. v7: full fingerprint envelope replaces the legacy
/// `u64` field (FINGERPRINT_SPEC §11 / audit §3.3).
const SHARD_VERSION: u16 = 7;

/// Hard cap for decompressed shard payload size (defense in depth).
///
/// Intentionally separate from [`super::codec::MAX_DECODE_UNCOMPRESSED`] — each
/// layer enforces its own limit independently.
const MAX_SHARD_UNCOMPRESSED_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB

/// Magic bytes for shard identification
const MAGIC: &[u8; 5] = b"SUBQL";

/// Shard header (49 bytes, fixed size — see module docs for layout).
const SHARD_HEADER_SIZE: usize = 49;

/// Numeric algorithm identifier for `AlgorithmId::Sha2_256` in the v7 header.
///
/// `SchemaFingerprint` exposes the algorithm as an enum; for the wire format we
/// project it to a single byte. New algorithms added to
/// [`sql_traits::structs::fingerprint::AlgorithmId`] must be assigned a stable
/// byte here, with shard-version bumps for any reassignment.
const ALGORITHM_ID_SHA2_256: u8 = 1;

/// The persisted projection of a [`SchemaFingerprint`] inside a shard header.
///
/// The on-disk header stores the envelope (algorithm + canonicalization
/// version + profile + 128-bit digest) directly so we can compare each field
/// without reconstructing a full [`SchemaFingerprint`] from bytes (the
/// constructor for which is crate-private in `sql-traits`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFingerprintEnvelope {
    /// Numeric algorithm identifier (1 = sha2-256).
    pub algorithm_id: u8,
    /// Canonicalization-rules version (BE on the wire).
    pub canonicalization_version: u16,
    /// Persistence profile identifier (BE on the wire).
    pub profile_id: u16,
    /// First 128 bits of the digest (fingerprint128 per spec §11).
    pub digest128: [u8; 16],
}

impl ShardFingerprintEnvelope {
    /// Project a live [`SchemaFingerprint`] into the on-disk envelope.
    ///
    /// **Wire-format invariant**: the `algorithm_id` byte is a stable on-disk
    /// identifier. Any new [`AlgorithmId`] variant added upstream MUST be
    /// assigned a fresh constant here (and SHOULD bump [`SHARD_VERSION`] so
    /// pre-existing shards can be distinguished). Reassigning an existing
    /// `u8` value would silently produce shards that validate but were
    /// computed under a different hash.
    #[must_use]
    pub fn from_schema(fp: &SchemaFingerprint) -> Self {
        let algorithm_id = match fp.algorithm_id() {
            AlgorithmId::Sha2_256 => ALGORITHM_ID_SHA2_256,
            // Any future variant gets a sentinel that does not match any
            // currently emitted shard, surfacing as a version/algorithm
            // mismatch on load rather than a silent acceptance.
            _ => 0,
        };
        Self {
            algorithm_id,
            canonicalization_version: fp.canonicalization_version(),
            profile_id: fp.profile_id(),
            digest128: fp.fingerprint128(),
        }
    }
}

impl std::fmt::Display for ShardFingerprintEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let algo = match self.algorithm_id {
            ALGORITHM_ID_SHA2_256 => "sha2-256",
            _ => "unknown",
        };
        write!(
            f,
            "{}:v{}:p{}:",
            algo, self.canonicalization_version, self.profile_id
        )?;
        for byte in &self.digest128 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

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
    /// Full schema fingerprint envelope.
    pub fingerprint: ShardFingerprintEnvelope,
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
        fingerprint: ShardFingerprintEnvelope,
        uncompressed_size: u64,
        compressed_size: u64,
    ) -> Self {
        Self {
            magic: *MAGIC,
            version: SHARD_VERSION,
            padding: 0,
            table_id,
            fingerprint,
            uncompressed_size,
            compressed_size,
        }
    }

    /// Validate header against the live catalog.
    ///
    /// Rejects malformed magic, version mismatch, and any difference in the
    /// fingerprint envelope (algorithm, canonicalization version, profile, or
    /// digest). There is no zero-fingerprint bypass — every live catalog must
    /// produce a fingerprint or the shard is rejected.
    pub fn validate<DB: DatabaseLike>(&self, database: &DB) -> Result<(), StorageError> {
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

        // Compute the live fingerprint via the sql-traits catalog. The catalog
        // *must* know about this table — there is no graceful fallback path.
        let live = catalog_helpers::schema_fingerprint(database, self.table_id)
            .map_err(|e| StorageError::Corrupt(format!("fingerprint computation failed: {e}")))?
            .ok_or_else(|| {
                StorageError::Corrupt(format!(
                    "shard references unknown table {} (no fingerprint available)",
                    self.table_id,
                ))
            })?;
        let expected = ShardFingerprintEnvelope::from_schema(&live);

        if expected != self.fingerprint {
            return Err(StorageError::SchemaMismatch {
                table_id: self.table_id,
                expected,
                got: self.fingerprint,
            });
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
    // Fingerprint envelope (FINGERPRINT_SPEC §10.1: BE for the two u16 fields).
    bytes[12] = header.fingerprint.algorithm_id;
    bytes[13..15].copy_from_slice(&header.fingerprint.canonicalization_version.to_be_bytes());
    bytes[15..17].copy_from_slice(&header.fingerprint.profile_id.to_be_bytes());
    bytes[17..33].copy_from_slice(&header.fingerprint.digest128);
    bytes[33..41].copy_from_slice(&header.uncompressed_size.to_le_bytes());
    bytes[41..49].copy_from_slice(&header.compressed_size.to_le_bytes());
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

    let mut digest128 = [0_u8; 16];
    digest128.copy_from_slice(&bytes[17..33]);

    Ok(ShardHeader {
        magic,
        version: u16::from_le_bytes([bytes[5], bytes[6]]),
        padding: bytes[7],
        table_id: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        fingerprint: ShardFingerprintEnvelope {
            algorithm_id: bytes[12],
            canonicalization_version: u16::from_be_bytes([bytes[13], bytes[14]]),
            profile_id: u16::from_be_bytes([bytes[15], bytes[16]]),
            digest128,
        },
        uncompressed_size: u64::from_le_bytes([
            bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39], bytes[40],
        ]),
        compressed_size: u64::from_le_bytes([
            bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47], bytes[48],
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
pub fn serialize_shard<I: IdTypes, DB: DatabaseLike>(
    table_id: TableId,
    payload: &ShardPayload<I>,
    database: &DB,
) -> Result<Vec<u8>, StorageError> {
    let uncompressed = codec::serialize(payload)?;

    // Compress payload (reuse the already serialized buffer)
    let compressed = codec::encode_serialized(&uncompressed)?;

    // Compute the full fingerprint envelope from the live catalog.
    let live = catalog_helpers::schema_fingerprint(database, table_id)
        .map_err(|e| StorageError::Corrupt(format!("fingerprint computation failed: {e}")))?
        .ok_or_else(|| {
            StorageError::Corrupt(format!("No schema fingerprint for table {table_id}"))
        })?;
    let fingerprint = ShardFingerprintEnvelope::from_schema(&live);

    // Create header
    let header = ShardHeader::new(
        table_id,
        fingerprint,
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
pub fn deserialize_shard<I: IdTypes, DB: DatabaseLike>(
    bytes: &[u8],
    database: &DB,
) -> Result<(ShardHeader, ShardPayload<I>), StorageError> {
    let header = decode_header(bytes)?;

    // Size sanity check first — a tampered header claiming `u64::MAX` would
    // otherwise burn a SHA-256 catalog recomputation in `validate` before
    // being rejected here.
    if header.uncompressed_size > MAX_SHARD_UNCOMPRESSED_SIZE {
        return Err(StorageError::Corrupt(format!(
            "Uncompressed payload too large: {} > {}",
            header.uncompressed_size, MAX_SHARD_UNCOMPRESSED_SIZE
        )));
    }

    // Validate header
    header.validate(database)?;

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
        empty_shard_payload, make_catalog, make_divergent_catalog, shard_payload_with_consumers,
    };
    use super::*;
    use crate::{catalog_helpers, DefaultIds};

    /// Resolve the single test-fixture table id from a [`ParserDB`].
    fn fixture_table_id(db: &sql_traits::structs::ParserDB) -> TableId {
        catalog_helpers::table_id(db, "orders").expect("fixture table 'orders' exists")
    }

    /// Serialize a shard, decode+mutate its header, and re-encode the tampered
    /// bytes. The payload bytes are kept unchanged.
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
        let tid = fixture_table_id(&catalog);
        let payload = shard_payload_with_consumers(vec![10, 20, 30], 1_234_567_890);

        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();
        let (header, decoded_payload) =
            deserialize_shard::<DefaultIds, _>(&bytes, &catalog).unwrap();

        assert_eq!(header.table_id, tid);
        let expected = catalog_helpers::schema_fingerprint(&catalog, tid)
            .unwrap()
            .unwrap();
        assert_eq!(
            header.fingerprint,
            ShardFingerprintEnvelope::from_schema(&expected)
        );
        assert_eq!(
            decoded_payload.consumer_dict.ordinal_to_consumer,
            vec![10, 20, 30]
        );
    }

    #[test]
    fn test_invalid_magic() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let live = catalog_helpers::schema_fingerprint(&catalog, tid)
            .unwrap()
            .unwrap();

        let mut header =
            ShardHeader::new(tid, ShardFingerprintEnvelope::from_schema(&live), 100, 80);
        header.magic = *b"WRONG";

        let result = header.validate(&catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_version_mismatch() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let live = catalog_helpers::schema_fingerprint(&catalog, tid)
            .unwrap()
            .unwrap();

        let mut header =
            ShardHeader::new(tid, ShardFingerprintEnvelope::from_schema(&live), 100, 80);
        header.version = 999;

        let result = header.validate(&catalog);
        assert!(matches!(result, Err(StorageError::VersionMismatch { .. })));
    }

    /// MIG-001: header with a different `canonicalization_version` than the
    /// live catalog must be rejected even when the digest happens to match.
    /// MIG-002: same for `profile_id`. We exercise both bits here against a
    /// well-formed digest.
    #[test]
    fn test_schema_mismatch_digest() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let live = catalog_helpers::schema_fingerprint(&catalog, tid)
            .unwrap()
            .unwrap();

        let mut env = ShardFingerprintEnvelope::from_schema(&live);
        // Flip a single bit of the digest — envelope metadata stays intact.
        env.digest128[0] ^= 0x01;
        let header = ShardHeader::new(tid, env, 100, 80);

        let result = header.validate(&catalog);
        assert!(matches!(result, Err(StorageError::SchemaMismatch { .. })));
    }

    #[test]
    fn test_deserialize_rejects_compressed_size_mismatch() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = shard_payload_with_consumers(vec![1, 2, 3], 1);

        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.compressed_size = hdr.compressed_size.saturating_add(1);
        });

        let result = deserialize_shard::<DefaultIds, _>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_deserialize_rejects_uncompressed_size_mismatch() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = shard_payload_with_consumers(vec![7, 8], 2);

        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.uncompressed_size = hdr.uncompressed_size.saturating_add(1);
        });

        let result = deserialize_shard::<DefaultIds, _>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    #[test]
    fn test_deserialize_rejects_oversized_uncompressed_header() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = shard_payload_with_consumers(vec![42], 3);

        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.uncompressed_size = u64::MAX;
        });

        let result = deserialize_shard::<DefaultIds, _>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    /// Shard references a table the live catalog doesn't know about. With v7
    /// there is no zero-fingerprint bypass — every catalog must produce a
    /// fingerprint for any table referenced by a shard.
    #[test]
    fn test_unknown_table_is_corrupt() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = empty_shard_payload(1000);
        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();

        // Use a catalog that doesn't know about the table id we wrote into the
        // shard. ParserDB::parse with a different table layout yields a
        // different id space; pick an obviously out-of-range table_id.
        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.table_id = u32::MAX;
        });

        let result = deserialize_shard::<DefaultIds, _>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::Corrupt(_))));
    }

    // ============================================================================
    // Step 3 verification tests (audit §3.3 / FINGERPRINT_SPEC §10–§12)
    // ============================================================================

    /// Every envelope field roundtrips through the on-wire header.
    #[test]
    fn test_v7_envelope_roundtrip() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = shard_payload_with_consumers(vec![1, 2, 3], 42);

        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();
        let (header, _) = deserialize_shard::<DefaultIds, _>(&bytes, &catalog).unwrap();

        assert_eq!(header.version, 7);
        assert_eq!(header.fingerprint.algorithm_id, ALGORITHM_ID_SHA2_256);
        assert_eq!(header.fingerprint.canonicalization_version, 1);
        assert_eq!(header.fingerprint.profile_id, 1);

        let live = catalog_helpers::schema_fingerprint(&catalog, tid)
            .unwrap()
            .unwrap();
        assert_eq!(header.fingerprint.digest128, live.fingerprint128());
    }

    /// Loading a shard whose header carries v6 must fail with
    /// `VersionMismatch` — no legacy decode path is supported.
    #[test]
    fn test_v6_rejected() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = empty_shard_payload(1);
        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();

        let tampered = tamper_shard_header(&bytes, |hdr| {
            hdr.version = 6;
        });

        let result = deserialize_shard::<DefaultIds, _>(&tampered, &catalog);
        assert!(
            matches!(
                &result,
                Err(StorageError::VersionMismatch {
                    expected: 7,
                    got: 6
                })
            ),
            "expected VersionMismatch{{expected: 7, got: 6}}, got {result:?}"
        );
    }

    /// Writing a shard under one schema and loading it under a different
    /// schema for the same table id must fail with `SchemaMismatch` — the
    /// digest differs even though all envelope metadata matches.
    #[test]
    fn test_envelope_mismatch_rejected() {
        let writer = make_catalog();
        let loader = make_divergent_catalog();
        let tid = fixture_table_id(&writer);
        assert_eq!(
            tid,
            fixture_table_id(&loader),
            "both fixtures must agree on the table id used by serialize_shard"
        );

        let payload = empty_shard_payload(1);
        let bytes = serialize_shard(tid, &payload, &writer).unwrap();

        let result = deserialize_shard::<DefaultIds, _>(&bytes, &loader);
        assert!(matches!(result, Err(StorageError::SchemaMismatch { .. })));
    }

    /// A header carrying an unknown algorithm_id (e.g. a future hash that the
    /// live catalog isn't emitting) must be rejected. The digest may be
    /// otherwise valid; the envelope metadata mismatch alone is fatal.
    #[test]
    fn test_algorithm_id_mismatch_rejected() {
        let catalog = make_catalog();
        let tid = fixture_table_id(&catalog);
        let payload = empty_shard_payload(1);
        let bytes = serialize_shard(tid, &payload, &catalog).unwrap();

        let tampered = tamper_shard_header(&bytes, |hdr| {
            // Pretend the digest was produced by some hash other than sha2-256.
            hdr.fingerprint.algorithm_id = ALGORITHM_ID_SHA2_256.wrapping_add(99);
        });

        let result = deserialize_shard::<DefaultIds, _>(&tampered, &catalog);
        assert!(matches!(result, Err(StorageError::SchemaMismatch { .. })));
    }

    #[test]
    fn decompression_caps_are_consistent() {
        assert!(
            MAX_SHARD_UNCOMPRESSED_SIZE <= super::codec::MAX_DECODE_UNCOMPRESSED as u64,
            "Shard cap must not exceed codec cap"
        );
    }
}
