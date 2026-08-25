//! The re-read answers' own file.
//!
//! A shard belongs to one table and every subscription in it points at a
//! compiled predicate. An answer that needs a database read has no predicate,
//! and one of them can be woken by several tables, so it is not a property of
//! one table and is not stored as if it were.
//!
//! Layout: a 15-byte header (magic, version, entry count, payload length)
//! followed by the encoded payload. Uncompressed on purpose: this file holds
//! one small record per re-read answer, so compression would buy nothing and
//! cost a dependency on the shard machinery's framing.
//!
//! Each entry carries the fingerprints of the tables it reads, so loading
//! judges every answer on its own tables rather than refusing the whole file
//! because one unrelated table changed.

use alloc::string::String;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

use crate::persistence::shard::ShardFingerprintEnvelope;
use crate::{IdTypes, ReadTier, StorageError, SubscriptionId, TableId};

/// Magic bytes for the re-read file, distinct from a shard's.
const MAGIC: [u8; 5] = *b"SUBQR";

const VERSION: u16 = 3;

/// Header length in bytes.
const HEADER_LEN: usize = 15;

/// Refuse a payload claiming more than this, so a tampered length cannot make
/// the loader allocate wildly. One entry is well under a kilobyte.
const MAX_PAYLOAD_LEN: u64 = 64 * 1024 * 1024;

/// One stored re-read answer.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ReadEntry<I: IdTypes> {
    /// Identity it had, and keeps: a caller holds this id.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered it.
    pub consumer_id: I::ConsumerId,
    /// Durable, or bound to a session.
    pub scope: crate::SubscriptionScope<I>,
    /// The statement, which is what the plan is rebuilt from.
    pub sql: String,
    /// Tables it reads, each with the fingerprint it was saved under.
    pub tables: Vec<(TableId, ShardFingerprintEnvelope)>,
    /// The tier it had when saved.
    pub tier: ReadTier,
    /// Whether each database read runs under the individual consumer's
    /// database identity.
    pub database_reads_per_consumer: bool,
}

impl<I: IdTypes> Clone for ReadEntry<I> {
    fn clone(&self) -> Self {
        Self {
            subscription_id: self.subscription_id,
            consumer_id: self.consumer_id,
            scope: self.scope,
            sql: self.sql.clone(),
            tables: self.tables.clone(),
            tier: self.tier,
            database_reads_per_consumer: self.database_reads_per_consumer,
        }
    }
}

/// Everything the file holds.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ReadsPayload<I: IdTypes> {
    /// One per re-read answer.
    pub entries: Vec<ReadEntry<I>>,
    /// When the file was written, milliseconds since the Unix epoch.
    pub created_at_unix_ms: u64,
}

/// Encode `payload` into the file's bytes.
pub fn serialize<I: IdTypes>(payload: &ReadsPayload<I>) -> Result<Vec<u8>, StorageError> {
    let body = crate::persistence::codec::serialize(payload)
        .map_err(|e| StorageError::Codec(alloc::format!("reads serialize failed: {e}")))?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    #[allow(clippy::cast_possible_truncation)]
    bytes.extend_from_slice(&(payload.entries.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(body.len() as u64).to_le_bytes()[..4]);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Decode a file written by [`serialize`].
///
/// Refuses a wrong magic, a version this build does not write, a truncated
/// body, and a length past this module's own ceiling.
pub fn deserialize<I: IdTypes>(bytes: &[u8]) -> Result<ReadsPayload<I>, StorageError> {
    if bytes.len() < HEADER_LEN {
        return Err(StorageError::Corrupt(String::from(
            "reads file shorter than its header",
        )));
    }
    if bytes[..5] != MAGIC {
        return Err(StorageError::Corrupt(String::from(
            "reads file has the wrong magic",
        )));
    }
    let version = u16::from_be_bytes([bytes[5], bytes[6]]);
    if version != VERSION {
        return Err(StorageError::Corrupt(alloc::format!(
            "reads file version {version}, this build writes {VERSION}"
        )));
    }
    let body_len = u64::from(u32::from_le_bytes([
        bytes[11], bytes[12], bytes[13], bytes[14],
    ]));
    if body_len > MAX_PAYLOAD_LEN {
        return Err(StorageError::Corrupt(alloc::format!(
            "reads file claims {body_len} bytes of payload"
        )));
    }
    let body = bytes
        .get(HEADER_LEN..)
        .filter(|rest| rest.len() as u64 == body_len)
        .ok_or_else(|| {
            StorageError::Corrupt(String::from("reads file payload length does not match"))
        })?;
    crate::persistence::codec::deserialize(body)
        .map_err(|e| StorageError::Codec(alloc::format!("reads deserialize failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultIds;

    fn payload() -> ReadsPayload<DefaultIds> {
        ReadsPayload {
            entries: alloc::vec![ReadEntry::<DefaultIds> {
                subscription_id: 7,
                consumer_id: 3,
                scope: crate::SubscriptionScope::Durable,
                sql: String::from("SELECT MIN(price) FROM orders"),
                tables: alloc::vec![(
                    1,
                    ShardFingerprintEnvelope {
                        algorithm_id: 1,
                        canonicalization_version: 1,
                        profile_id: 1,
                        digest128: [9u8; 16],
                    }
                )],
                tier: ReadTier::Scalar,
                database_reads_per_consumer: true,
            }],
            created_at_unix_ms: 42,
        }
    }

    #[test]
    fn a_written_file_reads_back_the_same() {
        let bytes = serialize(&payload()).expect("serialize");
        let back: ReadsPayload<DefaultIds> = deserialize(&bytes).expect("deserialize");
        assert_eq!(back.entries.len(), 1);
        assert_eq!(back.entries[0].subscription_id, 7);
        assert_eq!(back.entries[0].tier, ReadTier::Scalar);
        assert_eq!(back.entries[0].sql, "SELECT MIN(price) FROM orders");
        assert_eq!(back.created_at_unix_ms, 42);
    }

    #[test]
    fn a_file_from_another_format_is_refused() {
        let mut bytes = serialize(&payload()).expect("serialize");
        bytes[5..7].copy_from_slice(&99u16.to_be_bytes());
        let refused = deserialize::<DefaultIds>(&bytes).expect_err("version is checked");
        assert!(
            alloc::format!("{refused}").contains("version 99"),
            "the refusal names the version it found, got {refused}"
        );
    }

    #[test]
    fn a_truncated_file_is_refused() {
        let bytes = serialize(&payload()).expect("serialize");
        let refused =
            deserialize::<DefaultIds>(&bytes[..bytes.len() - 3]).expect_err("length is checked");
        assert!(alloc::format!("{refused}").contains("length"), "{refused}");
    }
}
