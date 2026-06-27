//! Error types for subql

use crate::TableId;
#[cfg(feature = "std")]
use crate::{persistence::shard::ShardFingerprintEnvelope, MergeJobId};
use alloc::string::String;
use thiserror::Error;

/// Errors returned by [`crate::SubscriptionEngine::advance_cursor`].
///
/// Triggered only when the materializer tries to install a cursor that
/// is strictly older than the one already stored for that
/// `(session, subscription)` pair, which would never be correct: a
/// successful dispatch never moves a client *backwards* in the CDC
/// stream. If a rewind is genuinely needed (snapshot bootstrap,
/// recovery from a stuck state), use
/// [`crate::SubscriptionEngine::force_set_cursor`] instead.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdvanceCursorError {
    /// The proposed cursor is strictly older than the one already
    /// stored. The map is NOT mutated when this error is returned.
    #[error("cursor rewind rejected: previous={previous:?}, attempted={attempted:?}")]
    NonMonotonic {
        /// Cursor already stored for `(session, subscription)` at the
        /// moment of the call.
        previous: crate::OpaqueCheckpoint,
        /// Cursor the caller tried to install.
        attempted: crate::OpaqueCheckpoint,
    },
}

/// Errors during subscription registration
#[derive(Error, Clone, Debug)]
#[non_exhaustive]
pub enum RegisterError {
    /// SQL parsing failed
    #[error("SQL parse error at line {line}, column {column}: {message}")]
    ParseError {
        line: usize,
        column: usize,
        message: String,
    },

    /// SQL uses unsupported features
    #[error("Unsupported SQL: {0}")]
    UnsupportedSql(String),

    /// Table name not found in catalog
    #[error("Unknown table: {0}")]
    UnknownTable(String),

    /// Table reference resolves to conflicting qualified/unqualified names
    #[error(
        "Ambiguous table reference '{reference}': matches both '{qualified}' and '{unqualified}'"
    )]
    AmbiguousTable {
        reference: String,
        qualified: String,
        unqualified: String,
    },

    /// Column name not found in table
    #[error("Unknown column '{column}' in table {table_id}")]
    UnknownColumn { table_id: TableId, column: String },

    /// Type mismatch in expression
    #[error("Type error: {0}")]
    TypeError(String),

    /// Generic schema-resolution error reported by the underlying database.
    #[error("Schema error: {0}")]
    Schema(String),

    /// Subscription registry is at its configured cap and the eviction
    /// policy is [`crate::EvictionPolicy::Reject`].
    #[error(
        "Subscription registry is full: cap={cap}; raise the cap or pick a different eviction policy"
    )]
    RegistryFull {
        /// Configured cap that the registration tried to exceed.
        cap: usize,
    },

    /// Aggregator subscription on a table with row-level security enabled.
    ///
    /// Under RLS, different viewers observe different result rows, so a
    /// single in-process IVM state cannot be shared across consumers. The
    /// reexec wrapper rejects such registrations until per-consumer total
    /// re-execution lands.
    #[error(
        "Aggregator subscription on RLS-protected table {table_id} requires total re-execution (not yet supported)"
    )]
    AggregatorOnRlsTable {
        /// Table whose RLS made the aggregator unsafe to capture.
        table_id: TableId,
    },

    /// Storage/persistence error during registration
    #[error("Storage error during registration: {0}")]
    Storage(String),
}

/// Errors during event dispatch
#[derive(Error, Clone, Debug)]
#[non_exhaustive]
pub enum DispatchError {
    /// Table not registered in engine
    #[error("Unknown table ID: {0}")]
    UnknownTableId(TableId),

    /// Table arity missing in schema catalog
    #[error("Unknown table arity for table ID: {0}")]
    UnknownTableArity(TableId),

    /// Event missing required row image
    #[error("Missing required row image: {0}")]
    MissingRequiredRowImage(&'static str),

    /// Aggregate UPDATE requires pre-update row image.
    ///
    /// CDC sources that omit `before`/`old` images cannot produce correct
    /// aggregate deltas for UPDATE events.
    #[error(
        "Aggregate UPDATE on table {0} requires old_row image — enable before/old images in CDC source"
    )]
    AggregateUpdateRequiresOldRow(TableId),

    /// UPDATE event requires a complete old_row for exact view-relative delta dispatch.
    ///
    /// Not returned by `consumers()` (which gracefully degrades to single-eval),
    /// but available for callers who require exact three-way splits and want to
    /// enforce REPLICA IDENTITY FULL at the application layer.
    #[error(
        "UPDATE on table {0} requires complete old_row — set REPLICA IDENTITY FULL on source table"
    )]
    UpdateRequiresOldRow(TableId),

    /// Row arity doesn't match schema
    #[error("Invalid row arity for table {table_id}: expected {expected} columns, got {got}")]
    InvalidRowArity {
        table_id: TableId,
        expected: usize,
        got: usize,
    },

    /// VM evaluation error
    #[error("VM evaluation error: {0}")]
    VmError(String),

    /// TRUNCATE received while aggregate subscriptions are active.
    ///
    /// The engine cannot compute count deltas for TRUNCATE (no row images).
    /// Caller must re-query the database to obtain the correct count.
    #[error("TRUNCATE on table {0} requires aggregate count reset — re-query the database")]
    TruncateRequiresReset(crate::TableId),
}

/// Errors during persistence operations
#[cfg(feature = "std")]
#[derive(Error, Clone, Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// I/O error during shard read/write
    #[error("I/O error: {0}")]
    Io(String),

    /// Parent directory fsync failed after shard rename (data is already committed).
    #[error("post_commit_dirsync: {0}")]
    PostCommitDirSync(String),

    /// Configuration error (e.g., missing storage path)
    #[error("Config error: {0}")]
    Config(String),

    /// Codec error (postcard/LZ4)
    #[error("Codec error: {0}")]
    Codec(String),

    /// Shard data is corrupt
    #[error("Corrupt shard: {0}")]
    Corrupt(String),

    /// Shard version incompatible
    #[error("Version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u16, got: u16 },

    /// Schema fingerprint envelope or digest does not match the live catalog.
    ///
    /// The [`ShardFingerprintEnvelope`] `Display` impl renders the envelope as
    /// `<algorithm>:v<canonicalization_version>:p<profile_id>:<hex digest>`
    /// so mismatches surface at any envelope field, not only the digest.
    #[error("Schema mismatch for table {table_id}: expected {expected}, got {got}")]
    SchemaMismatch {
        table_id: TableId,
        expected: ShardFingerprintEnvelope,
        got: ShardFingerprintEnvelope,
    },
}

/// Errors during merge operations
#[cfg(feature = "std")]
#[derive(Error, Clone, Debug)]
#[non_exhaustive]
pub enum MergeError {
    /// Merge job ID not found
    #[error("Unknown merge job: {0}")]
    UnknownJob(MergeJobId),

    /// Background merge build failed
    #[error("Merge build failed: {0}")]
    BuildFailed(String),

    /// Storage error during merge
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_error_display() {
        assert_eq!(
            RegisterError::ParseError {
                line: 3,
                column: 7,
                message: "unexpected token".to_string()
            }
            .to_string(),
            "SQL parse error at line 3, column 7: unexpected token"
        );
        assert_eq!(
            RegisterError::UnsupportedSql("JOIN".to_string()).to_string(),
            "Unsupported SQL: JOIN"
        );
        assert_eq!(
            RegisterError::UnknownTable("users".to_string()).to_string(),
            "Unknown table: users"
        );
        assert_eq!(
            RegisterError::AmbiguousTable {
                reference: "public.orders".to_string(),
                qualified: "public.orders".to_string(),
                unqualified: "orders".to_string(),
            }
            .to_string(),
            "Ambiguous table reference 'public.orders': matches both 'public.orders' and 'orders'"
        );
        assert_eq!(
            RegisterError::UnknownColumn {
                table_id: 9,
                column: "tenant_id".to_string()
            }
            .to_string(),
            "Unknown column 'tenant_id' in table 9"
        );
        assert_eq!(
            RegisterError::TypeError("cannot compare int and text".to_string()).to_string(),
            "Type error: cannot compare int and text"
        );
        assert_eq!(
            RegisterError::Schema("catalog unavailable".to_string()).to_string(),
            "Schema error: catalog unavailable"
        );
        assert_eq!(
            RegisterError::Storage("disk full".to_string()).to_string(),
            "Storage error during registration: disk full"
        );
    }

    #[test]
    fn test_dispatch_error_display() {
        assert_eq!(
            DispatchError::UnknownTableId(42).to_string(),
            "Unknown table ID: 42"
        );
        assert_eq!(
            DispatchError::UnknownTableArity(42).to_string(),
            "Unknown table arity for table ID: 42"
        );
        assert_eq!(
            DispatchError::MissingRequiredRowImage("old_row").to_string(),
            "Missing required row image: old_row"
        );
        assert_eq!(
            DispatchError::AggregateUpdateRequiresOldRow(7).to_string(),
            "Aggregate UPDATE on table 7 requires old_row image — enable before/old images in CDC source"
        );
        assert_eq!(
            DispatchError::UpdateRequiresOldRow(5).to_string(),
            "UPDATE on table 5 requires complete old_row — set REPLICA IDENTITY FULL on source table"
        );
        assert_eq!(
            DispatchError::InvalidRowArity {
                table_id: 3,
                expected: 5,
                got: 4
            }
            .to_string(),
            "Invalid row arity for table 3: expected 5 columns, got 4"
        );
        assert_eq!(
            DispatchError::VmError("stack underflow".to_string()).to_string(),
            "VM evaluation error: stack underflow"
        );
    }

    #[test]
    fn test_storage_error_display() {
        assert_eq!(
            StorageError::Io("permission denied".to_string()).to_string(),
            "I/O error: permission denied"
        );
        assert_eq!(
            StorageError::PostCommitDirSync("I/O error: injected failure".to_string()).to_string(),
            "post_commit_dirsync: I/O error: injected failure"
        );
        assert_eq!(
            StorageError::Config("missing storage path".to_string()).to_string(),
            "Config error: missing storage path"
        );
        assert_eq!(
            StorageError::Codec("lz4 decode failed".to_string()).to_string(),
            "Codec error: lz4 decode failed"
        );
        assert_eq!(
            StorageError::Corrupt("bad magic".to_string()).to_string(),
            "Corrupt shard: bad magic"
        );
        assert_eq!(
            StorageError::VersionMismatch {
                expected: 2,
                got: 1
            }
            .to_string(),
            "Version mismatch: expected 2, got 1"
        );
        let mut expected_digest = [0u8; 16];
        expected_digest[15] = 0xAA;
        let mut got_digest = [0u8; 16];
        got_digest[15] = 0xBB;
        let expected = crate::persistence::shard::ShardFingerprintEnvelope {
            algorithm_id: 1,
            canonicalization_version: 1,
            profile_id: 1,
            digest128: expected_digest,
        };
        let got = crate::persistence::shard::ShardFingerprintEnvelope {
            algorithm_id: 1,
            canonicalization_version: 1,
            profile_id: 1,
            digest128: got_digest,
        };
        let rendered = StorageError::SchemaMismatch {
            table_id: 7,
            expected,
            got,
        }
        .to_string();
        assert!(
            rendered.starts_with("Schema mismatch for table 7: expected sha2-256:v1:p1:"),
            "rendering: {rendered}"
        );
        assert!(rendered.contains("000000aa"));
        assert!(rendered.contains("000000bb"));
    }

    #[test]
    fn test_merge_error_display() {
        assert_eq!(
            MergeError::UnknownJob(11).to_string(),
            "Unknown merge job: 11"
        );
        assert_eq!(
            MergeError::BuildFailed("worker crashed".to_string()).to_string(),
            "Merge build failed: worker crashed"
        );
        assert_eq!(
            MergeError::Storage(StorageError::Config("missing path".to_string())).to_string(),
            "Storage error: Config error: missing path"
        );
    }
}
