//! Error types for subql

use crate::{MergeJobId, TableId};
use thiserror::Error;

/// Errors during subscription registration
#[derive(Error, Clone, Debug)]
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

    /// Schema catalog error
    #[error("Schema catalog error: {0}")]
    SchemaCatalog(String),

    /// Storage/persistence error during registration
    #[error("Storage error during registration: {0}")]
    Storage(String),
}

/// Errors during event dispatch
#[derive(Error, Clone, Debug)]
pub enum DispatchError {
    /// Table not registered in engine
    #[error("Unknown table ID: {0}")]
    UnknownTableId(TableId),

    /// Event missing required row image
    #[error("Missing required row image: {0}")]
    MissingRequiredRowImage(&'static str),

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
}

/// Errors during persistence operations
#[derive(Error, Clone, Debug)]
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

    /// Schema fingerprint doesn't match
    #[error("Schema mismatch for table {table_id}: expected fingerprint {expected:016x}, got {got:016x}")]
    SchemaMismatch {
        table_id: TableId,
        expected: u64,
        got: u64,
    },
}

/// Errors during merge operations
#[derive(Error, Clone, Debug)]
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
            RegisterError::SchemaCatalog("catalog unavailable".to_string()).to_string(),
            "Schema catalog error: catalog unavailable"
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
            DispatchError::MissingRequiredRowImage("old_row").to_string(),
            "Missing required row image: old_row"
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
        assert_eq!(
            StorageError::SchemaMismatch {
                table_id: 7,
                expected: 0xAA,
                got: 0xBB
            }
            .to_string(),
            "Schema mismatch for table 7: expected fingerprint 00000000000000aa, got 00000000000000bb"
        );
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
