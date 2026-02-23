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
