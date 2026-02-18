//! WAL stream parsing: convert raw CDC bytes into [`WalEvent`]s.
//!
//! The [`WalParser`] trait abstracts over format-specific encodings
//! (wal2json, Maxwell, Debezium, etc.) so callers can feed raw replication
//! messages and receive typed events.

mod pg_type;
mod wal2json;

pub use wal2json::{Wal2JsonV1Parser, Wal2JsonV2Parser};

use crate::{SchemaCatalog, TableId, WalEvent};
use thiserror::Error;

/// Trait for converting raw WAL bytes into typed [`WalEvent`]s.
pub trait WalParser: Send + Sync {
    /// Parse a raw WAL message into zero or more events.
    ///
    /// Batched formats (e.g. wal2json v1) may return multiple events per
    /// message; per-change formats (e.g. wal2json v2) return exactly one.
    fn parse_wal_message(
        &self,
        data: &[u8],
        catalog: &dyn SchemaCatalog,
    ) -> Result<Vec<WalEvent>, WalParseError>;
}

/// Errors that can occur during WAL message parsing.
#[derive(Error, Clone, Debug)]
pub enum WalParseError {
    /// Raw bytes were not valid UTF-8 (required by JSON formats).
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(String),

    /// JSON deserialization failed.
    #[error("JSON error: {0}")]
    JsonError(String),

    /// Unrecognized event kind / action value.
    #[error("Unknown event kind: {0}")]
    UnknownEventKind(String),

    /// Table not found in schema catalog.
    #[error("Unknown table: {schema}.{table}")]
    UnknownTable { schema: String, table: String },

    /// Column name not found in schema catalog.
    #[error("Unknown column '{column}' in table {table_id}")]
    UnknownColumn { table_id: TableId, column: String },

    /// A required JSON field was absent.
    #[error("Missing field: {0}")]
    MissingField(String),

    /// WAL column count does not match catalog arity.
    #[error("Arity mismatch for table {table_id}: WAL has {wal_count} columns, catalog has {catalog_arity}")]
    ArityMismatch {
        table_id: TableId,
        wal_count: usize,
        catalog_arity: usize,
    },
}
