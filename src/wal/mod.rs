//! WAL stream parsing: convert raw CDC bytes into [`WalEvent`]s.
//!
//! The [`WalParser`] trait abstracts over format-specific encodings
//! (wal2json, Maxwell, Debezium, etc.) so callers can feed raw replication
//! messages and receive typed events.

mod debezium;
mod maxwell;
mod pg_type;
mod pgoutput;
mod row_build;
mod wal2json;

pub use debezium::DebeziumParser;
pub use maxwell::MaxwellParser;
pub use pgoutput::PgOutputParser;
pub use wal2json::{Wal2JsonV1Parser, Wal2JsonV2Parser};

use crate::{Cell, ColumnId, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};
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

    /// Payload structure is malformed (mismatched lengths, invalid counts, etc.).
    #[error("Malformed payload: {0}")]
    MalformedPayload(String),

    /// WAL column count does not match catalog arity.
    #[error("Arity mismatch for table {table_id}: WAL has {wal_count} columns, catalog has {catalog_arity}")]
    ArityMismatch {
        table_id: TableId,
        wal_count: usize,
        catalog_arity: usize,
    },

    /// Binary message too short.
    #[error("Truncated binary message: expected {expected} bytes, got {actual}")]
    TruncatedMessage { expected: usize, actual: usize },

    /// DML references unknown relation OID (no preceding Relation message).
    #[error("Unknown relation OID: {0}")]
    UnknownRelationOid(u32),

    /// Unrecognized tuple data tag byte (expected 'n', 'u', or 't').
    #[error("Unknown tuple data tag: 0x{0:02X}")]
    UnknownTupleTag(u8),
}

// ============================================================================
// Shared helpers (used by wal2json and pgoutput)
// ============================================================================

/// Resolve table name through catalog, trying `table` then `schema.table`.
pub(crate) fn resolve_table(
    schema: &str,
    table: &str,
    catalog: &dyn SchemaCatalog,
) -> Result<TableId, WalParseError> {
    if let Some(id) = catalog.table_id(table) {
        return Ok(id);
    }
    let qualified = format!("{schema}.{table}");
    catalog
        .table_id(&qualified)
        .ok_or_else(|| WalParseError::UnknownTable {
            schema: schema.to_string(),
            table: table.to_string(),
        })
}

/// Build a [`PrimaryKey`] from resolved column/value pairs, filtering to only
/// the columns listed in `pk_col_ids`.
pub(crate) fn build_pk_from_resolved(
    resolved: &[(ColumnId, Cell)],
    pk_col_ids: &[ColumnId],
) -> PrimaryKey {
    let mut columns = Vec::with_capacity(pk_col_ids.len());
    let mut values = Vec::with_capacity(pk_col_ids.len());

    for &pk_col in pk_col_ids {
        if let Some((_, cell)) = resolved.iter().find(|(c, _)| *c == pk_col) {
            columns.push(pk_col);
            values.push(cell.clone());
        }
    }

    PrimaryKey {
        columns: std::sync::Arc::from(columns),
        values: std::sync::Arc::from(values),
    }
}

/// Compute changed columns between old and new row images.
pub(crate) fn changed_columns(old: &RowImage, new: &RowImage) -> Vec<ColumnId> {
    let len = old.cells.len().min(new.cells.len());
    let mut changed = Vec::new();

    for i in 0..len {
        let old_cell = &old.cells[i];
        let new_cell = &new.cells[i];
        // Only compare columns present in both images
        if !old_cell.is_missing() && !new_cell.is_missing() && old_cell != new_cell {
            #[allow(clippy::cast_possible_truncation)]
            changed.push(i as ColumnId);
        }
    }

    changed
}
