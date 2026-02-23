//! WAL stream parsing: convert raw CDC bytes into [`WalEvent`]s.
//!
//! The [`WalParser`] trait abstracts over format-specific encodings
//! (wal2json, Maxwell, Debezium, etc.) so callers can feed raw replication
//! messages and receive typed events.

mod debezium;
mod map_cdc;
mod maxwell;
mod pg_type;
mod pgoutput;
mod row_build;
mod wal2json;

pub use debezium::DebeziumParser;
pub use maxwell::MaxwellParser;
pub use pgoutput::PgOutputParser;
pub use wal2json::{Wal2JsonV1Parser, Wal2JsonV2Parser};

use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};
use std::collections::HashSet;
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

    /// Numeric value cannot be represented in target runtime type.
    #[error("Numeric overflow in '{field}': value {value} does not fit into {target}")]
    NumericOverflow {
        field: String,
        value: String,
        target: &'static str,
    },

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

/// Resolve PK metadata names to column IDs and require each resolved PK column
/// to be present in the provided row image data.
pub(crate) fn strict_pk_column_ids_from_names(
    table_id: TableId,
    pk_col_names: &[String],
    resolved: &[(ColumnId, Cell)],
    catalog: &dyn SchemaCatalog,
    context: &str,
) -> Result<Vec<ColumnId>, WalParseError> {
    let mut pk_col_ids = Vec::with_capacity(pk_col_names.len());
    let mut seen = HashSet::with_capacity(pk_col_names.len());

    for name in pk_col_names {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        if !seen.insert(col_id) {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} contains duplicate column '{name}' (id {col_id})"
            )));
        }
        if !resolved
            .iter()
            .any(|(resolved_col_id, _)| *resolved_col_id == col_id)
        {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} column '{name}' (id {col_id}) missing from row data"
            )));
        }
        pk_col_ids.push(col_id);
    }

    Ok(pk_col_ids)
}

/// Build PK from catalog metadata, or return an empty PK when metadata is unavailable.
pub(crate) fn pk_from_catalog_or_empty(
    resolved: &[(ColumnId, Cell)],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> PrimaryKey {
    catalog
        .primary_key_columns(table_id)
        .map_or_else(PrimaryKey::empty, |pk_cols| {
            build_pk_from_resolved(resolved, pk_cols)
        })
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

/// Build INSERT event with consistent defaults.
pub(crate) fn insert_event(table_id: TableId, pk: PrimaryKey, new_row: RowImage) -> WalEvent {
    WalEvent {
        kind: EventKind::Insert,
        table_id,
        pk,
        old_row: None,
        new_row: Some(new_row),
        changed_columns: std::sync::Arc::from([]),
    }
}

/// Build UPDATE event with consistent changed-column derivation.
pub(crate) fn update_event(
    table_id: TableId,
    pk: PrimaryKey,
    old_row: Option<RowImage>,
    new_row: RowImage,
) -> WalEvent {
    let changed = if let Some(ref old) = old_row {
        changed_columns(old, &new_row)
    } else {
        Vec::new()
    };

    WalEvent {
        kind: EventKind::Update,
        table_id,
        pk,
        old_row,
        new_row: Some(new_row),
        changed_columns: std::sync::Arc::from(changed),
    }
}

/// Build DELETE event with consistent defaults.
pub(crate) fn delete_event(table_id: TableId, pk: PrimaryKey, old_row: RowImage) -> WalEvent {
    WalEvent {
        kind: EventKind::Delete,
        table_id,
        pk,
        old_row: Some(old_row),
        new_row: None,
        changed_columns: std::sync::Arc::from([]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockCatalog;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_resolve_table_prefers_unqualified_name() {
        let tables = HashMap::from([
            ("users".to_string(), (1_u32, 2_usize)),
            ("public.users".to_string(), (2_u32, 2_usize)),
        ]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, 1);
    }

    #[test]
    fn test_resolve_table_falls_back_to_qualified_name() {
        let tables = HashMap::from([("public.users".to_string(), (2_u32, 2_usize))]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, 2);
    }

    #[test]
    fn test_resolve_table_unknown_table() {
        let catalog = MockCatalog {
            tables: HashMap::new(),
            columns: HashMap::new(),
        };

        let err = resolve_table("public", "users", &catalog).expect_err("must fail");
        match err {
            WalParseError::UnknownTable { schema, table } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_build_pk_from_resolved_filters_and_preserves_pk_order() {
        let resolved = vec![
            (2_u16, Cell::Int(20)),
            (0_u16, Cell::Int(10)),
            (2_u16, Cell::Int(99)),
        ];
        let pk = build_pk_from_resolved(&resolved, &[0, 1, 2]);

        assert_eq!(&*pk.columns, &[0, 2]);
        assert_eq!(&*pk.values, &[Cell::Int(10), Cell::Int(20)]);
    }

    #[test]
    fn test_changed_columns_skips_missing_and_out_of_range_columns() {
        let old = RowImage {
            cells: Arc::from(vec![
                Cell::Int(1),
                Cell::Missing,
                Cell::Int(3),
                Cell::Int(99),
            ]),
        };
        let new = RowImage {
            cells: Arc::from(vec![Cell::Int(1), Cell::Int(2), Cell::Int(4)]),
        };

        assert_eq!(changed_columns(&old, &new), vec![2]);
    }

    #[test]
    fn test_changed_columns_ignores_missing_in_new_row() {
        let old = RowImage {
            cells: Arc::from(vec![Cell::Int(1)]),
        };
        let new = RowImage {
            cells: Arc::from(vec![Cell::Missing]),
        };

        assert!(changed_columns(&old, &new).is_empty());
    }
}
