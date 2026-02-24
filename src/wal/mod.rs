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
#[cfg(test)]
mod test_support;
mod wal2json;

pub use debezium::DebeziumParser;
pub use maxwell::MaxwellParser;
pub use pgoutput::PgOutputParser;
pub use wal2json::{Wal2JsonV1Parser, Wal2JsonV2Parser};

use crate::table_resolution::{resolve_table_reference, TableResolutionError};
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

/// Parse a UTF-8 JSON message into a typed payload.
pub(crate) fn parse_json_message<T>(data: &[u8]) -> Result<T, WalParseError>
where
    T: serde::de::DeserializeOwned,
{
    let text = std::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))
}

/// Parse JSON payloads that may legally be tombstones (`null`) in CDC streams.
pub(crate) fn parse_json_message_or_tombstone<T>(data: &[u8]) -> Result<Option<T>, WalParseError>
where
    T: serde::de::DeserializeOwned,
{
    parse_json_message(data)
}

#[cfg(test)]
pub(crate) fn parse_single_json_event<T, F>(
    data: &[u8],
    build_event: F,
) -> Result<Vec<WalEvent>, WalParseError>
where
    T: serde::de::DeserializeOwned,
    F: FnOnce(&T) -> Result<WalEvent, WalParseError>,
{
    let message: Option<T> = parse_json_message_or_tombstone(data)?;
    let Some(message) = message else {
        return Ok(Vec::new());
    };
    Ok(vec![build_event(&message)?])
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

    /// Table reference resolves to conflicting qualified/unqualified IDs.
    #[error(
        "Ambiguous table resolution for {schema}.{table}: qualified '{qualified}' -> {qualified_id}, unqualified '{table}' -> {unqualified_id}"
    )]
    AmbiguousTable {
        schema: String,
        table: String,
        qualified: String,
        qualified_id: TableId,
        unqualified_id: TableId,
    },

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

/// Resolve table name through catalog with qualified-first semantics.
///
/// Resolution rules:
/// 1. If `schema.table` resolves, it is preferred.
/// 2. If only `table` resolves, use it.
/// 3. If both resolve to different IDs, return ambiguity instead of guessing.
pub(crate) fn resolve_table(
    schema: &str,
    table: &str,
    catalog: &dyn SchemaCatalog,
) -> Result<TableId, WalParseError> {
    let qualified = (!schema.is_empty()).then(|| format!("{schema}.{table}"));
    resolve_table_reference(qualified.as_deref(), table, catalog).map_err(|err| match err {
        TableResolutionError::Ambiguous {
            qualified,
            qualified_id,
            unqualified_id,
            ..
        } => WalParseError::AmbiguousTable {
            schema: schema.to_string(),
            table: table.to_string(),
            qualified,
            qualified_id,
            unqualified_id,
        },
        TableResolutionError::Unknown { .. } => WalParseError::UnknownTable {
            schema: schema.to_string(),
            table: table.to_string(),
        },
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

/// Build a [`PrimaryKey`] from resolved column/value pairs, requiring every
/// `pk_col_ids` entry to be present (and not `Cell::Missing`).
pub(crate) fn build_pk_from_resolved_strict(
    resolved: &[(ColumnId, Cell)],
    pk_col_ids: &[ColumnId],
    context: &str,
) -> Result<PrimaryKey, WalParseError> {
    let mut columns = Vec::with_capacity(pk_col_ids.len());
    let mut values = Vec::with_capacity(pk_col_ids.len());
    let mut seen = HashSet::with_capacity(pk_col_ids.len());

    for &pk_col in pk_col_ids {
        if !seen.insert(pk_col) {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} contains duplicate column id {pk_col}"
            )));
        }
        let Some((_, cell)) = resolved.iter().find(|(c, _)| *c == pk_col) else {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} column id {pk_col} missing from row data"
            )));
        };
        if cell.is_missing() {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} column id {pk_col} is missing in row data"
            )));
        }
        columns.push(pk_col);
        values.push(cell.clone());
    }

    Ok(PrimaryKey {
        columns: std::sync::Arc::from(columns),
        values: std::sync::Arc::from(values),
    })
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
) -> Result<PrimaryKey, WalParseError> {
    catalog.primary_key_columns(table_id).map_or_else(
        || Ok(PrimaryKey::empty()),
        |pk_cols| build_pk_from_resolved_strict(resolved, pk_cols, "catalog primary key"),
    )
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

/// Build UPDATE event while allowing parsers to disable changed-column
/// derivation when old-row images are known to be partial.
pub(crate) fn update_event_with_old_row_completeness(
    table_id: TableId,
    pk: PrimaryKey,
    old_row: Option<RowImage>,
    new_row: RowImage,
    old_row_complete: bool,
) -> WalEvent {
    let changed = if old_row_complete {
        old_row
            .as_ref()
            .map_or_else(Vec::new, |old| changed_columns(old, &new_row))
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

/// Build TRUNCATE event with consistent defaults.
pub(crate) fn truncate_event(table_id: TableId) -> WalEvent {
    WalEvent {
        kind: EventKind::Truncate,
        table_id,
        pk: PrimaryKey::empty(),
        old_row: None,
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
    fn test_resolve_table_conflicting_matches_errors() {
        let tables = HashMap::from([
            ("users".to_string(), (1_u32, 2_usize)),
            ("public.users".to_string(), (2_u32, 2_usize)),
        ]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let err = resolve_table("public", "users", &catalog).expect_err("must fail");
        assert!(matches!(
            err,
            WalParseError::AmbiguousTable {
                schema,
                table,
                qualified,
                qualified_id: 2,
                unqualified_id: 1,
            } if schema == "public" && table == "users" && qualified == "public.users"
        ));
    }

    #[test]
    fn test_resolve_table_falls_back_to_unqualified_name() {
        let tables = HashMap::from([("users".to_string(), (1_u32, 2_usize))]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, 1);
    }

    #[test]
    fn test_resolve_table_uses_qualified_when_available() {
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

    #[test]
    fn test_parse_json_message_invalid_utf8() {
        let err = parse_json_message::<serde_json::Value>(&[0xFF])
            .expect_err("invalid UTF-8 should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn test_parse_json_message_invalid_json() {
        let err =
            parse_json_message::<serde_json::Value>(b"{").expect_err("malformed JSON should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn test_parse_json_message_allows_tombstone_option() {
        let parsed: Option<serde_json::Value> =
            parse_json_message(b"null").expect("tombstone should parse to None");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_json_message_or_tombstone_object() {
        let parsed: Option<serde_json::Value> =
            parse_json_message_or_tombstone(br#"{"x":1}"#).expect("object should parse");
        assert!(parsed.is_some());
    }

    #[test]
    fn test_parse_single_json_event_tombstone_returns_empty() {
        let events =
            parse_single_json_event::<serde_json::Value, _>(b"null", |_| Ok(truncate_event(1)))
                .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[test]
    fn test_parse_single_json_event_wraps_one_event() {
        let events = parse_single_json_event::<serde_json::Value, _>(br#"{"x":1}"#, |_| {
            Ok(truncate_event(7))
        })
        .expect("object should parse");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].table_id, 7);
        assert_eq!(events[0].kind, EventKind::Truncate);
    }
}
