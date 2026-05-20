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
use crate::{catalog_helpers, Cell, ColumnId, EventKind, PrimaryKey, RowImage, TableId, WalEvent};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use hashbrown::HashSet;
use sql_traits::prelude::DatabaseLike;
use thiserror::Error;

/// Trait for converting raw WAL bytes into typed [`WalEvent`]s.
///
/// Parameterized by the concrete [`DatabaseLike`] implementation supplying
/// schema metadata at parse time. A single parser type can implement this
/// trait for every `DB` (see e.g. [`PgOutputParser`]).
pub trait WalParser<DB: DatabaseLike>: Send + Sync {
    /// Parse a raw WAL message into zero or more events.
    ///
    /// Batched formats (e.g. wal2json v1) may return multiple events per
    /// message; per-change formats (e.g. wal2json v2) return exactly one.
    ///
    /// # Examples
    /// ```
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{Cell, EventKind, Wal2JsonV2Parser, WalParseError, WalParser};
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    /// )
    /// .expect("DDL parses");
    ///
    /// let parser = Wal2JsonV2Parser;
    /// let message = r#"{
    ///   "action": "I",
    ///   "schema": "public",
    ///   "table": "orders",
    ///   "columns": [
    ///     {"name": "id", "type": "integer", "value": 7},
    ///     {"name": "status", "type": "text", "value": "paid"}
    ///   ],
    ///   "pk": [{"name": "id", "type": "integer"}]
    /// }"#;
    ///
    /// let events = parser.parse_wal_message(message.as_bytes(), &database)?;
    /// assert_eq!(events.len(), 1);
    /// assert_eq!(events[0].kind(), EventKind::Insert);
    /// assert_eq!(
    ///     events[0].new_row().as_ref().and_then(|row| row.get(1)),
    ///     Some(&Cell::String("paid".into()))
    /// );
    /// # Ok::<(), WalParseError>(())
    /// ```
    fn parse_wal_message(&self, data: &[u8], database: &DB)
        -> Result<Vec<WalEvent>, WalParseError>;
}

/// Parse a UTF-8 JSON message into a typed payload.
pub(crate) fn parse_json_message<T>(data: &[u8]) -> Result<T, WalParseError>
where
    T: serde::de::DeserializeOwned,
{
    let text = core::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))
}

/// Parse JSON payloads that may legally be tombstones (`null`) in CDC streams.
pub(crate) fn parse_json_message_or_tombstone<T>(data: &[u8]) -> Result<Option<T>, WalParseError>
where
    T: serde::de::DeserializeOwned,
{
    parse_json_message(data)
}

pub(crate) fn parse_single_json_event<T, F>(
    data: &[u8],
    build_event: F,
) -> Result<Vec<WalEvent>, WalParseError>
where
    T: serde::de::DeserializeOwned,
    F: FnOnce(&T) -> Result<Option<WalEvent>, WalParseError>,
{
    let message: Option<T> = parse_json_message_or_tombstone(data)?;
    let Some(message) = message else {
        return Ok(Vec::new());
    };
    Ok(build_event(&message)?.into_iter().collect())
}

pub(crate) fn skip_unknown_event_kind(
    result: Result<WalEvent, WalParseError>,
    parser_name: &'static str,
    token_name: &'static str,
) -> Result<Option<WalEvent>, WalParseError> {
    match result {
        Ok(event) => Ok(Some(event)),
        Err(WalParseError::UnknownEventKind(kind)) => {
            #[cfg(feature = "observability")]
            tracing::warn!("{parser_name}: skipping unknown {token_name} '{kind}'");
            #[cfg(not(feature = "observability"))]
            let _ = (&parser_name, &token_name, &kind);
            Ok(None)
        }
        Err(err) => Err(err),
    }
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

    /// Table not found in schema database.
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

    /// Column name not found in schema database.
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

    /// WAL column count does not match database arity.
    #[error("Arity mismatch for table {table_id}: WAL has {wal_count} columns, database has {catalog_arity}")]
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

/// Resolve table name through database with qualified-first semantics.
///
/// Resolution rules:
/// 1. If `schema.table` resolves, it is preferred.
/// 2. If only `table` resolves, use it.
/// 3. If both resolve to different IDs, return ambiguity instead of guessing.
pub(crate) fn resolve_table<DB: DatabaseLike>(
    schema: &str,
    table: &str,
    database: &DB,
) -> Result<TableId, WalParseError> {
    let qualified = (!schema.is_empty()).then(|| format!("{schema}.{table}"));
    resolve_table_reference(qualified.as_deref(), table, database).map_err(|err| match err {
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

    PrimaryKey::new(
        alloc::sync::Arc::from(columns),
        alloc::sync::Arc::from(values),
    )
    .expect("pk columns and values are built in lockstep")
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

    Ok(PrimaryKey::new(
        alloc::sync::Arc::from(columns),
        alloc::sync::Arc::from(values),
    )
    .expect("strict PK construction pushes columns and values in lockstep"))
}

/// Resolve PK metadata names to column IDs and require each resolved PK column
/// to be present in the provided row image data.
pub(crate) fn strict_pk_column_ids_from_names<DB: DatabaseLike>(
    table_id: TableId,
    pk_col_names: &[String],
    resolved: &[(ColumnId, Cell)],
    database: &DB,
    context: &str,
) -> Result<Vec<ColumnId>, WalParseError> {
    let mut pk_col_ids = Vec::with_capacity(pk_col_names.len());
    let mut seen = HashSet::with_capacity(pk_col_names.len());

    for name in pk_col_names {
        let col_id = catalog_helpers::column_id(database, table_id, name).ok_or_else(|| {
            WalParseError::UnknownColumn {
                table_id,
                column: name.clone(),
            }
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

/// Build PK from database metadata, or return an empty PK when metadata is unavailable.
pub(crate) fn pk_from_catalog_or_empty<DB: DatabaseLike>(
    resolved: &[(ColumnId, Cell)],
    table_id: TableId,
    database: &DB,
) -> Result<PrimaryKey, WalParseError> {
    catalog_helpers::primary_key_columns(database, table_id).map_or_else(
        || Ok(PrimaryKey::empty()),
        |pk_cols| build_pk_from_resolved_strict(resolved, &pk_cols, "database primary key"),
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

/// Returns true when every cell in the old row is non-Missing,
/// meaning the row image is complete and suitable for changed-column derivation.
pub(crate) fn old_row_is_complete(old_row: Option<&RowImage>) -> bool {
    old_row.is_some_and(|row| row.cells.iter().all(|cell| !cell.is_missing()))
}

/// Build INSERT event with consistent defaults.
pub(crate) fn insert_event(
    table_id: TableId,
    pk: PrimaryKey,
    new_row: RowImage,
) -> Result<WalEvent, WalParseError> {
    WalEvent::builder(table_id)
        .insert()
        .pk(pk)
        .new_row(new_row)
        .build()
        .map_err(|e| WalParseError::MalformedPayload(format!("invalid insert event: {e}")))
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

    WalEvent::builder(table_id)
        .update()
        .pk(pk)
        .new_row(new_row)
        .maybe_old_row(old_row)
        .changed_columns(alloc::sync::Arc::from(changed))
        .build()
        .expect("update_event_with_old_row_completeness should build valid update events")
}

/// Build DELETE event with consistent defaults.
pub(crate) fn delete_event(
    table_id: TableId,
    pk: PrimaryKey,
    old_row: RowImage,
) -> Result<WalEvent, WalParseError> {
    WalEvent::builder(table_id)
        .delete()
        .pk(pk)
        .old_row(old_row)
        .build()
        .map_err(|e| WalParseError::MalformedPayload(format!("invalid delete event: {e}")))
}

/// Build TRUNCATE event with consistent defaults.
pub(crate) fn truncate_event(table_id: TableId) -> Result<WalEvent, WalParseError> {
    WalEvent::builder(table_id)
        .truncate()
        .build()
        .map_err(|e| WalParseError::MalformedPayload(format!("invalid truncate event: {e}")))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_event_from_rows(
    kind: EventKind,
    table_id: TableId,
    pk: PrimaryKey,
    old_row: Option<RowImage>,
    new_row: Option<RowImage>,
    old_row_complete: bool,
    missing_new_field: &str,
    missing_old_field: &str,
) -> Result<WalEvent, WalParseError> {
    match kind {
        EventKind::Insert => {
            let new_row = new_row
                .ok_or_else(|| WalParseError::MissingField(missing_new_field.to_string()))?;
            insert_event(table_id, pk, new_row)
        }
        EventKind::Update => {
            let new_row = new_row
                .ok_or_else(|| WalParseError::MissingField(missing_new_field.to_string()))?;
            Ok(update_event_with_old_row_completeness(
                table_id,
                pk,
                old_row,
                new_row,
                old_row_complete,
            ))
        }
        EventKind::Delete => {
            let old_row = old_row
                .ok_or_else(|| WalParseError::MissingField(missing_old_field.to_string()))?;
            delete_event(table_id, pk, old_row)
        }
        EventKind::Truncate => truncate_event(table_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    #[test]
    fn test_resolve_table_conflicting_matches_errors() {
        use crate::table_resolution::{resolve_table_reference, TableResolutionError};

        // Two `users` tables in two different schemas. The wal2json caller
        // passes schema=`public`, table=`users`; the qualified `public.users`
        // resolves to one id, the bare `users` is also present (it has its
        // own ambient/no-schema entry via a second schema)..
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE other.users (id INT PRIMARY KEY, name TEXT);\n\
             CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT);",
        )
        .expect("ambiguous DDL parses");
        let qualified_id =
            crate::catalog_helpers::table_id(&catalog, "public.users").expect("public.users id");
        let unqualified_id =
            crate::catalog_helpers::table_id(&catalog, "other.users").expect("other.users id");
        assert_ne!(qualified_id, unqualified_id);

        // `resolve_table("public", "users", ...)` looks up both
        // "public.users" (qualified) and "users" (unqualified). With the
        // two schema-qualified `users` tables above and no bare `users`,
        // we expect an UnknownTable for the unqualified side and a hit on
        // the qualified side — i.e. resolution **succeeds** to
        // `public.users`. To exercise the ambiguity error we instead
        // delegate to `resolve_table_reference` with two distinct
        // table-name strings (one playing the role of qualified, the
        // other unqualified).
        let err = resolve_table_reference(Some("public.users"), "other.users", &catalog)
            .expect_err("ambiguous lookup must fail");
        assert!(matches!(
            err,
            TableResolutionError::Ambiguous {
                qualified_id: q,
                unqualified_id: u,
                ..
            } if q == qualified_id && u == unqualified_id
        ));
    }

    #[test]
    fn test_resolve_table_falls_back_to_unqualified_name() {
        let catalog =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE users (id INT PRIMARY KEY);")
                .expect("users DDL parses");
        let expected = crate::catalog_helpers::table_id(&catalog, "users").expect("users id");

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, expected);
    }

    #[test]
    fn test_resolve_table_uses_qualified_when_available() {
        // Declare the schema explicitly so the table is resolvable by
        // its qualified `public.users` name.
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA public;\n\
             CREATE TABLE public.users (id INT PRIMARY KEY);",
        )
        .expect("public.users DDL parses");
        let expected =
            crate::catalog_helpers::table_id(&catalog, "public.users").expect("public.users id");

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, expected);
    }

    #[test]
    fn test_resolve_table_unknown_table() {
        let catalog = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE other (id INT);")
            .expect("empty fixture DDL parses");

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
        let events = parse_single_json_event::<serde_json::Value, _>(b"null", |_| {
            Ok(Some(
                truncate_event(1).expect("truncate_event helper should build valid event"),
            ))
        })
        .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[test]
    fn test_parse_single_json_event_wraps_one_event() {
        let events = parse_single_json_event::<serde_json::Value, _>(br#"{"x":1}"#, |_| {
            Ok(Some(
                truncate_event(7).expect("truncate_event helper should build valid event"),
            ))
        })
        .expect("object should parse");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].table_id(), 7);
        assert_eq!(events[0].kind(), EventKind::Truncate);
    }
}
