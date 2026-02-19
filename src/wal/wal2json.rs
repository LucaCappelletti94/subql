//! wal2json v1 and v2 WAL parsers.
//!
//! [wal2json](https://github.com/eulerto/wal2json) is a PostgreSQL logical
//! decoding output plugin that emits changes as JSON. Version 1 batches all
//! changes in a transaction into a single message; version 2 emits one message
//! per change.

use std::sync::Arc;

use serde::Deserialize;

use super::pg_type::json_value_to_cell;
use super::{build_pk_from_resolved, changed_columns, resolve_table, WalParseError, WalParser};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

// ============================================================================
// Serde structs — v1
// ============================================================================

#[derive(Deserialize)]
struct Wal2JsonV1Message {
    #[allow(dead_code)]
    pub xid: Option<u64>,
    pub change: Vec<Wal2JsonV1Change>,
}

#[derive(Deserialize)]
struct Wal2JsonV1Change {
    pub kind: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub columnnames: Vec<String>,
    #[serde(default)]
    pub columntypes: Vec<String>,
    #[serde(default)]
    pub columnvalues: Vec<serde_json::Value>,
    pub oldkeys: Option<Wal2JsonV1OldKeys>,
}

#[derive(Deserialize)]
struct Wal2JsonV1OldKeys {
    pub keynames: Vec<String>,
    pub keytypes: Vec<String>,
    pub keyvalues: Vec<serde_json::Value>,
}

// ============================================================================
// Serde structs — v2
// ============================================================================

#[derive(Deserialize)]
struct Wal2JsonV2Message {
    pub action: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub columns: Option<Vec<Wal2JsonV2Column>>,
    #[serde(default)]
    pub identity: Option<Vec<Wal2JsonV2Column>>,
    #[serde(default)]
    pub pk: Option<Wal2JsonV2Pk>,
}

#[derive(Deserialize)]
struct Wal2JsonV2Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub value: serde_json::Value,
}

#[derive(Deserialize)]
struct Wal2JsonV2Pk {
    pub keynames: Vec<String>,
    #[allow(dead_code)]
    pub keytypes: Vec<String>,
}

// ============================================================================
// Parsers
// ============================================================================

/// wal2json **v1** parser (batched: one message per transaction).
pub struct Wal2JsonV1Parser;

/// wal2json **v2** parser (per-change: one message per row change).
pub struct Wal2JsonV2Parser;

impl WalParser for Wal2JsonV1Parser {
    fn parse_wal_message(
        &self,
        data: &[u8],
        catalog: &dyn SchemaCatalog,
    ) -> Result<Vec<WalEvent>, WalParseError> {
        let text =
            std::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;

        let msg: Wal2JsonV1Message =
            serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;

        let mut events = Vec::with_capacity(msg.change.len());
        for change in &msg.change {
            events.push(convert_v1_change(change, catalog)?);
        }
        Ok(events)
    }
}

impl WalParser for Wal2JsonV2Parser {
    fn parse_wal_message(
        &self,
        data: &[u8],
        catalog: &dyn SchemaCatalog,
    ) -> Result<Vec<WalEvent>, WalParseError> {
        let text =
            std::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;

        let msg: Wal2JsonV2Message =
            serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;

        let event = convert_v2_message(&msg, catalog)?;
        Ok(vec![event])
    }
}

// ============================================================================
// Shared helpers
// ============================================================================

/// Parse event kind from v1 string.
fn parse_v1_kind(kind: &str) -> Result<EventKind, WalParseError> {
    match kind {
        "insert" => Ok(EventKind::Insert),
        "update" => Ok(EventKind::Update),
        "delete" => Ok(EventKind::Delete),
        other => Err(WalParseError::UnknownEventKind(other.to_string())),
    }
}

/// Parse event kind from v2 single-char action.
fn parse_v2_kind(action: &str) -> Result<EventKind, WalParseError> {
    match action {
        "I" => Ok(EventKind::Insert),
        "U" => Ok(EventKind::Update),
        "D" => Ok(EventKind::Delete),
        other => Err(WalParseError::UnknownEventKind(other.to_string())),
    }
}

/// Build a [`RowImage`] from parallel name/type/value arrays.
///
/// Returns `(row_image, Vec<(ColumnId, Cell)>)` — the vec is used for PK
/// extraction.
fn build_row_from_arrays(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
    let arity = catalog
        .table_arity(table_id)
        .ok_or_else(|| WalParseError::UnknownTable {
            schema: String::new(),
            table: format!("table_id={table_id}"),
        })?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(names.len());

    for (i, name) in names.iter().enumerate() {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        let cell = json_value_to_cell(&values[i], &types[i]);
        if (col_id as usize) < arity {
            cells[col_id as usize] = cell.clone();
        }
        resolved.push((col_id, cell));
    }

    Ok((
        RowImage {
            cells: Arc::from(cells),
        },
        resolved,
    ))
}

/// Build a [`RowImage`] from v2 column structs.
fn build_row_from_v2_columns(
    columns: &[Wal2JsonV2Column],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
    let arity = catalog
        .table_arity(table_id)
        .ok_or_else(|| WalParseError::UnknownTable {
            schema: String::new(),
            table: format!("table_id={table_id}"),
        })?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(columns.len());

    for col in columns {
        let col_id =
            catalog
                .column_id(table_id, &col.name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: col.name.clone(),
                })?;
        let cell = json_value_to_cell(&col.value, &col.type_name);
        if (col_id as usize) < arity {
            cells[col_id as usize] = cell.clone();
        }
        resolved.push((col_id, cell));
    }

    Ok((
        RowImage {
            cells: Arc::from(cells),
        },
        resolved,
    ))
}

/// Build a [`PrimaryKey`] from the old-keys section (names resolved through catalog).
fn build_pk_from_key_arrays(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<PrimaryKey, WalParseError> {
    let mut pk_cols = Vec::with_capacity(names.len());
    let mut pk_vals = Vec::with_capacity(names.len());

    for (i, name) in names.iter().enumerate() {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        pk_cols.push(col_id);
        pk_vals.push(json_value_to_cell(&values[i], &types[i]));
    }

    Ok(PrimaryKey {
        columns: Arc::from(pk_cols),
        values: Arc::from(pk_vals),
    })
}

// ============================================================================
// v1 conversion
// ============================================================================

fn convert_v1_change(
    change: &Wal2JsonV1Change,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_v1_kind(&change.kind)?;
    let table_id = resolve_table(&change.schema, &change.table, catalog)?;

    let (new_row, new_resolved) = if kind == EventKind::Insert || kind == EventKind::Update {
        let (row, resolved) = build_row_from_arrays(
            &change.columnnames,
            &change.columntypes,
            &change.columnvalues,
            table_id,
            catalog,
        )?;
        (Some(row), resolved)
    } else {
        (None, Vec::new())
    };

    let (old_row, pk) = if let Some(ref oldkeys) = change.oldkeys {
        // Build old row from oldkeys (sparse — only key columns)
        let (row, _) = build_row_from_arrays(
            &oldkeys.keynames,
            &oldkeys.keytypes,
            &oldkeys.keyvalues,
            table_id,
            catalog,
        )?;

        let pk = build_pk_from_key_arrays(
            &oldkeys.keynames,
            &oldkeys.keytypes,
            &oldkeys.keyvalues,
            table_id,
            catalog,
        )?;

        (Some(row), pk)
    } else {
        // INSERT without oldkeys — extract PK from new row using catalog metadata
        let pk = catalog.primary_key_columns(table_id).map_or_else(
            || PrimaryKey {
                columns: Arc::from([]),
                values: Arc::from([]),
            },
            |pk_cols| build_pk_from_resolved(&new_resolved, pk_cols),
        );
        (None, pk)
    };

    let changed = if kind == EventKind::Update {
        if let (Some(ref old), Some(ref new)) = (&old_row, &new_row) {
            changed_columns(old, new)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Ok(WalEvent {
        kind,
        table_id,
        pk,
        old_row,
        new_row,
        changed_columns: Arc::from(changed),
    })
}

// ============================================================================
// v2 conversion
// ============================================================================

fn convert_v2_message(
    msg: &Wal2JsonV2Message,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_v2_kind(&msg.action)?;
    let table_id = resolve_table(&msg.schema, &msg.table, catalog)?;

    let (new_row, new_resolved) = if let Some(ref columns) = msg.columns {
        let (row, resolved) = build_row_from_v2_columns(columns, table_id, catalog)?;
        (Some(row), resolved)
    } else {
        (None, Vec::new())
    };

    let (old_row, identity_resolved) = if let Some(ref identity) = msg.identity {
        let (row, resolved) = build_row_from_v2_columns(identity, table_id, catalog)?;
        (Some(row), resolved)
    } else {
        (None, Vec::new())
    };

    // Build PK: prefer identity columns, then pk metadata, then catalog.
    // The three-way branching is clearer as if-let chains than map_or_else.
    #[allow(clippy::option_if_let_else)]
    let pk = if !identity_resolved.is_empty() {
        // Use identity columns as PK source (UPDATE/DELETE)
        if let Some(ref pk_meta) = msg.pk {
            let pk_col_ids: Vec<ColumnId> = pk_meta
                .keynames
                .iter()
                .filter_map(|name| catalog.column_id(table_id, name))
                .collect();
            build_pk_from_resolved(&identity_resolved, &pk_col_ids)
        } else {
            // No pk metadata — use all identity columns as PK
            let cols: Vec<ColumnId> = identity_resolved.iter().map(|(c, _)| *c).collect();
            let vals: Vec<Cell> = identity_resolved.iter().map(|(_, v)| v.clone()).collect();
            PrimaryKey {
                columns: Arc::from(cols),
                values: Arc::from(vals),
            }
        }
    } else if let Some(ref pk_meta) = msg.pk {
        // INSERT — extract PK from new row using pk metadata
        let pk_col_ids: Vec<ColumnId> = pk_meta
            .keynames
            .iter()
            .filter_map(|name| catalog.column_id(table_id, name))
            .collect();
        build_pk_from_resolved(&new_resolved, &pk_col_ids)
    } else {
        catalog.primary_key_columns(table_id).map_or_else(
            || PrimaryKey {
                columns: Arc::from([]),
                values: Arc::from([]),
            },
            |pk_cols| build_pk_from_resolved(&new_resolved, pk_cols),
        )
    };

    let changed = if kind == EventKind::Update {
        if let (Some(ref old), Some(ref new)) = (&old_row, &new_row) {
            changed_columns(old, new)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Ok(WalEvent {
        kind,
        table_id,
        pk,
        old_row,
        new_row,
        changed_columns: Arc::from(changed),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- Test catalog --------------------------------------------------------

    struct TestCatalog {
        tables: HashMap<String, (TableId, usize)>,
        columns: HashMap<(TableId, String), ColumnId>,
        primary_keys: HashMap<TableId, Vec<ColumnId>>,
    }

    impl TestCatalog {
        fn orders() -> Self {
            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), (1, 4));
            tables.insert("public.orders".to_string(), (1, 4));

            let mut columns = HashMap::new();
            // id=0, customer=1, amount=2, status=3
            columns.insert((1, "id".to_string()), 0);
            columns.insert((1, "customer".to_string()), 1);
            columns.insert((1, "amount".to_string()), 2);
            columns.insert((1, "status".to_string()), 3);

            let mut primary_keys = HashMap::new();
            primary_keys.insert(1, vec![0]); // id is PK

            Self {
                tables,
                columns,
                primary_keys,
            }
        }

        /// Same schema as `orders()` but without primary key metadata.
        fn orders_no_pk() -> Self {
            let mut cat = Self::orders();
            cat.primary_keys.clear();
            cat
        }
    }

    /// Catalog where table_id resolves but table_arity returns None.
    struct NoArityCatalog;

    impl SchemaCatalog for NoArityCatalog {
        fn table_id(&self, table_name: &str) -> Option<TableId> {
            if table_name == "orders" || table_name == "public.orders" {
                Some(1)
            } else {
                None
            }
        }

        fn column_id(&self, _table_id: TableId, _column_name: &str) -> Option<ColumnId> {
            None
        }

        fn table_arity(&self, _table_id: TableId) -> Option<usize> {
            None
        }

        fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
            Some(0)
        }
    }

    impl SchemaCatalog for TestCatalog {
        fn table_id(&self, table_name: &str) -> Option<TableId> {
            self.tables.get(table_name).map(|(id, _)| *id)
        }

        fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId> {
            self.columns
                .get(&(table_id, column_name.to_string()))
                .copied()
        }

        fn table_arity(&self, table_id: TableId) -> Option<usize> {
            self.tables
                .values()
                .find(|(id, _)| *id == table_id)
                .map(|(_, arity)| *arity)
        }

        fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
            Some(0)
        }

        fn primary_key_columns(&self, table_id: TableId) -> Option<&[ColumnId]> {
            self.primary_keys.get(&table_id).map(Vec::as_slice)
        }
    }

    // -- v1 INSERT -----------------------------------------------------------

    #[test]
    fn v1_insert() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 100,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 99.95, "pending"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Insert);
        assert_eq!(ev.table_id, 1);

        // New row present
        let new = ev.new_row.as_ref().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));
        assert_eq!(new.get(2), Some(&Cell::Float(99.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("pending"))));

        // No old row
        assert!(ev.old_row.is_none());

        // PK extracted from new row via catalog
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);

        // No changed columns for INSERT
        assert!(ev.changed_columns.is_empty());
    }

    // -- v1 UPDATE -----------------------------------------------------------

    #[test]
    fn v1_update() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 101,
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 149.95, "shipped"],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [1]
                }
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        // New row
        let new = ev.new_row.as_ref().expect("UPDATE should have new_row");
        assert_eq!(new.get(2), Some(&Cell::Float(149.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("shipped"))));

        // Old row (sparse — only key columns)
        let old = ev.old_row.as_ref().expect("UPDATE should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(1)));
        assert_eq!(old.get(1), Some(&Cell::Missing)); // not in oldkeys

        // PK from oldkeys
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v1 DELETE -----------------------------------------------------------

    #[test]
    fn v1_delete() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 102,
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders",
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [42]
                }
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Delete);
        assert!(ev.new_row.is_none());
        assert!(ev.old_row.is_some());

        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(42)]);

        assert!(ev.changed_columns.is_empty());
    }

    // -- v1 multi-change transaction -----------------------------------------

    #[test]
    fn v1_multi_change() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 200,
            "change": [
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "orders",
                    "columnnames": ["id", "customer", "amount", "status"],
                    "columntypes": ["integer", "text", "numeric", "text"],
                    "columnvalues": [10, "bob", 50.0, "new"]
                },
                {
                    "kind": "update",
                    "schema": "public",
                    "table": "orders",
                    "columnnames": ["id", "customer", "amount", "status"],
                    "columntypes": ["integer", "text", "numeric", "text"],
                    "columnvalues": [11, "carol", 75.0, "confirmed"],
                    "oldkeys": {
                        "keynames": ["id"],
                        "keytypes": ["integer"],
                        "keyvalues": [11]
                    }
                },
                {
                    "kind": "delete",
                    "schema": "public",
                    "table": "orders",
                    "oldkeys": {
                        "keynames": ["id"],
                        "keytypes": ["integer"],
                        "keyvalues": [12]
                    }
                }
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind, EventKind::Insert);
        assert_eq!(events[1].kind, EventKind::Update);
        assert_eq!(events[2].kind, EventKind::Delete);
    }

    // -- v2 INSERT -----------------------------------------------------------

    #[test]
    fn v2_insert() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 99.95},
                {"name": "status", "type": "text", "value": "pending"}
            ],
            "pk": {
                "keynames": ["id"],
                "keytypes": ["integer"]
            }
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Insert);
        assert_eq!(ev.table_id, 1);

        let new = ev.new_row.as_ref().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));

        assert!(ev.old_row.is_none());

        // PK from pk metadata + new row
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v2 UPDATE -----------------------------------------------------------

    #[test]
    fn v2_update() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 149.95},
                {"name": "status", "type": "text", "value": "shipped"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 1}
            ],
            "pk": {
                "keynames": ["id"],
                "keytypes": ["integer"]
            }
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        assert!(ev.new_row.is_some());
        assert!(ev.old_row.is_some());

        // PK from identity
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v2 DELETE -----------------------------------------------------------

    #[test]
    fn v2_delete() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "D",
            "schema": "public",
            "table": "orders",
            "identity": [
                {"name": "id", "type": "integer", "value": 42}
            ],
            "pk": {
                "keynames": ["id"],
                "keytypes": ["integer"]
            }
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Delete);
        assert!(ev.new_row.is_none());
        assert!(ev.old_row.is_some());

        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(42)]);
    }

    // -- Error paths ---------------------------------------------------------

    #[test]
    fn error_invalid_utf8() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn error_malformed_json() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn error_unknown_table() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "nonexistent",
                "columnnames": ["id"],
                "columntypes": ["integer"],
                "columnvalues": [1]
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    #[test]
    fn error_unknown_column() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "nonexistent_col"],
                "columntypes": ["integer", "text"],
                "columnvalues": [1, "value"]
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_unknown_event_kind() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "truncate",
                "schema": "public",
                "table": "orders",
                "columnnames": [],
                "columntypes": [],
                "columnvalues": []
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    #[test]
    fn error_unknown_event_kind_v2() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "T",
            "schema": "public",
            "table": "orders"
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    // -- Trait object safety --------------------------------------------------

    #[test]
    fn trait_object_compiles() {
        let parser: &dyn WalParser = &Wal2JsonV1Parser;
        let catalog = TestCatalog::orders();

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "test", 10.0, "new"]
            }]
        }"#;

        let result = parser.parse_wal_message(json.as_bytes(), &catalog);
        assert!(result.is_ok());
    }

    // -- v1 UPDATE with full old row (REPLICA IDENTITY FULL) ----------------

    #[test]
    fn v1_update_with_changed_columns() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        // Simulate REPLICA IDENTITY FULL: oldkeys contains all columns
        let json = r#"{
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 149.95, "shipped"],
                "oldkeys": {
                    "keynames": ["id", "customer", "amount", "status"],
                    "keytypes": ["integer", "text", "numeric", "text"],
                    "keyvalues": [1, "alice", 99.95, "pending"]
                }
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        // amount (col 2) and status (col 3) changed
        let changed: Vec<ColumnId> = ev.changed_columns.to_vec();
        assert!(changed.contains(&2), "amount should be changed");
        assert!(changed.contains(&3), "status should be changed");
        assert!(!changed.contains(&0), "id should NOT be changed");
        assert!(!changed.contains(&1), "customer should NOT be changed");
    }

    // -- v1 UPDATE without oldkeys (changed_columns branch: None old_row) ----

    #[test]
    fn v1_update_without_oldkeys() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 149.95, "shipped"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        // No oldkeys → no old_row → changed_columns is empty
        assert!(ev.old_row.is_none());
        assert!(ev.changed_columns.is_empty());
    }

    // -- v1 INSERT without catalog PK columns --------------------------------

    #[test]
    fn v1_insert_no_catalog_pk() {
        let catalog = TestCatalog::orders_no_pk();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 99.95, "pending"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // No oldkeys and no catalog PK → empty PK
        assert!(ev.pk.columns.is_empty());
        assert!(ev.pk.values.is_empty());
    }

    // -- v2 UPDATE with identity but no pk metadata --------------------------

    #[test]
    fn v2_update_identity_no_pk_metadata() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 149.95},
                {"name": "status", "type": "text", "value": "shipped"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 1}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        // PK from all identity columns (no pk metadata to filter)
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v2 INSERT without pk metadata AND without catalog PK ----------------

    #[test]
    fn v2_insert_no_pk_no_catalog() {
        let catalog = TestCatalog::orders_no_pk();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 5},
                {"name": "customer", "type": "text", "value": "eve"},
                {"name": "amount", "type": "numeric", "value": 10.0},
                {"name": "status", "type": "text", "value": "new"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // No pk metadata and no catalog PK → empty PK
        assert!(ev.pk.columns.is_empty());
        assert!(ev.pk.values.is_empty());
    }

    // -- v2 UPDATE without identity (changed_columns branch: None old_row) ---

    #[test]
    fn v2_update_without_identity() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 149.95},
                {"name": "status", "type": "text", "value": "shipped"}
            ],
            "pk": {
                "keynames": ["id"],
                "keytypes": ["integer"]
            }
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        // No identity → no old_row → changed_columns empty
        assert!(ev.old_row.is_none());
        assert!(ev.changed_columns.is_empty());
    }

    // -- v2 unknown column error ---------------------------------------------

    #[test]
    fn error_unknown_column_v2() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "bogus_col", "type": "text", "value": "x"}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    // -- Null handling -------------------------------------------------------

    #[test]
    fn v1_null_values() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, null, null, "active"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let new = events[0].new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Null));
        assert_eq!(new.get(2), Some(&Cell::Null));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("active"))));
    }

    // -- INSERT without PK metadata (catalog fallback) -----------------------

    #[test]
    fn v2_insert_no_pk_metadata() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 7},
                {"name": "customer", "type": "text", "value": "dave"},
                {"name": "amount", "type": "numeric", "value": 25.0},
                {"name": "status", "type": "text", "value": "new"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // PK should come from catalog.primary_key_columns()
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(7)]);
    }

    // -- Error: table_arity returns None (v1) --------------------------------

    #[test]
    fn error_no_arity_v1() {
        let catalog = NoArityCatalog;
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id"],
                "columntypes": ["integer"],
                "columnvalues": [1]
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    // -- Error: table_arity returns None (v2) --------------------------------

    #[test]
    fn error_no_arity_v2() {
        let catalog = NoArityCatalog;
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    // -- Error: unknown column in oldkeys ------------------------------------

    #[test]
    fn error_unknown_column_in_oldkeys() {
        let catalog = TestCatalog::orders();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders",
                "oldkeys": {
                    "keynames": ["nonexistent_key"],
                    "keytypes": ["integer"],
                    "keyvalues": [1]
                }
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    // -- Direct test: build_pk_from_key_arrays with unknown column -----------

    #[test]
    fn error_build_pk_unknown_column() {
        // Exercise build_pk_from_key_arrays directly (normally preempted
        // by build_row_from_arrays checking the same columns first).
        let catalog = TestCatalog::orders();
        let names = vec!["nonexistent".to_string()];
        let types = vec!["integer".to_string()];
        let values = vec![serde_json::json!(1)];

        let err = build_pk_from_key_arrays(&names, &types, &values, 1, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }
}
