//! Maxwell's Daemon CDC parser.
//!
//! [Maxwell](https://maxwells-daemon.io/) reads MySQL binlogs and emits
//! one JSON message per row change. Unlike wal2json, Maxwell provides no
//! column type information — values are bare JSON primitives, so we use
//! type inference via [`infer_cell_from_json`].

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use super::pg_type::infer_cell_from_json;
use super::row_build::build_row_from_map_with;
use super::{
    build_pk_from_resolved, changed_columns, pk_from_catalog_or_empty, resolve_table,
    WalParseError, WalParser,
};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

// ============================================================================
// Serde structs
// ============================================================================

#[derive(Deserialize)]
struct MaxwellMessage {
    database: String,
    table: String,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    data: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    old: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    primary_key_columns: Option<Vec<String>>,
    #[allow(dead_code)]
    #[serde(default)]
    ts: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    xid: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    commit: Option<bool>,
}

// ============================================================================
// Parser
// ============================================================================

/// Maxwell's Daemon CDC parser (per-change: one JSON message per row change).
pub struct MaxwellParser;

impl WalParser for MaxwellParser {
    fn parse_wal_message(
        &self,
        data: &[u8],
        catalog: &dyn SchemaCatalog,
    ) -> Result<Vec<WalEvent>, WalParseError> {
        let text =
            std::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;

        let msg: MaxwellMessage =
            serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;

        let event = convert_maxwell_message(&msg, catalog)?;
        Ok(vec![event])
    }
}

// ============================================================================
// Conversion logic
// ============================================================================

fn parse_maxwell_kind(event_type: &str) -> Result<EventKind, WalParseError> {
    match event_type {
        "insert" => Ok(EventKind::Insert),
        "update" => Ok(EventKind::Update),
        "delete" => Ok(EventKind::Delete),
        other => Err(WalParseError::UnknownEventKind(other.to_string())),
    }
}

fn convert_maxwell_message(
    msg: &MaxwellMessage,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_maxwell_kind(&msg.event_type)?;
    let table_id = resolve_table(&msg.database, &msg.table, catalog)?;

    let data = msg
        .data
        .as_ref()
        .ok_or_else(|| WalParseError::MissingField("data".to_string()))?;

    match kind {
        EventKind::Insert => {
            let (new_row, resolved) = build_row_from_map(data, table_id, catalog)?;
            let pk = build_maxwell_pk(
                msg.primary_key_columns.as_deref(),
                &resolved,
                table_id,
                catalog,
            );

            Ok(WalEvent {
                kind,
                table_id,
                pk,
                old_row: None,
                new_row: Some(new_row),
                changed_columns: Arc::from([]),
            })
        }

        EventKind::Update => {
            let (new_row, new_resolved) = build_row_from_map(data, table_id, catalog)?;

            let (old_row, changed) = if let Some(ref old_map) = msg.old {
                let (old_img, _) = build_row_from_map(old_map, table_id, catalog)?;
                let ch = changed_columns(&old_img, &new_row);
                (Some(old_img), ch)
            } else {
                (None, Vec::new())
            };

            let pk = build_maxwell_pk(
                msg.primary_key_columns.as_deref(),
                &new_resolved,
                table_id,
                catalog,
            );

            Ok(WalEvent {
                kind,
                table_id,
                pk,
                old_row,
                new_row: Some(new_row),
                changed_columns: Arc::from(changed),
            })
        }

        EventKind::Delete => {
            let (old_row, resolved) = build_row_from_map(data, table_id, catalog)?;
            let pk = build_maxwell_pk(
                msg.primary_key_columns.as_deref(),
                &resolved,
                table_id,
                catalog,
            );

            Ok(WalEvent {
                kind,
                table_id,
                pk,
                old_row: Some(old_row),
                new_row: None,
                changed_columns: Arc::from([]),
            })
        }
    }
}

// ============================================================================
// Row building
// ============================================================================

/// Build a [`RowImage`] from a Maxwell column→value map.
///
/// Returns `(row_image, Vec<(ColumnId, Cell)>)` for PK extraction.
fn build_row_from_map(
    map: &HashMap<String, serde_json::Value>,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
    build_row_from_map_with(map, table_id, catalog, infer_cell_from_json)
}

// ============================================================================
// PK helper
// ============================================================================

/// Build PK from Maxwell message data.
///
/// Priority: message `primary_key_columns` → `catalog.primary_key_columns()` → empty PK.
fn build_maxwell_pk(
    pk_col_names: Option<&[String]>,
    resolved: &[(ColumnId, Cell)],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> PrimaryKey {
    if let Some(names) = pk_col_names {
        // Resolve PK column names to IDs, then extract values from resolved data
        let pk_col_ids: Vec<ColumnId> = names
            .iter()
            .filter_map(|name| catalog.column_id(table_id, name))
            .collect();
        return build_pk_from_resolved(resolved, &pk_col_ids);
    }

    pk_from_catalog_or_empty(resolved, table_id, catalog)
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
        /// Maxwell test table: database="test", table="e",
        /// columns: id=0, m=1, c=2, comment=3, PK=[id].
        fn maxwell_e() -> Self {
            let mut tables = HashMap::new();
            tables.insert("e".to_string(), (1, 4));
            tables.insert("test.e".to_string(), (1, 4));

            let mut columns = HashMap::new();
            columns.insert((1, "id".to_string()), 0);
            columns.insert((1, "m".to_string()), 1);
            columns.insert((1, "c".to_string()), 2);
            columns.insert((1, "comment".to_string()), 3);

            let mut primary_keys = HashMap::new();
            primary_keys.insert(1, vec![0]); // id is PK

            Self {
                tables,
                columns,
                primary_keys,
            }
        }

        fn maxwell_e_no_pk() -> Self {
            let mut cat = Self::maxwell_e();
            cat.primary_keys.clear();
            cat
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

    // -- INSERT tests -------------------------------------------------------

    #[test]
    fn maxwell_insert() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert","ts":1477053217,
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
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
        assert_eq!(new.get(1), Some(&Cell::Float(4.2341)));
        assert_eq!(
            new.get(2),
            Some(&Cell::String(Arc::from("2016-10-21 05:33:37")))
        );
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("hello"))));

        assert!(ev.old_row.is_none());

        // PK from catalog
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns.is_empty());
    }

    #[test]
    fn maxwell_insert_with_pk_columns() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert","ts":1477053217,
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"},
            "primary_key_columns":["id","c"]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // PK from message — both id and c
        assert_eq!(ev.pk.columns.len(), 2);
        assert!(ev.pk.columns.contains(&0)); // id
        assert!(ev.pk.columns.contains(&2)); // c
    }

    // -- UPDATE tests -------------------------------------------------------

    #[test]
    fn maxwell_update() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"update",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"},
            "old":{"m":4.2341,"c":"2016-10-21 05:33:37"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        // New row present
        let new = ev.new_row.as_ref().expect("UPDATE should have new_row");
        assert_eq!(new.get(1), Some(&Cell::Float(5.444)));

        // Old row (sparse — only changed columns)
        let old = ev.old_row.as_ref().expect("UPDATE should have old_row");
        assert_eq!(old.get(1), Some(&Cell::Float(4.2341)));
        assert_eq!(
            old.get(2),
            Some(&Cell::String(Arc::from("2016-10-21 05:33:37")))
        );
        // Columns not in `old` are Missing
        assert_eq!(old.get(0), Some(&Cell::Missing));
        assert_eq!(old.get(3), Some(&Cell::Missing));

        // changed_columns: m(1) and c(2) differ
        let changed: Vec<ColumnId> = ev.changed_columns.to_vec();
        assert!(changed.contains(&1), "m should be changed");
        assert!(changed.contains(&2), "c should be changed");
        assert!(!changed.contains(&0), "id should NOT be changed");
        assert!(!changed.contains(&3), "comment should NOT be changed");
    }

    #[test]
    fn maxwell_update_without_old() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"update",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);
        assert!(ev.new_row.is_some());
        assert!(ev.old_row.is_none());
        assert!(ev.changed_columns.is_empty());
    }

    // -- DELETE tests -------------------------------------------------------

    #[test]
    fn maxwell_delete() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"delete",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Delete);
        assert!(ev.new_row.is_none());

        let old = ev.old_row.as_ref().expect("DELETE should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(1)));

        // PK from catalog
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns.is_empty());
    }

    #[test]
    fn maxwell_delete_with_pk_columns() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"delete",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"},
            "primary_key_columns":["id"]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);
    }

    // -- Edge cases ----------------------------------------------------------

    #[test]
    fn maxwell_null_values() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":null,"c":null,"comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let new = events[0].new_row.as_ref().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Null));
        assert_eq!(new.get(2), Some(&Cell::Null));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("hello"))));
    }

    #[test]
    fn maxwell_insert_no_catalog_pk() {
        let catalog = TestCatalog::maxwell_e_no_pk();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // No PK source → empty PK
        assert!(ev.pk.columns.is_empty());
        assert!(ev.pk.values.is_empty());
    }

    // -- Error paths ---------------------------------------------------------

    #[test]
    fn error_invalid_utf8() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn error_malformed_json() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn error_unknown_table() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"other","table":"nonexistent","type":"insert",
            "data":{"id":1}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    #[test]
    fn error_unknown_column() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"nonexistent_col":"value"}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_unknown_event_kind() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"truncate",
            "data":{"id":1}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    #[test]
    fn error_missing_data() {
        let catalog = TestCatalog::maxwell_e();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert"
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MissingField(_)));
    }

    // -- Trait checks -------------------------------------------------------

    #[test]
    fn trait_object_compiles() {
        let parser: &dyn WalParser = &MaxwellParser;
        let catalog = TestCatalog::maxwell_e();

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
        }"#;

        let result = parser.parse_wal_message(json.as_bytes(), &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MaxwellParser>();
    }
}
