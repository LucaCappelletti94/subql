//! Maxwell's Daemon CDC parser.
//!
//! [Maxwell](https://maxwells-daemon.io/) reads MySQL binlogs and emits
//! one JSON message per row change. Unlike wal2json, Maxwell provides no
//! column type information — values are bare JSON primitives, so we use
//! type inference via [`infer_cell_from_json`].

use serde::Deserialize;
use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

use super::map_cdc::{convert_map_cdc_event, parse_event_kind, MapCdcConfig};
use super::{resolve_table, WalParseError, WalParser};
#[cfg(test)]
use crate::{Cell, ColumnId};
use crate::{EventKind, SchemaCatalog, WalEvent};

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
        super::parse_single_json_event::<MaxwellMessage, _>(data, |msg| {
            convert_maxwell_message(msg, catalog)
        })
    }
}

// ============================================================================
// Conversion logic
// ============================================================================

fn parse_maxwell_kind(event_type: &str) -> Result<EventKind, WalParseError> {
    parse_event_kind(event_type, &["insert"], &["update"], &["delete"], &[])
}

fn convert_maxwell_message(
    msg: &MaxwellMessage,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_maxwell_kind(&msg.event_type)?;
    let table_id = resolve_table(&msg.database, &msg.table, catalog)?;

    let old_map = match kind {
        EventKind::Delete => msg.data.as_ref(),
        _ => msg.old.as_ref(),
    };
    let old_prefix = match kind {
        EventKind::Delete => "maxwell.data",
        _ => "maxwell.old",
    };

    convert_map_cdc_event(
        kind,
        table_id,
        msg.data.as_ref(),
        old_map,
        MapCdcConfig {
            required_new_field: "data",
            required_old_field: "data",
            new_field_prefix: "maxwell.data",
            old_field_prefix: old_prefix,
            pk_col_names: msg.primary_key_columns.as_deref(),
        },
        catalog,
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::test_support::TestCatalog;
    use super::*;
    use std::collections::HashMap;

    // -- Test catalog --------------------------------------------------------

    /// Maxwell test table: database="test", table="e",
    /// columns: id=0, m=1, c=2, comment=3, PK=[id].
    fn maxwell_e_catalog() -> TestCatalog {
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

        TestCatalog {
            tables,
            columns,
            primary_keys,
        }
    }

    fn maxwell_e_no_pk_catalog() -> TestCatalog {
        let mut cat = maxwell_e_catalog();
        cat.primary_keys.clear();
        cat
    }

    // -- INSERT tests -------------------------------------------------------

    #[test]
    fn maxwell_insert() {
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_no_pk_catalog();
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
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn error_malformed_json() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn maxwell_tombstone_null_is_ignored() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let events = parser
            .parse_wal_message(b"null", &catalog)
            .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[test]
    fn error_unknown_table() {
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
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
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert"
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MissingField(_)));
    }

    #[test]
    fn error_numeric_overflow() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":18446744073709551615}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("overflow should fail");
        assert!(matches!(err, WalParseError::NumericOverflow { .. }));
    }

    #[test]
    fn error_pk_metadata_unknown_column() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":4.2,"c":"2020-01-01","comment":"hello"},
            "primary_key_columns":["id","does_not_exist"]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("unknown PK metadata column should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_pk_metadata_column_missing_in_row() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"m":4.2,"c":"2020-01-01","comment":"hello"},
            "primary_key_columns":["id"]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("missing PK value in row should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- Trait checks -------------------------------------------------------

    #[test]
    fn trait_object_compiles() {
        let parser: &dyn WalParser = &MaxwellParser;
        let catalog = maxwell_e_catalog();

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
