//! Debezium CDC parser.
//!
//! [Debezium](https://debezium.io/) captures row-level changes from databases
//! and emits them as JSON envelope messages (typically via Kafka Connect).
//! Like Maxwell, Debezium provides bare JSON values without column type
//! metadata, so we use type inference via [`infer_cell_from_json`].

use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

use serde::Deserialize;

use super::map_cdc::{convert_map_cdc_event, parse_event_kind, MapCdcConfig};
use super::{resolve_table, truncate_event, WalParseError, WalParser};
#[cfg(test)]
use crate::{Cell, ColumnId};
use crate::{EventKind, SchemaCatalog, WalEvent};

// ============================================================================
// Serde structs
// ============================================================================

#[derive(Deserialize)]
struct DebeziumEnvelope {
    before: Option<HashMap<String, serde_json::Value>>,
    after: Option<HashMap<String, serde_json::Value>>,
    source: DebeziumSource,
    op: String,
    #[allow(dead_code)]
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct DebeziumSource {
    #[allow(dead_code)]
    connector: Option<String>,
    #[serde(default)]
    db: String,
    #[serde(default)]
    schema: String,
    table: String,
}

// ============================================================================
// Parser
// ============================================================================

/// Debezium CDC parser (per-change: one JSON envelope per row change).
pub struct DebeziumParser;

impl WalParser for DebeziumParser {
    fn parse_wal_message(
        &self,
        data: &[u8],
        catalog: &dyn SchemaCatalog,
    ) -> Result<Vec<WalEvent>, WalParseError> {
        let env: Option<DebeziumEnvelope> = super::parse_json_message_or_tombstone(data)?;
        let Some(env) = env else {
            // Debezium Kafka topics may emit tombstone messages ("null") for compaction.
            return Ok(Vec::new());
        };

        let event = convert_debezium_envelope(&env, catalog)?;
        Ok(vec![event])
    }
}

// ============================================================================
// Conversion logic
// ============================================================================

fn parse_debezium_op(op: &str) -> Result<EventKind, WalParseError> {
    parse_event_kind(op, &["c", "r"], &["u"], &["d"], &["t"])
}

fn convert_debezium_envelope(
    env: &DebeziumEnvelope,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_debezium_op(&env.op)?;

    // Try schema.table first, then fall back to db.table only when table is unknown.
    let table_id = match resolve_table(&env.source.schema, &env.source.table, catalog) {
        Ok(table_id) => table_id,
        Err(WalParseError::UnknownTable { .. }) => {
            resolve_table(&env.source.db, &env.source.table, catalog)?
        }
        Err(err) => return Err(err),
    };

    if kind == EventKind::Truncate {
        return Ok(truncate_event(table_id));
    }

    convert_map_cdc_event(
        kind,
        table_id,
        env.after.as_ref(),
        env.before.as_ref(),
        MapCdcConfig {
            required_new_field: "after",
            required_old_field: "before",
            new_field_prefix: "debezium.after",
            old_field_prefix: "debezium.before",
            pk_col_names: None,
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

    /// Debezium test table: schema="public", table="orders",
    /// columns: id=0, amount=1, status=2, comment=3, PK=[id].
    fn orders_catalog() -> TestCatalog {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), (1, 4));
        tables.insert("public.orders".to_string(), (1, 4));
        tables.insert("mydb.orders".to_string(), (1, 4));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "amount".to_string()), 1);
        columns.insert((1, "status".to_string()), 2);
        columns.insert((1, "comment".to_string()), 3);

        let mut primary_keys = HashMap::new();
        primary_keys.insert(1, vec![0]); // id is PK

        TestCatalog {
            tables,
            columns,
            primary_keys,
        }
    }

    fn orders_no_pk_catalog() -> TestCatalog {
        let mut cat = orders_catalog();
        cat.primary_keys.clear();
        cat
    }

    // -- INSERT tests -------------------------------------------------------

    #[test]
    fn debezium_insert() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1, "amount": 99.95, "status": "new", "comment": "rush"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
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
        assert_eq!(new.get(1), Some(&Cell::Float(99.95)));
        assert_eq!(new.get(2), Some(&Cell::String(Arc::from("new"))));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("rush"))));

        assert!(ev.old_row.is_none());

        // PK from catalog
        assert_eq!(ev.pk.columns.as_ref(), &[0]);
        assert_eq!(ev.pk.values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns.is_empty());
    }

    #[test]
    fn debezium_snapshot_read_as_insert() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 2, "amount": 50.0, "status": "done", "comment": "ok"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "r",
            "ts_ms": 1234567890
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Insert);
        assert!(ev.new_row.is_some());
        assert!(ev.old_row.is_none());
    }

    // -- UPDATE tests -------------------------------------------------------

    #[test]
    fn debezium_update_with_before() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": {"id": 1, "amount": 99.95, "status": "new", "comment": "rush"},
            "after": {"id": 1, "amount": 149.99, "status": "shipped", "comment": "rush"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "u",
            "ts_ms": 1234567891
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Update);

        let new = ev.new_row.as_ref().expect("UPDATE should have new_row");
        assert_eq!(new.get(1), Some(&Cell::Float(149.99)));
        assert_eq!(new.get(2), Some(&Cell::String(Arc::from("shipped"))));

        let old = ev.old_row.as_ref().expect("UPDATE should have old_row");
        assert_eq!(old.get(1), Some(&Cell::Float(99.95)));
        assert_eq!(old.get(2), Some(&Cell::String(Arc::from("new"))));

        // changed_columns: amount(1) and status(2) differ
        let changed: Vec<ColumnId> = ev.changed_columns.to_vec();
        assert!(changed.contains(&1), "amount should be changed");
        assert!(changed.contains(&2), "status should be changed");
        assert!(!changed.contains(&0), "id should NOT be changed");
        assert!(!changed.contains(&3), "comment should NOT be changed");
    }

    #[test]
    fn debezium_update_without_before() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1, "amount": 149.99, "status": "shipped", "comment": "rush"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "u",
            "ts_ms": 1234567891
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
    fn debezium_delete() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": {"id": 1, "amount": 99.95, "status": "new", "comment": "rush"},
            "after": null,
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "d",
            "ts_ms": 1234567892
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
    fn debezium_truncate() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": null,
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "t",
            "ts_ms": 1234567893
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::Truncate);
        assert_eq!(ev.table_id, 1);
        assert!(ev.pk.is_empty());
        assert!(ev.old_row.is_none());
        assert!(ev.new_row.is_none());
        assert!(ev.changed_columns.is_empty());
    }

    // -- Edge cases ----------------------------------------------------------

    #[test]
    fn debezium_null_values() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1, "amount": null, "status": null, "comment": "hello"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
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
    fn debezium_insert_no_catalog_pk() {
        let catalog = orders_no_pk_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1, "amount": 99.95, "status": "new", "comment": "rush"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
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
        let catalog = orders_catalog();
        let parser = DebeziumParser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn error_malformed_json() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn debezium_tombstone_null_is_ignored() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let events = parser
            .parse_wal_message(b"null", &catalog)
            .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[test]
    fn error_unknown_table() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1},
            "source": {"connector": "postgresql", "db": "other", "schema": "other", "table": "nonexistent"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    #[test]
    fn error_ambiguous_table_does_not_fallback_to_db_name() {
        let mut catalog = orders_catalog();
        catalog.tables.insert("public.orders".to_string(), (2, 4));
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("ambiguous schema.table vs table should fail");
        assert!(matches!(
            err,
            WalParseError::AmbiguousTable {
                schema,
                table,
                qualified,
                qualified_id: 2,
                unqualified_id: 1,
            } if schema == "public" && table == "orders" && qualified == "public.orders"
        ));
    }

    #[test]
    fn error_unknown_column() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1, "nonexistent_col": "value"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_unknown_op() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 1},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "x",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    #[test]
    fn error_missing_after_on_insert() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": null,
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MissingField(_)));
    }

    #[test]
    fn error_missing_before_on_delete() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": null,
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "d",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MissingField(_)));
    }

    #[test]
    fn error_numeric_overflow() {
        let catalog = orders_catalog();
        let parser = DebeziumParser;

        let json = r#"{
            "before": null,
            "after": {"id": 18446744073709551615},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("overflow should fail");
        assert!(matches!(err, WalParseError::NumericOverflow { .. }));
    }

    // -- Trait checks -------------------------------------------------------

    #[test]
    fn trait_object_compiles() {
        let parser: &dyn WalParser = &DebeziumParser;
        let catalog = orders_catalog();

        let json = r#"{
            "before": null,
            "after": {"id": 1, "amount": 99.95, "status": "new", "comment": "rush"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let result = parser.parse_wal_message(json.as_bytes(), &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DebeziumParser>();
    }
}
