//! Debezium CDC parser.
//!
//! [Debezium](https://debezium.io/) captures row-level changes from databases
//! and emits them as JSON envelope messages (typically via Kafka Connect).
//! Like Maxwell, Debezium provides bare JSON values without column type
//! metadata, so we use type inference via [`infer_cell_from_json`].

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use super::pg_type::infer_cell_from_json;
use super::row_build::build_row_from_map_with;
use super::{build_pk_from_resolved, changed_columns, resolve_table, WalParseError, WalParser};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

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
        let text =
            std::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;

        let env: DebeziumEnvelope =
            serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;

        let event = convert_debezium_envelope(&env, catalog)?;
        Ok(vec![event])
    }
}

// ============================================================================
// Conversion logic
// ============================================================================

fn parse_debezium_op(op: &str) -> Result<EventKind, WalParseError> {
    match op {
        "c" | "r" => Ok(EventKind::Insert),
        "u" => Ok(EventKind::Update),
        "d" => Ok(EventKind::Delete),
        other => Err(WalParseError::UnknownEventKind(other.to_string())),
    }
}

fn convert_debezium_envelope(
    env: &DebeziumEnvelope,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_debezium_op(&env.op)?;

    // Try schema.table first, then fall back to db.table
    let table_id = resolve_table(&env.source.schema, &env.source.table, catalog)
        .or_else(|_| resolve_table(&env.source.db, &env.source.table, catalog))?;

    match kind {
        EventKind::Insert => {
            let after = env
                .after
                .as_ref()
                .ok_or_else(|| WalParseError::MissingField("after".to_string()))?;

            let (new_row, resolved) = build_row_from_map(after, table_id, catalog)?;
            let pk = build_debezium_pk(&resolved, table_id, catalog);

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
            let after = env
                .after
                .as_ref()
                .ok_or_else(|| WalParseError::MissingField("after".to_string()))?;

            let (new_row, new_resolved) = build_row_from_map(after, table_id, catalog)?;

            let (old_row, changed) = if let Some(ref before) = env.before {
                let (old_img, _) = build_row_from_map(before, table_id, catalog)?;
                let ch = changed_columns(&old_img, &new_row);
                (Some(old_img), ch)
            } else {
                (None, Vec::new())
            };

            let pk = build_debezium_pk(&new_resolved, table_id, catalog);

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
            let before = env
                .before
                .as_ref()
                .ok_or_else(|| WalParseError::MissingField("before".to_string()))?;

            let (old_row, resolved) = build_row_from_map(before, table_id, catalog)?;
            let pk = build_debezium_pk(&resolved, table_id, catalog);

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

/// Build a [`RowImage`] from a Debezium column→value map.
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

/// Build PK from catalog metadata (Debezium's simplified envelope has no PK field).
fn build_debezium_pk(
    resolved: &[(ColumnId, Cell)],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> PrimaryKey {
    catalog.primary_key_columns(table_id).map_or_else(
        || PrimaryKey {
            columns: Arc::from([]),
            values: Arc::from([]),
        },
        |pk_cols| build_pk_from_resolved(resolved, pk_cols),
    )
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
        /// Debezium test table: schema="public", table="orders",
        /// columns: id=0, amount=1, status=2, comment=3, PK=[id].
        fn orders() -> Self {
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

            Self {
                tables,
                columns,
                primary_keys,
            }
        }

        fn orders_no_pk() -> Self {
            let mut cat = Self::orders();
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
    fn debezium_insert() {
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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

    // -- Edge cases ----------------------------------------------------------

    #[test]
    fn debezium_null_values() {
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders_no_pk();
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
        let catalog = TestCatalog::orders();
        let parser = DebeziumParser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn error_malformed_json() {
        let catalog = TestCatalog::orders();
        let parser = DebeziumParser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn error_unknown_table() {
        let catalog = TestCatalog::orders();
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
    fn error_unknown_column() {
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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
        let catalog = TestCatalog::orders();
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

    // -- Trait checks -------------------------------------------------------

    #[test]
    fn trait_object_compiles() {
        let parser: &dyn WalParser = &DebeziumParser;
        let catalog = TestCatalog::orders();

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
