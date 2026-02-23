use std::collections::HashMap;

use super::pg_type::infer_cell_from_json_strict;
use super::row_build::build_row_from_map_with;
use super::{
    build_pk_from_resolved, delete_event, insert_event, pk_from_catalog_or_empty,
    strict_pk_column_ids_from_names, update_event, WalParseError,
};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

pub(super) fn parse_event_kind(
    token: &str,
    insert_tokens: &[&str],
    update_tokens: &[&str],
    delete_tokens: &[&str],
) -> Result<EventKind, WalParseError> {
    if insert_tokens.contains(&token) {
        return Ok(EventKind::Insert);
    }
    if update_tokens.contains(&token) {
        return Ok(EventKind::Update);
    }
    if delete_tokens.contains(&token) {
        return Ok(EventKind::Delete);
    }
    Err(WalParseError::UnknownEventKind(token.to_string()))
}

pub(super) fn build_map_row(
    map: &HashMap<String, serde_json::Value>,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    field_prefix: &str,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
    build_row_from_map_with(map, table_id, catalog, |value, column| {
        infer_cell_from_json_strict(value, &format!("{field_prefix}.{column}"))
    })
}

pub(super) fn build_pk_from_optional_names(
    pk_col_names: Option<&[String]>,
    resolved: &[(ColumnId, Cell)],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<PrimaryKey, WalParseError> {
    if let Some(names) = pk_col_names {
        let pk_col_ids = strict_pk_column_ids_from_names(
            table_id,
            names,
            resolved,
            catalog,
            "primary_key_columns",
        )?;
        return Ok(build_pk_from_resolved(resolved, &pk_col_ids));
    }

    Ok(pk_from_catalog_or_empty(resolved, table_id, catalog))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn convert_map_cdc_event(
    kind: EventKind,
    table_id: TableId,
    new_map: Option<&HashMap<String, serde_json::Value>>,
    old_map: Option<&HashMap<String, serde_json::Value>>,
    required_new_field: &str,
    required_old_field: &str,
    new_field_prefix: &str,
    old_field_prefix: &str,
    pk_col_names: Option<&[String]>,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    match kind {
        EventKind::Insert => {
            let new_map = new_map
                .ok_or_else(|| WalParseError::MissingField(required_new_field.to_string()))?;
            let (new_row, resolved) = build_map_row(new_map, table_id, catalog, new_field_prefix)?;
            let pk = build_pk_from_optional_names(pk_col_names, &resolved, table_id, catalog)?;
            Ok(insert_event(table_id, pk, new_row))
        }
        EventKind::Update => {
            let new_map = new_map
                .ok_or_else(|| WalParseError::MissingField(required_new_field.to_string()))?;
            let (new_row, resolved) = build_map_row(new_map, table_id, catalog, new_field_prefix)?;
            let old_row = old_map
                .map(|old| build_map_row(old, table_id, catalog, old_field_prefix))
                .transpose()?
                .map(|(row, _)| row);
            let pk = build_pk_from_optional_names(pk_col_names, &resolved, table_id, catalog)?;
            Ok(update_event(table_id, pk, old_row, new_row))
        }
        EventKind::Delete => {
            let old_map = old_map
                .ok_or_else(|| WalParseError::MissingField(required_old_field.to_string()))?;
            let (old_row, resolved) = build_map_row(old_map, table_id, catalog, old_field_prefix)?;
            let pk = build_pk_from_optional_names(pk_col_names, &resolved, table_id, catalog)?;
            Ok(delete_event(table_id, pk, old_row))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct TestCatalog {
        tables: HashMap<String, (TableId, usize)>,
        columns: HashMap<(TableId, String), ColumnId>,
        primary_keys: HashMap<TableId, Vec<ColumnId>>,
    }

    impl TestCatalog {
        fn orders() -> Self {
            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), (1, 3));

            let mut columns = HashMap::new();
            columns.insert((1, "id".to_string()), 0);
            columns.insert((1, "amount".to_string()), 1);
            columns.insert((1, "status".to_string()), 2);

            let mut primary_keys = HashMap::new();
            primary_keys.insert(1, vec![0]);

            Self {
                tables,
                columns,
                primary_keys,
            }
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

    #[test]
    fn parse_event_kind_unknown_token() {
        let err = parse_event_kind("x", &["i"], &["u"], &["d"]).expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    #[test]
    fn convert_map_cdc_insert_missing_required_field() {
        let catalog = TestCatalog::orders();
        let err = convert_map_cdc_event(
            EventKind::Insert,
            1,
            None,
            None,
            "after",
            "before",
            "after",
            "before",
            None,
            &catalog,
        )
        .expect_err("missing new map should fail");
        assert!(matches!(err, WalParseError::MissingField(field) if field == "after"));
    }

    #[test]
    fn convert_map_cdc_update_without_old_map_is_allowed() {
        let catalog = TestCatalog::orders();
        let new_map = HashMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("amount".to_string(), serde_json::json!(10.5)),
        ]);
        let event = convert_map_cdc_event(
            EventKind::Update,
            1,
            Some(&new_map),
            None,
            "after",
            "before",
            "after",
            "before",
            None,
            &catalog,
        )
        .expect("update should parse");
        assert_eq!(event.kind, EventKind::Update);
        assert!(event.old_row.is_none());
        assert!(event.new_row.is_some());
    }

    #[test]
    fn build_pk_from_optional_names_unknown_column_errors() {
        let catalog = TestCatalog::orders();
        let resolved = vec![(0_u16, Cell::Int(1))];
        let names = vec!["id".to_string(), "missing".to_string()];

        let err = build_pk_from_optional_names(Some(&names), &resolved, 1, &catalog)
            .expect_err("unknown PK metadata column should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn build_pk_from_optional_names_missing_value_errors() {
        let catalog = TestCatalog::orders();
        let resolved = vec![(1_u16, Cell::Float(10.5))];
        let names = vec!["id".to_string()];

        let err = build_pk_from_optional_names(Some(&names), &resolved, 1, &catalog)
            .expect_err("missing PK value should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }
}
