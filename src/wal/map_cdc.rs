use std::collections::HashMap;

use super::pg_type::infer_cell_from_json_strict;
use super::row_build::build_row_from_map_with;
use super::{
    build_pk_from_resolved, delete_event, insert_event, pk_from_catalog_or_empty,
    strict_pk_column_ids_from_names, truncate_event, update_event_with_old_row_completeness,
    WalParseError,
};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

pub(super) fn parse_event_kind(
    token: &str,
    insert_tokens: &[&str],
    update_tokens: &[&str],
    delete_tokens: &[&str],
    truncate_tokens: &[&str],
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
    if truncate_tokens.contains(&token) {
        return Ok(EventKind::Truncate);
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

    pk_from_catalog_or_empty(resolved, table_id, catalog)
}

pub(super) struct MapCdcConfig<'a> {
    pub required_new_field: &'a str,
    pub required_old_field: &'a str,
    pub new_field_prefix: &'a str,
    pub old_field_prefix: &'a str,
    pub pk_col_names: Option<&'a [String]>,
}

pub(super) fn convert_map_cdc_event(
    kind: EventKind,
    table_id: TableId,
    new_map: Option<&HashMap<String, serde_json::Value>>,
    old_map: Option<&HashMap<String, serde_json::Value>>,
    config: &MapCdcConfig<'_>,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    match kind {
        EventKind::Insert => {
            let new_map = new_map.ok_or_else(|| {
                WalParseError::MissingField(config.required_new_field.to_string())
            })?;
            let (new_row, resolved) =
                build_map_row(new_map, table_id, catalog, config.new_field_prefix)?;
            let pk =
                build_pk_from_optional_names(config.pk_col_names, &resolved, table_id, catalog)?;
            Ok(insert_event(table_id, pk, new_row))
        }
        EventKind::Update => {
            let new_map = new_map.ok_or_else(|| {
                WalParseError::MissingField(config.required_new_field.to_string())
            })?;
            let (new_row, resolved) =
                build_map_row(new_map, table_id, catalog, config.new_field_prefix)?;
            let old_row = old_map
                .map(|old| build_map_row(old, table_id, catalog, config.old_field_prefix))
                .transpose()?
                .map(|(row, _)| row);
            let pk =
                build_pk_from_optional_names(config.pk_col_names, &resolved, table_id, catalog)?;
            // Always attempt to derive changed_columns from the old row.
            // `changed_columns()` skips Missing cells, so sparse old images
            // (e.g. Maxwell's partial-old format) work correctly: only
            // non-Missing columns are compared, and if none differ the result
            // is empty — which falls through to a conservative full re-evaluation.
            Ok(update_event_with_old_row_completeness(
                table_id,
                pk,
                old_row,
                new_row,
                true,
            ))
        }
        EventKind::Delete => {
            let old_map = old_map.ok_or_else(|| {
                WalParseError::MissingField(config.required_old_field.to_string())
            })?;
            let (old_row, resolved) =
                build_map_row(old_map, table_id, catalog, config.old_field_prefix)?;
            let pk =
                build_pk_from_optional_names(config.pk_col_names, &resolved, table_id, catalog)?;
            Ok(delete_event(table_id, pk, old_row))
        }
        EventKind::Truncate => Ok(truncate_event(table_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::TestCatalog;
    use super::*;
    use std::collections::HashMap;

    fn orders_catalog() -> TestCatalog {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), (1, 3));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "amount".to_string()), 1);
        columns.insert((1, "status".to_string()), 2);

        let mut primary_keys = HashMap::new();
        primary_keys.insert(1, vec![0]);

        TestCatalog {
            tables,
            columns,
            primary_keys,
        }
    }

    #[test]
    fn parse_event_kind_unknown_token() {
        let err = parse_event_kind("x", &["i"], &["u"], &["d"], &["t"]).expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownEventKind(_)));
    }

    #[test]
    fn convert_map_cdc_insert_missing_required_field() {
        let catalog = orders_catalog();
        let err = convert_map_cdc_event(
            EventKind::Insert,
            1,
            None,
            None,
            &MapCdcConfig {
                required_new_field: "after",
                required_old_field: "before",
                new_field_prefix: "after",
                old_field_prefix: "before",
                pk_col_names: None,
            },
            &catalog,
        )
        .expect_err("missing new map should fail");
        assert!(matches!(err, WalParseError::MissingField(field) if field == "after"));
    }

    #[test]
    fn convert_map_cdc_update_without_old_map_is_allowed() {
        let catalog = orders_catalog();
        let new_map = HashMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("amount".to_string(), serde_json::json!(10.5)),
        ]);
        let event = convert_map_cdc_event(
            EventKind::Update,
            1,
            Some(&new_map),
            None,
            &MapCdcConfig {
                required_new_field: "after",
                required_old_field: "before",
                new_field_prefix: "after",
                old_field_prefix: "before",
                pk_col_names: None,
            },
            &catalog,
        )
        .expect("update should parse");
        assert_eq!(event.kind, EventKind::Update);
        assert!(event.old_row.is_none());
        assert!(event.new_row.is_some());
    }

    #[test]
    fn convert_map_cdc_update_with_partial_old_map_computes_changed_columns() {
        let catalog = orders_catalog();
        let new_map = HashMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("amount".to_string(), serde_json::json!(10.5)),
            ("status".to_string(), serde_json::json!("new")),
        ]);
        // Sparse old row: only `amount` is present (Maxwell-style partial image).
        // changed_columns should be [1] (amount) because it differs; missing
        // columns (id, status) are skipped — no false negatives possible.
        let old_map = HashMap::from([("amount".to_string(), serde_json::json!(7.0))]);

        let event = convert_map_cdc_event(
            EventKind::Update,
            1,
            Some(&new_map),
            Some(&old_map),
            &MapCdcConfig {
                required_new_field: "after",
                required_old_field: "before",
                new_field_prefix: "after",
                old_field_prefix: "before",
                pk_col_names: None,
            },
            &catalog,
        )
        .expect("update with partial old row should parse");

        assert_eq!(event.kind, EventKind::Update);
        assert!(event.old_row.is_some());
        assert!(event.new_row.is_some());
        // amount (col 1) changed: 7.0 → 10.5
        assert_eq!(
            event.changed_columns.as_ref(),
            &[1u16],
            "sparse old row must still derive changed_columns for known-changed columns"
        );
    }

    #[test]
    fn convert_map_cdc_insert_missing_catalog_pk_column_errors() {
        let catalog = orders_catalog();
        let new_map = HashMap::from([
            ("amount".to_string(), serde_json::json!(10.5)),
            ("status".to_string(), serde_json::json!("new")),
        ]);
        let err = convert_map_cdc_event(
            EventKind::Insert,
            1,
            Some(&new_map),
            None,
            &MapCdcConfig {
                required_new_field: "after",
                required_old_field: "before",
                new_field_prefix: "after",
                old_field_prefix: "before",
                pk_col_names: None,
            },
            &catalog,
        )
        .expect_err("missing catalog PK column should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn build_pk_from_optional_names_unknown_column_errors() {
        let catalog = orders_catalog();
        let resolved = vec![(0_u16, Cell::Int(1))];
        let names = vec!["id".to_string(), "missing".to_string()];

        let err = build_pk_from_optional_names(Some(&names), &resolved, 1, &catalog)
            .expect_err("unknown PK metadata column should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn build_pk_from_optional_names_missing_value_errors() {
        let catalog = orders_catalog();
        let resolved = vec![(1_u16, Cell::Float(10.5))];
        let names = vec!["id".to_string()];

        let err = build_pk_from_optional_names(Some(&names), &resolved, 1, &catalog)
            .expect_err("missing PK value should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }
}
