use std::collections::HashMap;

use super::pg_type::infer_cell_from_json_strict;
use super::row_build::build_row_from_map_with;
use super::{
    build_event_from_rows, build_pk_from_resolved, pk_from_catalog_or_empty,
    strict_pk_column_ids_from_names, WalParseError,
};
use crate::{Cell, ColumnId, EventKind, PrimaryKey, RowImage, SchemaCatalog, TableId, WalEvent};

pub(super) trait MapCdcEnvelope {
    const PARSER_NAME: &'static str;
    const TOKEN_NAME: &'static str;

    fn event_token(&self) -> &str;

    fn parse_kind(token: &str) -> Result<EventKind, WalParseError>;

    fn resolve_table_id(&self, catalog: &dyn SchemaCatalog) -> Result<TableId, WalParseError>;

    fn map_config(&self, kind: EventKind) -> MapCdcConfig<'_>;

    fn new_map(&self, kind: EventKind) -> Option<&HashMap<String, serde_json::Value>>;

    fn old_map(&self, kind: EventKind) -> Option<&HashMap<String, serde_json::Value>>;

    fn skip_message(&self) -> bool {
        false
    }
}

pub(super) fn parse_map_cdc_json_message<T>(
    data: &[u8],
    catalog: &dyn SchemaCatalog,
) -> Result<Vec<WalEvent>, WalParseError>
where
    T: serde::de::DeserializeOwned + MapCdcEnvelope,
{
    super::parse_single_json_event::<T, _>(data, |message| {
        if message.skip_message() {
            return Ok(None);
        }
        super::skip_unknown_event_kind(
            convert_map_cdc_envelope(message, catalog),
            T::PARSER_NAME,
            T::TOKEN_NAME,
        )
    })
}

pub(super) fn convert_map_cdc_envelope<T: MapCdcEnvelope>(
    message: &T,
    catalog: &dyn SchemaCatalog,
) -> Result<WalEvent, WalParseError> {
    let kind = T::parse_kind(message.event_token())?;
    let table_id = message.resolve_table_id(catalog)?;
    let config = message.map_config(kind);
    convert_map_cdc_event(
        kind,
        table_id,
        message.new_map(kind),
        message.old_map(kind),
        &config,
        catalog,
    )
}

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
    /// When `true`, the old-row map contains **only the changed columns** by
    /// format contract (e.g. Maxwell's `old` field).  `Cell::Missing` in the
    /// old row therefore means "column was not changed", and `changed_columns`
    /// can be derived correctly even from a sparse old row.
    ///
    /// When `false` (e.g. Debezium, wal2json), the old row reflects whatever
    /// the replica identity captured; a sparse row may be missing columns that
    /// *did* change, so we only compute `changed_columns` when the old row is
    /// complete (conservative, avoids false negatives).
    pub old_is_changed_columns_only: bool,
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
            build_event_from_rows(
                kind,
                table_id,
                pk,
                None,
                Some(new_row),
                false,
                config.required_new_field,
                config.required_old_field,
            )
        }
        EventKind::Update => {
            let new_map = new_map.ok_or_else(|| {
                WalParseError::MissingField(config.required_new_field.to_string())
            })?;
            let (new_row, resolved) =
                build_map_row(new_map, table_id, catalog, config.new_field_prefix)?;
            let (old_row, old_resolved) = old_map
                .map(|old| build_map_row(old, table_id, catalog, config.old_field_prefix))
                .transpose()?
                .map_or((None, Vec::new()), |(row, res)| (Some(row), res));
            // Use pre-update PK when the old row is complete (all columns present).
            // A complete old row guarantees PK columns are present, enabling correct
            // index lookups when a PK column changes (e.g. id: 1 → 2).
            // Sparse old rows (e.g. Debezium DEFAULT replica identity, Maxwell changed-only)
            // may not contain PK columns, so we fall back to the new row's resolved values.
            let old_row_complete = super::old_row_is_complete(old_row.as_ref());
            let pk_resolved = if old_row_complete {
                &old_resolved
            } else {
                &resolved
            };
            let pk =
                build_pk_from_optional_names(config.pk_col_names, pk_resolved, table_id, catalog)?;
            // Derive changed_columns when either:
            // - The old row is complete (all columns present), or
            // - The format guarantees the old row contains only changed columns
            //   (e.g. Maxwell), in which case Missing means "not changed" and
            //   changed_columns() safely skips Missing cells.
            let compute_changed = config.old_is_changed_columns_only || old_row_complete;
            build_event_from_rows(
                kind,
                table_id,
                pk,
                old_row,
                Some(new_row),
                compute_changed,
                config.required_new_field,
                config.required_old_field,
            )
        }
        EventKind::Delete => {
            let old_map = old_map.ok_or_else(|| {
                WalParseError::MissingField(config.required_old_field.to_string())
            })?;
            let (old_row, resolved) =
                build_map_row(old_map, table_id, catalog, config.old_field_prefix)?;
            let pk =
                build_pk_from_optional_names(config.pk_col_names, &resolved, table_id, catalog)?;
            build_event_from_rows(
                kind,
                table_id,
                pk,
                Some(old_row),
                None,
                false,
                config.required_new_field,
                config.required_old_field,
            )
        }
        EventKind::Truncate => build_event_from_rows(
            kind,
            table_id,
            PrimaryKey::empty(),
            None,
            None,
            false,
            config.required_new_field,
            config.required_old_field,
        ),
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
                old_is_changed_columns_only: false,
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
                old_is_changed_columns_only: false,
            },
            &catalog,
        )
        .expect("update should parse");
        assert_eq!(event.kind, EventKind::Update);
        assert!(event.old_row.is_none());
        assert!(event.new_row.is_some());
    }

    #[test]
    fn convert_map_cdc_update_with_partial_old_map_gives_empty_changed_columns() {
        let catalog = orders_catalog();
        let new_map = HashMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("amount".to_string(), serde_json::json!(10.5)),
            ("status".to_string(), serde_json::json!("new")),
        ]);
        // Sparse old row: only `amount` is present; `id` and `status` are Missing.
        // Because the old row is incomplete, changed_columns is left empty to
        // avoid false negatives (conservative semantics).
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
                old_is_changed_columns_only: false,
            },
            &catalog,
        )
        .expect("update with partial old row should parse");

        assert_eq!(event.kind, EventKind::Update);
        assert!(event.old_row.is_some());
        assert!(event.new_row.is_some());
        // Partial old row → conservative: changed_columns is empty.
        assert!(
            event.changed_columns.is_empty(),
            "partial old row must produce empty changed_columns to avoid false negatives"
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
                old_is_changed_columns_only: false,
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
