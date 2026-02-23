use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{Cell, ColumnId, RowImage, SchemaCatalog, TableId};

use super::WalParseError;

/// Build a row image from a column->value map and return resolved `(ColumnId, Cell)` pairs.
pub(super) fn build_row_from_map_with<F>(
    map: &HashMap<String, serde_json::Value>,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    mut value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str) -> Result<Cell, WalParseError>,
{
    let arity = catalog
        .table_arity(table_id)
        .ok_or_else(|| WalParseError::UnknownTable {
            schema: String::new(),
            table: format!("table_id={table_id}"),
        })?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(map.len());

    for (name, value) in map {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        if (col_id as usize) >= arity {
            return Err(WalParseError::ArityMismatch {
                table_id,
                wal_count: map.len(),
                catalog_arity: arity,
            });
        }

        let cell = value_to_cell(value, name)?;
        cells[col_id as usize] = cell.clone();
        resolved.push((col_id, cell));
    }

    Ok((
        RowImage {
            cells: Arc::from(cells),
        },
        resolved,
    ))
}

/// Build a row image from parallel typed arrays.
pub(super) fn build_row_from_named_typed_values_with<F>(
    columns: &[(&str, &str, &serde_json::Value)],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    context: &str,
    mut value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str, &str) -> Result<Cell, WalParseError>,
{
    let arity = catalog
        .table_arity(table_id)
        .ok_or_else(|| WalParseError::UnknownTable {
            schema: String::new(),
            table: format!("table_id={table_id}"),
        })?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(columns.len());
    let mut seen = HashSet::with_capacity(columns.len());

    for (name, ty, value) in columns {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: (*name).to_string(),
                })?;
        if !seen.insert(col_id) {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} contains duplicate column id {col_id} ('{name}')"
            )));
        }
        if (col_id as usize) >= arity {
            return Err(WalParseError::ArityMismatch {
                table_id,
                wal_count: columns.len(),
                catalog_arity: arity,
            });
        }

        let cell = value_to_cell(value, ty, name)?;
        cells[col_id as usize] = cell.clone();
        resolved.push((col_id, cell));
    }

    Ok((
        RowImage {
            cells: Arc::from(cells),
        },
        resolved,
    ))
}

/// Build a row image from parallel typed arrays.
pub(super) fn build_row_from_typed_arrays_with<F>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    context: &str,
    value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str, &str) -> Result<Cell, WalParseError>,
{
    if names.len() != types.len() || names.len() != values.len() {
        return Err(WalParseError::MalformedPayload(format!(
            "{context} arrays length mismatch: names={}, types={}, values={}",
            names.len(),
            types.len(),
            values.len()
        )));
    }

    let columns: Vec<(&str, &str, &serde_json::Value)> = names
        .iter()
        .zip(types)
        .zip(values)
        .map(|((name, ty), value)| (name.as_str(), ty.as_str(), value))
        .collect();

    build_row_from_named_typed_values_with(&columns, table_id, catalog, context, value_to_cell)
}

/// Build a primary key from parallel typed arrays.
pub(super) fn build_pk_from_typed_arrays_with<F>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    context: &str,
    mut value_to_cell: F,
) -> Result<crate::PrimaryKey, WalParseError>
where
    F: FnMut(&serde_json::Value, &str, &str) -> Result<Cell, WalParseError>,
{
    if names.len() != types.len() || names.len() != values.len() {
        return Err(WalParseError::MalformedPayload(format!(
            "{context} arrays length mismatch: names={}, types={}, values={}",
            names.len(),
            types.len(),
            values.len()
        )));
    }

    let mut pk_cols = Vec::with_capacity(names.len());
    let mut pk_vals = Vec::with_capacity(names.len());
    let mut seen = HashSet::with_capacity(names.len());

    for ((name, ty), value) in names.iter().zip(types).zip(values) {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        if !seen.insert(col_id) {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} contains duplicate column id {col_id} ('{name}')"
            )));
        }
        pk_cols.push(col_id);
        pk_vals.push(value_to_cell(value, ty, name)?);
    }

    Ok(crate::PrimaryKey {
        columns: Arc::from(pk_cols),
        values: Arc::from(pk_vals),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockCatalog;
    use serde_json::json;

    fn make_catalog() -> MockCatalog {
        let tables = HashMap::from([("users".to_string(), (1_u32, 2_usize))]);
        let columns = HashMap::from([
            ((1_u32, "id".to_string()), 0_u16),
            ((1_u32, "name".to_string()), 1_u16),
            ((1_u32, "ghost".to_string()), 7_u16),
            ((1_u32, "tenant".to_string()), 2_u16),
        ]);
        MockCatalog { tables, columns }
    }

    fn json_to_cell(
        value: &serde_json::Value,
        _ty: &str,
        _name: &str,
    ) -> Result<Cell, WalParseError> {
        Ok(match value {
            serde_json::Value::Null => Cell::Null,
            serde_json::Value::Bool(b) => Cell::Bool(*b),
            serde_json::Value::Number(n) => n
                .as_i64()
                .map_or_else(|| n.as_f64().map_or(Cell::Missing, Cell::Float), Cell::Int),
            serde_json::Value::String(s) => Cell::String(s.clone().into()),
            _ => Cell::Missing,
        })
    }

    #[test]
    fn test_build_row_from_map_with_success() {
        let catalog = make_catalog();
        let map = HashMap::from([
            ("id".to_string(), json!(10)),
            ("name".to_string(), json!("alice")),
        ]);

        let (row, resolved) = build_row_from_map_with(&map, 1, &catalog, |value, _name| {
            json_to_cell(value, "", "")
        })
        .expect("map should build a row image");

        assert_eq!(row.cells.len(), 2);
        assert_eq!(row.cells[0], Cell::Int(10));
        assert_eq!(row.cells[1], Cell::String("alice".into()));
        assert_eq!(resolved.len(), 2);
        assert!(resolved
            .iter()
            .any(|(col, cell)| *col == 0 && *cell == Cell::Int(10)));
        assert!(resolved
            .iter()
            .any(|(col, cell)| *col == 1 && *cell == Cell::String("alice".into())));
    }

    #[test]
    fn test_build_row_from_map_with_unknown_table() {
        let catalog = make_catalog();
        let map = HashMap::from([("id".to_string(), json!(10))]);

        let err = build_row_from_map_with(&map, 999, &catalog, |value, _name| {
            json_to_cell(value, "", "")
        })
        .expect_err("must fail");
        match err {
            WalParseError::UnknownTable { schema, table } => {
                assert!(schema.is_empty());
                assert_eq!(table, "table_id=999");
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_build_row_from_map_with_unknown_column() {
        let catalog = make_catalog();
        let map = HashMap::from([("missing".to_string(), json!(10))]);

        let err = build_row_from_map_with(&map, 1, &catalog, |value, _name| {
            json_to_cell(value, "", "")
        })
        .expect_err("must fail");
        match err {
            WalParseError::UnknownColumn { table_id, column } => {
                assert_eq!(table_id, 1);
                assert_eq!(column, "missing");
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_build_row_from_map_with_out_of_range_column_id() {
        let catalog = make_catalog();
        let map = HashMap::from([("ghost".to_string(), json!(99))]);

        let err = build_row_from_map_with(&map, 1, &catalog, |value, _name| {
            json_to_cell(value, "", "")
        })
        .expect_err("out-of-range column id should fail");

        assert!(matches!(
            err,
            WalParseError::ArityMismatch {
                table_id: 1,
                wal_count: 1,
                catalog_arity: 2
            }
        ));
    }

    #[test]
    fn test_build_row_from_named_typed_values_with_success() {
        let catalog = make_catalog();
        let id = json!(10);
        let name = json!("alice");
        let columns = [("id", "int4", &id), ("name", "text", &name)];

        let (row, resolved) = build_row_from_named_typed_values_with(
            &columns,
            1,
            &catalog,
            "columns",
            |value, ty, col| json_to_cell(value, ty, col),
        )
        .expect("typed values should build row");

        assert_eq!(row.cells.len(), 2);
        assert_eq!(row.cells[0], Cell::Int(10));
        assert_eq!(row.cells[1], Cell::String("alice".into()));
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn test_build_row_from_named_typed_values_with_duplicate_column() {
        let catalog = make_catalog();
        let id1 = json!(10);
        let id2 = json!(11);
        let columns = [("id", "int4", &id1), ("id", "int4", &id2)];

        let err = build_row_from_named_typed_values_with(
            &columns,
            1,
            &catalog,
            "columns",
            |value, ty, col| json_to_cell(value, ty, col),
        )
        .expect_err("duplicate typed column should fail");

        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn test_build_row_from_typed_arrays_with_length_mismatch() {
        let catalog = make_catalog();
        let names = vec!["id".to_string()];
        let types = vec!["int4".to_string(), "text".to_string()];
        let values = vec![json!(10)];

        let err = build_row_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "new_row",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect_err("must fail");

        match err {
            WalParseError::MalformedPayload(message) => {
                assert_eq!(
                    message,
                    "new_row arrays length mismatch: names=1, types=2, values=1"
                );
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_build_row_from_typed_arrays_with_out_of_range_column_id() {
        let catalog = make_catalog();
        let names = vec!["id".to_string(), "ghost".to_string()];
        let types = vec!["int4".to_string(), "int4".to_string()];
        let values = vec![json!(10), json!(99)];

        let err = build_row_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "new_row",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect_err("out-of-range column id should fail");

        assert!(matches!(
            err,
            WalParseError::ArityMismatch {
                table_id: 1,
                wal_count: 2,
                catalog_arity: 2
            }
        ));
    }

    #[test]
    fn test_build_pk_from_typed_arrays_with_length_mismatch() {
        let catalog = make_catalog();
        let names = vec!["id".to_string()];
        let types = vec!["int4".to_string(), "int4".to_string()];
        let values = vec![json!(10)];

        let err = build_pk_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "oldkeys",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect_err("must fail");

        match err {
            WalParseError::MalformedPayload(message) => {
                assert_eq!(
                    message,
                    "oldkeys arrays length mismatch: names=1, types=2, values=1"
                );
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_build_pk_from_typed_arrays_with_unknown_column() {
        let catalog = make_catalog();
        let names = vec!["missing".to_string()];
        let types = vec!["text".to_string()];
        let values = vec![json!("x")];

        let err = build_pk_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "oldkeys",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect_err("must fail");

        match err {
            WalParseError::UnknownColumn { table_id, column } => {
                assert_eq!(table_id, 1);
                assert_eq!(column, "missing");
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_build_pk_from_typed_arrays_with_success() {
        let catalog = make_catalog();
        let names = vec!["id".to_string(), "tenant".to_string()];
        let types = vec!["int4".to_string(), "int4".to_string()];
        let values = vec![json!(10), json!(42)];

        let pk = build_pk_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "oldkeys",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect("pk should be built");

        assert_eq!(&*pk.columns, &[0, 2]);
        assert_eq!(&*pk.values, &[Cell::Int(10), Cell::Int(42)]);
    }

    #[test]
    fn test_build_row_from_typed_arrays_with_duplicate_column_id() {
        let catalog = make_catalog();
        let names = vec!["id".to_string(), "id".to_string()];
        let types = vec!["int4".to_string(), "int4".to_string()];
        let values = vec![json!(10), json!(20)];

        let err = build_row_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "new_row",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect_err("duplicate column IDs should fail");

        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn test_build_pk_from_typed_arrays_with_duplicate_column_id() {
        let catalog = make_catalog();
        let names = vec!["id".to_string(), "id".to_string()];
        let types = vec!["int4".to_string(), "int4".to_string()];
        let values = vec![json!(10), json!(20)];

        let err = build_pk_from_typed_arrays_with(
            &names,
            &types,
            &values,
            1,
            &catalog,
            "oldkeys",
            |value, ty, name| json_to_cell(value, ty, name),
        )
        .expect_err("duplicate column IDs should fail");

        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }
}
