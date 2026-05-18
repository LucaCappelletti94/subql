use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{catalog_helpers, Cell, ColumnId, RowImage, TableId};
use sql_traits::prelude::DatabaseLike;

use super::WalParseError;

fn resolve_table_arity<DB: DatabaseLike>(
    table_id: TableId,
    database: &DB,
) -> Result<usize, WalParseError> {
    catalog_helpers::table_arity(database, table_id).ok_or_else(|| WalParseError::UnknownTable {
        schema: String::new(),
        table: format!("table_id={table_id}"),
    })
}

fn validate_typed_arrays_lengths(
    names_len: usize,
    types_len: usize,
    values_len: usize,
    context: &str,
) -> Result<(), WalParseError> {
    if names_len != types_len || names_len != values_len {
        return Err(WalParseError::MalformedPayload(format!(
            "{context} arrays length mismatch: names={names_len}, types={types_len}, values={values_len}"
        )));
    }

    Ok(())
}

fn resolve_unique_column_id<DB: DatabaseLike>(
    table_id: TableId,
    name: &str,
    database: &DB,
    seen: &mut HashSet<ColumnId>,
    duplicate_context: &str,
) -> Result<ColumnId, WalParseError> {
    let col_id = catalog_helpers::column_id(database, table_id, name).ok_or_else(|| {
        WalParseError::UnknownColumn {
            table_id,
            column: name.to_string(),
        }
    })?;
    if !seen.insert(col_id) {
        return Err(WalParseError::MalformedPayload(format!(
            "{duplicate_context} contains duplicate column id {col_id} ('{name}')"
        )));
    }
    Ok(col_id)
}

const fn ensure_column_in_arity(
    table_id: TableId,
    col_id: ColumnId,
    wal_count: usize,
    catalog_arity: usize,
) -> Result<(), WalParseError> {
    if (col_id as usize) >= catalog_arity {
        return Err(WalParseError::ArityMismatch {
            table_id,
            wal_count,
            catalog_arity,
        });
    }
    Ok(())
}

fn row_image_from_cells(cells: Vec<Cell>) -> RowImage {
    RowImage {
        cells: Arc::from(cells),
    }
}

/// Build a row image from a column->value map and return resolved `(ColumnId, Cell)` pairs.
pub(super) fn build_row_from_map_with<F, DB: DatabaseLike>(
    map: &HashMap<String, serde_json::Value>,
    table_id: TableId,
    database: &DB,
    mut value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str) -> Result<Cell, WalParseError>,
{
    let arity = resolve_table_arity(table_id, database)?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(map.len());
    let mut seen = HashSet::with_capacity(map.len());

    for (name, value) in map {
        let col_id = resolve_unique_column_id(table_id, name, database, &mut seen, "map row")?;
        ensure_column_in_arity(table_id, col_id, map.len(), arity)?;

        let cell = value_to_cell(value, name)?;
        cells[col_id as usize] = cell.clone();
        resolved.push((col_id, cell));
    }

    Ok((row_image_from_cells(cells), resolved))
}

/// Build a row image from parallel typed arrays.
pub(super) fn build_row_from_named_typed_values_with<F, DB: DatabaseLike>(
    columns: &[(&str, &str, &serde_json::Value)],
    table_id: TableId,
    database: &DB,
    context: &str,
    mut value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str, &str) -> Result<Cell, WalParseError>,
{
    let arity = resolve_table_arity(table_id, database)?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(columns.len());
    let mut seen = HashSet::with_capacity(columns.len());

    for (name, ty, value) in columns {
        let col_id = resolve_unique_column_id(table_id, name, database, &mut seen, context)?;
        ensure_column_in_arity(table_id, col_id, columns.len(), arity)?;

        let cell = value_to_cell(value, ty, name)?;
        cells[col_id as usize] = cell.clone();
        resolved.push((col_id, cell));
    }

    Ok((row_image_from_cells(cells), resolved))
}

/// Build a row image from parallel typed arrays.
pub(super) fn build_row_from_typed_arrays_with<F, DB: DatabaseLike>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    database: &DB,
    context: &str,
    value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str, &str) -> Result<Cell, WalParseError>,
{
    validate_typed_arrays_lengths(names.len(), types.len(), values.len(), context)?;

    let columns: Vec<(&str, &str, &serde_json::Value)> = names
        .iter()
        .zip(types)
        .zip(values)
        .map(|((name, ty), value)| (name.as_str(), ty.as_str(), value))
        .collect();

    build_row_from_named_typed_values_with(&columns, table_id, database, context, value_to_cell)
}

/// Build a primary key from parallel typed arrays.
pub(super) fn build_pk_from_typed_arrays_with<F, DB: DatabaseLike>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    database: &DB,
    context: &str,
    mut value_to_cell: F,
) -> Result<crate::PrimaryKey, WalParseError>
where
    F: FnMut(&serde_json::Value, &str, &str) -> Result<Cell, WalParseError>,
{
    validate_typed_arrays_lengths(names.len(), types.len(), values.len(), context)?;

    let mut pk_cols = Vec::with_capacity(names.len());
    let mut pk_vals = Vec::with_capacity(names.len());
    let mut seen = HashSet::with_capacity(names.len());

    for ((name, ty), value) in names.iter().zip(types).zip(values) {
        let col_id = resolve_unique_column_id(table_id, name, database, &mut seen, context)?;
        pk_cols.push(col_id);
        pk_vals.push(value_to_cell(value, ty, name)?);
    }

    Ok(
        crate::PrimaryKey::new(Arc::from(pk_cols), Arc::from(pk_vals))
            .expect("pk columns and values are built in lockstep"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_helpers;
    use serde_json::json;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn make_catalog() -> ParserDB {
        // `tenant` is needed by the pk_from_typed test that uses 2 PK
        // columns. `users` has 3 columns total: id=0, name=1, tenant=2.
        // The test asserts use both `users_tid` lookups and concrete cell
        // ordinals.
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE users (id INT, name TEXT, tenant INT);")
            .expect("users DDL parses")
    }

    fn users_tid(catalog: &ParserDB) -> crate::TableId {
        catalog_helpers::table_id(catalog, "users").expect("users id")
    }

    fn json_to_cell(value: &serde_json::Value, _ty: &str, _name: &str) -> Cell {
        match value {
            serde_json::Value::Null => Cell::Null,
            serde_json::Value::Bool(b) => Cell::Bool(*b),
            serde_json::Value::Number(n) => n
                .as_i64()
                .map_or_else(|| n.as_f64().map_or(Cell::Missing, Cell::Float), Cell::Int),
            serde_json::Value::String(s) => Cell::String(s.clone().into()),
            _ => Cell::Missing,
        }
    }

    #[test]
    fn test_build_row_from_map_with_success() {
        let catalog = make_catalog();
        let map = HashMap::from([
            ("id".to_string(), json!(10)),
            ("name".to_string(), json!("alice")),
        ]);

        let (row, resolved) =
            build_row_from_map_with(&map, users_tid(&catalog), &catalog, |value, _name| {
                Ok(json_to_cell(value, "", ""))
            })
            .expect("map should build a row image");

        // Row size matches catalog arity; only the supplied cells are populated.
        let arity =
            crate::catalog_helpers::table_arity(&catalog, users_tid(&catalog)).expect("arity");
        assert_eq!(row.cells.len(), arity);
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

    // Removed `test_build_row_from_map_with_duplicate_resolved_column_id`:
    // this test injected a duplicate column entry into a `MockCatalog`.
    // ParserDB resolves each column name to a unique ordinal, so the case
    // is no longer reachable.

    #[test]
    fn test_build_row_from_map_with_unknown_table() {
        let catalog = make_catalog();
        let map = HashMap::from([("id".to_string(), json!(10))]);

        let err = build_row_from_map_with(&map, 999, &catalog, |value, _name| {
            Ok(json_to_cell(value, "", ""))
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

        let err = build_row_from_map_with(&map, users_tid(&catalog), &catalog, |value, _name| {
            Ok(json_to_cell(value, "", ""))
        })
        .expect_err("must fail");
        match err {
            WalParseError::UnknownColumn { table_id, column } => {
                assert_eq!(table_id, users_tid(&catalog));
                assert_eq!(column, "missing");
            }
            _ => panic!("unexpected error variant"),
        }
    }

    // Removed `test_build_row_from_map_with_out_of_range_column_id`: this
    // test relied on injecting a `ghost` column with ordinal 7 into a 2-arity
    // catalog. ParserDB always reports column ordinals in [0, arity), so an
    // out-of-range column id cannot be produced via the public API.

    #[test]
    fn test_build_row_from_named_typed_values_with_success() {
        let catalog = make_catalog();
        let id = json!(10);
        let name = json!("alice");
        let columns = [("id", "int4", &id), ("name", "text", &name)];

        let (row, resolved) = build_row_from_named_typed_values_with(
            &columns,
            users_tid(&catalog),
            &catalog,
            "columns",
            |value, ty, col| Ok(json_to_cell(value, ty, col)),
        )
        .expect("typed values should build row");

        let arity =
            crate::catalog_helpers::table_arity(&catalog, users_tid(&catalog)).expect("arity");
        assert_eq!(row.cells.len(), arity);
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
            users_tid(&catalog),
            &catalog,
            "columns",
            |value, ty, col| Ok(json_to_cell(value, ty, col)),
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
            users_tid(&catalog),
            &catalog,
            "new_row",
            |value, ty, name| Ok(json_to_cell(value, ty, name)),
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

    // Removed `test_build_row_from_typed_arrays_with_out_of_range_column_id`:
    // see deletion note on `test_build_row_from_map_with_out_of_range_column_id`.

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
            users_tid(&catalog),
            &catalog,
            "oldkeys",
            |value, ty, name| Ok(json_to_cell(value, ty, name)),
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
            users_tid(&catalog),
            &catalog,
            "oldkeys",
            |value, ty, name| Ok(json_to_cell(value, ty, name)),
        )
        .expect_err("must fail");

        match err {
            WalParseError::UnknownColumn { table_id, column } => {
                assert_eq!(table_id, users_tid(&catalog));
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
            users_tid(&catalog),
            &catalog,
            "oldkeys",
            |value, ty, name| Ok(json_to_cell(value, ty, name)),
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
            users_tid(&catalog),
            &catalog,
            "new_row",
            |value, ty, name| Ok(json_to_cell(value, ty, name)),
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
            users_tid(&catalog),
            &catalog,
            "oldkeys",
            |value, ty, name| Ok(json_to_cell(value, ty, name)),
        )
        .expect_err("duplicate column IDs should fail");

        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }
}
