use std::collections::HashMap;
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
    F: FnMut(&serde_json::Value) -> Cell,
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
        let cell = value_to_cell(value);
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

/// Build a row image from parallel typed arrays.
pub(super) fn build_row_from_typed_arrays_with<F>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    context: &str,
    mut value_to_cell: F,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError>
where
    F: FnMut(&serde_json::Value, &str) -> Cell,
{
    if names.len() != types.len() || names.len() != values.len() {
        return Err(WalParseError::MalformedPayload(format!(
            "{context} arrays length mismatch: names={}, types={}, values={}",
            names.len(),
            types.len(),
            values.len()
        )));
    }

    let arity = catalog
        .table_arity(table_id)
        .ok_or_else(|| WalParseError::UnknownTable {
            schema: String::new(),
            table: format!("table_id={table_id}"),
        })?;

    let mut cells = vec![Cell::Missing; arity];
    let mut resolved = Vec::with_capacity(names.len());

    for ((name, ty), value) in names.iter().zip(types).zip(values) {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        let cell = value_to_cell(value, ty);
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
    F: FnMut(&serde_json::Value, &str) -> Cell,
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

    for ((name, ty), value) in names.iter().zip(types).zip(values) {
        let col_id =
            catalog
                .column_id(table_id, name)
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
        pk_cols.push(col_id);
        pk_vals.push(value_to_cell(value, ty));
    }

    Ok(crate::PrimaryKey {
        columns: Arc::from(pk_cols),
        values: Arc::from(pk_vals),
    })
}
