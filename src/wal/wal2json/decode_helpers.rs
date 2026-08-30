use sql_traits::prelude::DatabaseLike;
use wal2json_events::Column;

use crate::backend::{Postgres, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, TableId};
use crate::wal::pg_type::json_value_to_pg_value_by_kind;

/// Decode one wal2json JSON cell against the catalog's declared type.
/// `None` (the wire did not carry the column) yields `Ok(Value::Missing)`,
/// a JSON null yields `Ok(Value::Null)`, and a carried cell of a known
/// kind that will not decode yields `Err`.
pub fn decode_cell<DB: DatabaseLike>(
    value: Option<&serde_json::Value>,
    db: &DB,
    table_id: TableId,
    col: ColumnId,
) -> Result<Value<Postgres>, crate::ValueError> {
    match value {
        None => Ok(Value::Missing),
        Some(v) if v.is_null() => Ok(Value::Null),
        Some(v) => catalog_helpers::column_scalar_kind::<Postgres, DB>(db, table_id, col).map_or(
            Ok(Value::Missing),
            |kind| {
                crate::backend::decode_cell(col, kind, |builtin| {
                    json_value_to_pg_value_by_kind(v, builtin)
                })
            },
        ),
    }
}

/// The cell `name` carries in `columns`, if any. An entry without a value
/// (as in the `pk` listing) reads as an absent cell.
pub fn column_value<'a>(columns: &'a [Column], name: &str) -> Option<&'a serde_json::Value> {
    columns
        .iter()
        .find(|c| c.name == name)
        .and_then(|c| c.value.as_ref())
}
