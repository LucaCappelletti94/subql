use core::hash::{Hash, Hasher};

use sql_traits::prelude::DatabaseLike;
use wal2json_events::Column;

use crate::backend::{Postgres, ScalarFamily, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, TableId};
use crate::wal::pg_type::json_value_to_pg_value_by_kind;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexedName<'a>(&'a str);

impl<'a> IndexedName<'a> {
    pub(super) const fn new(name: &'a str) -> Self {
        Self(name)
    }
}

impl Hash for IndexedName<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        #[cfg(test)]
        INDEX_HASHES.with(|hashes| hashes.set(hashes.get() + 1));
        self.0.hash(state);
    }
}

#[cfg(test)]
std::thread_local! {
    static INDEX_HASHES: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn take_index_hashes() -> usize {
    INDEX_HASHES.with(|hashes| hashes.replace(0))
}

/// Decode one wal2json JSON cell against the catalog's declared type.
/// `None` (the wire did not carry the column) yields `Ok(Value::Missing)`,
/// a JSON null yields `Ok(Value::Null)`, and a carried cell of a known
/// kind that will not decode yields `Err`.
///
/// JSON has no `NaN` or infinity, and wal2json writes `null` for them in a
/// float or numeric column, so a `null` there may not be `NULL` and yields
/// `Ok(Value::Missing)`.
pub fn decode_cell<DB: DatabaseLike>(
    value: Option<&serde_json::Value>,
    db: &DB,
    table_id: TableId,
    col: ColumnId,
) -> Result<Value<Postgres>, crate::ValueError> {
    let Some(value) = value else {
        return Ok(Value::Missing);
    };
    let Some(kind) = catalog_helpers::column_scalar_kind::<Postgres, DB>(db, table_id, col) else {
        return Ok(if value.is_null() {
            Value::Null
        } else {
            Value::Missing
        });
    };
    if value.is_null() {
        return Ok(
            if matches!(
                kind.family(),
                Some(ScalarFamily::Float | ScalarFamily::Decimal)
            ) {
                Value::Missing
            } else {
                Value::Null
            },
        );
    }
    crate::backend::decode_cell(col, kind, |builtin| {
        json_value_to_pg_value_by_kind(value, builtin)
    })
}

/// The cell `name` carries in `columns`, if any. An entry without a value
/// (as in the `pk` listing) reads as an absent cell.
pub fn column_value<'a>(columns: &'a [Column], name: &str) -> Option<&'a serde_json::Value> {
    columns
        .iter()
        .find(|c| c.name == name)
        .and_then(|c| c.value.as_ref())
}
