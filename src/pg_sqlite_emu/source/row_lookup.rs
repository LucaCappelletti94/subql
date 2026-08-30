//! Row lookup for the unchanged-column fallback in
//! [`super::PgSqliteEmuSource`].

use alloc::vec::Vec;

use diesel::SqliteConnection;
use sqlite_diff_rs::Value as WireValue;

use super::super::error::PgSqliteEmuError;
use super::TableMeta;

/// The dynamic table the row lookup reads through. Named at runtime, so no
/// `table!` macro can describe it.
pub(super) type DynTable = diesel_dynamic_schema::Table<String, String>;

/// One `WHERE` conjunct of the row lookup, boxed because the column's SQL type
/// is decided by the wire value at runtime.
type KeyPredicate = alloc::boxed::Box<
    dyn diesel::expression::BoxableExpression<
        DynTable,
        diesel::sqlite::Sqlite,
        SqlType = diesel::sql_types::Bool,
    >,
>;

/// Owned wire row image indexed by column ordinal. Result payload of
/// [`fetch_current_row`] and the `Some` shape of
/// [`super::PgSqliteEmuSource::fallback_row_for`].
pub(super) type FallbackRow = Vec<WireValue<String, Vec<u8>>>;

/// Fetch the current post-image of one row through SQLite. Used only
/// when the changeset carried `(None, None)` on some non-PK columns of
/// an UPDATE.
///
/// Built through diesel's dynamic query builder rather than assembled as text:
/// the table and column names come from the catalog, and diesel's
/// `push_identifier` is what escapes a name carrying its own delimiter. The key
/// values ride as binds for the same reason.
///
/// The row comes back through [`crate::diesel_decode::DynamicRow`], which reads
/// each field by its real SQLite storage class. The previous shape projected
/// `json_array(...)` and decoded the JSON, which could not represent a BLOB at
/// all: SQLite answers `json_array` over a BLOB column with "JSON cannot hold
/// BLOB values" and the whole read failed.
pub(super) fn fetch_current_row(
    connection: &mut SqliteConnection,
    meta: &TableMeta,
    pk_values: &[WireValue<String, Vec<u8>>],
) -> Result<FallbackRow, PgSqliteEmuError> {
    use diesel::prelude::*;
    use diesel::sql_types::Untyped;
    use diesel_dynamic_schema::DynamicSelectClause;

    if pk_values.len() != meta.pk_column_indices.len() {
        return Err(PgSqliteEmuError::UnknownTable(alloc::format!(
            "{} pk length mismatch: expected {}, got {}",
            meta.sqlite_table,
            meta.pk_column_indices.len(),
            pk_values.len()
        )));
    }

    let table: DynTable = diesel_dynamic_schema::table(meta.sqlite_table.clone());
    let mut projection = DynamicSelectClause::new();
    for column in &meta.columns {
        projection.add_field(table.column::<Untyped, _>(column.name.clone()));
    }

    let mut conjuncts = Vec::with_capacity(pk_values.len());
    for (&index, value) in meta.pk_column_indices.iter().zip(pk_values) {
        let name = meta.columns[index].name.clone();
        conjuncts.push(key_predicate(&table, name, value, &meta.sqlite_table)?);
    }
    // A row lookup with no key would match the whole table, so refuse rather
    // than read an arbitrary row.
    let Some(predicate) = conjuncts
        .into_iter()
        .reduce(|left, right| alloc::boxed::Box::new(left.and(right)))
    else {
        return Err(PgSqliteEmuError::UnknownTable(alloc::format!(
            "{} has no primary key to look a row up by",
            meta.sqlite_table
        )));
    };

    let row: crate::diesel_decode::DynamicRow<crate::backend::SQLite> = table
        .select(projection)
        .filter(predicate)
        .get_result(connection)?;

    row.values
        .into_iter()
        .map(|value| {
            wire_from_value(&value).ok_or_else(|| {
                PgSqliteEmuError::UnknownTable(alloc::format!(
                    "{} row lookup read a value SQLite has no storage class for: {value:?}",
                    meta.sqlite_table
                ))
            })
        })
        .collect()
}

/// `column = value`, with the column's SQL type chosen by the wire value.
///
/// A primary-key column is `NOT NULL`, so a null key is a contract violation
/// rather than an `IS NULL` to render.
pub(super) fn key_predicate(
    table: &DynTable,
    name: String,
    value: &WireValue<String, Vec<u8>>,
    table_name: &str,
) -> Result<KeyPredicate, PgSqliteEmuError> {
    use diesel::prelude::*;
    use diesel::sql_types::{BigInt, Binary, Double, Text};

    Ok(match value {
        WireValue::Integer(i) => alloc::boxed::Box::new(table.column::<BigInt, _>(name).eq(*i)),
        WireValue::Real(f) => alloc::boxed::Box::new(table.column::<Double, _>(name).eq(*f)),
        WireValue::Text(s) => alloc::boxed::Box::new(table.column::<Text, _>(name).eq(s.clone())),
        WireValue::Blob(b) => alloc::boxed::Box::new(table.column::<Binary, _>(name).eq(b.clone())),
        WireValue::Null => {
            return Err(PgSqliteEmuError::UnknownTable(alloc::format!(
                "{table_name} row lookup received a null primary-key value for {name}"
            )))
        }
    })
}

/// A decoded field as the pgoutput encoder's wire value.
///
/// Total over what SQLite can store, which is what
/// [`crate::diesel_decode::RowFieldDecode`] for SQLite produces. Any other
/// variant means the decode convention grew a shape this emulator has not been
/// taught, so it refuses rather than guessing.
fn wire_from_value(
    value: &crate::backend::Value<crate::backend::SQLite>,
) -> Option<WireValue<String, Vec<u8>>> {
    use crate::backend::Value;

    match value {
        Value::Null => Some(WireValue::Null),
        Value::Int(i) => Some(WireValue::Integer(*i)),
        Value::Float(f) => Some(WireValue::Real(*f)),
        Value::String(s) => Some(WireValue::Text(s.clone())),
        Value::Bytes(b) => Some(WireValue::Blob(b.clone())),
        _ => None,
    }
}
