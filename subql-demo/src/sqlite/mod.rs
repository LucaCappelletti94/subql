//! SQL helpers for driving the in-browser sqlite through `SqliteCdcSource`.
//!
//! The demo no longer hand-rolls change capture. It builds DML statement
//! strings here and hands them to [`subql::SqliteCdcSource::execute`], which
//! captures every row mutation via triggers and surfaces real `WalEvent`s.

use subql::Cell;

/// Render a `Cell` as a sqlite literal.
pub fn cell_to_sql_literal(cell: &Cell) -> String {
    match cell {
        Cell::Missing | Cell::Null => "NULL".into(),
        Cell::Bool(b) => (if *b { "1" } else { "0" }).into(),
        Cell::Int(i) => i.to_string(),
        Cell::Float(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                "NULL".into()
            }
        }
        Cell::String(s) => format!("'{}'", s.replace('\'', "''")),
        // Cell is `#[non_exhaustive]`; render unknown variants as NULL.
        _ => "NULL".into(),
    }
}

/// `INSERT INTO <table> (<cols>) VALUES (<lits>)`.
#[must_use]
pub fn insert_sql(table: &str, columns: &[String], row: &[Cell]) -> String {
    let cols = columns.join(", ");
    let values = row
        .iter()
        .map(cell_to_sql_literal)
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} ({cols}) VALUES ({values})")
}

/// `UPDATE <table> SET <col = lit, ...> WHERE <pk_col> = <pk_lit>`.
#[must_use]
pub fn update_sql(
    table: &str,
    columns: &[String],
    pk_col: &str,
    pk_val: &Cell,
    new_row: &[Cell],
) -> String {
    let sets = columns
        .iter()
        .zip(new_row.iter())
        .map(|(c, v)| format!("{c} = {}", cell_to_sql_literal(v)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "UPDATE {table} SET {sets} WHERE {pk_col} = {}",
        cell_to_sql_literal(pk_val)
    )
}

/// `DELETE FROM <table> WHERE <pk_col> = <pk_lit>`.
#[must_use]
pub fn delete_sql(table: &str, pk_col: &str, pk_val: &Cell) -> String {
    format!(
        "DELETE FROM {table} WHERE {pk_col} = {}",
        cell_to_sql_literal(pk_val)
    )
}

/// `DELETE FROM <table>` (sqlite has no `TRUNCATE`; a whole-table delete is
/// captured by the per-row triggers, one event per row).
#[must_use]
pub fn truncate_sql(table: &str) -> String {
    format!("DELETE FROM {table}")
}
