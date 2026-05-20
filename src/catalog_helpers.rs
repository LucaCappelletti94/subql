//! Adapter helpers between subql's compact ID types and the `sql-traits`
//! schema model.
//!
//! subql's runtime works with `TableId = u32` and `ColumnId = u16` (compact
//! integers chosen for bytecode/bitmap density). `sql-traits` exposes
//! schemas through [`DatabaseLike`], [`TableLike`] and [`ColumnLike`], which
//! identify columns and tables via `usize`. These free functions perform
//! the `usize` ↔ `u32`/`u16` boundary conversion and centralize the
//! identifier-resolution rules so call sites never reinvent them.
//!
//! Functions: [`table_id`], [`column_id`], [`table_arity`],
//! [`schema_fingerprint`], [`primary_key_columns`], [`column_type`].

use alloc::vec::Vec;
use sql_traits::{
    prelude::{ColumnLike, DatabaseLike, TableLike},
    structs::{FingerprintError, SchemaFingerprint},
    utils::{
        fingerprint_type_token::canonical_type_token,
        identifier_resolution::stored_identifier_matches_lookup,
    },
};

use crate::types::{ColumnId, ColumnType, TableId};

/// Resolve a table name (unquoted or quoted form) to subql's compact
/// [`TableId`].
///
/// Accepts either a bare name (`orders`) or a schema-qualified name
/// (`public.orders`). Schema qualification is detected by the presence
/// of a single `.` separator outside of quotes; both halves are passed
/// to [`DatabaseLike::table`] unchanged. Note: the quote-detection is a
/// simple `.contains('"')` heuristic and does not handle escaped quotes
/// inside quoted identifiers (e.g. `"a""b".c`); callers parsing such
/// identifiers must pre-resolve them.
///
/// Returns `None` when the table is not found **or** when the database's
/// index for the table exceeds `u32::MAX` — the two cases are
/// indistinguishable to callers. The latter is effectively unreachable
/// with any realistic catalog (it implies > 4 billion declared tables),
/// but consumers writing against untrusted introspection sources should
/// be aware of the silent collapse.
#[must_use]
pub fn table_id<DB: DatabaseLike>(database: &DB, table_name: &str) -> Option<TableId> {
    // Heuristic split on the first unquoted '.' for schema-qualified names.
    // Names like `"a.b"` (a single quoted identifier containing a dot) are
    // preserved verbatim, since splitting them would change identifier
    // semantics. This matches what consumers of `resolve_table` expect.
    let (schema, bare) = if let Some((s, b)) = table_name.split_once('.') {
        if !s.is_empty() && !s.contains('"') && !b.contains('"') {
            (Some(s), b)
        } else {
            (None, table_name)
        }
    } else {
        (None, table_name)
    };
    let table = database.table(schema, bare)?;
    let id = database.table_id(table)?;
    u32::try_from(id).ok()
}

/// Resolve a column name within a table to subql's compact [`ColumnId`].
///
/// Identifier matching uses sql-traits' PostgreSQL semantics
/// (quoted/unquoted aware). Returns `None` when the table is unknown,
/// the column is not present, or the column's ordinal exceeds `u16::MAX`
/// (≥ 65536 columns in a single table — unreachable in any sane schema).
///
/// **Complexity**: O(n) per call where `n = table.number_of_columns()`.
/// Callers that need repeated lookups should cache results — this helper
/// performs a linear scan over the table's columns on every invocation.
#[must_use]
pub fn column_id<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_name: &str,
) -> Option<ColumnId> {
    let table = database.table_by_id(table_id as usize)?;
    let column = table.columns(database).find(|col| {
        stored_identifier_matches_lookup(
            col.column_name(),
            col.column_name_is_quoted(),
            column_name,
        )
    })?;
    let ordinal = column.column_id(database)?;
    u16::try_from(ordinal).ok()
}

/// Number of columns in the table.
#[must_use]
pub fn table_arity<DB: DatabaseLike>(database: &DB, table_id: TableId) -> Option<usize> {
    let table = database.table_by_id(table_id as usize)?;
    Some(table.number_of_columns(database))
}

/// Compute the spec-compliant [`SchemaFingerprint`] for the table.
///
/// Returns:
/// - `Ok(Some(fp))` when the table is known and its canonical model is
///   well-formed.
/// - `Ok(None)` when the table id is unknown to the database.
/// - `Err(FingerprintError)` when the table is known but its column model
///   is malformed (non-contiguous ordinals, duplicate or out-of-range
///   primary-key ordinals).
///
/// # Errors
///
/// See above — propagates the validation errors from
/// [`compute_persistence_v1`](sql_traits::structs::canonical_bytes_v1).
pub fn schema_fingerprint<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<Option<SchemaFingerprint>, FingerprintError> {
    let Some(table) = database.table_by_id(table_id as usize) else {
        return Ok(None);
    };
    table.schema_fingerprint(database).map(Some)
}

/// Primary-key column ordinals for the table, in declaration order.
///
/// Returns `None` when the table id is unknown. Returns `Some(vec![])`
/// for a table with no declared primary key. Columns whose ordinal
/// exceeds `u16::MAX` are silently skipped (extreme schemas only).
#[must_use]
pub fn primary_key_columns<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Option<Vec<ColumnId>> {
    let table = database.table_by_id(table_id as usize)?;
    Some(
        table
            .primary_key_columns(database)
            .filter_map(|col| col.column_id(database))
            .filter_map(|id| u16::try_from(id).ok())
            .collect(),
    )
}

/// Resolve a column's type into subql's [`ColumnType`] enum.
///
/// Maps the canonical sql-traits type token onto subql's coarse type
/// taxonomy (`Int`/`Float`/`Bool`/`String`/`Unknown`). Returns `None`
/// when the table or column id is unknown.
#[must_use]
pub fn column_type<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<ColumnType> {
    let table = database.table_by_id(table_id as usize)?;
    let column = table.column_by_id(column_id as usize, database)?;
    Some(column_type_from_token(
        canonical_type_token(column.data_type(database)).as_str(),
    ))
}

/// Map a canonical sql-traits type token onto subql's [`ColumnType`].
///
/// `INT` → `Int`, `FLOAT`/`DECIMAL` → `Float` (both are numeric for
/// SUM/AVG validation), `BOOL` → `Bool`, `STRING` → `String`; everything
/// else (`BYTES`, `DATE`, `TIME`, `TIMESTAMP`, `UUID`, `JSON`, `OTHER:*`)
/// maps to `Unknown` so SUM/AVG over such columns is permitted without
/// the engine asserting a numeric type.
fn column_type_from_token(token: &str) -> ColumnType {
    match token {
        "INT" => ColumnType::Int,
        "FLOAT" | "DECIMAL" => ColumnType::Float,
        "BOOL" => ColumnType::Bool,
        "STRING" => ColumnType::String,
        _ => ColumnType::Unknown,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::GenericDialect;

    fn make_db() -> ParserDB {
        ParserDB::parse::<GenericDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("DDL parses")
    }

    #[test]
    fn table_id_resolves_known_table() {
        let db = make_db();
        let tid = table_id(&db, "orders").expect("orders table exists");
        assert_eq!(table_arity(&db, tid), Some(3));
    }

    #[test]
    fn table_id_none_for_unknown_table() {
        let db = make_db();
        assert!(table_id(&db, "no_such_table").is_none());
    }

    #[test]
    fn column_id_resolves_each_column_to_its_ordinal() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert_eq!(column_id(&db, tid, "id"), Some(0));
        assert_eq!(column_id(&db, tid, "amount"), Some(1));
        assert_eq!(column_id(&db, tid, "status"), Some(2));
    }

    #[test]
    fn column_id_is_case_insensitive_for_unquoted_lookup() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert_eq!(column_id(&db, tid, "AMOUNT"), Some(1));
    }

    #[test]
    fn column_id_none_for_unknown_column() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert!(column_id(&db, tid, "nope").is_none());
    }

    #[test]
    fn primary_key_columns_returns_pk_ordinals() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert_eq!(primary_key_columns(&db, tid), Some(vec![0]));
    }

    #[test]
    fn primary_key_columns_empty_when_no_pk() {
        let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (a INT, b TEXT);")
            .expect("DDL parses");
        let tid = table_id(&db, "t").unwrap();
        assert_eq!(primary_key_columns(&db, tid), Some(vec![]));
    }

    #[test]
    fn column_type_maps_known_tokens() {
        let db = ParserDB::parse::<GenericDialect>(
            "CREATE TABLE t (i INT, f REAL, d DECIMAL, b BOOLEAN, s TEXT, j JSON);",
        )
        .expect("DDL parses");
        let tid = table_id(&db, "t").unwrap();
        assert_eq!(column_type(&db, tid, 0), Some(ColumnType::Int));
        assert_eq!(column_type(&db, tid, 1), Some(ColumnType::Float));
        assert_eq!(column_type(&db, tid, 2), Some(ColumnType::Float)); // DECIMAL → Float
        assert_eq!(column_type(&db, tid, 3), Some(ColumnType::Bool));
        assert_eq!(column_type(&db, tid, 4), Some(ColumnType::String));
        assert_eq!(column_type(&db, tid, 5), Some(ColumnType::Unknown)); // JSON
    }

    #[test]
    fn schema_fingerprint_round_trips_for_same_schema() {
        let db_a = make_db();
        let db_b = make_db();
        let tid_a = table_id(&db_a, "orders").unwrap();
        let tid_b = table_id(&db_b, "orders").unwrap();
        let fp_a = schema_fingerprint(&db_a, tid_a).unwrap().unwrap();
        let fp_b = schema_fingerprint(&db_b, tid_b).unwrap().unwrap();
        assert_eq!(fp_a, fp_b);
    }

    #[test]
    fn schema_fingerprint_differs_for_different_schemas() {
        let db_a = make_db();
        let db_b = ParserDB::parse::<GenericDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, total INT, status TEXT);",
        )
        .unwrap();
        let tid_a = table_id(&db_a, "orders").unwrap();
        let tid_b = table_id(&db_b, "orders").unwrap();
        let fp_a = schema_fingerprint(&db_a, tid_a).unwrap().unwrap();
        let fp_b = schema_fingerprint(&db_b, tid_b).unwrap().unwrap();
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn schema_fingerprint_none_for_unknown_table() {
        let db = make_db();
        assert!(schema_fingerprint(&db, 9_999).unwrap().is_none());
    }
}
