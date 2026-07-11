//! Adapter helpers between subql's compact ID types and the `sql-traits`
//! schema model.
//!
//! subql's runtime works with `TableId = u32` and `ColumnId = u16` (compact
//! integers chosen for bytecode/bitmap density). `sql-traits` exposes
//! schemas through [`DatabaseLike`], [`TableLike`] and [`ColumnLike`], which
//! identify columns and tables via `usize`. These free functions perform
//! the `usize` <-> `u32`/`u16` boundary conversion and centralize the
//! identifier-resolution rules so call sites never reinvent them.
//!
//! Functions: [`table_id`], [`column_id`], [`resolve_table`], [`table_arity`],
//! [`schema_fingerprint`], [`primary_key_columns`], [`column_scalar_kind`],
//! [`table_has_rls`].

use alloc::vec::Vec;
use sql_traits::{
    prelude::{ColumnLike, DatabaseLike, TableLike},
    structs::{FingerprintError, SchemaFingerprint},
    utils::{
        fingerprint_type_token::canonical_type_token,
        identifier_resolution::stored_identifier_matches_lookup,
    },
};

use crate::backend::ScalarKind;
use crate::types::{ColumnId, TableId};

/// Resolve a table name (unquoted or quoted form) to subql's compact
/// [`TableId`].
///
/// Accepts either a bare name (`orders`) or a schema-qualified name
/// (`public.orders`). Schema qualification is detected by the presence
/// of a single `.` separator outside of quotes. Both halves are passed
/// to [`DatabaseLike::table`] unchanged. Note: the quote-detection is a
/// simple `.contains('"')` heuristic and does not handle escaped quotes
/// inside quoted identifiers (e.g. `"a""b".c`). Callers parsing such
/// identifiers must pre-resolve them.
///
/// Returns `None` when the table is not found, or when the database's
/// index for the table exceeds `u32::MAX`. The two cases are
/// indistinguishable to callers. The overflow case is effectively
/// unreachable (it implies > 4 billion declared tables) but can collapse
/// silently when introspecting untrusted sources.
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
/// (>= 65536 columns in a single table, unreachable in any sane schema).
///
/// **Complexity**: O(n) per call where `n = table.number_of_columns()`.
/// Callers that need repeated lookups should cache results: this helper
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
/// See above. Propagates the validation errors from
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

/// Resolve a column's stored name from its compact [`ColumnId`].
///
/// The inverse of [`column_id`]. Returns `None` when the table or column id is
/// unknown. O(n) over the table's columns.
#[must_use]
pub fn column_name<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<alloc::string::String> {
    use alloc::string::ToString;
    let table = database.table_by_id(table_id as usize)?;
    table
        .columns(database)
        .find(|col| col.column_id(database) == Some(column_id as usize))
        .map(|col| col.column_name().to_string())
}

/// A table resolved to subql's compact ids, returned by [`resolve_table`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedTable {
    /// Compact id of the table.
    pub table_id: TableId,
    /// Ids of the requested columns, in the order their names were passed.
    pub column_ids: Vec<ColumnId>,
    /// Primary-key column ids, in declaration order (empty when none).
    pub primary_key: Vec<ColumnId>,
}

/// Resolve a table and a set of its columns in one call.
///
/// Bundles [`table_id`], a [`column_id`] lookup per name, and
/// [`primary_key_columns`]. Returns `None` if the table or any named column is
/// not found.
///
/// ```
/// use sql_traits::structs::ParserDB;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use subql::catalog_helpers;
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
/// )?;
/// let t = catalog_helpers::resolve_table(&db, "orders", &["id", "amount", "status"]).unwrap();
/// assert_eq!(t.column_ids, vec![0, 1, 2]);
/// assert_eq!(t.primary_key, vec![0]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[must_use]
pub fn resolve_table<DB: DatabaseLike, S: AsRef<str>>(
    database: &DB,
    table_name: &str,
    columns: &[S],
) -> Option<ResolvedTable> {
    let table_id = table_id(database, table_name)?;
    let mut column_ids = Vec::with_capacity(columns.len());
    for name in columns {
        column_ids.push(column_id(database, table_id, name.as_ref())?);
    }
    let primary_key = primary_key_columns(database, table_id)?;
    Some(ResolvedTable {
        table_id,
        column_ids,
        primary_key,
    })
}

/// Resolve a column's declared SQL type into a backend-neutral
/// [`ScalarKind`].
///
/// Distinguishes every scalar subql cares about (`JSONB` vs `JSON`,
/// `TIMESTAMPTZ` vs `TIMESTAMP`, and so on) via
/// [`canonical_type_token`](sql_traits::utils::fingerprint_type_token).
/// The compiler tags every
/// [`crate::compiler::Instruction::LoadColumn`] instruction with the
/// result.
///
/// Returns `None` when the table / column id is unknown or when the
/// declared type doesn't match any supported scalar (compiler surfaces
/// this as [`crate::RegisterError::UnsupportedSql`]).
#[must_use]
pub fn column_scalar_kind<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<ScalarKind> {
    let table = database.table_by_id(table_id as usize)?;
    let column = table.column_by_id(column_id as usize, database)?;
    scalar_kind_from_raw(column.data_type(database))
}

/// Map a raw SQL declared type string to its [`ScalarKind`] via
/// [`canonical_type_token`](sql_traits::utils::fingerprint_type_token::canonical_type_token).
fn scalar_kind_from_raw(raw: &str) -> Option<ScalarKind> {
    match canonical_type_token(raw).as_str() {
        "INT" => Some(ScalarKind::Int),
        "FLOAT" => Some(ScalarKind::Float),
        "DECIMAL" => Some(ScalarKind::Decimal),
        "BOOL" => Some(ScalarKind::Bool),
        "STRING" => Some(ScalarKind::String),
        "BYTES" => Some(ScalarKind::Bytes),
        "UUID" => Some(ScalarKind::Uuid),
        "DATE" => Some(ScalarKind::Date),
        "TIME" => Some(ScalarKind::Time),
        "TIMESTAMP" => Some(ScalarKind::Timestamp),
        "TIMESTAMPTZ" => Some(ScalarKind::TimestampTz),
        "JSON" => Some(ScalarKind::Json),
        "JSONB" => Some(ScalarKind::Jsonb),
        _ => None,
    }
}

/// Whether the table has row-level security enabled (per
/// [`TableLike::has_row_level_security`]).
///
/// Returns `None` when the table id is unknown. The reexec wrapper consults
/// this when classifying aggregator queries: under RLS, different viewers
/// observe different result rows, so a single in-process IVM state would be
/// unsafe to share across consumers. The wrapper rejects such registrations
/// until per-consumer total re-execution lands.
#[must_use]
pub fn table_has_rls<DB: DatabaseLike>(database: &DB, table_id: TableId) -> Option<bool> {
    let table = database.table_by_id(table_id as usize)?;
    Some(table.has_row_level_security(database))
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
    #[test]
    fn scalar_kind_from_raw_distinguishes_timestamp_variants() {
        assert_eq!(
            scalar_kind_from_raw("TIMESTAMP"),
            Some(ScalarKind::Timestamp)
        );
        assert_eq!(
            scalar_kind_from_raw("TIMESTAMPTZ"),
            Some(ScalarKind::TimestampTz)
        );
        assert_eq!(
            scalar_kind_from_raw("TIMESTAMP WITH TIME ZONE"),
            Some(ScalarKind::TimestampTz)
        );
        // Case-insensitive.
        assert_eq!(
            scalar_kind_from_raw("timestamptz"),
            Some(ScalarKind::TimestampTz)
        );
    }

    #[test]
    fn scalar_kind_from_raw_distinguishes_json_variants() {
        assert_eq!(scalar_kind_from_raw("JSON"), Some(ScalarKind::Json));
        assert_eq!(scalar_kind_from_raw("JSONB"), Some(ScalarKind::Jsonb));
        assert_eq!(scalar_kind_from_raw("jsonb"), Some(ScalarKind::Jsonb));
    }

    #[test]
    fn scalar_kind_from_raw_maps_canonical_tokens() {
        // Every string here mirrors what `normalize_sqlparser_type`
        // hands back for the corresponding `sqlparser::ast::DataType`
        // (parens already stripped). Values that are not one of the
        // canonical spellings fall through to `OTHER:...` upstream and
        // resolve to `None` here.
        assert_eq!(scalar_kind_from_raw("BIGINT"), Some(ScalarKind::Int));
        assert_eq!(
            scalar_kind_from_raw("DOUBLE PRECISION"),
            Some(ScalarKind::Float)
        );
        assert_eq!(scalar_kind_from_raw("NUMERIC"), Some(ScalarKind::Decimal));
        assert_eq!(scalar_kind_from_raw("BOOLEAN"), Some(ScalarKind::Bool));
        assert_eq!(scalar_kind_from_raw("VARCHAR"), Some(ScalarKind::String));
        assert_eq!(scalar_kind_from_raw("BYTEA"), Some(ScalarKind::Bytes));
        assert_eq!(scalar_kind_from_raw("UUID"), Some(ScalarKind::Uuid));
        assert_eq!(scalar_kind_from_raw("DATE"), Some(ScalarKind::Date));
        assert_eq!(scalar_kind_from_raw("TIME"), Some(ScalarKind::Time));
    }

    #[test]
    fn scalar_kind_from_raw_returns_none_for_unknown() {
        assert_eq!(scalar_kind_from_raw("SOME_UNKNOWN_TYPE"), None);
    }
}
