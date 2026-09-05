//! Table metadata construction for [`super::PgSqliteEmuSource`].

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use hashbrown::{HashMap, HashSet};
use sql_traits::prelude::TableLike;
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::pg_walstream_reverse::Oid;

use super::super::error::PgSqliteEmuError;
use super::{ColumnMeta, TableMeta};
use crate::backend::ScalarFamily;
use crate::{catalog_helpers, TableId};

/// Build the per-table metadata map from the Postgres catalog.
pub(super) fn build_table_meta(
    pg_catalog: &ParserDB,
) -> Result<HashMap<String, TableMeta>, PgSqliteEmuError> {
    use sql_traits::prelude::{ColumnLike, DatabaseLike};

    let mut out = HashMap::new();
    let n_tables = pg_catalog.number_of_tables();
    for idx in 0..n_tables {
        let table = pg_catalog
            .table_by_id(idx)
            .ok_or_else(|| PgSqliteEmuError::UnknownTable(alloc::format!("id {idx}")))?;
        let table_id: TableId = u32::try_from(idx).map_err(|_| {
            PgSqliteEmuError::UnknownTable(alloc::format!("id {idx} exceeds TableId space"))
        })?;
        let name = table.table_name().to_string();
        let arity = catalog_helpers::table_arity(pg_catalog, table_id).map_err(|error| {
            PgSqliteEmuError::Catalog(alloc::string::ToString::to_string(&error))
        })?;
        let pk_cols: HashSet<crate::ColumnId> =
            catalog_helpers::primary_key_columns(pg_catalog, table_id)
                .map_err(|error| {
                    PgSqliteEmuError::Catalog(alloc::string::ToString::to_string(&error))
                })?
                .into_iter()
                .collect();

        let mut columns: Vec<ColumnMeta> = Vec::with_capacity(arity);
        let mut pk_column_indices: Vec<usize> = Vec::new();
        let mut column_iter = table.columns(pg_catalog).map_err(|error| {
            PgSqliteEmuError::UnknownTable(alloc::format!(
                "table {name} columns unavailable from catalog: {error}"
            ))
        })?;
        for col_idx in 0..arity {
            let column_id = u16::try_from(col_idx).map_err(|_| {
                PgSqliteEmuError::UnknownTable(alloc::format!(
                    "column id {col_idx} exceeds ColumnId space"
                ))
            })?;
            let column = column_iter.next().ok_or_else(|| {
                PgSqliteEmuError::UnknownTable(alloc::format!(
                    "table {name} column index {col_idx} missing from catalog"
                ))
            })?;
            let column_name = column.column_name().to_string();
            let scalar_kind =
                catalog_helpers::column_scalar_family(pg_catalog, table_id, column_id);
            let pg_type_oid = pg_type_oid_for_kind(scalar_kind);
            let is_pk = pk_cols.contains(&column_id);
            if is_pk {
                pk_column_indices.push(col_idx);
            }
            columns.push(ColumnMeta {
                name: column_name,
                pg_type_oid,
                is_pk,
            });
        }

        out.insert(
            name.clone(),
            TableMeta {
                oid: synth_oid(table_id),
                sqlite_table: name,
                columns,
                pk_column_indices,
            },
        );
    }
    Ok(out)
}

/// Synthetic `pgoutput` relation id derived from subql's compact
/// [`TableId`]. Add `1_000` so the OID stays clear of the tiny values
/// PostgreSQL reserves for its own catalog rows.
const fn synth_oid(table_id: TableId) -> Oid {
    1_000 + table_id
}

/// Map subql's [`ScalarFamily`] to a PostgreSQL type OID for the encoded
/// `pgoutput` relation message. The OID labels the column on the wire,
/// while the engine decodes each cell against the catalog scalar kind.
/// Unknown or composite columns fall back to `TEXT` (25).
const fn pg_type_oid_for_kind(kind: Option<ScalarFamily>) -> Oid {
    match kind {
        Some(ScalarFamily::Bool) => 16,
        Some(ScalarFamily::Int) => 20,
        Some(ScalarFamily::Float) => 701,
        Some(ScalarFamily::Bytes) => 17,
        Some(ScalarFamily::Uuid) => 2950,
        Some(ScalarFamily::Timestamp) => 1114,
        Some(ScalarFamily::TimestampTz) => 1184,
        Some(ScalarFamily::Date) => 1082,
        Some(ScalarFamily::Time) => 1083,
        Some(ScalarFamily::Decimal) => 1700,
        Some(ScalarFamily::Json) => 114,
        Some(ScalarFamily::Jsonb) => 3802,
        Some(ScalarFamily::String) | None => 25,
    }
}
