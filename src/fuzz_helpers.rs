//! Shared helpers for fuzzing harnesses

use crate::{TableId, ColumnId, SchemaCatalog};

/// Maximally permissive schema catalog for fuzzing.
///
/// Accepts any table/column name so the fuzzer can exercise deep code paths
/// without being rejected at schema resolution.
pub struct FuzzCatalog;

impl SchemaCatalog for FuzzCatalog {
    fn table_id(&self, _table_name: &str) -> Option<TableId> {
        Some(1)
    }

    fn column_id(&self, _table_id: TableId, column_name: &str) -> Option<ColumnId> {
        // Deterministic column ID derived from name (hash mod 64)
        let hash = column_name.bytes().fold(0u16, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(u16::from(b))
        });
        Some(hash % 64)
    }

    fn table_arity(&self, _table_id: TableId) -> Option<usize> {
        Some(64)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0xF022_F022_F022_F022)
    }
}
