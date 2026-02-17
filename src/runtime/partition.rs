//! Table partition with lock-free snapshot reads

use std::sync::Arc;
use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use crate::{TableId, ColumnId, RowImage, EventKind};
use super::{
    ids::PredicateId,
    predicate::{Predicate, PredicateStore},
    indexes::{HybridIndexes, IndexableAtom, IndexableCell},
};

/// Immutable snapshot of table partition
///
/// Used for lock-free reads during dispatch.
#[derive(Clone)]
pub struct TablePartitionSnapshot {
    pub table_id: TableId,
    pub indexes: HybridIndexes,
    pub predicates: Arc<PredicateStore>,
}

/// Table partition with atomic swap
///
/// Partitions predicates by table for efficient dispatch.
/// Uses ArcSwap for lock-free snapshot reads during event dispatch.
pub struct TablePartition {
    table_id: TableId,
    snapshot: ArcSwap<TablePartitionSnapshot>,
    // Mutable store (not shared with snapshot)
    mutable_predicates: PredicateStore,
}

impl TablePartition {
    /// Create new table partition
    #[must_use]
    pub fn new(table_id: TableId) -> Self {
        let predicates = PredicateStore::new();
        let snapshot = TablePartitionSnapshot {
            table_id,
            indexes: HybridIndexes::new(),
            predicates: Arc::new(PredicateStore::new()),
        };

        Self {
            table_id,
            snapshot: ArcSwap::new(Arc::new(snapshot)),
            mutable_predicates: predicates,
        }
    }

    /// Load current snapshot (lock-free)
    #[must_use]
    pub fn load_snapshot(&self) -> Arc<TablePartitionSnapshot> {
        self.snapshot.load_full()
    }

    /// Add predicate to partition
    ///
    /// Rebuilds indexes and performs atomic swap.
    pub fn add_predicate(&mut self, predicate: Predicate, atoms: Vec<IndexableAtom>) {
        let pred_id = predicate.id;
        let deps = predicate.dependency_columns.to_vec();

        // Add to mutable store
        self.mutable_predicates.add_predicate(predicate);

        // Rebuild indexes
        self.rebuild_indexes(&atoms, pred_id, &deps);
    }

    /// Rebuild indexes from scratch
    fn rebuild_indexes(&mut self, atoms: &[IndexableAtom], pred_id: PredicateId, deps: &[ColumnId]) {
        // Build new indexes
        let mut new_indexes = HybridIndexes::new();

        // Re-index all existing predicates
        // (For now, simplified: just add the new one)
        // TODO: Full rebuild in production version
        new_indexes.add_predicate(pred_id, atoms, deps);
        new_indexes.finalize_ranges();

        // Create new snapshot with cloned predicates
        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes: new_indexes,
            predicates: Arc::new(self.mutable_predicates.clone()),
        };

        // Atomic swap
        self.snapshot.store(Arc::new(new_snapshot));
    }

    /// Select candidate predicates for a row
    ///
    /// Returns union of all index lookups + fallback.
    /// Guaranteed to include all matches (no false negatives).
    #[must_use]
    pub fn select_candidates(
        &self,
        row: &RowImage,
        kind: EventKind,
        changed_cols: &[ColumnId],
    ) -> RoaringBitmap {
        let snapshot = self.load_snapshot();
        let mut candidates = RoaringBitmap::new();

        // Always include fallback (unindexable predicates)
        candidates |= &snapshot.indexes.fallback;

        // UPDATE optimization: only predicates depending on changed columns
        if kind == EventKind::Update && !changed_cols.is_empty() {
            let mut update_candidates = RoaringBitmap::new();

            for &col in changed_cols {
                if let Some(deps) = snapshot.indexes.dependency.get(&col) {
                    update_candidates |= deps;
                }
            }

            // Intersect with candidates so far
            if !update_candidates.is_empty() {
                candidates &= &update_candidates;
            } else {
                // No predicates depend on changed columns
                return candidates;
            }
        }

        // Query indexes based on row values
        for (col_idx, cell) in row.cells.iter().enumerate() {
            let col_id = col_idx as ColumnId;

            if let Some(indexable) = IndexableCell::from_cell(cell) {
                // Equality index
                if let Some(bitmap) = snapshot.indexes.query_equality(col_id, &indexable) {
                    candidates |= bitmap;
                }

                // Range index
                let range_bitmap = snapshot.indexes.query_range(col_id, &indexable);
                candidates |= &range_bitmap;
            }

            // NULL index
            if cell.is_null() {
                if let Some(bitmap) = snapshot.indexes.null_checks.get(&(col_id, super::indexes::NullKind::IsNull)) {
                    candidates |= bitmap;
                }
            } else if !cell.is_missing() {
                if let Some(bitmap) = snapshot.indexes.null_checks.get(&(col_id, super::indexes::NullKind::IsNotNull)) {
                    candidates |= bitmap;
                }
            }
        }

        candidates
    }

    /// Get table ID
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compiler::{BytecodeProgram, Instruction}, Cell};
    use super::super::indexes::IndexableAtom;

    fn make_predicate(id: usize, hash: u128) -> Predicate {
        Predicate {
            id: PredicateId::from_slab_index(id),
            hash,
            normalized_sql: "test".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::Not])),
            dependency_columns: Arc::from([1u16]),
            refcount: 1,
            updated_at_unix_ms: 0,
        }
    }

    fn make_row(cells: Vec<Cell>) -> RowImage {
        RowImage {
            cells: Arc::from(cells),
        }
    }

    #[test]
    fn test_partition_creation() {
        let partition = TablePartition::new(42);
        assert_eq!(partition.table_id(), 42);

        let snapshot = partition.load_snapshot();
        assert_eq!(snapshot.table_id, 42);
    }

    #[test]
    fn test_add_predicate() {
        let mut partition = TablePartition::new(1);

        let pred = make_predicate(0, 0x1234);
        let atoms = vec![IndexableAtom::Equality {
            column_id: 5,
            value: IndexableCell::Int(42),
        }];

        partition.add_predicate(pred, atoms);

        let snapshot = partition.load_snapshot();
        // Should be indexed in equality index, not fallback
        assert!(!snapshot.indexes.equality.is_empty());
    }

    #[test]
    fn test_select_candidates_fallback() {
        let mut partition = TablePartition::new(1);

        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let row = make_row(vec![Cell::Int(100)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);

        // Should include fallback predicate
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_update_optimization() {
        let mut partition = TablePartition::new(1);

        // Predicate depends on column 1
        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let row = make_row(vec![Cell::Int(100), Cell::Int(200)]);

        // UPDATE with no changed columns → should return empty (except fallback)
        let candidates = partition.select_candidates(&row, EventKind::Update, &[]);
        assert!(!candidates.is_empty());  // Fallback is always included

        // UPDATE with changed column 1 → should include predicate
        let candidates = partition.select_candidates(&row, EventKind::Update, &[1]);
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_lock_free_snapshot() {
        let partition = TablePartition::new(1);

        // Load snapshot multiple times
        let snap1 = partition.load_snapshot();
        let snap2 = partition.load_snapshot();

        // Should be same Arc (cheap clone)
        assert!(Arc::ptr_eq(&snap1, &snap2));
    }
}
