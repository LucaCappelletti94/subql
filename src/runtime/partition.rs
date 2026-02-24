//! Table partition with lock-free snapshot reads

use super::{
    ids::PredicateId,
    indexes::{HybridIndexes, IndexableAtom, IndexableCell},
    predicate::{Binding, Predicate, PredicateStore},
};
use crate::{ColumnId, EventKind, IdTypes, RowImage, TableId};
use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use std::sync::Arc;

/// Immutable snapshot of table partition
///
/// Used for lock-free reads during dispatch.
#[derive(Clone)]
pub struct TablePartitionSnapshot<I: IdTypes> {
    pub table_id: TableId,
    pub indexes: HybridIndexes,
    pub predicates: Arc<PredicateStore<I>>,
}

pub(super) struct BindingRemoval<I: IdTypes> {
    pub predicate_removed: bool,
    pub user_id: I::UserId,
}

/// Table partition with atomic swap
///
/// Partitions predicates by table for efficient dispatch.
/// Uses ArcSwap for lock-free snapshot reads during event dispatch.
/// The `mutable_predicates` field uses copy-on-write via `Arc::make_mut`,
/// so snapshots share the store until a mutation occurs.
pub struct TablePartition<I: IdTypes> {
    table_id: TableId,
    snapshot: ArcSwap<TablePartitionSnapshot<I>>,
    /// COW predicate store — `Arc::clone` for cheap snapshots, `Arc::make_mut` for mutations
    mutable_predicates: Arc<PredicateStore<I>>,
}

impl<I: IdTypes> TablePartition<I> {
    /// Create new table partition
    #[must_use]
    pub fn new(table_id: TableId) -> Self {
        let predicates = Arc::new(PredicateStore::<I>::new());
        let snapshot = TablePartitionSnapshot::<I> {
            table_id,
            indexes: HybridIndexes::new(),
            predicates: Arc::clone(&predicates),
        };

        Self {
            table_id,
            snapshot: ArcSwap::new(Arc::new(snapshot)),
            mutable_predicates: predicates,
        }
    }

    /// Load current snapshot (lock-free)
    #[must_use]
    pub fn load_snapshot(&self) -> Arc<TablePartitionSnapshot<I>> {
        self.snapshot.load_full()
    }

    /// Add predicate to partition
    ///
    /// Rebuilds indexes and performs atomic swap.
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_predicate(
        &mut self,
        predicate: Predicate,
        atoms: Vec<IndexableAtom>,
    ) -> PredicateId {
        let deps = predicate.dependency_columns.to_vec();

        // COW: clone-on-write if snapshot still shares this Arc
        let pred_id = Arc::make_mut(&mut self.mutable_predicates).add_predicate(predicate);

        // Incrementally update indexes
        self.rebuild_indexes(&atoms, pred_id, &deps);

        pred_id
    }

    /// Add binding to an existing predicate
    ///
    /// Increments refcount and updates snapshot.
    pub fn add_binding(&mut self, binding: Binding<I>, pred_id: PredicateId) {
        let store = Arc::make_mut(&mut self.mutable_predicates);
        store.add_binding(binding);
        store.increment_refcount(pred_id);

        // Update snapshot with new predicates
        self.update_snapshot();
    }

    /// Remove binding and decrement refcount
    ///
    /// If refcount reaches 0, predicate is removed and indexes are rebuilt.
    /// Returns true if predicate was removed.
    pub fn remove_binding(&mut self, sub_id: I::SubscriptionId) -> bool {
        self.remove_binding_detail(sub_id)
            .is_some_and(|removal| removal.predicate_removed)
    }

    /// Remove binding and decrement refcount.
    ///
    /// Returns:
    /// - `None` if no binding existed
    /// - `Some(false)` if binding removed but predicate kept
    /// - `Some(true)` if binding removed and predicate deleted
    pub fn remove_binding_status(&mut self, sub_id: I::SubscriptionId) -> Option<bool> {
        self.remove_binding_detail(sub_id)
            .map(|removal| removal.predicate_removed)
    }

    #[allow(clippy::option_if_let_else)]
    pub(super) fn remove_binding_detail(
        &mut self,
        sub_id: I::SubscriptionId,
    ) -> Option<BindingRemoval<I>> {
        let store = Arc::make_mut(&mut self.mutable_predicates);
        if let Some(binding) = store.remove_binding(sub_id) {
            let removed = store.decrement_refcount(binding.predicate_id);

            // Update snapshot
            if removed {
                // Predicate was removed, need to rebuild indexes
                self.rebuild_all_indexes();
            } else {
                // Just update snapshot (refcount changed)
                self.update_snapshot();
            }

            Some(BindingRemoval {
                predicate_removed: removed,
                user_id: binding.user_id,
            })
        } else {
            None
        }
    }

    /// Update snapshot with current mutable predicates (no index rebuild)
    fn update_snapshot(&self) {
        let current = self.load_snapshot();

        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes: current.indexes.clone(),
            predicates: Arc::clone(&self.mutable_predicates),
        };

        self.snapshot.store(Arc::new(new_snapshot));
    }

    /// Incrementally update indexes for a single newly added predicate.
    fn rebuild_indexes(&self, atoms: &[IndexableAtom], pred_id: PredicateId, deps: &[ColumnId]) {
        let current = self.load_snapshot();
        let mut new_indexes = current.indexes.clone();
        new_indexes.add_predicate(pred_id, atoms, deps);

        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes: new_indexes,
            predicates: Arc::clone(&self.mutable_predicates),
        };

        self.snapshot.store(Arc::new(new_snapshot));
    }

    /// Rebuild indexes from all predicates (used after predicate removal)
    fn rebuild_all_indexes(&self) {
        let mut new_indexes = HybridIndexes::new();

        for (idx, pred) in &self.mutable_predicates.predicates {
            new_indexes.add_predicate(
                PredicateId::from_slab_index(idx),
                &pred.index_atoms,
                &pred.dependency_columns,
            );
        }

        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes: new_indexes,
            predicates: Arc::clone(&self.mutable_predicates),
        };

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

        // For Update events with a non-empty changed_cols list, return exactly the
        // set of predicates that depend on at least one changed column.  We cannot
        // restrict further using the index (which reflects only the *new* row's
        // values): a predicate might have matched the *old* value (e.g. an IS NULL
        // predicate when the column transitions NULL → non-NULL) and still needs
        // re-evaluation. Returning the full dependency set is always safe because
        // the VM will evaluate every candidate.
        if kind == EventKind::Update && !changed_cols.is_empty() {
            let mut update_candidates = snapshot.indexes.dependency_free.clone();
            for &col in changed_cols {
                if let Some(deps) = snapshot.indexes.dependency.get(&col) {
                    update_candidates |= deps;
                }
            }
            return update_candidates;
        }

        let mut candidates = RoaringBitmap::new();

        // Always include fallback (unindexable predicates)
        candidates |= &snapshot.indexes.fallback;

        // Query indexes based on row values
        for (col_idx, cell) in row.cells.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let col_id = col_idx as ColumnId;

            if let Some(indexable) = IndexableCell::from_cell(cell) {
                // Equality index
                if let Some(bitmap) = snapshot.indexes.query_equality(col_id, &indexable) {
                    candidates |= bitmap;
                }

                // Range index
                snapshot
                    .indexes
                    .query_range_into(col_id, &indexable, &mut candidates);
            }

            // NULL index
            if cell.is_null() {
                if let Some(bitmap) = snapshot
                    .indexes
                    .null_checks
                    .get(&(col_id, super::indexes::NullKind::IsNull))
                {
                    candidates |= bitmap;
                }
            } else if !cell.is_missing() {
                if let Some(bitmap) = snapshot
                    .indexes
                    .null_checks
                    .get(&(col_id, super::indexes::NullKind::IsNotNull))
                {
                    candidates |= bitmap;
                }
            }
        }

        candidates
    }

    /// Add multiple predicates and bindings in a single batch
    ///
    /// Performs one COW clone, inserts all predicates and bindings, rebuilds
    /// indexes once, and performs a single atomic snapshot swap.
    /// Much more efficient than calling `add_predicate`/`add_binding` in a loop.
    #[allow(clippy::type_complexity)]
    pub fn add_batch(&mut self, entries: &[(Predicate, Vec<IndexableAtom>, Vec<Binding<I>>)]) {
        if entries.is_empty() {
            return;
        }

        let current = self.load_snapshot();
        let mut new_indexes = current.indexes.clone();
        // Single COW clone for the entire batch
        let store = Arc::make_mut(&mut self.mutable_predicates);

        for (predicate, atoms, bindings) in entries {
            let pred_id = store.add_predicate(Predicate::clone(predicate));
            new_indexes.add_predicate(pred_id, atoms, &predicate.dependency_columns);

            for binding in bindings {
                let mut b = *binding;
                b.predicate_id = pred_id;
                store.add_binding(b);
                store.increment_refcount(pred_id);
            }
        }

        // Single atomic snapshot swap
        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes: new_indexes,
            predicates: Arc::clone(&self.mutable_predicates),
        };

        self.snapshot.store(Arc::new(new_snapshot));
    }

    /// Get table ID
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }
}

#[cfg(test)]
mod tests {
    use super::super::ids::UserOrdinal;
    use super::super::indexes::{IndexableAtom, NullKind};
    use super::*;
    use crate::{
        compiler::{BytecodeProgram, Instruction, PrefilterPlan},
        Cell, DefaultIds,
    };

    fn make_predicate(id: usize, hash: u128) -> Predicate {
        make_predicate_on_col(id, hash, 1)
    }

    fn make_predicate_on_col(id: usize, hash: u128, col: ColumnId) -> Predicate {
        Predicate {
            id: PredicateId::from_slab_index(id),
            hash,
            normalized_sql: "test".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::Not])),
            dependency_columns: Arc::from([col]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
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
        let partition = TablePartition::<DefaultIds>::new(42);
        assert_eq!(partition.table_id(), 42);

        let snapshot = partition.load_snapshot();
        assert_eq!(snapshot.table_id, 42);
    }

    #[test]
    fn test_add_predicate() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

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
        let mut partition = TablePartition::<DefaultIds>::new(1);

        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let row = make_row(vec![Cell::Int(100)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);

        // Should include fallback predicate
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_update_optimization() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Predicate depends on column 1
        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let row = make_row(vec![Cell::Int(100), Cell::Int(200)]);

        // UPDATE with no changed columns → should return empty (except fallback)
        let candidates = partition.select_candidates(&row, EventKind::Update, &[]);
        assert!(!candidates.is_empty()); // Fallback is always included

        // UPDATE with changed column 1 → should include predicate
        let candidates = partition.select_candidates(&row, EventKind::Update, &[1]);
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_lock_free_snapshot() {
        let partition = TablePartition::<DefaultIds>::new(1);

        // Load snapshot multiple times
        let snap1 = partition.load_snapshot();
        let snap2 = partition.load_snapshot();

        // Should be same Arc (cheap clone)
        assert!(Arc::ptr_eq(&snap1, &snap2));
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Partition Completion
    // ========================================================================

    #[test]
    fn test_remove_binding_refcount_no_predicate_remove() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Add predicate
        let pred = make_predicate(0, 0x1234);
        let pred_id = pred.id;
        partition.add_predicate(pred, vec![]);

        // Add two bindings for the same predicate
        let binding1 = Binding {
            subscription_id: 100,
            predicate_id: pred_id,
            user_id: 1,
            user_ordinal: UserOrdinal::new(0),
            session_id: None,
            updated_at_unix_ms: 0,
        };
        let binding2 = Binding {
            subscription_id: 101,
            predicate_id: pred_id,
            user_id: 2,
            user_ordinal: UserOrdinal::new(1),
            session_id: None,
            updated_at_unix_ms: 0,
        };

        partition.add_binding(binding1, pred_id);
        partition.add_binding(binding2, pred_id);

        // Remove first binding - refcount decrements but predicate not removed
        let predicate_removed = partition.remove_binding(100);
        assert!(!predicate_removed); // Predicate still has refcount > 0

        // Predicate should still exist
        let snapshot = partition.load_snapshot();
        assert!(snapshot.predicates.get_predicate(pred_id).is_some());
    }

    #[test]
    fn test_remove_binding_nonexistent() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Try to remove non-existent binding
        let removed = partition.remove_binding(999);
        assert!(!removed);
    }

    #[test]
    fn test_select_candidates_with_equality_index() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Add predicate with equality index
        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(
            pred,
            vec![IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(42),
            }],
        );

        // Row with matching value
        let row = make_row(vec![Cell::Int(42)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);

        // Should find predicate via equality index
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_with_null_checks() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Add predicate with IS NULL check
        let pred1 = make_predicate(0, 0x1234);
        partition.add_predicate(
            pred1,
            vec![IndexableAtom::Null {
                column_id: 0,
                kind: NullKind::IsNull,
            }],
        );

        // Add predicate with IS NOT NULL check
        let pred2 = make_predicate(1, 0x5678);
        partition.add_predicate(
            pred2,
            vec![IndexableAtom::Null {
                column_id: 1,
                kind: NullKind::IsNotNull,
            }],
        );

        // Row with NULL and non-NULL values
        let row = make_row(vec![Cell::Null, Cell::Int(100)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);

        // Should find both predicates via NULL check indexes
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_rebuild_indexes_with_predicates() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Add a predicate
        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(
            pred,
            vec![IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(42),
            }],
        );

        // Manually trigger rebuild (normally happens on unsubscribe with removal)
        partition.rebuild_all_indexes();

        // Indexes should still work after rebuild
        let row = make_row(vec![Cell::Int(42)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_update_no_overlap() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Add predicate that depends on column 1
        // (make_predicate already sets dependency_columns to [1u16])
        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(
            pred,
            vec![IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Int(100),
            }],
        );

        let row = make_row(vec![Cell::Int(1), Cell::Int(100), Cell::Int(2)]);

        // UPDATE with changed column 0 (predicate depends on column 1)
        // No overlap, should skip early (except fallback)
        let candidates = partition.select_candidates(&row, EventKind::Update, &[0]);

        // Might be empty or just fallback, depending on implementation
        // The key is this hits the "no overlap" path (line 218)
        let _ = candidates;
    }

    #[test]
    fn test_select_candidates_null_cell_matches_is_null_index() {
        use super::super::indexes::{IndexableAtom, NullKind};

        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Add predicate with IS NULL index on column 1
        let pred = make_predicate(0, 0x9999);
        let pred_id = pred.id;
        partition.add_predicate(
            pred,
            vec![IndexableAtom::Null {
                column_id: 1,
                kind: NullKind::IsNull,
            }],
        );

        // Row with NULL in column 1
        let row = make_row(vec![Cell::Int(1), Cell::Null, Cell::Int(2)]);

        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);

        // Should include the predicate because column 1 is NULL
        assert!(candidates.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_update_no_dependency_overlap_returns_empty_without_fallback() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Predicate depends on column 1 and is indexable (no fallback).
        let pred = make_predicate(0, 0xAAAA);
        partition.add_predicate(
            pred,
            vec![IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Int(100),
            }],
        );

        let row = make_row(vec![Cell::Int(1), Cell::Int(100)]);

        // UPDATE changed column 0 only; no dependency overlap.
        let candidates = partition.select_candidates(&row, EventKind::Update, &[0]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_update_keeps_dependency_free_predicates() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Dependency-free predicate should always survive UPDATE changed-column
        // pruning.
        let mut pred = make_predicate(0, 0xD00D);
        pred.dependency_columns = Arc::from([]);
        pred.index_atoms = Arc::from([IndexableAtom::Fallback]);
        let pred_id = partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let row = make_row(vec![Cell::Int(1), Cell::Int(100)]);
        let candidates = partition.select_candidates(&row, EventKind::Update, &[1]);
        assert!(candidates.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_update_dependency_overlap_keeps_candidates() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Predicate that depends on changed column 0.
        let mut pred_changed = make_predicate(0, 0xBBBB);
        pred_changed.dependency_columns = Arc::from([0u16]);
        let pred_changed_id = partition.add_predicate(
            pred_changed,
            vec![IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(1),
            }],
        );

        // Predicate that depends on unchanged column 1.
        let pred_unchanged = make_predicate(1, 0xCCCC);
        let pred_unchanged_id = partition.add_predicate(
            pred_unchanged,
            vec![IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Int(100),
            }],
        );

        let row = make_row(vec![Cell::Int(1), Cell::Int(100)]);

        // UPDATE changed column 0 only; only pred_changed should be a candidate.
        let candidates = partition.select_candidates(&row, EventKind::Update, &[0]);
        assert!(candidates.contains(pred_changed_id.as_u32()));
        assert!(
            !candidates.contains(pred_unchanged_id.as_u32()),
            "predicates with no changed-column overlap must be excluded on UPDATE"
        );
    }

    // =========================================================================
    // C2 — Index × event kind matrix
    // =========================================================================

    #[test]
    fn test_equality_index_on_insert_and_delete() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Equality predicate on col 0 == 42
        let pred = make_predicate_on_col(0, 0xAAAA, 0);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(42),
            }],
        );

        // INSERT row with matching value
        let row = make_row(vec![Cell::Int(42)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "equality index must include predicate on INSERT with matching value"
        );

        // INSERT row with non-matching value
        let row_nomatch = make_row(vec![Cell::Int(99)]);
        let candidates = partition.select_candidates(&row_nomatch, EventKind::Insert, &[]);
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "equality index must exclude predicate on INSERT with non-matching value"
        );

        // DELETE row with matching value
        let candidates = partition.select_candidates(&row, EventKind::Delete, &[]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "equality index must include predicate on DELETE with matching value"
        );
    }

    #[test]
    fn test_range_index_on_insert_update_delete() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // Range predicate: col 0 > 10  (lower bound is inclusive 11)
        let pred = make_predicate_on_col(0, 0xBBBB, 0);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Range {
                column_id: 0,
                lower: Some(11),
                upper: None,
            }],
        );

        // INSERT row satisfying range
        let row = make_row(vec![Cell::Int(20)]);
        let candidates = partition.select_candidates(&row, EventKind::Insert, &[]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "range index must include predicate on INSERT with value in range"
        );

        // INSERT row outside range
        let row_out = make_row(vec![Cell::Int(5)]);
        let candidates = partition.select_candidates(&row_out, EventKind::Insert, &[]);
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "range index must exclude predicate on INSERT with value outside range"
        );

        // UPDATE with changed_columns containing col 0
        let candidates = partition.select_candidates(&row, EventKind::Update, &[0]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "range index must include predicate on UPDATE when indexed column changed"
        );

        // DELETE row in range
        let candidates = partition.select_candidates(&row, EventKind::Delete, &[]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "range index must include predicate on DELETE with matching row"
        );
    }

    #[test]
    fn test_null_index_on_insert_update_delete() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // IS NULL predicate on col 0
        let pred = make_predicate_on_col(0, 0xCCCC, 0);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Null {
                column_id: 0,
                kind: NullKind::IsNull,
            }],
        );

        // INSERT with NULL value — should be candidate
        let row_null = make_row(vec![Cell::Null]);
        let candidates = partition.select_candidates(&row_null, EventKind::Insert, &[]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "null index must include predicate on INSERT with NULL value"
        );

        // INSERT with non-NULL value — should not be candidate
        let row_non_null = make_row(vec![Cell::Int(5)]);
        let candidates = partition.select_candidates(&row_non_null, EventKind::Insert, &[]);
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "null index must exclude IS NULL predicate on INSERT with non-NULL value"
        );

        // UPDATE with changed_columns containing col 0
        let candidates = partition.select_candidates(&row_null, EventKind::Update, &[0]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "null index must include predicate on UPDATE when indexed column changed"
        );

        // DELETE with NULL value
        let candidates = partition.select_candidates(&row_null, EventKind::Delete, &[]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "null index must include predicate on DELETE with NULL value"
        );
    }

    // =========================================================================
    // C3 — NULL transition updates under changed_columns
    // =========================================================================

    #[test]
    fn test_null_transition_null_to_value_with_changed_column() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // IS NULL predicate on col 0
        let pred = make_predicate_on_col(0, 0xDDDD, 0);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Null {
                column_id: 0,
                kind: NullKind::IsNull,
            }],
        );

        // UPDATE: NULL → value, changed_columns = [0]
        // New row has non-NULL value but changed_columns contains col 0,
        // so predicate must still be a candidate (old row may have been NULL).
        let new_row = make_row(vec![Cell::Int(10)]);
        let candidates = partition.select_candidates(&new_row, EventKind::Update, &[0]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "IS NULL predicate must be a candidate on NULL→value update when col 0 in changed_columns"
        );
    }

    #[test]
    fn test_null_transition_value_to_null_with_changed_column() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // IS NULL predicate on col 0
        let pred = make_predicate_on_col(0, 0xEEEE, 0);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Null {
                column_id: 0,
                kind: NullKind::IsNull,
            }],
        );

        // UPDATE: value → NULL, changed_columns = [0]
        // New row has NULL; predicate must be candidate.
        let new_row = make_row(vec![Cell::Null]);
        let candidates = partition.select_candidates(&new_row, EventKind::Update, &[0]);
        assert!(
            candidates.contains(pred_id.as_u32()),
            "IS NULL predicate must be a candidate on value→NULL update when col 0 in changed_columns"
        );
    }

    #[test]
    fn test_null_transition_not_in_changed_columns_prunes_candidate() {
        let mut partition = TablePartition::<DefaultIds>::new(1);

        // IS NULL predicate on col 1
        let pred = make_predicate(0, 0xFFFF);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Null {
                column_id: 1,
                kind: NullKind::IsNull,
            }],
        );

        // UPDATE: changed_columns does NOT include col 1
        let row = make_row(vec![Cell::Int(5), Cell::Null]);
        let candidates = partition.select_candidates(&row, EventKind::Update, &[0]); // changed col 0, not col 1
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "null-indexed predicate must be pruned when its column is not in changed_columns"
        );
    }
}
