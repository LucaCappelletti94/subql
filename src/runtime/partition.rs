//! Table partition with lock-free snapshot reads

use super::{
    ids::PredicateId,
    indexes::{HybridIndexes, IndexableAtom, IndexableCell},
    predicate::{Predicate, PredicateStore, SubscriptionBinding},
};
use crate::backend::Backend;
use crate::{
    compiler::sql_shape::QueryProjection, ColumnId, EventKind, IdTypes, SubscriptionId, TableId,
};
use alloc::sync::Arc;
use alloc::vec::Vec;
use arc_swap::ArcSwap;
use roaring::RoaringBitmap;

/// Three-valued presence flag reported by
/// [`TablePartition::select_candidates`]'s column-probe closure.
///
/// Names whether a probed cell was missing, null, or present, without a
/// payload: the caller conveys the payload via the paired
/// [`ColumnProbe::value`] field.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CellPresence {
    /// The source's row image did not carry this cell.
    Missing,
    /// The cell carries SQL `NULL`.
    Null,
    /// The cell carries a value (see [`ColumnProbe::value`]).
    Present,
    /// The source carried the cell but subql could not decode it to its
    /// declared type. The prefilter cannot index it, so every predicate
    /// depending on the column is selected as a candidate and the VM
    /// surfaces the decode error.
    Undecodable,
}

/// Column probe result yielded by
/// [`TablePartition::select_candidates`]'s caller-supplied closure.
///
/// Combines a presence flag with an optional indexable payload. Only
/// cells whose scalar payload downcasts to one of the four indexable
/// primitives (`bool` / `i64` / `f64` / `String`) carry a `value`.
#[derive(Clone, Debug)]
pub struct ColumnProbe {
    /// Whether the cell was `Missing`, `Null`, or `Present`.
    pub presence: CellPresence,
    /// The indexable payload if the cell is present and downcasts to a
    /// primitive supported by the equality / range indexes.
    pub value: Option<IndexableCell>,
}

impl ColumnProbe {
    /// Convenience: a `Missing` cell with no value.
    #[must_use]
    pub const fn missing() -> Self {
        Self {
            presence: CellPresence::Missing,
            value: None,
        }
    }

    /// Convenience: a `Null` cell with no value.
    #[must_use]
    pub const fn null() -> Self {
        Self {
            presence: CellPresence::Null,
            value: None,
        }
    }

    /// Convenience: a `Present` cell. `value` may still be `None` when
    /// the scalar payload does not downcast to an indexable primitive
    /// (e.g. a UUID or JSONB cell on Postgres).
    #[must_use]
    pub const fn present(value: Option<IndexableCell>) -> Self {
        Self {
            presence: CellPresence::Present,
            value,
        }
    }

    /// Convenience: an `Undecodable` cell. The prefilter selects every
    /// predicate depending on this column so the VM re-reads and errors.
    #[must_use]
    pub const fn undecodable() -> Self {
        Self {
            presence: CellPresence::Undecodable,
            value: None,
        }
    }
}

/// Immutable snapshot of table partition
///
/// Used for lock-free reads during dispatch.
#[derive(Clone)]
pub struct TablePartitionSnapshot<I: IdTypes, B: Backend> {
    pub table_id: TableId,
    pub indexes: HybridIndexes,
    pub predicates: Arc<PredicateStore<I, B>>,
}

pub(super) struct BindingRemoval<I: IdTypes> {
    pub predicate_removed: bool,
    pub consumer_id: I::ConsumerId,
}

/// Table partition with atomic swap
///
/// Partitions predicates by table for efficient dispatch.
/// Uses ArcSwap for lock-free snapshot reads during event dispatch.
/// The `mutable_predicates` field uses copy-on-write via `Arc::make_mut`,
/// so snapshots share the store until a mutation occurs.
pub struct TablePartition<I: IdTypes, B: Backend> {
    table_id: TableId,
    snapshot: ArcSwap<TablePartitionSnapshot<I, B>>,
    /// COW predicate store: `Arc::clone` for cheap snapshots, `Arc::make_mut` for mutations.
    mutable_predicates: Arc<PredicateStore<I, B>>,
}

impl<I: IdTypes, B: Backend> TablePartition<I, B> {
    /// Create new table partition
    #[must_use]
    pub fn new(table_id: TableId) -> Self {
        let predicates = Arc::new(PredicateStore::<I, B>::new());
        let snapshot = TablePartitionSnapshot::<I, B> {
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
    pub fn load_snapshot(&self) -> Arc<TablePartitionSnapshot<I, B>> {
        self.snapshot.load_full()
    }

    /// Add predicate to partition
    ///
    /// Rebuilds indexes and performs atomic swap.
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_predicate(
        &mut self,
        predicate: Predicate<B>,
        atoms: Vec<IndexableAtom>,
    ) -> PredicateId {
        let deps = predicate.dependency_columns.to_vec();
        let projection = predicate.projection.clone();

        // COW: clone-on-write if snapshot still shares this Arc
        let pred_id = Arc::make_mut(&mut self.mutable_predicates).add_predicate(predicate);

        // Incrementally update indexes
        self.rebuild_indexes(&atoms, pred_id, &deps, &projection);

        pred_id
    }

    /// Add binding to an existing predicate
    ///
    /// Increments refcount and updates snapshot.
    pub fn add_binding(&mut self, binding: SubscriptionBinding<I>, pred_id: PredicateId) {
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
    pub fn remove_binding(&mut self, sub_id: SubscriptionId) -> bool {
        self.remove_binding_detail(sub_id)
            .is_some_and(|removal| removal.predicate_removed)
    }

    /// Remove binding and decrement refcount.
    ///
    /// Returns:
    /// - `None` if no binding existed
    /// - `Some(false)` if binding removed but predicate kept
    /// - `Some(true)` if binding removed and predicate deleted
    pub fn remove_binding_status(&mut self, sub_id: SubscriptionId) -> Option<bool> {
        self.remove_binding_detail(sub_id)
            .map(|removal| removal.predicate_removed)
    }

    #[allow(clippy::option_if_let_else)]
    pub(super) fn remove_binding_detail(
        &mut self,
        sub_id: SubscriptionId,
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
                consumer_id: binding.consumer_id,
            })
        } else {
            None
        }
    }

    /// Update snapshot with current mutable predicates (no index rebuild)
    fn store_snapshot_with_indexes(&self, indexes: HybridIndexes) {
        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes,
            predicates: Arc::clone(&self.mutable_predicates),
        };

        self.snapshot.store(Arc::new(new_snapshot));
    }

    /// Update snapshot with current mutable predicates (no index rebuild)
    fn update_snapshot(&self) {
        let current = self.load_snapshot();
        self.store_snapshot_with_indexes(current.indexes.clone());
    }

    /// Incrementally update indexes for a single newly added predicate.
    fn rebuild_indexes(
        &self,
        atoms: &[IndexableAtom],
        pred_id: PredicateId,
        deps: &[ColumnId],
        projection: &QueryProjection,
    ) {
        let current = self.load_snapshot();
        let mut new_indexes = current.indexes.clone();
        new_indexes.add_predicate(pred_id, atoms, deps, projection);
        self.store_snapshot_with_indexes(new_indexes);
    }

    /// Rebuild indexes from all predicates (used after predicate removal)
    fn rebuild_all_indexes(&self) {
        let mut new_indexes = HybridIndexes::new();

        for (idx, pred) in &self.mutable_predicates.predicates {
            new_indexes.add_predicate(
                PredicateId::from_slab_index(idx),
                &pred.index_atoms,
                &pred.dependency_columns,
                &pred.projection,
            );
        }

        self.store_snapshot_with_indexes(new_indexes);
    }

    /// Select candidate predicates for one row image, as INSERT and DELETE
    /// dispatch do.
    ///
    /// The `read_col` closure yields the current column value as an
    /// [`IndexableCell`] (or `None` for `Null` / `Missing`), together
    /// with the null flag. Callers derived from a
    /// [`crate::backend::CdcEvent`] compose this closure by iterating
    /// column ids and dispatching on their pre-cached
    /// [`crate::backend::ScalarKind`]. Returns the union of all index
    /// lookups plus the fallback set (predicates that can't be indexed).
    ///
    /// Guaranteed no false negatives: every predicate that could match
    /// the row is in the returned bitmap.
    ///
    /// An UPDATE is dispatched against two row images, so probing one of them
    /// cannot select its candidates: a predicate the new image fails may be one
    /// the old image matched. Use
    /// [`select_update_candidates`](Self::select_update_candidates).
    #[must_use]
    pub fn select_candidates(
        &self,
        arity: usize,
        mut read_col: impl FnMut(ColumnId) -> ColumnProbe,
    ) -> RoaringBitmap {
        let snapshot = self.load_snapshot();

        let mut candidates = RoaringBitmap::new();

        // Always include fallback (unindexable predicates).
        candidates |= &snapshot.indexes.fallback;

        for col_idx in 0..arity {
            #[allow(clippy::cast_possible_truncation)]
            let col_id = col_idx as ColumnId;
            let probe = read_col(col_id);

            if let Some(indexable) = probe.value.as_ref() {
                if let Some(bitmap) = snapshot.indexes.query_equality(col_id, indexable) {
                    candidates |= bitmap;
                }
                snapshot
                    .indexes
                    .query_range_into(col_id, indexable, &mut candidates);
            }

            match probe.presence {
                CellPresence::Null => {
                    if let Some(bitmap) = snapshot
                        .indexes
                        .null_checks
                        .get(&(col_id, super::indexes::NullKind::IsNull))
                    {
                        candidates |= bitmap;
                    }
                }
                CellPresence::Present => {
                    if let Some(bitmap) = snapshot
                        .indexes
                        .null_checks
                        .get(&(col_id, super::indexes::NullKind::IsNotNull))
                    {
                        candidates |= bitmap;
                    }
                }
                CellPresence::Missing => {}
                CellPresence::Undecodable => {
                    if let Some(deps) = snapshot.indexes.dependency.get(&col_id) {
                        candidates |= deps;
                    }
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
    pub fn add_batch(
        &mut self,
        entries: &[(
            Predicate<B>,
            Vec<IndexableAtom>,
            Vec<SubscriptionBinding<I>>,
        )],
    ) {
        if entries.is_empty() {
            return;
        }

        let current = self.load_snapshot();
        let mut new_indexes = current.indexes.clone();
        // Single COW clone for the entire batch
        let store = Arc::make_mut(&mut self.mutable_predicates);

        for (predicate, atoms, bindings) in entries {
            let pred_id = store.add_predicate(Predicate::clone(predicate));
            new_indexes.add_predicate(
                pred_id,
                atoms,
                &predicate.dependency_columns,
                &predicate.projection,
            );

            for binding in bindings {
                let mut b = *binding;
                b.predicate_id = pred_id;
                store.add_binding(b);
                store.increment_refcount(pred_id);
            }
        }

        // Single atomic snapshot swap
        self.store_snapshot_with_indexes(new_indexes);
    }

    /// Select candidate row predicates for an UPDATE.
    ///
    /// Delegates to [`HybridIndexes::select_update_candidates`], which explains
    /// why no row image and no changed-column list enter into it.
    #[must_use]
    pub fn select_update_candidates(&self) -> RoaringBitmap {
        self.load_snapshot().indexes.select_update_candidates()
    }

    /// Select candidate agg predicates for an event.
    ///
    /// Delegates to `HybridIndexes::select_agg_candidates`.
    #[must_use]
    pub fn select_agg_candidates(
        &self,
        kind: EventKind,
        changed_cols: &[ColumnId],
    ) -> roaring::RoaringBitmap {
        let snapshot = self.load_snapshot();
        snapshot.indexes.select_agg_candidates(kind, changed_cols)
    }

    /// Get table ID
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }
}

#[cfg(test)]
mod tests {
    use super::super::ids::ConsumerOrdinal;
    use super::super::indexes::{IndexableAtom, NullKind};
    use super::*;
    use crate::backend::{Postgres, Value};
    use crate::{
        compiler::{BytecodeProgram, Instruction, PrefilterPlan},
        DefaultIds, SubscriptionScope,
    };

    fn make_predicate(id: usize, hash: u128) -> Predicate<Postgres> {
        make_predicate_on_col(id, hash, 1)
    }

    fn make_predicate_on_col(id: usize, hash: u128, col: ColumnId) -> Predicate<Postgres> {
        use crate::compiler::sql_shape::QueryProjection;
        Predicate {
            id: PredicateId::from_slab_index(id),
            hash,
            normalized_sql: "test".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::Not])),
            dependency_columns: Arc::from([col]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            projection: QueryProjection::Rows,
            refcount: 1,
            updated_at_unix_ms: 0,
        }
    }

    /// `SUM(col)` over a WHERE clause reading `col`. Aggregates are the
    /// projection whose candidate set the changed-column prune narrows: their
    /// value moves only when a column they read moves.
    fn make_agg_predicate_on_col(id: usize, hash: u128, col: ColumnId) -> Predicate<Postgres> {
        use crate::compiler::sql_shape::{AggSpec, QueryProjection};
        Predicate {
            projection: QueryProjection::Aggregate(AggSpec::Sum { column: col }),
            ..make_predicate_on_col(id, hash, col)
        }
    }

    fn make_row(cells: Vec<Value<Postgres>>) -> Vec<Value<Postgres>> {
        cells
    }

    fn probe_from_row(row: &[Value<Postgres>]) -> impl FnMut(ColumnId) -> ColumnProbe + '_ {
        move |col: ColumnId| match row.get(col as usize) {
            None | Some(Value::Missing) => ColumnProbe::missing(),
            Some(Value::Null) => ColumnProbe::null(),
            Some(v) => ColumnProbe::present(super::super::indexes::IndexableCell::from_value(v)),
        }
    }

    #[test]
    fn test_partition_creation() {
        let partition = TablePartition::<DefaultIds, Postgres>::new(42);
        assert_eq!(partition.table_id(), 42);

        let snapshot = partition.load_snapshot();
        assert_eq!(snapshot.table_id, 42);
    }

    #[test]
    fn test_add_predicate() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        let pred = make_predicate(0, 0x1234);
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let row = make_row(vec![Value::<Postgres>::Int(100)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));

        // Should include fallback predicate
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_lock_free_snapshot() {
        let partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Load snapshot multiple times
        let snap1 = partition.load_snapshot();
        let snap2 = partition.load_snapshot();

        // Should be same Arc (cheap clone)
        assert!(Arc::ptr_eq(&snap1, &snap2));
    }

    #[test]
    fn test_remove_binding_refcount_no_predicate_remove() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Add predicate
        let pred = make_predicate(0, 0x1234);
        let pred_id = pred.id;
        partition.add_predicate(pred, vec![]);

        // Add two bindings for the same predicate
        let binding1 = SubscriptionBinding {
            subscription_id: 100,
            predicate_id: pred_id,
            consumer_id: 1,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        };
        let binding2 = SubscriptionBinding {
            subscription_id: 101,
            predicate_id: pred_id,
            consumer_id: 2,
            consumer_ordinal: ConsumerOrdinal::new(1),
            scope: SubscriptionScope::Durable,
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
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Try to remove non-existent binding
        let removed = partition.remove_binding(999);
        assert!(!removed);
    }

    #[test]
    fn test_select_candidates_with_equality_index() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let row = make_row(vec![Value::<Postgres>::Int(42)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));

        // Should find predicate via equality index
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_with_null_checks() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let row = make_row(vec![Value::<Postgres>::Null, Value::<Postgres>::Int(100)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));

        // Should find both predicates via NULL check indexes
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_rebuild_indexes_with_predicates() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let row = make_row(vec![Value::<Postgres>::Int(42)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_null_cell_matches_is_null_index() {
        use super::super::indexes::{IndexableAtom, NullKind};

        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let row = make_row(vec![
            Value::<Postgres>::Int(1),
            Value::<Postgres>::Null,
            Value::<Postgres>::Int(2),
        ]);

        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));

        // Should include the predicate because column 1 is NULL
        assert!(candidates.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_update_no_dependency_overlap_prunes_aggregate() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Aggregate reading column 1 only.
        let pred = make_agg_predicate_on_col(0, 0xAAAA, 1);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Int(100),
            }],
        );

        // UPDATE changed column 0 only, which the aggregate does not read, so
        // its value cannot have moved.
        let candidates = partition.select_agg_candidates(EventKind::Update, &[0]);
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "aggregate reading no changed column must be pruned on UPDATE"
        );
    }

    #[test]
    fn test_update_selects_predicate_reading_no_column() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // A predicate reading no column at all is still an UPDATE candidate.
        let mut pred = make_predicate(0, 0xD00D);
        pred.dependency_columns = Arc::from([]);
        pred.index_atoms = Arc::from([IndexableAtom::Fallback]);
        let pred_id = partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let candidates = partition.select_update_candidates();
        assert!(candidates.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_update_dependency_overlap_keeps_aggregate_candidates() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        let changed = make_agg_predicate_on_col(0, 0xBBBB, 0);
        let changed_id = partition.add_predicate(
            changed,
            vec![IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(1),
            }],
        );

        let unchanged = make_agg_predicate_on_col(1, 0xCCCC, 1);
        let unchanged_id = partition.add_predicate(
            unchanged,
            vec![IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Int(100),
            }],
        );

        // UPDATE changed column 0 only.
        let candidates = partition.select_agg_candidates(EventKind::Update, &[0]);
        assert!(candidates.contains(changed_id.as_u32()));
        assert!(
            !candidates.contains(unchanged_id.as_u32()),
            "aggregates with no changed-column overlap must be excluded on UPDATE"
        );
    }

    #[test]
    fn test_equality_index_on_insert_and_delete() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let row = make_row(vec![Value::<Postgres>::Int(42)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));
        assert!(
            candidates.contains(pred_id.as_u32()),
            "equality index must include predicate on INSERT with matching value"
        );

        // INSERT row with non-matching value
        let row_nomatch = make_row(vec![Value::<Postgres>::Int(99)]);
        let candidates =
            partition.select_candidates(row_nomatch.len(), probe_from_row(&row_nomatch));
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "equality index must exclude predicate on INSERT with non-matching value"
        );

        // DELETE row with matching value
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));
        assert!(
            candidates.contains(pred_id.as_u32()),
            "equality index must include predicate on DELETE with matching value"
        );
    }

    #[test]
    fn test_range_index_on_insert_and_delete() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

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
        let row = make_row(vec![Value::<Postgres>::Int(20)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));
        assert!(
            candidates.contains(pred_id.as_u32()),
            "range index must include predicate on INSERT with value in range"
        );

        // INSERT row outside range
        let row_out = make_row(vec![Value::<Postgres>::Int(5)]);
        let candidates = partition.select_candidates(row_out.len(), probe_from_row(&row_out));
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "range index must exclude predicate on INSERT with value outside range"
        );

        // DELETE row in range
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));
        assert!(
            candidates.contains(pred_id.as_u32()),
            "range index must include predicate on DELETE with matching row"
        );
    }

    #[test]
    fn test_null_index_on_insert_and_delete() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // IS NULL predicate on col 0
        let pred = make_predicate_on_col(0, 0xCCCC, 0);
        let pred_id = partition.add_predicate(
            pred,
            vec![IndexableAtom::Null {
                column_id: 0,
                kind: NullKind::IsNull,
            }],
        );

        // INSERT with NULL value: should be candidate
        let row_null = make_row(vec![Value::<Postgres>::Null]);
        let candidates = partition.select_candidates(row_null.len(), probe_from_row(&row_null));
        assert!(
            candidates.contains(pred_id.as_u32()),
            "null index must include predicate on INSERT with NULL value"
        );

        // INSERT with non-NULL value: should not be candidate
        let row_non_null = make_row(vec![Value::<Postgres>::Int(5)]);
        let candidates =
            partition.select_candidates(row_non_null.len(), probe_from_row(&row_non_null));
        assert!(
            !candidates.contains(pred_id.as_u32()),
            "null index must exclude IS NULL predicate on INSERT with non-NULL value"
        );

        // DELETE with NULL value
        let candidates = partition.select_candidates(row_null.len(), probe_from_row(&row_null));
        assert!(
            candidates.contains(pred_id.as_u32()),
            "null index must include predicate on DELETE with NULL value"
        );
    }

    // =========================================================================
    // C3: UPDATE candidate set
    // =========================================================================

    /// An UPDATE selects every row predicate, whatever its index atom. No row
    /// image enters the selection: dispatch evaluates both of them, so a
    /// predicate the new image fails may be the one the row left the view with,
    /// and a full-row subscription observes every column anyway.
    #[test]
    fn test_update_selects_every_row_predicate() {
        let atoms = [
            ("unindexable", IndexableAtom::Fallback),
            (
                "equality",
                IndexableAtom::Equality {
                    column_id: 1,
                    value: IndexableCell::Int(100),
                },
            ),
            (
                "IS NULL",
                IndexableAtom::Null {
                    column_id: 1,
                    kind: NullKind::IsNull,
                },
            ),
        ];

        for (label, atom) in atoms {
            let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);
            let pred_id = partition.add_predicate(make_predicate(0, 0x0A11), vec![atom]);

            assert!(
                partition
                    .select_update_candidates()
                    .contains(pred_id.as_u32()),
                "row predicate must be an UPDATE candidate ({label})"
            );
        }
    }
}
