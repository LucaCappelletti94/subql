//! Table partition with lock-free snapshot reads

use super::{
    ids::{ConsumerOrdinal, PredicateId},
    indexes::{HybridIndexes, IndexableCell},
    predicate::{Predicate, PredicateStore, SubscriptionBinding},
};
use crate::backend::Backend;
use crate::term::TermKey;
use crate::{ColumnId, EventKind, IdTypes, SubscriptionId, TableId};
use alloc::sync::Arc;
use alloc::vec::Vec;
use arc_swap::ArcSwap;
use roaring::RoaringBitmap;

pub use crate::backend::CellPresence;

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

/// What removing a binding removed: the consumer it belonged to, and whether
/// its predicate went with it.
pub struct BindingRemoval<I: IdTypes> {
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
    /// Snapshots published so far, so tests can assert that related mutations
    /// publish once.
    #[cfg(test)]
    publications: core::sync::atomic::AtomicU64,
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
            #[cfg(test)]
            publications: core::sync::atomic::AtomicU64::new(0),
        }
    }

    /// How many snapshots this partition has published.
    #[cfg(test)]
    pub(crate) fn publication_count(&self) -> u64 {
        self.publications
            .load(core::sync::atomic::Ordering::Relaxed)
    }

    /// Load current snapshot (lock-free)
    #[must_use]
    pub fn load_snapshot(&self) -> Arc<TablePartitionSnapshot<I, B>> {
        self.snapshot.load_full()
    }

    /// Apply one logical mutation and publish at most one new snapshot.
    ///
    /// Every change lands through the transaction handed to `f`: the store is
    /// cloned at most once however many operations run, the indexes are
    /// settled once, and the snapshot is published once at the end, or not at
    /// all when nothing changed.
    pub fn mutate<R>(&mut self, f: impl FnOnce(&mut PartitionTxn<'_, I, B>) -> R) -> R {
        let mut txn = PartitionTxn {
            partition: self,
            indexes: None,
            rebuild_all: false,
            dirty: false,
        };
        let result = f(&mut txn);
        txn.commit();
        result
    }

    /// Update snapshot with current mutable predicates (no index rebuild)
    fn store_snapshot_with_indexes(&self, indexes: HybridIndexes) {
        let new_snapshot = TablePartitionSnapshot {
            table_id: self.table_id,
            indexes,
            predicates: Arc::clone(&self.mutable_predicates),
        };

        self.snapshot.store(Arc::new(new_snapshot));
        #[cfg(test)]
        self.publications
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
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
                // A cell the event does not carry cannot be indexed, so
                // pruning on it would drop a predicate the comparator
                // would have judged, exactly as for a cell that failed to
                // decode. Every predicate reading the column stays a
                // candidate and the evaluation decides.
                CellPresence::Missing | CellPresence::Undecodable => {
                    if let Some(deps) = snapshot.indexes.dependency.get(&col_id) {
                        candidates |= deps;
                    }
                }
            }
        }

        candidates
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

/// One partition mutation in flight: operations accumulate against the
/// mutable store, and the snapshot is published once on commit.
///
/// Reads through [`Self::store`] observe the operations already applied in
/// this transaction, exactly as they observed the intermediate snapshots when
/// each operation published its own.
pub struct PartitionTxn<'a, I: IdTypes, B: Backend> {
    partition: &'a mut TablePartition<I, B>,
    /// Indexes patched incrementally, cloned from the current snapshot at the
    /// first predicate add. `None` until then, and discarded when a removal
    /// forces a full rebuild.
    indexes: Option<HybridIndexes>,
    /// A predicate was removed, so commit rebuilds the indexes from the final
    /// store rather than patching incrementally.
    rebuild_all: bool,
    dirty: bool,
}

impl<I: IdTypes, B: Backend> PartitionTxn<'_, I, B> {
    /// The store as this transaction has left it so far.
    #[must_use]
    pub fn store(&self) -> &PredicateStore<I, B> {
        &self.partition.mutable_predicates
    }

    /// Clone-on-write handle. The deep clone happens on the first call only:
    /// after it, the published snapshot no longer shares the `Arc`.
    fn store_mut(&mut self) -> &mut PredicateStore<I, B> {
        Arc::make_mut(&mut self.partition.mutable_predicates)
    }

    /// Add a predicate, patching its own
    /// [`index_atoms`](Predicate::index_atoms) into the indexes.
    ///
    /// The stored atoms are the only index source, so the incremental patch
    /// here and the full rebuild after a removal can never diverge.
    pub fn add_predicate(&mut self, predicate: Predicate<B>) -> PredicateId {
        let atoms = Arc::clone(&predicate.index_atoms);
        let deps = Arc::clone(&predicate.dependency_columns);
        let projection = predicate.projection.clone();
        let pred_id = self.store_mut().add_predicate(predicate);
        self.dirty = true;
        if !self.rebuild_all {
            let indexes = self
                .indexes
                .get_or_insert_with(|| self.partition.load_snapshot().indexes.clone());
            indexes.add_predicate(pred_id, &atoms, &deps, &projection);
        }
        pred_id
    }

    /// Bind a subscription to an existing predicate, taking a refcount.
    pub fn add_binding(&mut self, binding: SubscriptionBinding<I>, pred_id: PredicateId) {
        let store = self.store_mut();
        store.add_binding(binding);
        store.increment_refcount(pred_id);
        self.dirty = true;
    }

    /// Record which subscribers each of a predicate's terms admits, for one
    /// newly bound subscription.
    ///
    /// `seeds` is indexed by term slot: `seeds[i]` is what this subscriber
    /// states it matches through slot `i` today.
    pub fn seed_terms(
        &mut self,
        pred_id: PredicateId,
        ordinal: ConsumerOrdinal,
        subscriber: &TermKey<B>,
        seeds: &[Vec<crate::term::TermRow<B>>],
    ) {
        if seeds.is_empty() {
            return;
        }
        let store = self.store_mut();
        for (slot, values) in seeds.iter().enumerate() {
            let Ok(slot) = u16::try_from(slot) else {
                continue;
            };
            store.seed_term(pred_id, slot, ordinal, subscriber.clone(), values.clone());
        }
        self.dirty = true;
    }

    /// Move who one term admits, as a changed membership row does.
    ///
    /// `values` is what the row keys, and `ordinals` are the subscribers it
    /// names. Nothing moves when the term is not tracked here, which is
    /// checked on the shared view first so that case pays no store clone.
    pub fn move_term_members(
        &mut self,
        pred_id: PredicateId,
        slot: u16,
        values: crate::term::TermRow<B>,
        ordinals: &RoaringBitmap,
        widen: bool,
    ) {
        if !self.store().term_members.contains_key(&(pred_id, slot)) {
            return;
        }
        let store = self.store_mut();
        let Some(members) = store.term_members.get_mut(&(pred_id, slot)) else {
            return;
        };
        if widen {
            members.widen(values, ordinals);
        } else {
            members.narrow(&values, ordinals);
        }
        self.dirty = true;
    }

    /// Withdraw every value one term admits, and report what was withdrawn.
    ///
    /// Used when the table carrying the memberships is truncated: the values
    /// are only knowable from the sets themselves, and the caller reports
    /// them.
    pub fn clear_term_admissions(
        &mut self,
        pred_id: PredicateId,
        slot: u16,
    ) -> Vec<(crate::term::TermRow<B>, RoaringBitmap)> {
        if !self.store().term_members.contains_key(&(pred_id, slot)) {
            return Vec::new();
        }
        let store = self.store_mut();
        let Some(members) = store.term_members.get_mut(&(pred_id, slot)) else {
            return Vec::new();
        };
        let withdrawn = members.clear_admissions();
        if !withdrawn.is_empty() {
            self.dirty = true;
        }
        withdrawn
    }

    /// Remove a binding and drop its refcount, removing the predicate when the
    /// count reaches zero.
    ///
    /// Returns what was removed, or `None` when no binding existed, which is
    /// checked on the shared view first so that case pays no store clone.
    pub fn remove_binding(&mut self, sub_id: SubscriptionId) -> Option<BindingRemoval<I>> {
        if !self.store().bindings.contains_key(&sub_id) {
            return None;
        }
        let store = self.store_mut();
        let binding = store.remove_binding(sub_id)?;
        let removed = store.decrement_refcount(binding.predicate_id);
        self.dirty = true;
        if removed {
            // The predicate is gone, so any incremental patches are moot:
            // commit rebuilds from the final store.
            self.rebuild_all = true;
            self.indexes = None;
        }
        Some(BindingRemoval {
            predicate_removed: removed,
            consumer_id: binding.consumer_id,
        })
    }

    /// Publish the accumulated changes as one snapshot, or none at all when no
    /// operation changed anything.
    fn commit(self) {
        if !self.dirty {
            return;
        }
        let indexes = if self.rebuild_all {
            let mut rebuilt = HybridIndexes::new();
            for (idx, pred) in &self.partition.mutable_predicates.predicates {
                rebuilt.add_predicate(
                    PredicateId::from_slab_index(idx),
                    &pred.index_atoms,
                    &pred.dependency_columns,
                    &pred.projection,
                );
            }
            rebuilt
        } else if let Some(indexes) = self.indexes {
            indexes
        } else {
            self.partition.load_snapshot().indexes.clone()
        };
        self.partition.store_snapshot_with_indexes(indexes);
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

    /// Test spelling that states the atoms in one place: they are written
    /// into the predicate, which is the index's single source.
    fn add_predicate(
        partition: &mut TablePartition<DefaultIds, Postgres>,
        mut pred: Predicate<Postgres>,
        atoms: &[IndexableAtom],
    ) -> PredicateId {
        pred.index_atoms = Arc::from(atoms);
        partition.mutate(|txn| txn.add_predicate(pred))
    }

    fn add_binding(
        partition: &mut TablePartition<DefaultIds, Postgres>,
        binding: SubscriptionBinding<DefaultIds>,
        pred_id: PredicateId,
    ) {
        partition.mutate(|txn| txn.add_binding(binding, pred_id));
    }

    /// Whether removing the binding removed its predicate too.
    fn remove_binding(
        partition: &mut TablePartition<DefaultIds, Postgres>,
        sub_id: SubscriptionId,
    ) -> bool {
        partition
            .mutate(|txn| txn.remove_binding(sub_id))
            .is_some_and(|removal| removal.predicate_removed)
    }

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
            group_key_encoder: None,
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
        let atoms = &[IndexableAtom::Equality {
            column_id: 5,
            value: IndexableCell::Int(42),
        }];

        add_predicate(&mut partition, pred, atoms);

        let snapshot = partition.load_snapshot();
        // Should be indexed in equality index, not fallback
        assert!(!snapshot.indexes.equality.is_empty());
    }

    #[test]
    fn test_select_candidates_fallback() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        let pred = make_predicate(0, 0x1234);
        add_predicate(&mut partition, pred, &[IndexableAtom::Fallback]);

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
        add_predicate(&mut partition, pred, &[]);

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

        add_binding(&mut partition, binding1, pred_id);
        add_binding(&mut partition, binding2, pred_id);

        // Remove first binding - refcount decrements but predicate not removed
        let predicate_removed = remove_binding(&mut partition, 100);
        assert!(!predicate_removed); // Predicate still has refcount > 0

        // Predicate should still exist
        let snapshot = partition.load_snapshot();
        assert!(snapshot.predicates.get_predicate(pred_id).is_some());
    }

    #[test]
    fn test_remove_binding_nonexistent() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Try to remove non-existent binding
        let removed = remove_binding(&mut partition, 999);
        assert!(!removed);
    }

    #[test]
    fn test_select_candidates_with_equality_index() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Add predicate with equality index
        let pred = make_predicate(0, 0x1234);
        add_predicate(
            &mut partition,
            pred,
            &[IndexableAtom::Equality {
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
        add_predicate(
            &mut partition,
            pred1,
            &[IndexableAtom::Null {
                column_id: 0,
                kind: NullKind::IsNull,
            }],
        );

        // Add predicate with IS NOT NULL check
        let pred2 = make_predicate(1, 0x5678);
        add_predicate(
            &mut partition,
            pred2,
            &[IndexableAtom::Null {
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

    /// Removing the last binding of a predicate rebuilds the indexes from the
    /// remaining store, so a surviving predicate still matches and the removed
    /// one is gone from the index.
    #[test]
    fn test_removal_rebuilds_indexes_from_remaining_predicates() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        let kept = make_predicate(0, 0x1234);
        let kept_id = add_predicate(
            &mut partition,
            kept,
            &[IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(42),
            }],
        );
        let mut removed = make_predicate(1, 0x5678);
        removed.refcount = 0;
        let removed_id = add_predicate(
            &mut partition,
            removed,
            &[IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(42),
            }],
        );
        add_binding(
            &mut partition,
            SubscriptionBinding {
                subscription_id: 100,
                predicate_id: removed_id,
                consumer_id: 1,
                consumer_ordinal: ConsumerOrdinal::new(0),
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 0,
            },
            removed_id,
        );

        assert!(
            remove_binding(&mut partition, 100),
            "the only binding takes its predicate with it"
        );

        let row = make_row(vec![Value::<Postgres>::Int(42)]);
        let candidates = partition.select_candidates(row.len(), probe_from_row(&row));
        assert!(
            candidates.contains(kept_id.as_u32()),
            "the surviving predicate is still indexed after the rebuild"
        );
        assert!(
            !candidates.contains(removed_id.as_u32()),
            "the removed predicate left the index"
        );
    }

    /// One transaction spanning several operations publishes one snapshot, and
    /// a transaction that changes nothing publishes none.
    #[test]
    fn a_transaction_publishes_once_and_a_noop_not_at_all() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        partition.mutate(|txn| {
            let pred_id = txn.add_predicate(make_predicate(0, 0x1234));
            txn.add_binding(
                SubscriptionBinding {
                    subscription_id: 100,
                    predicate_id: pred_id,
                    consumer_id: 1,
                    consumer_ordinal: ConsumerOrdinal::new(0),
                    scope: SubscriptionScope::Durable,
                    updated_at_unix_ms: 0,
                },
                pred_id,
            );
        });
        assert_eq!(
            partition.publication_count(),
            1,
            "predicate and binding ride one snapshot"
        );

        partition.mutate(|txn| {
            assert!(txn.remove_binding(999).is_none());
            txn.move_term_members(
                PredicateId::from_slab_index(0),
                0,
                Vec::new(),
                &RoaringBitmap::new(),
                true,
            );
            assert!(txn
                .clear_term_admissions(PredicateId::from_slab_index(0), 0)
                .is_empty());
        });
        assert_eq!(
            partition.publication_count(),
            1,
            "a transaction that changed nothing publishes no snapshot"
        );
    }

    #[test]
    fn test_select_candidates_null_cell_matches_is_null_index() {
        use super::super::indexes::{IndexableAtom, NullKind};

        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        // Add predicate with IS NULL index on column 1
        let pred = make_predicate(0, 0x9999);
        let pred_id = pred.id;
        add_predicate(
            &mut partition,
            pred,
            &[IndexableAtom::Null {
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
        let pred_id = add_predicate(
            &mut partition,
            pred,
            &[IndexableAtom::Equality {
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
        let pred_id = add_predicate(&mut partition, pred, &[IndexableAtom::Fallback]);

        let candidates = partition.select_update_candidates();
        assert!(candidates.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_update_dependency_overlap_keeps_aggregate_candidates() {
        let mut partition = TablePartition::<DefaultIds, Postgres>::new(1);

        let changed = make_agg_predicate_on_col(0, 0xBBBB, 0);
        let changed_id = add_predicate(
            &mut partition,
            changed,
            &[IndexableAtom::Equality {
                column_id: 0,
                value: IndexableCell::Int(1),
            }],
        );

        let unchanged = make_agg_predicate_on_col(1, 0xCCCC, 1);
        let unchanged_id = add_predicate(
            &mut partition,
            unchanged,
            &[IndexableAtom::Equality {
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
        let pred_id = add_predicate(
            &mut partition,
            pred,
            &[IndexableAtom::Equality {
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
        let pred_id = add_predicate(
            &mut partition,
            pred,
            &[IndexableAtom::Range {
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
        let pred_id = add_predicate(
            &mut partition,
            pred,
            &[IndexableAtom::Null {
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

    // C3: UPDATE candidate set

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
            let pred_id = add_predicate(&mut partition, make_predicate(0, 0x0A11), &[atom]);

            assert!(
                partition
                    .select_update_candidates()
                    .contains(pred_id.as_u32()),
                "row predicate must be an UPDATE candidate ({label})"
            );
        }
    }
}
