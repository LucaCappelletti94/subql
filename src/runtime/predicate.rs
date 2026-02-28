//! Predicate storage with deduplication and refcounting

use super::ids::{ConsumerOrdinal, PredicateHash, PredicateId};
use super::indexes::IndexableAtom;
use crate::{
    compiler::{sql_shape::QueryProjection, BytecodeProgram, PrefilterPlan},
    ColumnId, IdTypes, SubscriptionId, SubscriptionScope,
};
use ahash::{AHashMap, AHashSet};
use roaring::RoaringBitmap;
use slab::Slab;
use std::sync::Arc;

/// Compiled predicate with metadata
#[derive(Clone, Debug)]
pub struct Predicate {
    /// Stable predicate ID (slab index)
    pub id: PredicateId,
    /// Hash of normalized SQL (for deduplication)
    pub hash: PredicateHash,
    /// Normalized/canonicalized SQL WHERE clause
    pub normalized_sql: Arc<str>,
    /// Compiled bytecode for VM evaluation
    pub bytecode: Arc<BytecodeProgram>,
    /// Columns referenced by this predicate (for UPDATE optimization)
    pub dependency_columns: Arc<[ColumnId]>,
    /// Precomputed indexable atoms for this predicate.
    pub index_atoms: Arc<[IndexableAtom]>,
    /// Planner metadata used for OR/NOT-aware candidate pruning.
    pub prefilter_plan: Arc<PrefilterPlan>,
    /// Projection kind: row events or aggregate deltas.
    pub projection: QueryProjection,
    /// Reference count (number of subscriptions using this predicate)
    pub refcount: u32,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch)
    pub updated_at_unix_ms: u64,
}

/// Subscription binding (consumer → predicate → subscription)
#[derive(Debug)]
pub struct SubscriptionBinding<I: IdTypes> {
    /// Engine-assigned subscription identifier
    pub subscription_id: SubscriptionId,
    /// Predicate this subscription uses
    pub predicate_id: PredicateId,
    /// Consumer who owns this subscription
    pub consumer_id: I::ConsumerId,
    /// Dense consumer ordinal for bitmap indexing
    pub consumer_ordinal: ConsumerOrdinal,
    /// Lifetime scope: durable or session-bound
    pub scope: SubscriptionScope<I>,
    /// Timestamp for conflict resolution
    pub updated_at_unix_ms: u64,
}

impl<I: IdTypes> Clone for SubscriptionBinding<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: IdTypes> Copy for SubscriptionBinding<I> {}

/// Predicate storage with deduplication
///
/// Manages predicates with slab allocation and hash-based deduplication.
/// Tracks refcounts and automatically removes predicates when refcount reaches 0.
pub struct PredicateStore<I: IdTypes> {
    /// Slab-allocated predicates (stable IDs)
    pub predicates: Slab<Predicate>,
    /// Hash → candidate PredicateIds (for deduplication with collision checks)
    pub hash_index: AHashMap<PredicateHash, Vec<PredicateId>>,
    /// SubscriptionId → SubscriptionBinding
    pub bindings: AHashMap<SubscriptionId, SubscriptionBinding<I>>,
    /// SessionId → `Vec<SubscriptionId>` (for session cleanup)
    pub scope_index: AHashMap<I::SessionId, Vec<SubscriptionId>>,
    /// PredicateId → `RoaringBitmap<ConsumerOrdinal>` (consumers interested in this predicate)
    pub predicate_consumers: AHashMap<PredicateId, RoaringBitmap>,
}

impl<I: IdTypes> Clone for PredicateStore<I> {
    fn clone(&self) -> Self {
        Self {
            predicates: self.predicates.clone(),
            hash_index: self.hash_index.clone(),
            bindings: self.bindings.clone(),
            scope_index: self.scope_index.clone(),
            predicate_consumers: self.predicate_consumers.clone(),
        }
    }
}

impl<I: IdTypes> PredicateStore<I> {
    /// Create new empty predicate store
    #[must_use]
    pub fn new() -> Self {
        Self {
            predicates: Slab::new(),
            hash_index: AHashMap::new(),
            bindings: AHashMap::new(),
            scope_index: AHashMap::new(),
            predicate_consumers: AHashMap::new(),
        }
    }

    /// Find predicate by hash (for deduplication)
    #[must_use]
    pub fn find_by_hash(&self, hash: PredicateHash) -> Option<PredicateId> {
        self.hash_index
            .get(&hash)
            .and_then(|ids| ids.first().copied())
    }

    /// Find predicate by hash and normalized SQL.
    #[must_use]
    pub fn find_by_hash_and_sql(
        &self,
        hash: PredicateHash,
        normalized_sql: &str,
    ) -> Option<PredicateId> {
        let ids = self.hash_index.get(&hash)?;
        ids.iter().copied().find(|id| {
            self.get_predicate(*id)
                .is_some_and(|pred| pred.normalized_sql.as_ref() == normalized_sql)
        })
    }

    /// Get predicate by ID
    #[must_use]
    pub fn get_predicate(&self, id: PredicateId) -> Option<&Predicate> {
        self.predicates.get(id.to_slab_index())
    }

    /// Get mutable predicate by ID
    #[must_use]
    pub fn get_predicate_mut(&mut self, id: PredicateId) -> Option<&mut Predicate> {
        self.predicates.get_mut(id.to_slab_index())
    }

    /// Add new predicate
    ///
    /// Returns allocated `PredicateId` from slab insertion.
    pub fn add_predicate(&mut self, mut predicate: Predicate) -> PredicateId {
        let entry = self.predicates.vacant_entry();
        let id = PredicateId::from_slab_index(entry.key());
        let hash = predicate.hash;
        predicate.id = id;

        entry.insert(predicate);
        self.hash_index.entry(hash).or_default().push(id);

        id
    }

    /// Increment predicate refcount
    ///
    /// Returns true if predicate exists.
    pub fn increment_refcount(&mut self, id: PredicateId) -> bool {
        if let Some(pred) = self.get_predicate_mut(id) {
            pred.refcount += 1;
            true
        } else {
            false
        }
    }

    /// Decrement predicate refcount, remove if reaches 0
    ///
    /// Returns true if predicate was removed.
    pub fn decrement_refcount(&mut self, id: PredicateId) -> bool {
        let should_remove = if let Some(pred) = self.get_predicate_mut(id) {
            pred.refcount = pred.refcount.saturating_sub(1);
            pred.refcount == 0
        } else {
            false
        };

        if should_remove {
            self.remove_predicate(id);
            true
        } else {
            false
        }
    }

    /// Remove predicate completely
    fn remove_predicate(&mut self, id: PredicateId) {
        if let Some(pred) = self.predicates.try_remove(id.to_slab_index()) {
            if let Some(ids) = self.hash_index.get_mut(&pred.hash) {
                ids.retain(|existing| *existing != id);
                if ids.is_empty() {
                    self.hash_index.remove(&pred.hash);
                }
            }
            self.predicate_consumers.remove(&id);
        }
    }

    /// Add subscription binding
    pub fn add_binding(&mut self, binding: SubscriptionBinding<I>) {
        let sub_id = binding.subscription_id;

        // Overwrite-safe upsert: remove previous secondary index entries when
        // replacing an existing subscription ID.
        if let Some(previous) = self.bindings.insert(sub_id, binding) {
            self.remove_binding_indexes(previous);
        }

        self.add_binding_indexes(binding);
    }

    /// Remove subscription binding
    ///
    /// Returns the removed binding if it existed.
    pub fn remove_binding(&mut self, sub_id: SubscriptionId) -> Option<SubscriptionBinding<I>> {
        let binding = self.bindings.remove(&sub_id)?;

        self.remove_binding_indexes(binding);

        Some(binding)
    }

    /// Get all subscription IDs for a session
    #[must_use]
    pub fn get_session_subscriptions(&self, session_id: I::SessionId) -> Option<&[SubscriptionId]> {
        self.scope_index
            .get(&session_id)
            .map(std::vec::Vec::as_slice)
    }

    /// Returns `true` if any active binding references the given consumer.
    #[must_use]
    pub fn is_consumer_referenced(&self, consumer_id: I::ConsumerId) -> bool {
        self.bindings.values().any(|b| b.consumer_id == consumer_id)
    }

    /// Collect the set of distinct consumer IDs across all active bindings.
    #[must_use]
    pub fn active_consumer_ids(&self) -> AHashSet<I::ConsumerId> {
        self.bindings.values().map(|b| b.consumer_id).collect()
    }

    fn add_binding_indexes(&mut self, binding: SubscriptionBinding<I>) {
        let sub_id = binding.subscription_id;
        let pred_id = binding.predicate_id;
        let consumer_ord = binding.consumer_ordinal;

        if let SubscriptionScope::Session(sid) = binding.scope {
            self.scope_index.entry(sid).or_default().push(sub_id);
        }

        self.predicate_consumers
            .entry(pred_id)
            .or_default()
            .insert(consumer_ord.get());
    }

    fn remove_binding_indexes(&mut self, binding: SubscriptionBinding<I>) {
        let sub_id = binding.subscription_id;

        let has_other_same_consumer_binding = self.bindings.values().any(|existing| {
            existing.predicate_id == binding.predicate_id
                && existing.consumer_ordinal == binding.consumer_ordinal
        });

        if !has_other_same_consumer_binding {
            if let Some(bitmap) = self.predicate_consumers.get_mut(&binding.predicate_id) {
                bitmap.remove(binding.consumer_ordinal.get());
                if bitmap.is_empty() {
                    self.predicate_consumers.remove(&binding.predicate_id);
                }
            }
        }

        if let SubscriptionScope::Session(session_id) = binding.scope {
            if let Some(subs) = self.scope_index.get_mut(&session_id) {
                subs.retain(|&id| id != sub_id);
                if subs.is_empty() {
                    self.scope_index.remove(&session_id);
                }
            }
        }
    }
}

impl<I: IdTypes> Default for PredicateStore<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::clone_on_copy)]
mod tests {
    use super::*;
    use crate::compiler::{Instruction, PrefilterPlan};
    use crate::DefaultIds;

    fn make_predicate(id: usize, hash: u128, refcount: u32) -> Predicate {
        Predicate {
            id: PredicateId::from_slab_index(id),
            hash,
            normalized_sql: "test".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::Not])),
            dependency_columns: Arc::from([]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            projection: QueryProjection::Rows,
            refcount,
            updated_at_unix_ms: 0,
        }
    }

    #[test]
    fn test_add_and_find_predicate() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred = make_predicate(0, 0x1234, 1);
        let id = store.add_predicate(pred);

        assert_eq!(store.find_by_hash(0x1234), Some(id));
        assert!(store.get_predicate(id).is_some());
    }

    #[test]
    fn test_refcount_increment() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred = make_predicate(0, 0x1234, 1);
        let id = store.add_predicate(pred);

        assert_eq!(store.get_predicate(id).unwrap().refcount, 1);

        store.increment_refcount(id);
        assert_eq!(store.get_predicate(id).unwrap().refcount, 2);
    }

    #[test]
    fn test_refcount_decrement() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred = make_predicate(0, 0x1234, 2);
        let id = store.add_predicate(pred);

        let removed = store.decrement_refcount(id);
        assert!(!removed);
        assert_eq!(store.get_predicate(id).unwrap().refcount, 1);

        let removed = store.decrement_refcount(id);
        assert!(removed);
        assert!(store.get_predicate(id).is_none());
    }

    #[test]
    fn test_binding_lifecycle() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let binding = SubscriptionBinding {
            subscription_id: 100,
            predicate_id: PredicateId::from_slab_index(0),
            consumer_id: 42,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Session(1000),
            updated_at_unix_ms: 0,
        };

        store.add_binding(binding.clone());

        assert!(store.bindings.contains_key(&100));
        assert!(store.get_session_subscriptions(1000).is_some());

        let removed = store.remove_binding(100);
        assert!(removed.is_some());
        assert!(!store.bindings.contains_key(&100));
    }

    #[test]
    fn test_predicate_consumers_bitmap() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred_id = PredicateId::from_slab_index(0);

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

        store.add_binding(binding1);
        store.add_binding(binding2);

        let bitmap = store.predicate_consumers.get(&pred_id).unwrap();
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(1));
        assert_eq!(bitmap.len(), 2);
    }

    #[test]
    fn test_add_binding_overwrite_cleans_secondary_indexes() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred1 = make_predicate(0, 0x1111, 0);
        let pred2 = make_predicate(1, 0x2222, 0);
        let pred1_id = store.add_predicate(pred1);
        let pred2_id = store.add_predicate(pred2);

        store.add_binding(SubscriptionBinding {
            subscription_id: 100,
            predicate_id: pred1_id,
            consumer_id: 10,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Session(500),
            updated_at_unix_ms: 1,
        });
        store.add_binding(SubscriptionBinding {
            subscription_id: 100, // overwrite same subscription id
            predicate_id: pred2_id,
            consumer_id: 20,
            consumer_ordinal: ConsumerOrdinal::new(1),
            scope: SubscriptionScope::Session(600),
            updated_at_unix_ms: 2,
        });

        assert!(!store
            .predicate_consumers
            .get(&pred1_id)
            .is_some_and(|bitmap| bitmap.contains(0)));
        assert!(store
            .predicate_consumers
            .get(&pred2_id)
            .is_some_and(|bitmap| bitmap.contains(1)));
        assert!(store.get_session_subscriptions(500).is_none());
        assert_eq!(store.get_session_subscriptions(600), Some(&[100][..]));
    }

    #[test]
    fn test_remove_binding_keeps_bitmap_when_same_consumer_has_another_binding() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred = make_predicate(0, 0x3333, 0);
        let pred_id = store.add_predicate(pred);

        store.add_binding(SubscriptionBinding {
            subscription_id: 201,
            predicate_id: pred_id,
            consumer_id: 42,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 1,
        });
        store.add_binding(SubscriptionBinding {
            subscription_id: 202,
            predicate_id: pred_id,
            consumer_id: 42,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 2,
        });

        let _ = store.remove_binding(201);
        let bitmap = store
            .predicate_consumers
            .get(&pred_id)
            .expect("bitmap should remain while one binding still exists");
        assert!(bitmap.contains(0));

        let _ = store.remove_binding(202);
        assert!(
            !store.predicate_consumers.contains_key(&pred_id),
            "bitmap should be removed after last binding is removed"
        );
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Predicate Completion
    // ========================================================================

    #[test]
    fn test_increment_refcount_nonexistent() {
        let mut store = PredicateStore::<DefaultIds>::new();

        // Try to increment refcount of non-existent predicate
        let fake_id = PredicateId::from_slab_index(999);
        let result = store.increment_refcount(fake_id);
        assert!(!result);
    }

    #[test]
    fn test_decrement_refcount_nonexistent() {
        let mut store = PredicateStore::<DefaultIds>::new();

        // Try to decrement refcount of non-existent predicate
        let fake_id = PredicateId::from_slab_index(999);
        let result = store.decrement_refcount(fake_id);
        assert!(!result);
    }

    #[test]
    fn test_predicate_store_default() {
        let store = PredicateStore::<DefaultIds>::default();
        assert!(store.predicates.is_empty());
    }

    #[test]
    fn test_predicate_store_rejects_or_normalizes_mismatched_predicate_id() {
        let mut store = PredicateStore::<DefaultIds>::new();

        // Intentionally provide an ID that does not match the first free slab slot.
        let pred = make_predicate(99, 0xBEEF, 1);
        let returned_id = store.add_predicate(pred);

        // Returned ID must point to a real predicate.
        let stored = store
            .get_predicate(returned_id)
            .expect("returned ID should resolve to stored predicate");
        assert_eq!(stored.hash, 0xBEEF);

        // Hash index must resolve to the same valid predicate ID.
        let by_hash = store
            .find_by_hash(0xBEEF)
            .expect("hash index should contain inserted predicate");
        assert_eq!(by_hash, returned_id);
        assert!(store.get_predicate(by_hash).is_some());
    }

    #[test]
    fn test_hash_collision_lookup_uses_normalized_sql() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let mut pred1 = make_predicate(0, 0x00C0_FFEE, 1);
        pred1.normalized_sql = "amount > 100".into();
        let id1 = store.add_predicate(pred1);

        let mut pred2 = make_predicate(1, 0x00C0_FFEE, 1);
        pred2.normalized_sql = "status = 'paid'".into();
        let id2 = store.add_predicate(pred2);

        assert_eq!(
            store.find_by_hash_and_sql(0x00C0_FFEE, "amount > 100"),
            Some(id1)
        );
        assert_eq!(
            store.find_by_hash_and_sql(0x00C0_FFEE, "status = 'paid'"),
            Some(id2)
        );
        assert_eq!(
            store.find_by_hash_and_sql(0x00C0_FFEE, "amount > 0"),
            None,
            "different SQL under same hash must not be treated as equivalent"
        );
    }

    #[test]
    fn test_is_consumer_referenced() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred = make_predicate(0, 0xAABB, 1);
        let pred_id = store.add_predicate(pred);

        assert!(!store.is_consumer_referenced(42));

        store.add_binding(SubscriptionBinding {
            subscription_id: 1,
            predicate_id: pred_id,
            consumer_id: 42,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        });

        assert!(store.is_consumer_referenced(42));
        assert!(!store.is_consumer_referenced(99));
    }

    #[test]
    fn test_active_consumer_ids() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred = make_predicate(0, 0xCCDD, 1);
        let pred_id = store.add_predicate(pred);

        assert!(store.active_consumer_ids().is_empty());

        store.add_binding(SubscriptionBinding {
            subscription_id: 1,
            predicate_id: pred_id,
            consumer_id: 10,
            consumer_ordinal: ConsumerOrdinal::new(0),
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        });
        store.add_binding(SubscriptionBinding {
            subscription_id: 2,
            predicate_id: pred_id,
            consumer_id: 20,
            consumer_ordinal: ConsumerOrdinal::new(1),
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        });

        let ids = store.active_consumer_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&10));
        assert!(ids.contains(&20));
    }
}
