//! Predicate storage with deduplication and refcounting

use super::ids::{ConsumerOrdinal, PredicateHash, PredicateId};
use super::indexes::IndexableAtom;
use crate::backend::Backend;
use crate::term::TermKey;
use crate::{
    compiler::{sql_shape::QueryProjection, BytecodeProgram, PrefilterPlan},
    ColumnId, IdTypes, SubscriptionId, SubscriptionScope,
};
use alloc::sync::Arc;
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use roaring::RoaringBitmap;
use slab::Slab;

/// Compiled predicate with metadata.
///
/// Storage-shaped type parameterised on the observed [`Backend`]. The
/// bytecode program pins the backend it was compiled against; the runtime
/// re-executes it via `Vm<B>` in response to every `E: CdcEvent<Backend = B>`.
pub struct Predicate<B: Backend> {
    /// Stable predicate ID (slab index).
    pub id: PredicateId,
    /// Hash of normalized SQL (for deduplication).
    pub hash: PredicateHash,
    /// Normalized / canonicalised SQL WHERE clause.
    pub normalized_sql: Arc<str>,
    /// Compiled bytecode for VM evaluation.
    pub bytecode: Arc<BytecodeProgram<B>>,
    /// Columns the predicate reads, plus the aggregated column for a column
    /// aggregate. Prunes aggregate UPDATE candidates, and selects row
    /// predicates reading a cell that failed to decode.
    pub dependency_columns: Arc<[ColumnId]>,
    /// Precomputed indexable atoms for this predicate.
    pub index_atoms: Arc<[IndexableAtom]>,
    /// Planner metadata used for OR/NOT-aware candidate pruning.
    pub prefilter_plan: Arc<PrefilterPlan>,
    /// Projection kind: row events or aggregate deltas.
    pub projection: QueryProjection,
    /// Reference count (number of subscriptions using this predicate).
    pub refcount: u32,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch).
    pub updated_at_unix_ms: u64,
}

// `Clone` and `Debug` are hand-implemented so their bounds fall on the
// `Arc<BytecodeProgram<B>>` field (which is always `Clone + Debug`
// regardless of `B`) rather than on `B` itself. `#[derive(...)]` would
// defensively add `B: Clone` / `B: Debug`, which is not implied by
// `Backend`.

impl<B: Backend> Clone for Predicate<B> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            hash: self.hash,
            normalized_sql: Arc::clone(&self.normalized_sql),
            bytecode: Arc::clone(&self.bytecode),
            dependency_columns: Arc::clone(&self.dependency_columns),
            index_atoms: Arc::clone(&self.index_atoms),
            prefilter_plan: Arc::clone(&self.prefilter_plan),
            projection: self.projection.clone(),
            refcount: self.refcount,
            updated_at_unix_ms: self.updated_at_unix_ms,
        }
    }
}

impl<B: Backend> core::fmt::Debug for Predicate<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Predicate")
            .field("id", &self.id)
            .field("hash", &self.hash)
            .field("normalized_sql", &self.normalized_sql)
            .field("bytecode", &self.bytecode)
            .field("dependency_columns", &self.dependency_columns)
            .field("index_atoms", &self.index_atoms)
            .field("prefilter_plan", &self.prefilter_plan)
            .field("projection", &self.projection)
            .field("refcount", &self.refcount)
            .field("updated_at_unix_ms", &self.updated_at_unix_ms)
            .finish()
    }
}

/// Subscription binding (consumer -> predicate -> subscription)
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

/// Which subscribers one membership term admits, for one predicate's slot.
///
/// Two indexes over the same bindings, because the set moves from both ends. A
/// changed row of the subscribed table carries the compared value and asks which
/// subscribers it admits. A changed row of the membership table carries a
/// subscriber and a value, and asks which of this predicate's subscribers claim
/// that identity, so it can move them under that value.
pub struct TermMembers<B: Backend> {
    /// Which consumer ordinals a compared value admits.
    by_value: HashMap<TermKey<B>, RoaringBitmap>,
    /// Which consumer ordinals claim each subscriber identity.
    by_subscriber: HashMap<TermKey<B>, RoaringBitmap>,
}

// `Clone` and `Debug` are hand-implemented so their bounds fall on the scalar
// types `TermKey<B>` names rather than on the backend marker `B`, for the same
// reason `Value<B>`'s are.
impl<B: Backend> Clone for TermMembers<B> {
    fn clone(&self) -> Self {
        Self {
            by_value: self.by_value.clone(),
            by_subscriber: self.by_subscriber.clone(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for TermMembers<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TermMembers")
            .field("by_value", &self.by_value)
            .field("by_subscriber", &self.by_subscriber)
            .finish()
    }
}

impl<B: Backend> Default for TermMembers<B> {
    fn default() -> Self {
        Self {
            by_value: HashMap::new(),
            by_subscriber: HashMap::new(),
        }
    }
}

impl<B: Backend> TermMembers<B> {
    /// The consumer ordinals `value` admits, empty when it admits none.
    #[must_use]
    pub fn admits(&self, value: &TermKey<B>) -> Option<&RoaringBitmap> {
        self.by_value.get(value)
    }

    /// Record that `ordinal` matches `value` through this term.
    fn admit(&mut self, value: TermKey<B>, ordinal: ConsumerOrdinal) {
        self.by_value
            .entry(value)
            .or_default()
            .insert(ordinal.get());
    }

    /// Record that `ordinal` filters for `subscriber`.
    fn claim(&mut self, subscriber: TermKey<B>, ordinal: ConsumerOrdinal) {
        self.by_subscriber
            .entry(subscriber)
            .or_default()
            .insert(ordinal.get());
    }

    /// The consumer ordinals filtering for `subscriber`.
    #[must_use]
    pub fn claimed_by(&self, subscriber: &TermKey<B>) -> Option<&RoaringBitmap> {
        self.by_subscriber.get(subscriber)
    }

    /// Add `ordinals` to the set `value` admits, as a membership row appearing
    /// does.
    pub fn widen(&mut self, value: TermKey<B>, ordinals: &RoaringBitmap) {
        *self.by_value.entry(value).or_default() |= ordinals;
    }

    /// Take `ordinals` out of the set `value` admits, as a membership row
    /// disappearing does.
    pub fn narrow(&mut self, value: &TermKey<B>, ordinals: &RoaringBitmap) {
        if let Some(admitted) = self.by_value.get_mut(value) {
            *admitted -= ordinals;
            if admitted.is_empty() {
                self.by_value.remove(value);
            }
        }
    }

    /// Take every value this term admits, leaving it admitting none.
    ///
    /// The subscriber claims stay: the subscriptions are still registered and
    /// still filter for the same identities, so a membership row appearing again
    /// moves them back.
    pub fn clear_admissions(&mut self) -> Vec<(TermKey<B>, RoaringBitmap)> {
        self.by_value.drain().collect()
    }

    /// Drop `ordinal` from every set, as unbinding its subscription does.
    fn forget(&mut self, ordinal: ConsumerOrdinal) {
        for set in self
            .by_value
            .values_mut()
            .chain(self.by_subscriber.values_mut())
        {
            set.remove(ordinal.get());
        }
        self.by_value.retain(|_, set| !set.is_empty());
        self.by_subscriber.retain(|_, set| !set.is_empty());
    }
}

/// Predicate storage with deduplication.
///
/// Manages predicates with slab allocation and hash-based deduplication.
/// Tracks refcounts and automatically removes predicates when refcount
/// reaches 0.
pub struct PredicateStore<I: IdTypes, B: Backend> {
    /// Slab-allocated predicates (stable IDs).
    pub predicates: Slab<Predicate<B>>,
    /// Hash -> candidate PredicateIds (for deduplication with collision checks).
    pub hash_index: HashMap<PredicateHash, Vec<PredicateId>>,
    /// SubscriptionId -> SubscriptionBinding.
    pub bindings: HashMap<SubscriptionId, SubscriptionBinding<I>>,
    /// SessionId -> `Vec<SubscriptionId>` (for session cleanup).
    pub scope_index: HashMap<I::SessionId, Vec<SubscriptionId>>,
    /// PredicateId -> `RoaringBitmap<ConsumerOrdinal>` (consumers interested in this predicate).
    pub predicate_consumers: HashMap<PredicateId, RoaringBitmap>,
    /// (PredicateId, ConsumerOrdinal) -> SubscriptionIds bound to that pair.
    ///
    /// A single (predicate, consumer) pair may carry multiple subscription
    /// ids when the same consumer subscribes under different scopes (e.g.
    /// one durable and one session-scoped). Used by activity-aware
    /// eviction policies to stamp the matched subscriptions after
    /// dispatch in O(1) per matched pair instead of an O(B) scan over
    /// `bindings`.
    pub binding_lookup: HashMap<(PredicateId, ConsumerOrdinal), Vec<SubscriptionId>>,
    /// (PredicateId, term slot) -> which subscribers that term admits.
    ///
    /// Empty for every predicate carrying no membership term, which is every
    /// predicate until one is registered, so a term-free engine pays one absent
    /// hash lookup per event and nothing else.
    pub term_members: HashMap<(PredicateId, u16), TermMembers<B>>,
}

impl<I: IdTypes, B: Backend> Clone for PredicateStore<I, B> {
    fn clone(&self) -> Self {
        Self {
            predicates: self.predicates.clone(),
            hash_index: self.hash_index.clone(),
            bindings: self.bindings.clone(),
            scope_index: self.scope_index.clone(),
            predicate_consumers: self.predicate_consumers.clone(),
            binding_lookup: self.binding_lookup.clone(),
            term_members: self.term_members.clone(),
        }
    }
}

impl<I: IdTypes, B: Backend> PredicateStore<I, B> {
    /// Create new empty predicate store
    #[must_use]
    pub fn new() -> Self {
        Self {
            predicates: Slab::new(),
            hash_index: HashMap::new(),
            bindings: HashMap::new(),
            scope_index: HashMap::new(),
            predicate_consumers: HashMap::new(),
            binding_lookup: HashMap::new(),
            term_members: HashMap::new(),
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
    pub fn get_predicate(&self, id: PredicateId) -> Option<&Predicate<B>> {
        self.predicates.get(id.to_slab_index())
    }

    /// Get mutable predicate by ID
    #[must_use]
    pub fn get_predicate_mut(&mut self, id: PredicateId) -> Option<&mut Predicate<B>> {
        self.predicates.get_mut(id.to_slab_index())
    }

    /// Add new predicate
    ///
    /// Returns allocated `PredicateId` from slab insertion.
    pub fn add_predicate(&mut self, mut predicate: Predicate<B>) -> PredicateId {
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
            self.term_members.retain(|(pred, _), _| *pred != id);
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
            .map(alloc::vec::Vec::as_slice)
    }

    /// Returns `true` if any active binding references the given consumer.
    #[must_use]
    pub fn is_consumer_referenced(&self, consumer_id: I::ConsumerId) -> bool {
        self.bindings.values().any(|b| b.consumer_id == consumer_id)
    }

    /// Collect the set of distinct consumer IDs across all active bindings.
    #[must_use]
    pub fn active_consumer_ids(&self) -> HashSet<I::ConsumerId> {
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

        let subs = self
            .binding_lookup
            .entry((pred_id, consumer_ord))
            .or_default();
        if !subs.contains(&sub_id) {
            subs.push(sub_id);
        }
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
            // Under the same guard as the bitmap: the ordinal is what a term
            // admits, so it stays while any binding still holds it, and a
            // stale ordinal would admit rows to a subscription that is gone.
            for ((pred, _), members) in &mut self.term_members {
                if *pred == binding.predicate_id {
                    members.forget(binding.consumer_ordinal);
                }
            }
        }

        let lookup_key = (binding.predicate_id, binding.consumer_ordinal);
        if let Some(subs) = self.binding_lookup.get_mut(&lookup_key) {
            subs.retain(|&id| id != sub_id);
            if subs.is_empty() {
                self.binding_lookup.remove(&lookup_key);
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

    /// Record that `ordinal` filters for `subscriber` through the term in
    /// `slot` of `pred`, and matches `values` today.
    ///
    /// The values are what the subscription stated at registration, and the
    /// identity is what a changed membership row is matched against. An empty
    /// `values` admits nobody until such a row arrives, which is the partial
    /// list a client is allowed to send.
    pub fn seed_term(
        &mut self,
        pred: PredicateId,
        slot: u16,
        ordinal: ConsumerOrdinal,
        subscriber: TermKey<B>,
        values: Vec<TermKey<B>>,
    ) {
        let members = self.term_members.entry((pred, slot)).or_default();
        members.claim(subscriber, ordinal);
        for value in values {
            members.admit(value, ordinal);
        }
    }

    /// The subscribers one term admits, or [`None`] when the predicate carries
    /// no term in that slot.
    #[must_use]
    pub fn term_members(&self, pred: PredicateId, slot: u16) -> Option<&TermMembers<B>> {
        self.term_members.get(&(pred, slot))
    }
}

impl<I: IdTypes, B: Backend> Default for PredicateStore<I, B> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::clone_on_copy)]
mod tests {
    use super::*;
    use crate::backend::Postgres;
    use crate::compiler::{Instruction, PrefilterPlan};
    use crate::DefaultIds;

    fn make_predicate(id: usize, hash: u128, refcount: u32) -> Predicate<Postgres> {
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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        let pred = make_predicate(0, 0x1234, 1);
        let id = store.add_predicate(pred);

        assert_eq!(store.find_by_hash(0x1234), Some(id));
        assert!(store.get_predicate(id).is_some());
    }

    #[test]
    fn test_refcount_increment() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        let pred = make_predicate(0, 0x1234, 1);
        let id = store.add_predicate(pred);

        assert_eq!(store.get_predicate(id).unwrap().refcount, 1);

        store.increment_refcount(id);
        assert_eq!(store.get_predicate(id).unwrap().refcount, 2);
    }

    #[test]
    fn test_refcount_decrement() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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

    #[test]
    fn test_increment_refcount_nonexistent() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        // Try to increment refcount of non-existent predicate
        let fake_id = PredicateId::from_slab_index(999);
        let result = store.increment_refcount(fake_id);
        assert!(!result);
    }

    #[test]
    fn test_decrement_refcount_nonexistent() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        // Try to decrement refcount of non-existent predicate
        let fake_id = PredicateId::from_slab_index(999);
        let result = store.decrement_refcount(fake_id);
        assert!(!result);
    }

    #[test]
    fn test_predicate_store_default() {
        let store = PredicateStore::<DefaultIds, Postgres>::default();
        assert!(store.predicates.is_empty());
    }

    #[test]
    fn test_predicate_store_rejects_or_normalizes_mismatched_predicate_id() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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

    /// `binding_lookup` resolves
    /// `(predicate_id, consumer_ordinal) -> Vec<SubscriptionId>`. Used by
    /// activity-aware eviction policies to stamp the right subscriptions
    /// after dispatch matches a predicate.
    #[test]
    fn test_binding_lookup_resolves_subscription_id() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();
        let pred = make_predicate(0, 0xBEEF, 1);
        let pred_id = store.add_predicate(pred);
        let ord = ConsumerOrdinal::new(7);

        store.add_binding(SubscriptionBinding {
            subscription_id: 555,
            predicate_id: pred_id,
            consumer_id: 42,
            consumer_ordinal: ord,
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        });

        assert_eq!(
            store.binding_lookup.get(&(pred_id, ord)),
            Some(&vec![555]),
            "binding_lookup must surface the subscription id for the matched pair"
        );

        let _ = store.remove_binding(555);
        assert!(
            !store.binding_lookup.contains_key(&(pred_id, ord)),
            "binding_lookup must be pruned when no bindings remain"
        );
    }

    /// Two scopes on the same (predicate, consumer) pair both
    /// surface from `binding_lookup`.
    #[test]
    fn test_binding_lookup_handles_multiple_scopes_per_pair() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();
        let pred = make_predicate(0, 0xCAFE, 1);
        let pred_id = store.add_predicate(pred);
        let ord = ConsumerOrdinal::new(0);

        store.add_binding(SubscriptionBinding {
            subscription_id: 1,
            predicate_id: pred_id,
            consumer_id: 9,
            consumer_ordinal: ord,
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        });
        store.add_binding(SubscriptionBinding {
            subscription_id: 2,
            predicate_id: pred_id,
            consumer_id: 9,
            consumer_ordinal: ord,
            scope: SubscriptionScope::Session(100),
            updated_at_unix_ms: 0,
        });

        let mut subs = store
            .binding_lookup
            .get(&(pred_id, ord))
            .cloned()
            .unwrap_or_default();
        subs.sort_unstable();
        assert_eq!(subs, vec![1, 2]);

        let _ = store.remove_binding(1);
        assert_eq!(
            store.binding_lookup.get(&(pred_id, ord)),
            Some(&vec![2]),
            "removing one of two bindings leaves the other in the lookup"
        );
    }

    #[test]
    fn test_active_consumer_ids() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

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
