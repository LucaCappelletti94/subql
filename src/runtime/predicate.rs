//! Predicate storage with deduplication and refcounting

use super::ids::{ConsumerOrdinal, PredicateHash, PredicateId};
use super::indexes::IndexableAtom;
use crate::backend::Backend;
use crate::term::{TermKey, TermRow};
use crate::{
    compiler::{sql_shape::QueryProjection, BytecodeProgram, PrefilterPlan},
    ColumnId, IdTypes, SubscriptionId, SubscriptionScope,
};
use alloc::sync::Arc;
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use roaring::RoaringBitmap;
use rpds::{HashTrieMapSync, RedBlackTreeSetSync};

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
    /// Canonical group identity selected during planning.
    pub group_key_encoder: Option<crate::backend::GroupKeyEncoder<B>>,
    /// Reference count (number of subscriptions using this predicate).
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
            group_key_encoder: self.group_key_encoder.clone(),
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
            .field("group_key_encoder", &self.group_key_encoder)
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
///
/// A caller states a set of subjects, so the second question is ambiguous when a
/// row disappears: two of its subjects may grant the same value, and dropping the
/// caller because one of them stopped would lose rows the other still reaches,
/// permanently. The granting subjects are therefore kept, for the ordinals
/// claiming more than one and no others.
pub struct TermMembers<B: Backend> {
    /// Which consumer ordinals a compared value row admits, keyed by the
    /// values in the filter's column order (one-wide for a single-column
    /// term).
    by_value: TermAdmissions<B>,
    /// Which consumer ordinals claim each subscriber identity.
    by_subscriber: HashTrieMapSync<TermKey<B>, RoaringBitmap>,
    /// Ordinals holding at least one claim, so seeding one a second time is
    /// recognised without reading the claim index backwards.
    claimed: RoaringBitmap,
    /// Ordinals claiming more than one subject, the only ones whose admissions
    /// are attributed.
    several: RoaringBitmap,
    /// Which subjects of a several-subject ordinal grant a value row, keyed by
    /// ordinal first so a withdrawal reaches them through the row it holds.
    granted_by: HashTrieMapSync<ConsumerOrdinal, TermGrants<B>>,
}

/// Which ordinals each compared value row admits.
pub type TermAdmissions<B> = HashTrieMapSync<TermRow<B>, RoaringBitmap>;

/// Which subjects of one ordinal grant each value row it holds.
pub type TermGrants<B> = HashTrieMapSync<TermRow<B>, Vec<TermKey<B>>>;

// `Clone` and `Debug` are hand-implemented so their bounds fall on the scalar
// types `TermKey<B>` names rather than on the backend marker `B`, for the same
// reason `Value<B>`'s are. The maps are tries, so this shares their nodes.
impl<B: Backend> Clone for TermMembers<B> {
    fn clone(&self) -> Self {
        Self {
            by_value: self.by_value.clone(),
            by_subscriber: self.by_subscriber.clone(),
            claimed: self.claimed.clone(),
            several: self.several.clone(),
            granted_by: self.granted_by.clone(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for TermMembers<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TermMembers")
            .field("by_value", &self.by_value)
            .field("by_subscriber", &self.by_subscriber)
            .field("claimed", &self.claimed)
            .field("several", &self.several)
            .field("granted_by", &self.granted_by)
            .finish()
    }
}

impl<B: Backend> Default for TermMembers<B> {
    fn default() -> Self {
        Self {
            by_value: TermAdmissions::new_sync(),
            by_subscriber: HashTrieMapSync::new_sync(),
            claimed: RoaringBitmap::new(),
            several: RoaringBitmap::new(),
            granted_by: HashTrieMapSync::new_sync(),
        }
    }
}

impl<B: Backend> TermMembers<B> {
    /// The consumer ordinals `values` admit, empty when they admit none.
    #[must_use]
    pub fn admits(&self, values: &[TermKey<B>]) -> Option<&RoaringBitmap> {
        self.by_value.get(values)
    }

    /// The ordinals `values` admit today, ready to be changed and put back.
    fn admitted(&self, values: &[TermKey<B>]) -> RoaringBitmap {
        self.by_value.get(values).cloned().unwrap_or_default()
    }

    /// Record that `ordinal` matches `values` through this term, granted by
    /// `subject`.
    fn admit(&mut self, subject: &TermKey<B>, values: TermRow<B>, ordinal: ConsumerOrdinal) {
        if self.several.contains(ordinal.get()) {
            self.grant(ordinal, &values, subject);
        }
        let mut admitted = self.admitted(&values);
        admitted.insert(ordinal.get());
        self.by_value.insert_mut(values, admitted);
    }

    /// Record that `subject` grants `values` to `ordinal`.
    fn grant(&mut self, ordinal: ConsumerOrdinal, values: &TermRow<B>, subject: &TermKey<B>) {
        let mut rows = self
            .granted_by
            .get(&ordinal)
            .cloned()
            .unwrap_or_else(TermGrants::new_sync);
        let mut granting = rows.get(values).cloned().unwrap_or_default();
        if !granting.contains(subject) {
            granting.push(subject.clone());
        }
        rows.insert_mut(values.clone(), granting);
        self.granted_by.insert_mut(ordinal, rows);
    }

    /// Record that `ordinal` filters for every subject in `subjects`.
    ///
    /// An ordinal claiming several of them has its admissions attributed from
    /// here on. One that claimed a single subject until now was admitted
    /// through that subject and no other, so the rows it already holds are
    /// attributed to it here rather than left for the first withdrawal to take.
    fn claim(&mut self, subjects: &[TermKey<B>], ordinal: ConsumerOrdinal) {
        let held = if self.claimed.contains(ordinal.get()) && !self.several.contains(ordinal.get())
        {
            self.subjects_of(ordinal)
        } else {
            Vec::new()
        };
        for subject in subjects {
            let mut claiming = self.by_subscriber.get(subject).cloned().unwrap_or_default();
            claiming.insert(ordinal.get());
            self.by_subscriber.insert_mut(subject.clone(), claiming);
        }
        self.claimed.insert(ordinal.get());

        if self.several.contains(ordinal.get()) {
            return;
        }
        let distinct = held.len() + subjects.iter().filter(|new| !held.contains(new)).count();
        if distinct < 2 {
            return;
        }
        self.several.insert(ordinal.get());
        let [granted] = held.as_slice() else {
            return;
        };
        let mut backfilled = TermGrants::new_sync();
        for (values, admitted) in &self.by_value {
            if admitted.contains(ordinal.get()) {
                backfilled.insert_mut(values.clone(), alloc::vec![granted.clone()]);
            }
        }
        self.granted_by.insert_mut(ordinal, backfilled);
    }

    /// The subjects `ordinal` claims today.
    ///
    /// Reads the claim index backwards, which only a second seeding of an
    /// ordinal already claiming one subject needs.
    fn subjects_of(&self, ordinal: ConsumerOrdinal) -> Vec<TermKey<B>> {
        self.by_subscriber
            .iter()
            .filter(|(_, ordinals)| ordinals.contains(ordinal.get()))
            .map(|(subject, _)| subject.clone())
            .collect()
    }

    /// The consumer ordinals filtering for `subscriber`.
    #[must_use]
    pub fn claimed_by(&self, subscriber: &TermKey<B>) -> Option<&RoaringBitmap> {
        self.by_subscriber.get(subscriber)
    }

    /// Add `ordinals` to the set `values` admit, as a membership row naming
    /// `subject` appearing does, and report the ones that did not hold it
    /// already.
    pub fn widen(
        &mut self,
        values: TermRow<B>,
        subject: &TermKey<B>,
        ordinals: &RoaringBitmap,
    ) -> RoaringBitmap {
        for ordinal in &(ordinals & &self.several) {
            self.grant(ConsumerOrdinal::new(ordinal), &values, subject);
        }
        let mut admitted = self.admitted(&values);
        let entered = ordinals - &admitted;
        admitted |= ordinals;
        self.by_value.insert_mut(values, admitted);
        entered
    }

    /// Take `ordinals` out of the set `values` admit, as a membership row
    /// naming `subject` disappearing does, and report the ones that left.
    ///
    /// An ordinal claiming several subjects keeps the row while another of them
    /// still grants it, which is the whole reason the grants are kept.
    pub fn narrow(
        &mut self,
        values: &[TermKey<B>],
        subject: &TermKey<B>,
        ordinals: &RoaringBitmap,
    ) -> RoaringBitmap {
        let Some(admitted) = self.by_value.get(values) else {
            return RoaringBitmap::new();
        };
        let mut left = ordinals & admitted;
        for ordinal in &(&left & &self.several) {
            let ordinal = ConsumerOrdinal::new(ordinal);
            let Some(rows) = self.granted_by.get(&ordinal) else {
                continue;
            };
            let Some(granting) = rows.get(values) else {
                continue;
            };
            let mut granting = granting.clone();
            granting.retain(|held| held != subject);
            let mut rows = rows.clone();
            if granting.is_empty() {
                rows.remove_mut(values);
            } else {
                left.remove(ordinal.get());
                rows.insert_mut(values.to_vec(), granting);
            }
            self.granted_by.insert_mut(ordinal, rows);
        }
        let mut admitted = self.admitted(values);
        admitted -= &left;
        if admitted.is_empty() {
            self.by_value.remove_mut(values);
        } else {
            self.by_value.insert_mut(values.to_vec(), admitted);
        }
        left
    }

    /// Take every value this term admits, leaving it admitting none.
    ///
    /// The subscriber claims stay: the subscriptions are still registered and
    /// still filter for the same identities, so a membership row appearing again
    /// moves them back.
    pub fn clear_admissions(&mut self) -> Vec<(TermRow<B>, RoaringBitmap)> {
        let withdrawn = self
            .by_value
            .iter()
            .map(|(values, admitted)| (values.clone(), admitted.clone()))
            .collect();
        self.by_value = TermAdmissions::new_sync();
        self.granted_by = HashTrieMapSync::new_sync();
        withdrawn
    }

    /// Drop `ordinal` from every set, as unbinding its subscription does.
    fn forget(&mut self, ordinal: ConsumerOrdinal) {
        self.by_value = forgotten(&self.by_value, ordinal);
        self.by_subscriber = forgotten(&self.by_subscriber, ordinal);
        self.granted_by.remove_mut(&ordinal);
        self.claimed.remove(ordinal.get());
        self.several.remove(ordinal.get());
    }
}

/// `index` without `ordinal`, dropping every key it leaves admitting nobody.
///
/// Only the keys that held the ordinal are rewritten, so unbinding one
/// subscription reads the index but copies the paths it actually changes.
fn forgotten<K>(
    index: &HashTrieMapSync<K, RoaringBitmap>,
    ordinal: ConsumerOrdinal,
) -> HashTrieMapSync<K, RoaringBitmap>
where
    K: Eq + core::hash::Hash + Clone,
{
    let mut kept = index.clone();
    for (key, set) in index {
        if !set.contains(ordinal.get()) {
            continue;
        }
        let mut set = set.clone();
        set.remove(ordinal.get());
        if set.is_empty() {
            kept.remove_mut(key);
        } else {
            kept.insert_mut(key.clone(), set);
        }
    }
    kept
}

/// Every predicate of one table, by id.
pub type PredicateMap<B> = HashTrieMapSync<PredicateId, Predicate<B>>;

/// Which subscribers each of a predicate's term slots admits, one shared entry
/// per slot.
pub type TermSlots<B> = HashMap<(PredicateId, u16), Arc<TermMembers<B>>>;

/// Every index is shared rather than inline.
///
/// A snapshot is published after each mutation, and the next mutation clones
/// whatever it finds shared. Inline, that was the whole store on every
/// membership event, including the bindings and predicates the event never
/// reads. Shared, the clone copies seven pointers and deepens only into the
/// index the mutation reaches.
/// What a removed binding took with it.
#[derive(Clone, Copy, Debug)]
pub struct BindingRemoved<I: IdTypes> {
    /// The binding that was removed.
    pub binding: SubscriptionBinding<I>,
    /// Whether it was the last reference, so the predicate went too.
    pub predicate_removed: bool,
}

/// Subscriptions, ordered so a walk over them is deterministic and an insert
/// copies a path instead of the whole set.
pub type SubscriptionSet = RedBlackTreeSetSync<SubscriptionId>;

/// Everything a predicate's bindings answer.
///
/// One record rather than three maps keyed alike, because a binding used to
/// write all three and they could only agree by hand.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PredicateBindings {
    /// How many bindings hold the predicate.
    pub refcount: u32,
    /// The consumer ordinals holding it.
    pub consumers: RoaringBitmap,
    /// The subscriptions each ordinal holds it through.
    pub by_ordinal: HashTrieMapSync<ConsumerOrdinal, SubscriptionSet>,
}

pub struct PredicateStore<I: IdTypes, B: Backend> {
    /// Every predicate on this table, by id.
    pub predicates: PredicateMap<B>,
    /// The next id to hand out when nothing has been released.
    next_predicate: usize,
    /// Ids released by removed predicates, newest first.
    ///
    /// An id is a bit in every candidate bitmap, so handing them out fresh
    /// forever would spread those bitmaps over an ever higher range and walk
    /// a churning partition towards the `u32` ceiling. Reusing the last
    /// released id keeps the space as dense as the slab this replaced.
    released: Vec<PredicateId>,
    /// Hash -> candidate PredicateIds (for deduplication with collision checks).
    pub hash_index: HashTrieMapSync<PredicateHash, Vec<PredicateId>>,
    /// SubscriptionId -> SubscriptionBinding.
    pub bindings: HashTrieMapSync<SubscriptionId, SubscriptionBinding<I>>,
    /// SessionId -> `Vec<SubscriptionId>` (for session cleanup).
    pub scope_index: HashTrieMapSync<I::SessionId, SubscriptionSet>,
    /// PredicateId -> who holds it, and through which subscriptions.
    pub bound: HashTrieMapSync<PredicateId, PredicateBindings>,
    /// How many bindings each consumer holds on this table.
    ///
    /// Answers whether a consumer is still referenced without walking every
    /// binding, which unbinding asked once per subscription.
    pub consumer_bindings: HashTrieMapSync<I::ConsumerId, u32>,
    /// (PredicateId, term slot) -> which subscribers that term admits.
    ///
    /// Empty for every predicate carrying no membership term, which is every
    /// predicate until one is registered, so a term-free engine pays one absent
    /// hash lookup per event and nothing else.
    ///
    /// Each slot is shared on its own, so a membership row touching one term
    /// copies that term's members and leaves every other slot's pointer alone.
    pub term_members: Arc<TermSlots<B>>,
}

// Every field is shared, so this is seven pointer copies. It stays hand-written
// for the same reason `TermMembers`'s is, to keep the bounds off the backend
// marker, and every mutation below goes through `Arc::make_mut` so the sharing
// stays invisible to a reader.
impl<I: IdTypes, B: Backend> Clone for PredicateStore<I, B> {
    fn clone(&self) -> Self {
        Self {
            predicates: self.predicates.clone(),
            next_predicate: self.next_predicate,
            released: self.released.clone(),
            hash_index: self.hash_index.clone(),
            bindings: self.bindings.clone(),
            scope_index: self.scope_index.clone(),
            bound: self.bound.clone(),
            consumer_bindings: self.consumer_bindings.clone(),
            term_members: Arc::clone(&self.term_members),
        }
    }
}

impl<I: IdTypes, B: Backend> PredicateStore<I, B> {
    /// Create new empty predicate store
    #[must_use]
    pub fn new() -> Self {
        Self {
            predicates: PredicateMap::new_sync(),
            next_predicate: 0,
            released: Vec::new(),
            hash_index: HashTrieMapSync::new_sync(),
            bindings: HashTrieMapSync::new_sync(),
            scope_index: HashTrieMapSync::new_sync(),
            bound: HashTrieMapSync::new_sync(),
            consumer_bindings: HashTrieMapSync::new_sync(),
            term_members: Arc::new(HashMap::new()),
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
        self.predicates.get(&id)
    }

    /// The members of one term slot, ready to be changed.
    ///
    /// Deepens the sharing exactly twice, once for the slot table and once for
    /// the slot itself, so a membership row leaves every other slot shared.
    pub(super) fn term_members_mut(
        &mut self,
        pred: PredicateId,
        slot: u16,
    ) -> Option<&mut TermMembers<B>> {
        Arc::make_mut(&mut self.term_members)
            .get_mut(&(pred, slot))
            .map(Arc::make_mut)
    }

    /// Add new predicate
    ///
    /// Returns allocated `PredicateId` from slab insertion.
    pub fn add_predicate(&mut self, mut predicate: Predicate<B>) -> PredicateId {
        let id = self.released.pop().unwrap_or_else(|| {
            let id = PredicateId::from_slab_index(self.next_predicate);
            self.next_predicate += 1;
            id
        });
        let hash = predicate.hash;
        predicate.id = id;

        self.predicates.insert_mut(id, predicate);
        let mut candidates = self.hash_index.get(&hash).cloned().unwrap_or_default();
        candidates.push(id);
        self.hash_index.insert_mut(hash, candidates);

        id
    }

    /// How many bindings hold `id`.
    #[must_use]
    pub fn refcount(&self, id: PredicateId) -> u32 {
        self.bound.get(&id).map_or(0, |held| held.refcount)
    }

    /// The consumer ordinals holding `id`.
    #[must_use]
    pub fn consumers_of(&self, id: PredicateId) -> Option<&RoaringBitmap> {
        self.bound.get(&id).map(|held| &held.consumers)
    }

    /// The subscriptions through which `ordinal` holds `id`.
    #[must_use]
    pub fn subscriptions_of(
        &self,
        id: PredicateId,
        ordinal: ConsumerOrdinal,
    ) -> Option<&SubscriptionSet> {
        self.bound
            .get(&id)
            .and_then(|held| held.by_ordinal.get(&ordinal))
    }

    /// The ids [`subscriptions_of`](Self::subscriptions_of) holds, in order.
    ///
    /// A tree iterator heap-allocates its stack, so the one-id set, which
    /// dedup makes the usual case, is read through `first` instead.
    pub fn subscription_ids_of(
        &self,
        id: PredicateId,
        ordinal: ConsumerOrdinal,
    ) -> impl Iterator<Item = SubscriptionId> + '_ {
        let set = self.subscriptions_of(id, ordinal);
        let one = set
            .filter(|set| set.size() == 1)
            .and_then(|set| set.first());
        let many = set.filter(|set| set.size() > 1).map(|set| set.iter());
        one.into_iter().chain(many.into_iter().flatten()).copied()
    }

    /// Every predicate that has a holder, with the ordinals holding it.
    pub fn held_predicates(&self) -> impl Iterator<Item = (PredicateId, &RoaringBitmap)> {
        self.bound
            .iter()
            .map(|(pred_id, held)| (*pred_id, &held.consumers))
    }

    /// Read, change and write one predicate's record in a single path copy.
    fn with_bindings(&mut self, id: PredicateId, change: impl FnOnce(&mut PredicateBindings)) {
        let mut held = self.bound.get(&id).cloned().unwrap_or_default();
        change(&mut held);
        if held.refcount == 0 && held.by_ordinal.is_empty() {
            self.bound.remove_mut(&id);
        } else {
            self.bound.insert_mut(id, held);
        }
    }

    /// Remove predicate completely
    fn remove_predicate(&mut self, id: PredicateId) {
        let Some(hash) = self.predicates.get(&id).map(|pred| pred.hash) else {
            return;
        };
        self.predicates.remove_mut(&id);
        self.released.push(id);
        {
            if let Some(ids) = self.hash_index.get(&hash) {
                let mut ids = ids.clone();
                ids.retain(|existing| *existing != id);
                if ids.is_empty() {
                    self.hash_index.remove_mut(&hash);
                } else {
                    self.hash_index.insert_mut(hash, ids);
                }
            }
            self.bound.remove_mut(&id);
            Arc::make_mut(&mut self.term_members).retain(|(pred, _), _| *pred != id);
        }
    }

    /// Add subscription binding
    pub fn add_binding(&mut self, binding: SubscriptionBinding<I>) {
        let sub_id = binding.subscription_id;

        // Overwrite-safe upsert: remove previous secondary index entries when
        // replacing an existing subscription ID.
        let previous = self.bindings.get(&sub_id).copied();
        self.bindings.insert_mut(sub_id, binding);
        if let Some(previous) = previous {
            self.remove_binding_indexes(previous);
        }

        self.add_binding_indexes(binding);
    }

    /// Remove subscription binding
    ///
    /// Returns the removed binding if it existed.
    pub fn remove_binding(&mut self, sub_id: SubscriptionId) -> Option<BindingRemoved<I>> {
        let binding = self.bindings.get(&sub_id).copied()?;
        self.bindings.remove_mut(&sub_id);

        self.remove_binding_indexes(binding);

        // The binding was the reference, so the predicate goes with the last
        // one. Nothing else takes or drops a reference, which is why the
        // count cannot disagree with the bindings that produced it.
        let predicate_removed = self.refcount(binding.predicate_id) == 0
            && self.get_predicate(binding.predicate_id).is_some();
        if predicate_removed {
            self.remove_predicate(binding.predicate_id);
        }

        Some(BindingRemoved {
            binding,
            predicate_removed,
        })
    }

    /// Get all subscription IDs for a session
    #[must_use]
    pub fn get_session_subscriptions(&self, session_id: I::SessionId) -> Option<&SubscriptionSet> {
        self.scope_index.get(&session_id)
    }

    /// Returns `true` if any active binding references the given consumer.
    #[must_use]
    pub fn is_consumer_referenced(&self, consumer_id: I::ConsumerId) -> bool {
        self.consumer_bindings.contains_key(&consumer_id)
    }

    /// Collect the set of distinct consumer IDs across all active bindings.
    #[must_use]
    pub fn active_consumer_ids(&self) -> HashSet<I::ConsumerId> {
        self.consumer_bindings.keys().copied().collect()
    }

    fn add_binding_indexes(&mut self, binding: SubscriptionBinding<I>) {
        let sub_id = binding.subscription_id;
        let pred_id = binding.predicate_id;
        let consumer_ord = binding.consumer_ordinal;

        if let SubscriptionScope::Session(sid) = binding.scope {
            let mut session = self.scope_index.get(&sid).cloned().unwrap_or_default();
            session.insert_mut(sub_id);
            self.scope_index.insert_mut(sid, session);
        }

        let held = self
            .consumer_bindings
            .get(&binding.consumer_id)
            .copied()
            .unwrap_or_default();
        self.consumer_bindings
            .insert_mut(binding.consumer_id, held + 1);

        self.with_bindings(pred_id, |held| {
            held.refcount += 1;
            held.consumers.insert(consumer_ord.get());
            let mut bound = held
                .by_ordinal
                .get(&consumer_ord)
                .cloned()
                .unwrap_or_default();
            bound.insert_mut(sub_id);
            held.by_ordinal.insert_mut(consumer_ord, bound);
        });
    }

    fn remove_binding_indexes(&mut self, binding: SubscriptionBinding<I>) {
        let sub_id = binding.subscription_id;
        let pred_id = binding.predicate_id;
        let consumer_ord = binding.consumer_ordinal;

        let held = self
            .consumer_bindings
            .get(&binding.consumer_id)
            .copied()
            .unwrap_or_default()
            .saturating_sub(1);
        if held == 0 {
            self.consumer_bindings.remove_mut(&binding.consumer_id);
        } else {
            self.consumer_bindings.insert_mut(binding.consumer_id, held);
        }

        // The record is keyed by exactly this question, and the upsert path
        // asks it while the replacement already sits under the same id.
        let replaced_in_place = self.bindings.get(&sub_id).is_some_and(|replacement| {
            replacement.predicate_id == pred_id && replacement.consumer_ordinal == consumer_ord
        });
        let mut ordinal_left = false;
        self.with_bindings(pred_id, |held| {
            held.refcount = held.refcount.saturating_sub(1);
            let remaining = held.by_ordinal.get(&consumer_ord).map(|bound| {
                let mut bound = bound.clone();
                bound.remove_mut(&sub_id);
                bound
            });
            match remaining {
                Some(bound) if bound.is_empty() => {
                    held.by_ordinal.remove_mut(&consumer_ord);
                }
                Some(bound) => {
                    held.by_ordinal.insert_mut(consumer_ord, bound);
                    ordinal_left = true;
                }
                None => {}
            }
            if !ordinal_left && !replaced_in_place {
                held.consumers.remove(consumer_ord.get());
            }
        });

        if !ordinal_left && !replaced_in_place {
            // Under the same guard as the ordinal: the ordinal is what a term
            // admits, so it stays while any binding still holds it, and a
            // stale ordinal would admit rows to a subscription that is gone.
            for ((pred, _), members) in Arc::make_mut(&mut self.term_members) {
                if *pred == pred_id {
                    Arc::make_mut(members).forget(consumer_ord);
                }
            }
        }

        if let SubscriptionScope::Session(session_id) = binding.scope {
            if let Some(session) = self.scope_index.get(&session_id) {
                let mut session = session.clone();
                session.remove_mut(&sub_id);
                if session.is_empty() {
                    self.scope_index.remove_mut(&session_id);
                } else {
                    self.scope_index.insert_mut(session_id, session);
                }
            }
        }
    }

    /// Record that `ordinal` filters for `subjects` through the term in `slot`
    /// of `pred`, and matches `rows` today, each through the subject granting
    /// it.
    ///
    /// The rows are what the subscription stated at registration, and the
    /// subjects are what a changed membership row is matched against. Empty
    /// `rows` admit nobody until such a row arrives, which is the partial list
    /// a client is allowed to send.
    pub fn seed_term(
        &mut self,
        pred: PredicateId,
        slot: u16,
        ordinal: ConsumerOrdinal,
        subjects: &[TermKey<B>],
        rows: Vec<(TermKey<B>, TermRow<B>)>,
    ) {
        let members = Arc::make_mut(
            Arc::make_mut(&mut self.term_members)
                .entry((pred, slot))
                .or_default(),
        );
        members.claim(subjects, ordinal);
        for (subject, row) in rows {
            members.admit(&subject, row, ordinal);
        }
    }

    /// The subscribers one term admits, or [`None`] when the predicate carries
    /// no term in that slot.
    #[must_use]
    pub fn term_members(&self, pred: PredicateId, slot: u16) -> Option<&TermMembers<B>> {
        self.term_members.get(&(pred, slot)).map(|slot| &**slot)
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

    fn make_predicate(id: usize, hash: u128) -> Predicate<Postgres> {
        Predicate {
            id: PredicateId::from_slab_index(id),
            hash,
            normalized_sql: "test".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::Not])),
            dependency_columns: Arc::from([]),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            projection: QueryProjection::Rows,
            group_key_encoder: None,
            updated_at_unix_ms: 0,
        }
    }

    #[test]
    fn test_add_and_find_predicate() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        let pred = make_predicate(0, 0x1234);
        let id = store.add_predicate(pred);

        assert_eq!(store.find_by_hash(0x1234), Some(id));
        assert!(store.get_predicate(id).is_some());
    }

    /// A binding is the reference, so two of them hold the predicate and the
    /// second removal is what takes it away.
    #[test]
    fn the_last_binding_takes_the_predicate_with_it() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();
        let id = store.add_predicate(make_predicate(0, 0x1234));

        for sub_id in [1u64, 2] {
            store.add_binding(SubscriptionBinding {
                subscription_id: sub_id,
                predicate_id: id,
                consumer_id: sub_id,
                consumer_ordinal: ConsumerOrdinal::new(u32::try_from(sub_id).unwrap()),
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 0,
            });
        }
        assert_eq!(store.refcount(id), 2);

        let first = store.remove_binding(1).expect("bound above");
        assert!(!first.predicate_removed, "one holder is left");
        assert_eq!(store.refcount(id), 1);
        assert!(store.get_predicate(id).is_some());

        let second = store.remove_binding(2).expect("bound above");
        assert!(second.predicate_removed, "and none after this one");
        assert!(store.get_predicate(id).is_none());
        assert_eq!(store.refcount(id), 0);
    }

    #[test]
    fn admitting_one_row_leaves_the_other_sets_where_they_were() {
        let subject = TermKey::<Postgres>::String("alice".into());
        let mut members = TermMembers::<Postgres>::default();
        let ordinal = ConsumerOrdinal::new(0);
        members.claim(core::slice::from_ref(&subject), ordinal);
        for value in 0..256i64 {
            members.admit(&subject, alloc::vec![TermKey::Int(value)], ordinal);
        }

        let before = members.clone();
        members.admit(&subject, alloc::vec![TermKey::Int(1_000)], ordinal);

        let moved = (0..256i64)
            .filter(|value| {
                let row = [TermKey::Int(*value)];
                let (Some(then), Some(now)) = (before.admits(&row), members.admits(&row)) else {
                    return true;
                };
                !core::ptr::eq(then, now)
            })
            .count();
        assert!(
            moved <= 8,
            "admitting one row rewrote the set of {moved} rows it never named, and only the \
             entries sharing the rewritten leaf may move"
        );
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

        let bitmap = store.consumers_of(pred_id).unwrap();
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(1));
        assert_eq!(bitmap.len(), 2);
    }

    #[test]
    fn test_add_binding_overwrite_cleans_secondary_indexes() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        let pred1 = make_predicate(0, 0x1111);
        let pred2 = make_predicate(1, 0x2222);
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
            .consumers_of(pred1_id)
            .is_some_and(|bitmap| bitmap.contains(0)));
        assert!(store
            .consumers_of(pred2_id)
            .is_some_and(|bitmap| bitmap.contains(1)));
        assert!(store.get_session_subscriptions(500).is_none());
        assert_eq!(
            store
                .get_session_subscriptions(600)
                .map(|held| held.iter().copied().collect::<Vec<_>>()),
            Some(vec![100])
        );
    }

    #[test]
    fn test_remove_binding_keeps_bitmap_when_same_consumer_has_another_binding() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        let pred = make_predicate(0, 0x3333);
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
            .consumers_of(pred_id)
            .expect("bitmap should remain while one binding still exists");
        assert!(bitmap.contains(0));

        let _ = store.remove_binding(202);
        assert!(
            store.consumers_of(pred_id).is_none(),
            "bitmap should be removed after last binding is removed"
        );
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
        let pred = make_predicate(99, 0xBEEF);
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

        let mut pred1 = make_predicate(0, 0x00C0_FFEE);
        pred1.normalized_sql = "amount > 100".into();
        let id1 = store.add_predicate(pred1);

        let mut pred2 = make_predicate(1, 0x00C0_FFEE);
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

        let pred = make_predicate(0, 0xAABB);
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
        let pred = make_predicate(0, 0xBEEF);
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
            store
                .subscriptions_of(pred_id, ord)
                .map(|bound| bound.iter().copied().collect::<Vec<_>>()),
            Some(vec![555]),
            "the record must surface the subscription id for the matched pair"
        );

        let _ = store.remove_binding(555);
        assert!(
            store.subscriptions_of(pred_id, ord).is_none(),
            "the ordinal must be pruned when no bindings remain"
        );
    }

    /// Two scopes on the same (predicate, consumer) pair both
    /// surface from `binding_lookup`.
    #[test]
    fn test_binding_lookup_handles_multiple_scopes_per_pair() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();
        let pred = make_predicate(0, 0xCAFE);
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

        let subs: Vec<_> = store
            .subscriptions_of(pred_id, ord)
            .map(|bound| bound.iter().copied().collect())
            .unwrap_or_default();
        assert_eq!(subs, vec![1, 2]);

        let _ = store.remove_binding(1);
        assert_eq!(
            store
                .subscriptions_of(pred_id, ord)
                .map(|bound| bound.iter().copied().collect::<Vec<_>>()),
            Some(vec![2]),
            "removing one of two bindings leaves the other on the record"
        );
    }

    /// The count `is_consumer_referenced` reads has to say what a walk over
    /// the bindings would. A scan cannot drift, a counter can, and this one
    /// decides whether a live consumer is still addressed, so it is checked
    /// against the walk it replaced through a register, an overwrite onto
    /// another consumer, and an unbind.
    #[test]
    fn the_consumer_count_says_what_walking_the_bindings_says() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();
        let pred_id = store.add_predicate(make_predicate(0, 0xAABB));
        let bind = |store: &mut PredicateStore<DefaultIds, Postgres>,
                    subscription_id: u64,
                    consumer_id: u64| {
            store.add_binding(SubscriptionBinding {
                subscription_id,
                predicate_id: pred_id,
                consumer_id,
                consumer_ordinal: ConsumerOrdinal::new(
                    u32::try_from(consumer_id).expect("the fixture's consumers are small"),
                ),
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 0,
            });
        };
        let agrees = |store: &PredicateStore<DefaultIds, Postgres>| {
            (0..4u64).all(|consumer| {
                let walked = store
                    .bindings
                    .values()
                    .filter(|binding| binding.consumer_id == consumer)
                    .count();
                store.is_consumer_referenced(consumer) == (walked > 0)
            })
        };

        bind(&mut store, 1, 1);
        bind(&mut store, 2, 1);
        bind(&mut store, 3, 2);
        assert!(agrees(&store), "after registering three bindings");

        // The same subscription id under another consumer: the replacement
        // takes the reference the previous binding held.
        bind(&mut store, 3, 3);
        assert!(
            agrees(&store),
            "after overwriting one onto another consumer"
        );
        assert!(
            !store.is_consumer_referenced(2),
            "consumer 2 held only that one"
        );

        store.remove_binding(1);
        assert!(agrees(&store), "after unbinding one of consumer 1's two");
        assert!(
            store.is_consumer_referenced(1),
            "the other one still holds it"
        );

        store.remove_binding(2);
        store.remove_binding(3);
        assert!(agrees(&store), "after unbinding the rest");
        assert!(
            store.consumer_bindings.is_empty(),
            "no binding left, so no consumer is counted"
        );
    }

    #[test]
    fn test_active_consumer_ids() {
        let mut store = PredicateStore::<DefaultIds, Postgres>::new();

        let pred = make_predicate(0, 0xCCDD);
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
