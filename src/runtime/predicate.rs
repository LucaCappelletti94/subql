//! Predicate storage with deduplication and refcounting

use std::sync::Arc;
use slab::Slab;
use ahash::AHashMap;
use roaring::RoaringBitmap;
use crate::{IdTypes, compiler::BytecodeProgram, ColumnId};
use super::ids::{PredicateId, UserOrdinal, PredicateHash};

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
    /// Reference count (number of subscriptions using this predicate)
    pub refcount: u32,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch)
    pub updated_at_unix_ms: u64,
}

/// Subscription binding (user → predicate → subscription)
#[derive(Debug)]
pub struct Binding<I: IdTypes> {
    /// Subscription identifier
    pub subscription_id: I::SubscriptionId,
    /// Predicate this subscription uses
    pub predicate_id: PredicateId,
    /// User who owns this subscription
    pub user_id: I::UserId,
    /// Dense user ordinal for bitmap indexing
    pub user_ordinal: UserOrdinal,
    /// Session ID if session-bound, None if durable
    pub session_id: Option<I::SessionId>,
    /// Timestamp for conflict resolution
    pub updated_at_unix_ms: u64,
}

impl<I: IdTypes> Clone for Binding<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: IdTypes> Copy for Binding<I> {}

/// Predicate storage with deduplication
///
/// Manages predicates with slab allocation and hash-based deduplication.
/// Tracks refcounts and automatically removes predicates when refcount reaches 0.
pub struct PredicateStore<I: IdTypes> {
    /// Slab-allocated predicates (stable IDs)
    pub predicates: Slab<Predicate>,
    /// Hash → PredicateId (for deduplication)
    pub hash_index: AHashMap<PredicateHash, PredicateId>,
    /// SubscriptionId → Binding
    pub bindings: AHashMap<I::SubscriptionId, Binding<I>>,
    /// SessionId → Vec<SubscriptionId> (for session cleanup)
    pub session_index: AHashMap<I::SessionId, Vec<I::SubscriptionId>>,
    /// PredicateId → RoaringBitmap<UserOrdinal> (users interested in this predicate)
    pub predicate_users: AHashMap<PredicateId, RoaringBitmap>,
}

impl<I: IdTypes> Clone for PredicateStore<I> {
    fn clone(&self) -> Self {
        Self {
            predicates: self.predicates.clone(),
            hash_index: self.hash_index.clone(),
            bindings: self.bindings.clone(),
            session_index: self.session_index.clone(),
            predicate_users: self.predicate_users.clone(),
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
            session_index: AHashMap::new(),
            predicate_users: AHashMap::new(),
        }
    }

    /// Find predicate by hash (for deduplication)
    #[must_use]
    pub fn find_by_hash(&self, hash: PredicateHash) -> Option<PredicateId> {
        self.hash_index.get(&hash).copied()
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
        self.hash_index.insert(hash, id);

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
            self.hash_index.remove(&pred.hash);
            self.predicate_users.remove(&id);
        }
    }

    /// Add subscription binding
    pub fn add_binding(&mut self, binding: Binding<I>) {
        let sub_id = binding.subscription_id;
        let pred_id = binding.predicate_id;
        let user_ord = binding.user_ordinal;
        let session_id = binding.session_id;

        // Add to bindings
        self.bindings.insert(sub_id, binding);

        // Track session
        if let Some(sid) = session_id {
            self.session_index
                .entry(sid)
                .or_default()
                .push(sub_id);
        }

        // Track user interest in predicate
        self.predicate_users
            .entry(pred_id)
            .or_default()
            .insert(user_ord.get());
    }

    /// Remove subscription binding
    ///
    /// Returns the removed binding if it existed.
    pub fn remove_binding(&mut self, sub_id: I::SubscriptionId) -> Option<Binding<I>> {
        let binding = self.bindings.remove(&sub_id)?;

        // Remove from predicate_users
        if let Some(bitmap) = self.predicate_users.get_mut(&binding.predicate_id) {
            bitmap.remove(binding.user_ordinal.get());
            if bitmap.is_empty() {
                self.predicate_users.remove(&binding.predicate_id);
            }
        }

        // Remove from session index
        if let Some(session_id) = binding.session_id {
            if let Some(subs) = self.session_index.get_mut(&session_id) {
                subs.retain(|&id| id != sub_id);
                if subs.is_empty() {
                    self.session_index.remove(&session_id);
                }
            }
        }

        Some(binding)
    }

    /// Get all subscription IDs for a session
    #[must_use]
    pub fn get_session_subscriptions(&self, session_id: I::SessionId) -> Option<&[I::SubscriptionId]> {
        self.session_index.get(&session_id).map(std::vec::Vec::as_slice)
    }
}

impl<I: IdTypes> Default for PredicateStore<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::Instruction;
    use crate::DefaultIds;

    fn make_predicate(id: usize, hash: u128, refcount: u32) -> Predicate {
        Predicate {
            id: PredicateId::from_slab_index(id),
            hash,
            normalized_sql: "test".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::Not])),
            dependency_columns: Arc::from([]),
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

        let binding = Binding {
            subscription_id: 100,
            predicate_id: PredicateId::from_slab_index(0),
            user_id: 42,
            user_ordinal: UserOrdinal::new(0),
            session_id: Some(1000),
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
    fn test_predicate_users_bitmap() {
        let mut store = PredicateStore::<DefaultIds>::new();

        let pred_id = PredicateId::from_slab_index(0);

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

        store.add_binding(binding1);
        store.add_binding(binding2);

        let bitmap = store.predicate_users.get(&pred_id).unwrap();
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(1));
        assert_eq!(bitmap.len(), 2);
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
        let stored = store.get_predicate(returned_id)
            .expect("returned ID should resolve to stored predicate");
        assert_eq!(stored.hash, 0xBEEF);

        // Hash index must resolve to the same valid predicate ID.
        let by_hash = store.find_by_hash(0xBEEF)
            .expect("hash index should contain inserted predicate");
        assert_eq!(by_hash, returned_id);
        assert!(store.get_predicate(by_hash).is_some());
    }
}
