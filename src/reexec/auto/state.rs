//! Per-subscription resolve state and the queue of pending reads.

use super::{Backend, IdTypes, ScalarFamily, SubscriptionId, Vec};

/// Per-query state needed to drive an automatic re-execution.
///
/// Shared by both the sync and async engines. Private to the `reexec`
/// module so the engine internals can read its fields directly.
pub struct ResolveContext<I: IdTypes, B: Backend, A> {
    /// Executable query produced by the plan.
    pub query: crate::reexec::BoundQuery<B>,
    /// Decode kind for the scalar result. Meaningless for a whole re-read,
    /// which has no single column.
    pub column_kind: ScalarFamily,
    /// Initial grouped extreme read, present only for that tier.
    pub grouped_bootstrap: Option<crate::AggregateBootstrap<B>>,
    /// Whether resolving means reading one scalar or re-reading every row.
    /// The trigger does not say, and the two are resolved differently.
    pub whole_result: bool,
    /// Whether resolving means asking only about the rows that changed.
    pub keyed: bool,
    /// Set when the engine, not a read, maintains this subscription, in
    /// which case the stored query is held for one contingency rather than
    /// being how the answer is produced. `None` for every read tier.
    pub in_process: Option<InProcessKind>,
    /// Which re-read the next page belongs to, so a consumer can tell a new
    /// answer from a continuation of the old one.
    pub generation: u64,
    /// Session owning the query, used to drop contexts on
    /// [`unregister_session`](AutoResolvingEngine::unregister_session).
    pub session: Option<I::SessionId>,
    /// Per-subscription auth state, passed verbatim to the connector.
    pub auth: A,
}

impl<I: IdTypes, B: Backend, A> ResolveContext<I, B, A> {
    /// Whether a snapshot has nothing to prime for this subscription.
    ///
    /// One predicate, read by both the sync and the async engine, rather
    /// than a check spelled once in each. Spelled twice it was spelled
    /// once: the async engine never had it, and because a
    /// [`InProcessKind::StreamServedFilter`] context sets `whole_result`,
    /// the async path reached its whole-result branch and issued a read
    /// its twin never issues, answering
    /// [`ReExecError::Cursor`](crate::reexec::ReExecError::Cursor) where the sync
    /// engine answers `Ok(None)`.
    ///
    /// The retained query answers a report, not a snapshot: it exists for
    /// the one case the stream cannot answer, a cell the event did not
    /// carry.
    pub fn stream_answers_the_filter(&self) -> bool {
        self.in_process == Some(InProcessKind::StreamServedFilter)
    }
}

/// Why an in-process subscription holds a resolve context at all, since
/// neither kind is resolved by a read while it stays in process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InProcessKind {
    /// A still-folding aggregate, keeping the auth and session for a
    /// possible demotion to a whole re-read: an update without its old row
    /// image demotes an ungrouped `SUM`/`AVG`. The fold runs in the engine,
    /// so snapshot skips it and no read is issued until a demotion sets
    /// `whole_result`.
    FoldingAggregate,
    /// A row filter the stream answers, keeping its own query for the one
    /// case the stream cannot answer: a cell the event did not carry. That
    /// is a report-driven read, not a snapshot, so priming such a
    /// subscription from the database still reads nothing.
    StreamServedFilter,
}

/// Queued reads in arrival order, indexed by `(subscription, group)`.
///
/// The order lives in a `VecDeque` whose entries can be tombstoned in
/// place, and the index maps each live key to a monotonically assigned
/// sequence number, so enqueue deduplication, in-place replacement, and
/// removal by key are each one hash lookup instead of a scan of the queue.
pub struct ReadQueue<I: IdTypes, C: crate::Checkpoint, B: Backend> {
    entries: alloc::collections::VecDeque<Option<crate::reexec::ReExecutionTrigger<I, C, B>>>,
    /// Live keys to the sequence number of their entry.
    queued: hashbrown::HashMap<(SubscriptionId, Option<Vec<u8>>), u64>,
    /// Sequence number of the front entry of `entries`.
    head_seq: u64,
    /// Tombstoned entries still holding a slot, compacted away as soon as
    /// they outnumber the live ones, so storage stays proportional to the
    /// queue and every walk stays amortized constant per operation.
    tombstones: usize,
}

impl<I: IdTypes, C: crate::Checkpoint, B: Backend> ReadQueue<I, C, B> {
    pub fn new() -> Self {
        Self {
            entries: alloc::collections::VecDeque::new(),
            queued: hashbrown::HashMap::new(),
            head_seq: 0,
            tombstones: 0,
        }
    }
    pub fn key_of(
        trigger: &crate::reexec::ReExecutionTrigger<I, C, B>,
    ) -> (SubscriptionId, Option<Vec<u8>>) {
        (
            trigger.subscription_id,
            trigger.read.group_key().map(<[u8]>::to_vec),
        )
    }

    pub fn index_of(&self, seq: u64) -> usize {
        usize::try_from(seq - self.head_seq).expect("a live sequence number is within the queue")
    }

    pub fn len(&self) -> usize {
        self.queued.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Queue `trigger`, replacing a queued read of the same subscription and
    /// group in place, so a burst keeps one read at its original position.
    pub fn enqueue(&mut self, trigger: crate::reexec::ReExecutionTrigger<I, C, B>) {
        let key = Self::key_of(&trigger);
        if let Some(&seq) = self.queued.get(&key) {
            let index = self.index_of(seq);
            self.entries[index] = Some(trigger);
            return;
        }
        let seq = self.head_seq + self.entries.len() as u64;
        self.entries.push_back(Some(trigger));
        self.queued.insert(key, seq);
    }

    /// Drop the queued read of `(subscription_id, group_key)`, tombstoning
    /// its entry so no position shifts, and compacting once tombstones
    /// outnumber live reads.
    pub fn remove(&mut self, subscription_id: SubscriptionId, group_key: Option<&[u8]>) {
        let key = (subscription_id, group_key.map(<[u8]>::to_vec));
        if let Some(seq) = self.queued.remove(&key) {
            let index = self.index_of(seq);
            self.entries[index] = None;
            self.tombstones += 1;
            self.compact_if_mostly_dead();
        }
    }

    /// The oldest queued read, skipping tombstones.
    pub fn pop_front(&mut self) -> Option<crate::reexec::ReExecutionTrigger<I, C, B>> {
        while let Some(slot) = self.entries.pop_front() {
            self.head_seq += 1;
            if let Some(trigger) = slot {
                self.queued.remove(&Self::key_of(&trigger));
                return Some(trigger);
            }
            self.tombstones -= 1;
        }
        None
    }

    /// Put back the read `pop_front` just handed out, at the front sequence
    /// it vacated. Only that read may come back, which is what keeps the
    /// head sequence from underflowing.
    pub fn push_front(&mut self, trigger: crate::reexec::ReExecutionTrigger<I, C, B>) {
        self.head_seq -= 1;
        self.queued.insert(Self::key_of(&trigger), self.head_seq);
        self.entries.push_front(Some(trigger));
    }

    /// Tombstone every queued read `keep` refuses. Positions do not shift,
    /// so the index stays valid until the compaction that runs when the
    /// tombstones outnumber the live reads.
    pub fn retain(
        &mut self,
        mut keep: impl FnMut(&crate::reexec::ReExecutionTrigger<I, C, B>) -> bool,
    ) {
        for slot in &mut self.entries {
            let Some(trigger) = slot else {
                continue;
            };
            if !keep(trigger) {
                self.queued.remove(&Self::key_of(trigger));
                *slot = None;
                self.tombstones += 1;
            }
        }
        self.compact_if_mostly_dead();
    }

    /// Reclaim tombstoned slots once they outnumber the live reads, in one
    /// pass that keeps arrival order and reindexes the survivors from a
    /// fresh head sequence. Amortized constant per removal.
    pub fn compact_if_mostly_dead(&mut self) {
        if self.tombstones <= self.queued.len() {
            return;
        }
        self.entries.retain(Option::is_some);
        self.head_seq = 0;
        self.tombstones = 0;
        self.queued.clear();
        for (index, slot) in self.entries.iter().enumerate() {
            let trigger = slot.as_ref().expect("compaction kept only live entries");
            self.queued.insert(Self::key_of(trigger), index as u64);
        }
    }

    /// The live queued reads in order, cloned.
    pub fn snapshot(&self) -> Vec<crate::reexec::ReExecutionTrigger<I, C, B>> {
        self.entries.iter().flatten().cloned().collect()
    }

    /// Storage slots held, tombstones included: the reclamation tests'
    /// window into what [`Self::len`] cannot see.
    #[cfg(test)]
    pub fn entry_slots(&self) -> usize {
        self.entries.len()
    }
}
