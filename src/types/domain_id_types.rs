//! Non-generic domain-level ID type aliases and the CDC event kind.

/// Table identifier (from schema catalog)
pub type TableId = u32;

/// Column identifier (ordinal within table, 0-indexed)
pub type ColumnId = u16;

/// Shard identifier (for persistence)
pub type ShardId = u64;

/// Names one background merge, and is the only way to reach it.
///
/// Dropping it orphans the merge: the work finishes, its result is held,
/// and the shard is never swapped in. Recover one from
/// `SubscriptionEngine::pending_merges`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[must_use = "a dropped merge job is never swapped in"]
pub struct MergeJobId(u64);

impl MergeJobId {
    /// Name the merge that `raw` identifies.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The number behind the name.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for MergeJobId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// CDC event kind
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// Row insertion
    Insert,
    /// Row update (old -> new)
    Update,
    /// Row deletion
    Delete,
    /// Table truncate: all rows in the table are removed.
    ///
    /// **Fanout semantics**: TRUNCATE does not carry a row image, so `consumers()`
    /// skips predicate VM evaluation and notifies row subscriptions for the
    /// table. Aggregate subscriptions are handled separately by
    /// `aggregate_updates()`, which empties each of that table's held values
    /// and reports the ones that moved.
    Truncate,
}
