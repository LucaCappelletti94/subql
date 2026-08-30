//! Non-generic domain-level ID type aliases and the CDC event kind.

/// Table identifier (from schema catalog)
pub type TableId = u32;

/// Column identifier (ordinal within table, 0-indexed)
pub type ColumnId = u16;

/// Shard identifier (for persistence)
pub type ShardId = u64;

/// Merge job identifier (for background operations)
pub type MergeJobId = u64;

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
