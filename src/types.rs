//! Core type definitions for subql

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use serde::{Serialize, de::DeserializeOwned};

// ============================================================================
// Generic ID Types
// ============================================================================

/// Marker trait for ID types used in the subscription engine.
///
/// Any type satisfying these bounds can be used as a user, session, or
/// subscription identifier.
pub trait Id: Copy + Eq + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}

/// Blanket implementation: every type meeting the bounds is automatically an `Id`.
impl<T: Copy + Eq + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Id for T {}

/// Associated types that pin the three consumer-facing ID representations.
///
/// No default type parameters — `SubscriptionEngine<D, I>` always requires
/// both, forcing an explicit choice and preventing hidden bugs.
pub trait IdTypes: 'static {
    /// User identifier (globally unique)
    type UserId: Id;
    /// Session identifier (per-connection)
    type SessionId: Id;
    /// Subscription identifier (globally unique, assigned by caller)
    type SubscriptionId: Id;
}

/// Default ID configuration using `u64` for all three identifiers.
pub struct DefaultIds;

impl IdTypes for DefaultIds {
    type UserId = u64;
    type SessionId = u64;
    type SubscriptionId = u64;
}

// ============================================================================
// Domain ID Types (non-generic, internal)
// ============================================================================

/// Table identifier (from schema catalog)
pub type TableId = u32;

/// Column identifier (ordinal within table, 0-indexed)
pub type ColumnId = u16;

/// Shard identifier (for persistence)
pub type ShardId = u64;

/// Merge job identifier (for background operations)
pub type MergeJobId = u64;

// ============================================================================
// Event Types
// ============================================================================

/// CDC event kind
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// Row insertion
    Insert,
    /// Row update (old → new)
    Update,
    /// Row deletion
    Delete,
}

/// Cell value in a row image
///
/// Three-valued logic:
/// - `Missing`: Column not present in this image (UPDATE `old_row` may be incomplete)
/// - `Null`: SQL NULL value
/// - Typed value: Bool, Int, Float, String
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Cell {
    /// Column not present in row image
    Missing,
    /// SQL NULL
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit float
    Float(f64),
    /// UTF-8 string (Arc for cheap cloning)
    String(Arc<str>),
}

impl Cell {
    /// Returns true if this cell is SQL NULL
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns true if this cell is missing from row image
    #[must_use]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Returns true if this cell has a concrete value (not NULL or Missing)
    #[must_use]
    pub const fn is_present(&self) -> bool {
        !matches!(self, Self::Null | Self::Missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_is_null() {
        assert!(Cell::Null.is_null());
        assert!(!Cell::Missing.is_null());
        assert!(!Cell::Bool(true).is_null());
        assert!(!Cell::Int(42).is_null());
        assert!(!Cell::Float(3.14).is_null());
        assert!(!Cell::String("test".into()).is_null());
    }

    #[test]
    fn test_cell_is_missing() {
        assert!(Cell::Missing.is_missing());
        assert!(!Cell::Null.is_missing());
        assert!(!Cell::Bool(false).is_missing());
        assert!(!Cell::Int(0).is_missing());
        assert!(!Cell::Float(0.0).is_missing());
        assert!(!Cell::String("".into()).is_missing());
    }

    #[test]
    fn test_cell_is_present() {
        assert!(Cell::Bool(true).is_present());
        assert!(Cell::Int(42).is_present());
        assert!(Cell::Float(3.14).is_present());
        assert!(Cell::String("test".into()).is_present());
        assert!(!Cell::Null.is_present());
        assert!(!Cell::Missing.is_present());
    }

    #[test]
    fn test_row_image_get() {
        let row = RowImage {
            cells: Arc::from(vec![Cell::Int(1), Cell::Int(2), Cell::Int(3)]),
        };

        assert_eq!(row.get(0), Some(&Cell::Int(1)));
        assert_eq!(row.get(1), Some(&Cell::Int(2)));
        assert_eq!(row.get(2), Some(&Cell::Int(3)));
        assert_eq!(row.get(3), None);
        assert_eq!(row.get(100), None);
    }

    #[test]
    fn test_row_image_len() {
        let empty = RowImage { cells: Arc::from(vec![]) };
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let row = RowImage {
            cells: Arc::from(vec![Cell::Int(1), Cell::Int(2)]),
        };
        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_event_kind() {
        assert_ne!(EventKind::Insert, EventKind::Update);
        assert_ne!(EventKind::Update, EventKind::Delete);
        assert_eq!(EventKind::Insert, EventKind::Insert);
    }
}

/// Row image: array of cells indexed by `ColumnId`
///
/// Cells are stored in column-ordinal order (`ColumnId` is index).
/// Missing columns are represented as `Cell::Missing`.
#[derive(Clone, Debug)]
pub struct RowImage {
    pub cells: Arc<[Cell]>,
}

impl RowImage {
    /// Get cell by column ID
    #[must_use]
    pub fn get(&self, col_id: ColumnId) -> Option<&Cell> {
        self.cells.get(col_id as usize)
    }

    /// Number of columns in this image
    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Returns true if row image is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

/// Primary key columns and values
#[derive(Clone, Debug)]
pub struct PrimaryKey {
    /// Column IDs comprising the primary key
    pub columns: Arc<[ColumnId]>,
    /// Values of the primary key columns
    pub values: Arc<[Cell]>,
}

/// WAL event from PostgreSQL CDC
#[derive(Clone, Debug)]
pub struct WalEvent {
    /// Event type
    pub kind: EventKind,
    /// Table this event belongs to
    pub table_id: TableId,
    /// Primary key of affected row
    pub pk: PrimaryKey,
    /// Old row image (for UPDATE/DELETE)
    pub old_row: Option<RowImage>,
    /// New row image (for INSERT/UPDATE)
    pub new_row: Option<RowImage>,
    /// Columns that changed (for UPDATE optimization)
    pub changed_columns: Arc<[ColumnId]>,
}

// ============================================================================
// Subscription Types
// ============================================================================

/// Subscription specification provided by user
#[derive(Clone, Debug)]
pub struct SubscriptionSpec<I: IdTypes> {
    /// Unique subscription ID (assigned by caller)
    pub subscription_id: I::SubscriptionId,
    /// User who owns this subscription
    pub user_id: I::UserId,
    /// Session ID if session-bound, None if durable
    pub session_id: Option<I::SessionId>,
    /// SQL SELECT statement with WHERE clause
    pub sql: String,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch)
    pub updated_at_unix_ms: u64,
}

/// Result of successful subscription registration
#[derive(Clone, Debug)]
pub struct RegisterResult {
    /// Table this subscription applies to
    pub table_id: TableId,
    /// Normalized/canonicalized SQL
    pub normalized_sql: String,
    /// Hash of the predicate (for deduplication)
    pub predicate_hash: u128,
    /// True if a new predicate was created, false if reused existing
    pub created_new_predicate: bool,
}

/// Report from pruning session subscriptions
#[derive(Clone, Debug)]
pub struct PruneReport {
    /// Number of subscription bindings removed
    pub removed_bindings: usize,
    /// Number of predicates removed (refcount reached 0)
    pub removed_predicates: usize,
    /// Number of user dictionary entries removed
    pub removed_users: usize,
}

/// Report from background merge operation
#[derive(Clone, Debug)]
pub struct MergeReport {
    /// Number of input shards merged
    pub input_shards: usize,
    /// Number of predicates in output shard
    pub output_predicates: usize,
    /// Number of bindings in output shard
    pub output_bindings: usize,
    /// Deduplication ratio (1.0 = no dedup, 2.0 = 50% reduction)
    pub dedup_ratio: f32,
    /// Time spent building merged shard (milliseconds)
    pub build_ms: u64,
}

// ============================================================================
// Trait Definitions
// ============================================================================

/// Schema catalog providing table/column metadata
///
/// This trait abstracts the schema information needed to compile and validate
/// SQL subscriptions. Implementations might query PostgreSQL system catalogs,
/// maintain an in-memory schema cache, or use a schema registry.
pub trait SchemaCatalog: Send + Sync {
    /// Resolve table name to TableId
    fn table_id(&self, table_name: &str) -> Option<TableId>;

    /// Resolve column name to ColumnId within a table
    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId>;

    /// Get number of columns in table (for row validation)
    fn table_arity(&self, table_id: TableId) -> Option<usize>;

    /// Get schema fingerprint for compatibility checking
    ///
    /// Fingerprint changes when table schema changes (add/remove/rename columns).
    /// Used to detect shard incompatibility on load.
    fn schema_fingerprint(&self, table_id: TableId) -> Option<u64>;
}

/// Subscription registration operations
pub trait SubscriptionRegistration<I: IdTypes>: Send + Sync {
    /// Register a new subscription
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and binds user.
    /// Returns error if SQL is unparseable or unsupported.
    fn register(&mut self, spec: SubscriptionSpec<I>)
        -> Result<RegisterResult, crate::RegisterError>;

    /// Unregister a subscription by ID
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate is removed.
    /// Returns true if subscription existed and was removed.
    fn unregister_subscription(&mut self, subscription_id: I::SubscriptionId) -> bool;
}

/// Event dispatch operations
pub trait SubscriptionDispatch<I: IdTypes>: Send + Sync {
    /// Iterator over matched user IDs
    type UserIter<'a>: Iterator<Item = I::UserId> where Self: 'a;

    /// Get interested users for a WAL event
    ///
    /// Returns a zero-alloc iterator of matched user IDs.
    fn users(&mut self, event: &WalEvent) -> Result<Self::UserIter<'_>, crate::DispatchError>;
}

/// Session lifecycle operations
pub trait SubscriptionPruning<I: IdTypes>: Send + Sync {
    /// Unregister all subscriptions for a session
    ///
    /// Removes all session-bound subscriptions, decrements refcounts, prunes predicates.
    /// Durable subscriptions (session_id = None) are NOT affected.
    fn unregister_session(&mut self, session_id: I::SessionId) -> PruneReport;
}

/// Durable shard storage operations
pub trait DurableShardStore: Send + Sync {
    /// Serialize table partition to bytes
    ///
    /// Returns bincode + LZ4 compressed shard.
    fn snapshot_table(&self, table_id: TableId) -> Result<Vec<u8>, crate::StorageError>;

    /// Load shard from bytes and return ShardId
    ///
    /// Validates version, fingerprint, and codec. Merges loaded predicates into runtime.
    fn load_shard(&mut self, shard_bytes: &[u8]) -> Result<ShardId, crate::StorageError>;
}

/// Background merge operations
pub trait DurableShardMerge: Send + Sync {
    /// Start background merge of shards for a table
    ///
    /// Returns job ID immediately. Merge runs in background thread.
    fn merge_shards_background(
        &self,
        table_id: TableId,
        shard_ids: &[ShardId],
    ) -> Result<MergeJobId, crate::MergeError>;

    /// Check if merge is complete and swap if ready
    ///
    /// Returns Some(report) if merge complete and swap succeeded.
    /// Returns None if merge still running.
    fn try_swap_merged(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, crate::MergeError>;
}
