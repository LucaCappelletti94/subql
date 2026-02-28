//! Core type definitions for subql

use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;

// ============================================================================
// Generic ID Types
// ============================================================================

/// Marker trait for ID types used in the subscription engine.
///
/// Any type satisfying these bounds can be used as a consumer, session, or
/// subscription identifier.
pub trait Id:
    Copy + Ord + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

/// Blanket implementation: every type meeting the bounds is automatically an `Id`.
impl<T: Copy + Ord + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Id for T {}

/// Engine-assigned subscription identifier (always `u64`).
pub type SubscriptionId = u64;

/// Associated types that pin the consumer-facing ID representations.
///
/// `SubscriptionId` is always `u64` and auto-assigned by the engine.
pub trait IdTypes: 'static {
    /// Consumer identifier (globally unique)
    type ConsumerId: Id;
    /// Session identifier (per-connection)
    type SessionId: Id;
}

/// Default ID configuration using `u64` for consumer and session identifiers.
pub struct DefaultIds;

impl IdTypes for DefaultIds {
    type ConsumerId = u64;
    type SessionId = u64;
}

/// Lifetime scope of a subscription.
///
/// Wire-compatible with `Option<SessionId>` under postcard's positional
/// encoding: `Durable` = variant 0 = `None`, `Session(id)` = variant 1 =
/// `Some(id)`.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
pub enum SubscriptionScope<I: IdTypes> {
    /// Persists until explicitly unregistered.
    Durable,
    /// Bound to a session; auto-removed when the session ends.
    Session(I::SessionId),
}

// Manual impls avoid derived bounds that would require `I: Copy/Clone/Debug/…`.
// Only `I::SessionId` (already `Copy + Debug + Hash + …` via `Id`) is needed.
impl<I: IdTypes> Copy for SubscriptionScope<I> {}
impl<I: IdTypes> Clone for SubscriptionScope<I> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<I: IdTypes> std::fmt::Debug for SubscriptionScope<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Durable => write!(f, "Durable"),
            Self::Session(id) => f.debug_tuple("Session").field(id).finish(),
        }
    }
}
impl<I: IdTypes> PartialEq for SubscriptionScope<I> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Durable, Self::Durable) => true,
            (Self::Session(a), Self::Session(b)) => a == b,
            _ => false,
        }
    }
}
impl<I: IdTypes> Eq for SubscriptionScope<I> {}
impl<I: IdTypes> std::hash::Hash for SubscriptionScope<I> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        if let Self::Session(id) = self {
            id.hash(state);
        }
    }
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
    /// Table truncate — all rows in the table are removed.
    ///
    /// **Fanout semantics**: TRUNCATE does not carry a row image, so `consumers()`
    /// skips predicate VM evaluation and notifies row subscriptions for the
    /// table. Aggregate subscriptions are handled separately by
    /// `aggregate_deltas()`, which returns `TruncateRequiresReset`.
    Truncate,
}

/// Cell value in a row image
///
/// Three-valued logic:
/// - `Missing`: Column not present in this image (UPDATE `old_row` may be incomplete)
/// - `Null`: SQL NULL value
/// - Typed value: Bool, Int, Float, String
#[non_exhaustive]
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
#[allow(clippy::approx_constant, clippy::unwrap_used)]
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
        let empty = RowImage {
            cells: Arc::from(vec![]),
        };
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

    #[test]
    fn subscription_scope_wire_compatible_with_option() {
        let none_bytes = crate::persistence::codec::serialize(&Option::<u64>::None).unwrap();
        let durable_bytes =
            crate::persistence::codec::serialize(&SubscriptionScope::<DefaultIds>::Durable)
                .unwrap();
        assert_eq!(none_bytes, durable_bytes);

        let some_bytes = crate::persistence::codec::serialize(&Some(42u64)).unwrap();
        let session_bytes =
            crate::persistence::codec::serialize(&SubscriptionScope::<DefaultIds>::Session(42))
                .unwrap();
        assert_eq!(some_bytes, session_bytes);
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

impl PrimaryKey {
    /// Empty primary key (used when PK metadata is unavailable).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            columns: Arc::from([]),
            values: Arc::from([]),
        }
    }

    /// Returns true when no PK columns/values are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.values.is_empty()
    }
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

/// Subscription request provided by caller
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionRequest<I: IdTypes> {
    /// Consumer who owns this subscription
    pub(crate) consumer_id: I::ConsumerId,
    /// Lifetime scope: durable or session-bound
    pub(crate) scope: SubscriptionScope<I>,
    /// SQL SELECT statement with WHERE clause
    pub(crate) sql: String,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch)
    pub(crate) updated_at_unix_ms: u64,
}

impl<I: IdTypes> SubscriptionRequest<I> {
    /// Create a new subscription request with default scope (`Durable`) and timestamp (`0`).
    pub fn new(consumer_id: I::ConsumerId, sql: impl Into<String>) -> Self {
        Self {
            consumer_id,
            scope: SubscriptionScope::Durable,
            sql: sql.into(),
            updated_at_unix_ms: 0,
        }
    }

    /// Set the subscription scope (default: `SubscriptionScope::Durable`).
    #[must_use]
    pub const fn scope(mut self, scope: SubscriptionScope<I>) -> Self {
        self.scope = scope;
        self
    }

    /// Set the conflict-resolution timestamp in milliseconds since Unix epoch (default: `0`).
    #[must_use]
    pub const fn updated_at_unix_ms(mut self, ts: u64) -> Self {
        self.updated_at_unix_ms = ts;
        self
    }
}

/// Result of successful subscription registration
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterResult {
    /// Engine-assigned subscription identifier
    pub subscription_id: SubscriptionId,
    /// Table this subscription applies to
    pub table_id: TableId,
    /// Normalized/canonicalized SQL
    pub normalized_sql: String,
    /// Hash of the predicate (for deduplication)
    pub predicate_hash: u128,
    /// True if a new predicate was created, false if reused existing
    pub created_new_predicate: bool,
    /// Projection kind for this subscription
    pub projection: crate::compiler::sql_shape::QueryProjection,
}

/// Durability policy for registration writes when storage is enabled.
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// Registration succeeds even if snapshot/rotation persistence fails.
    BestEffort,
    /// Registration fails (and is rolled back) if persistence fails.
    Required,
}

/// Report from pruning session subscriptions
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnregisterReport {
    /// Number of subscription bindings removed
    pub removed_bindings: usize,
    /// Number of predicates removed (refcount reached 0)
    pub removed_predicates: usize,
    /// Number of consumer dictionary entries removed
    pub removed_consumers: usize,
}

/// Report from background merge operation
#[derive(Clone, Debug, PartialEq)]
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

    /// Get primary key column IDs for a table
    ///
    /// Used by WAL parsers (e.g. wal2json INSERT events) to extract PK values
    /// from the new row when `oldkeys` is absent.
    fn primary_key_columns(&self, _table_id: TableId) -> Option<&[ColumnId]> {
        None
    }

    /// Get the value type of a column (optional, for aggregate validation).
    ///
    /// When this returns `Some(ColumnType::Bool | ColumnType::String)`, the
    /// engine rejects `SUM(col)` and `AVG(col)` subscriptions at registration
    /// with `RegisterError::UnsupportedSql`. Catalogs that cannot provide type
    /// information should return `None` (the default), which disables the check.
    fn column_type(&self, _table_id: TableId, _column_id: ColumnId) -> Option<ColumnType> {
        None
    }
}

/// Subscription registration operations
pub trait SubscriptionRegistration<I: IdTypes>: Send {
    /// Register a new subscription
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and binds consumer.
    /// Returns error if SQL is unparseable or unsupported.
    fn register(
        &mut self,
        spec: SubscriptionRequest<I>,
    ) -> Result<RegisterResult, crate::RegisterError>;

    /// Unregister a subscription by ID
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate is removed.
    /// Returns true if subscription existed and was removed.
    fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool;
}

/// Event dispatch operations
pub trait SubscriptionDispatch<I: IdTypes>: Send {
    /// Iterator over matched consumer IDs
    type ConsumerIter<'a>: Iterator<Item = I::ConsumerId>
    where
        Self: 'a;

    /// Get interested consumers for a WAL event
    ///
    /// Returns a zero-alloc iterator of matched consumer IDs.
    fn consumers(
        &mut self,
        event: &WalEvent,
    ) -> Result<Self::ConsumerIter<'_>, crate::DispatchError>;
}

/// Session lifecycle operations
pub trait SubscriptionUnregistration<I: IdTypes>: Send {
    /// Unregister all subscriptions for a session
    ///
    /// Removes all session-bound subscriptions, decrements refcounts, prunes predicates.
    /// Durable subscriptions (`SubscriptionScope::Durable`) are NOT affected.
    fn unregister_session(&mut self, session_id: I::SessionId) -> UnregisterReport;

    /// Unregister all subscriptions for a consumer matching a specific SQL query.
    ///
    /// Parses the SQL just enough to compute the predicate hash (no bytecode
    /// compilation), then removes all bindings for `consumer_id` that share that
    /// hash. Returns a [`UnregisterReport`] with the counts of removed bindings,
    /// predicates, and consumer-dictionary entries.
    fn unregister_query(
        &mut self,
        _consumer_id: I::ConsumerId,
        _sql: &str,
    ) -> Result<UnregisterReport, crate::RegisterError> {
        Err(crate::RegisterError::UnsupportedSql(
            "unregister_query not supported".to_string(),
        ))
    }
}

/// Durable shard storage operations
pub trait DurableShardStore: Send {
    /// Snapshot a table partition to durable storage.
    fn snapshot_table(&self, table_id: TableId) -> Result<(), crate::StorageError>;
}

/// Column value type for aggregate validation at registration time.
///
/// Returned by [`SchemaCatalog::column_type`]. Catalogs that cannot provide
/// type information return `None` (the default), which disables type checking.
#[non_exhaustive]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    /// 64-bit signed integer
    Int,
    /// 64-bit float
    Float,
    /// Boolean
    Bool,
    /// UTF-8 string
    String,
    /// Unknown or mixed type (treated as potentially numeric — no error)
    Unknown,
}

/// Typed signed delta from an aggregate subscription.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AggDelta {
    /// COUNT(*) / COUNT(column) delta — always ±1 per matching (non-NULL) row.
    Count(i64),
    /// SUM(column) delta — signed change in the column sum.
    Sum(f64),
    /// AVG(column) delta — both components needed to update a running average.
    ///
    /// Caller maintains `running_sum` and `running_count` separately:
    /// ```text
    /// running_sum   += sum_delta
    /// running_count += count_delta
    /// avg            = running_sum / running_count  (when running_count > 0)
    /// ```
    Avg { sum_delta: f64, count_delta: i64 },
}

/// Aggregate dispatch — delivers typed signed deltas for aggregate subscriptions.
///
/// Separate from [`SubscriptionDispatch`] because:
/// - UPDATE events require evaluating **both** the old row and the new row.
/// - Returns signed deltas, not consumer bitmaps.
/// - Aggregate predicates are **never** included in `consumers()` results.
///
/// # Caller contract
///
/// The engine handles only WAL-driven deltas. Callers must:
/// 1. **Bootstrap** — query the DB for the initial aggregate **before** subscribing.
/// 2. **Accumulate** — `running_value += delta` on each call.
/// 3. **Reset on policy change** — RLS/ACL changes produce no WAL events;
///    re-query the DB and replace the stored value.
/// 4. **Reset on TRUNCATE** — engine returns `Err(TruncateRequiresReset)`;
///    caller must re-query and replace the stored value.
pub trait AggregateDispatch<I: IdTypes>: Send {
    /// Compute typed signed deltas for all matching aggregate subscriptions.
    ///
    /// Returns `Vec<(ConsumerId, AggDelta)>` where each entry is the signed change
    /// for that consumer's subscription. Zero-net entries are omitted.
    /// The same consumer may appear multiple times (once per aggregate kind).
    fn aggregate_deltas(
        &mut self,
        event: &WalEvent,
    ) -> Result<Vec<(I::ConsumerId, AggDelta)>, crate::DispatchError>;
}

/// Background merge operations
pub trait DurableShardMerge: Send {
    /// Start background merge of shard files for a table.
    fn merge_shards_background(
        &mut self,
        table_id: TableId,
        shard_paths: &[PathBuf],
    ) -> Result<MergeJobId, crate::MergeError>;

    /// Check whether merge has completed and atomically swap if ready.
    fn try_complete_merge(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, crate::MergeError>;
}
