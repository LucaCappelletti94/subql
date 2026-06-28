//! Core type definitions for subql

use crate::checkpoint::{Checkpoint, NoCheckpoint};
use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::hash::Hash;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(feature = "std")]
use std::path::PathBuf;

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
#[derive(Debug)]
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
    /// Bound to a session. Auto-removed when the session ends.
    Session(I::SessionId),
}

// Manual impls avoid derived bounds that would require `I: Copy/Clone/Debug/...`.
// Only `I::SessionId` (already `Copy + Debug + Hash + ...` via `Id`) is needed.
impl<I: IdTypes> Copy for SubscriptionScope<I> {}
impl<I: IdTypes> Clone for SubscriptionScope<I> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<I: IdTypes> core::fmt::Debug for SubscriptionScope<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
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
impl<I: IdTypes> core::hash::Hash for SubscriptionScope<I> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
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
    /// Row update (old -> new)
    Update,
    /// Row deletion
    Delete,
    /// Table truncate: all rows in the table are removed.
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
    fn test_insert_builder_success() {
        let event = WalEvent::builder(7)
            .insert()
            .pk_cell(0, Cell::Int(1))
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::String("ok".into())]),
            })
            .build()
            .expect("insert builder should succeed");

        assert_eq!(event.kind(), EventKind::Insert);
        assert_eq!(event.table_id(), 7);
        assert_eq!(event.pk().columns(), &[0]);
        assert_eq!(event.pk().values(), &[Cell::Int(1)]);
        assert!(event.old_row().is_none());
        assert!(event.new_row().is_some());
    }

    #[test]
    fn test_insert_builder_missing_new_row() {
        let err = WalEvent::builder(1)
            .insert()
            .pk_cell(0, Cell::Int(1))
            .build()
            .expect_err("insert without new_row must fail");
        assert_eq!(err, WalEventBuildError::MissingNewRow);
    }

    #[test]
    fn test_delete_builder_missing_old_row() {
        let err = WalEvent::builder(1)
            .delete()
            .pk_cell(0, Cell::Int(1))
            .build()
            .expect_err("delete without old_row must fail");
        assert_eq!(err, WalEventBuildError::MissingOldRow);
    }

    #[test]
    fn test_update_builder_changed_columns_and_old_row() {
        let event = WalEvent::builder(1)
            .update()
            .pk_cell(0, Cell::Int(1))
            .old_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(10)]),
            })
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(20)]),
            })
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("update builder should succeed");

        assert_eq!(event.kind(), EventKind::Update);
        assert_eq!(event.changed_columns(), &[1]);
        assert!(event.old_row().is_some());
        assert!(event.new_row().is_some());
    }

    #[test]
    fn test_builder_rejects_duplicate_pk_column() {
        let err = WalEvent::builder(1)
            .insert()
            .pk_cell(0, Cell::Int(1))
            .pk_cell(0, Cell::Int(2))
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1)]),
            })
            .build()
            .expect_err("duplicate PK columns must fail");
        assert_eq!(err, WalEventBuildError::DuplicatePkColumn(0));
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

    #[test]
    fn test_subscriptions_view_iterates_metadata_entries() {
        let entries: Vec<SubscriptionMetadata<DefaultIds>> = vec![
            SubscriptionMetadata {
                subscription_id: 1,
                consumer_id: 10,
                scope: SubscriptionScope::Durable,
                last_dispatch_at: None,
                dispatch_count: 0,
            },
            SubscriptionMetadata {
                subscription_id: 2,
                consumer_id: 20,
                scope: SubscriptionScope::Session(99),
                last_dispatch_at: Some(1_000_000),
                dispatch_count: 4,
            },
        ];

        let view = SubscriptionsView::<DefaultIds>::new(&entries);
        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());

        let collected: Vec<u64> = view.iter().map(|m| m.subscription_id).collect();
        assert_eq!(collected, vec![1, 2]);

        let coldest = view
            .iter()
            .min_by_key(|m| m.dispatch_count)
            .map(|m| m.subscription_id);
        assert_eq!(coldest, Some(1));

        let least_active = view
            .iter()
            .min_by_key(|m| m.last_dispatch_at.unwrap_or(0))
            .map(|m| m.subscription_id);
        assert_eq!(least_active, Some(1));

        let session_count = view
            .iter()
            .filter(|m| matches!(m.scope, SubscriptionScope::Session(_)))
            .count();
        assert_eq!(session_count, 1);
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
    pub(crate) columns: Arc<[ColumnId]>,
    /// Values of the primary key columns
    pub(crate) values: Arc<[Cell]>,
}

/// Error returned when constructing an invalid [`PrimaryKey`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimaryKeyError {
    columns_len: usize,
    values_len: usize,
}

impl core::fmt::Display for PrimaryKeyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "primary key columns/values length mismatch: columns={}, values={}",
            self.columns_len, self.values_len
        )
    }
}

impl core::error::Error for PrimaryKeyError {}

impl PrimaryKeyError {
    /// Number of column IDs in the invalid input.
    #[must_use]
    pub const fn columns_len(&self) -> usize {
        self.columns_len
    }

    /// Number of values in the invalid input.
    #[must_use]
    pub const fn values_len(&self) -> usize {
        self.values_len
    }
}

impl PrimaryKey {
    /// Create a validated primary key.
    pub fn new(columns: Arc<[ColumnId]>, values: Arc<[Cell]>) -> Result<Self, PrimaryKeyError> {
        if columns.len() != values.len() {
            return Err(PrimaryKeyError {
                columns_len: columns.len(),
                values_len: values.len(),
            });
        }
        Ok(Self { columns, values })
    }

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

    /// Number of key columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Primary-key column IDs.
    #[must_use]
    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
    }

    /// Primary-key values.
    #[must_use]
    pub fn values(&self) -> &[Cell] {
        &self.values
    }
}

/// WAL event from PostgreSQL CDC.
///
/// Generic in `C: Checkpoint` so events can carry the source's position
/// token. Synthetic tests and checkpoint-free contexts may use the default
/// `C = NoCheckpoint`. CDC sources (wal2json, Maxwell, Debezium) pin their
/// own concrete `Checkpoint` impl ([`crate::PgLsn`],
/// [`crate::MysqlBinlogPos`], etc.).
#[derive(Clone, Debug)]
pub enum WalEvent<C: Checkpoint = NoCheckpoint> {
    /// Row insertion.
    Insert {
        /// Table this event belongs to.
        table_id: TableId,
        /// Primary key of affected row.
        pk: PrimaryKey,
        /// New row image.
        new_row: RowImage,
        /// Position of this event in the source stream, when known.
        checkpoint: Option<C>,
    },
    /// Row update.
    Update {
        /// Table this event belongs to.
        table_id: TableId,
        /// Primary key of affected row.
        pk: PrimaryKey,
        /// Old row image (may be missing from source CDC).
        old_row: Option<RowImage>,
        /// New row image.
        new_row: RowImage,
        /// Columns that changed (for UPDATE optimization).
        changed_columns: Arc<[ColumnId]>,
        /// Position of this event in the source stream, when known.
        checkpoint: Option<C>,
    },
    /// Row deletion.
    Delete {
        /// Table this event belongs to.
        table_id: TableId,
        /// Primary key of affected row.
        pk: PrimaryKey,
        /// Old row image.
        old_row: RowImage,
        /// Position of this event in the source stream, when known.
        checkpoint: Option<C>,
    },
    /// Table truncate.
    Truncate {
        /// Table this event belongs to.
        table_id: TableId,
        /// Position of this event in the source stream, when known.
        checkpoint: Option<C>,
    },
}

/// Errors returned by `WalEvent` fluent builders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalEventBuildError {
    /// INSERT/UPDATE requires `new_row`.
    MissingNewRow,
    /// DELETE requires `old_row`.
    MissingOldRow,
    /// PK columns and values had different lengths.
    MismatchedPkLengths {
        /// Number of PK columns.
        columns_len: usize,
        /// Number of PK values.
        values_len: usize,
    },
    /// Duplicate PK column ID was provided.
    DuplicatePkColumn(ColumnId),
    /// A field was configured for an incompatible event kind.
    FieldNotAllowedForKind(&'static str),
}

impl core::fmt::Display for WalEventBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingNewRow => write!(f, "missing required field: new_row"),
            Self::MissingOldRow => write!(f, "missing required field: old_row"),
            Self::MismatchedPkLengths {
                columns_len,
                values_len,
            } => {
                write!(
                    f,
                    "primary key columns/values length mismatch: columns={columns_len}, values={values_len}"
                )
            }
            Self::DuplicatePkColumn(col) => write!(f, "duplicate primary key column: {col}"),
            Self::FieldNotAllowedForKind(field) => {
                write!(f, "field '{field}' is not allowed for this event kind")
            }
        }
    }
}

impl core::error::Error for WalEventBuildError {}

/// Start of fluent `WalEvent` construction.
///
/// Call [`with_checkpoint`](Self::with_checkpoint) before [`insert`](Self::insert)
/// / [`update`](Self::update) / [`delete`](Self::delete) /
/// [`truncate`](Self::truncate) to anchor the resulting event to a
/// position. Defaults to `NoCheckpoint` and `checkpoint: None`.
#[derive(Clone, Debug)]
pub struct WalEventBuilderStart<C: Checkpoint = NoCheckpoint> {
    table_id: TableId,
    checkpoint: Option<C>,
}

/// Fluent builder for [`WalEvent::Insert`].
#[derive(Clone, Debug)]
pub struct InsertEventBuilder<C: Checkpoint = NoCheckpoint> {
    table_id: TableId,
    pk_columns: Vec<ColumnId>,
    pk_values: Vec<Cell>,
    new_row: Option<RowImage>,
    checkpoint: Option<C>,
}

/// Fluent builder for [`WalEvent::Update`].
#[derive(Clone, Debug)]
pub struct UpdateEventBuilder<C: Checkpoint = NoCheckpoint> {
    table_id: TableId,
    pk_columns: Vec<ColumnId>,
    pk_values: Vec<Cell>,
    old_row: Option<RowImage>,
    new_row: Option<RowImage>,
    changed_columns: Arc<[ColumnId]>,
    checkpoint: Option<C>,
}

/// Fluent builder for [`WalEvent::Delete`].
#[derive(Clone, Debug)]
pub struct DeleteEventBuilder<C: Checkpoint = NoCheckpoint> {
    table_id: TableId,
    pk_columns: Vec<ColumnId>,
    pk_values: Vec<Cell>,
    old_row: Option<RowImage>,
    checkpoint: Option<C>,
}

/// Fluent builder for [`WalEvent::Truncate`].
#[derive(Clone, Debug)]
pub struct TruncateEventBuilder<C: Checkpoint = NoCheckpoint> {
    table_id: TableId,
    checkpoint: Option<C>,
}

fn build_primary_key(
    columns: Vec<ColumnId>,
    values: Vec<Cell>,
) -> Result<PrimaryKey, WalEventBuildError> {
    if columns.len() != values.len() {
        return Err(WalEventBuildError::MismatchedPkLengths {
            columns_len: columns.len(),
            values_len: values.len(),
        });
    }
    let mut seen = BTreeSet::new();
    for col in &columns {
        if !seen.insert(*col) {
            return Err(WalEventBuildError::DuplicatePkColumn(*col));
        }
    }
    PrimaryKey::new(Arc::from(columns), Arc::from(values)).map_err(|e| {
        WalEventBuildError::MismatchedPkLengths {
            columns_len: e.columns_len(),
            values_len: e.values_len(),
        }
    })
}

#[allow(clippy::needless_pass_by_value)]
fn apply_pk(columns: &mut Vec<ColumnId>, values: &mut Vec<Cell>, pk: PrimaryKey) {
    columns.clear();
    values.clear();
    for (&col, value) in pk.columns().iter().zip(pk.values().iter()) {
        columns.push(col);
        values.push(value.clone());
    }
}

impl<C: Checkpoint> WalEventBuilderStart<C> {
    /// Anchor the resulting event to a position. Subsequent
    /// `insert`/`update`/`delete`/`truncate` calls preserve the checkpoint.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: C) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Type-transition the builder to a different checkpoint type by
    /// providing a checkpoint value. Useful when a no-checkpoint default
    /// builder needs to become typed once the parser has read the source
    /// position (`WalEvent::builder(t).with_typed_checkpoint(PgLsn(42))`).
    #[must_use]
    pub fn with_typed_checkpoint<C2: Checkpoint>(self, checkpoint: C2) -> WalEventBuilderStart<C2> {
        WalEventBuilderStart {
            table_id: self.table_id,
            checkpoint: Some(checkpoint),
        }
    }

    /// Start building an INSERT event.
    #[must_use]
    pub fn insert(self) -> InsertEventBuilder<C> {
        InsertEventBuilder {
            table_id: self.table_id,
            pk_columns: Vec::new(),
            pk_values: Vec::new(),
            new_row: None,
            checkpoint: self.checkpoint,
        }
    }

    /// Start building an UPDATE event.
    #[must_use]
    pub fn update(self) -> UpdateEventBuilder<C> {
        UpdateEventBuilder {
            table_id: self.table_id,
            pk_columns: Vec::new(),
            pk_values: Vec::new(),
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
            checkpoint: self.checkpoint,
        }
    }

    /// Start building a DELETE event.
    #[must_use]
    pub fn delete(self) -> DeleteEventBuilder<C> {
        DeleteEventBuilder {
            table_id: self.table_id,
            pk_columns: Vec::new(),
            pk_values: Vec::new(),
            old_row: None,
            checkpoint: self.checkpoint,
        }
    }

    /// Start building a TRUNCATE event.
    #[must_use]
    pub fn truncate(self) -> TruncateEventBuilder<C> {
        TruncateEventBuilder {
            table_id: self.table_id,
            checkpoint: self.checkpoint,
        }
    }
}

impl<C: Checkpoint> InsertEventBuilder<C> {
    /// Set primary key from a prebuilt value.
    #[must_use]
    pub fn pk(mut self, pk: PrimaryKey) -> Self {
        apply_pk(&mut self.pk_columns, &mut self.pk_values, pk);
        self
    }

    /// Append one PK `(column, value)` pair.
    #[must_use]
    pub fn pk_cell(mut self, column: ColumnId, value: Cell) -> Self {
        self.pk_columns.push(column);
        self.pk_values.push(value);
        self
    }

    /// Append multiple PK `(column, value)` pairs.
    #[must_use]
    pub fn pk_cells(mut self, pairs: impl IntoIterator<Item = (ColumnId, Cell)>) -> Self {
        for (column, value) in pairs {
            self.pk_columns.push(column);
            self.pk_values.push(value);
        }
        self
    }

    /// Set full new-row image.
    #[must_use]
    pub fn new_row(mut self, row: RowImage) -> Self {
        self.new_row = Some(row);
        self
    }

    /// Anchor the resulting event to a position.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: C) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Build the insert event.
    pub fn build(self) -> Result<WalEvent<C>, WalEventBuildError> {
        let new_row = self.new_row.ok_or(WalEventBuildError::MissingNewRow)?;
        let pk = build_primary_key(self.pk_columns, self.pk_values)?;
        Ok(WalEvent::Insert {
            table_id: self.table_id,
            pk,
            new_row,
            checkpoint: self.checkpoint,
        })
    }
}

impl<C: Checkpoint> UpdateEventBuilder<C> {
    /// Set primary key from a prebuilt value.
    #[must_use]
    pub fn pk(mut self, pk: PrimaryKey) -> Self {
        apply_pk(&mut self.pk_columns, &mut self.pk_values, pk);
        self
    }

    /// Append one PK `(column, value)` pair.
    #[must_use]
    pub fn pk_cell(mut self, column: ColumnId, value: Cell) -> Self {
        self.pk_columns.push(column);
        self.pk_values.push(value);
        self
    }

    /// Append multiple PK `(column, value)` pairs.
    #[must_use]
    pub fn pk_cells(mut self, pairs: impl IntoIterator<Item = (ColumnId, Cell)>) -> Self {
        for (column, value) in pairs {
            self.pk_columns.push(column);
            self.pk_values.push(value);
        }
        self
    }

    /// Set old row image.
    #[must_use]
    pub fn old_row(mut self, old_row: RowImage) -> Self {
        self.old_row = Some(old_row);
        self
    }

    /// Set old row image directly (including `None`).
    #[must_use]
    pub fn maybe_old_row(mut self, old_row: Option<RowImage>) -> Self {
        self.old_row = old_row;
        self
    }

    /// Set full new-row image.
    #[must_use]
    pub fn new_row(mut self, row: RowImage) -> Self {
        self.new_row = Some(row);
        self
    }

    /// Set changed columns.
    #[must_use]
    pub fn changed_columns(mut self, changed_columns: Arc<[ColumnId]>) -> Self {
        self.changed_columns = changed_columns;
        self
    }

    /// Anchor the resulting event to a position.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: C) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Build the update event.
    pub fn build(self) -> Result<WalEvent<C>, WalEventBuildError> {
        let new_row = self.new_row.ok_or(WalEventBuildError::MissingNewRow)?;
        let pk = build_primary_key(self.pk_columns, self.pk_values)?;
        Ok(WalEvent::Update {
            table_id: self.table_id,
            pk,
            old_row: self.old_row,
            new_row,
            changed_columns: self.changed_columns,
            checkpoint: self.checkpoint,
        })
    }
}

impl<C: Checkpoint> DeleteEventBuilder<C> {
    /// Set primary key from a prebuilt value.
    #[must_use]
    pub fn pk(mut self, pk: PrimaryKey) -> Self {
        apply_pk(&mut self.pk_columns, &mut self.pk_values, pk);
        self
    }

    /// Append one PK `(column, value)` pair.
    #[must_use]
    pub fn pk_cell(mut self, column: ColumnId, value: Cell) -> Self {
        self.pk_columns.push(column);
        self.pk_values.push(value);
        self
    }

    /// Append multiple PK `(column, value)` pairs.
    #[must_use]
    pub fn pk_cells(mut self, pairs: impl IntoIterator<Item = (ColumnId, Cell)>) -> Self {
        for (column, value) in pairs {
            self.pk_columns.push(column);
            self.pk_values.push(value);
        }
        self
    }

    /// Set full old-row image.
    #[must_use]
    pub fn old_row(mut self, row: RowImage) -> Self {
        self.old_row = Some(row);
        self
    }

    /// Anchor the resulting event to a position.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: C) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Build the delete event.
    pub fn build(self) -> Result<WalEvent<C>, WalEventBuildError> {
        let old_row = self.old_row.ok_or(WalEventBuildError::MissingOldRow)?;
        let pk = build_primary_key(self.pk_columns, self.pk_values)?;
        Ok(WalEvent::Delete {
            table_id: self.table_id,
            pk,
            old_row,
            checkpoint: self.checkpoint,
        })
    }
}

impl<C: Checkpoint> TruncateEventBuilder<C> {
    /// Anchor the resulting event to a position.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: C) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Build the truncate event.
    pub fn build(self) -> Result<WalEvent<C>, WalEventBuildError> {
        Ok(WalEvent::Truncate {
            table_id: self.table_id,
            checkpoint: self.checkpoint,
        })
    }
}

impl WalEvent<NoCheckpoint> {
    /// Start fluent construction for a WAL event with no checkpoint.
    ///
    /// Returns a builder pinned to `NoCheckpoint`. This is what synthetic
    /// tests and checkpoint-free contexts want. To produce events with a
    /// concrete checkpoint type, use
    /// [`WalEventBuilderStart::with_typed_checkpoint`] or
    /// [`WalEventBuilderStart::new`].
    #[must_use]
    pub const fn builder(table_id: TableId) -> WalEventBuilderStart<NoCheckpoint> {
        WalEventBuilderStart {
            table_id,
            checkpoint: None,
        }
    }
}

impl<C: Checkpoint> WalEventBuilderStart<C> {
    /// Construct a builder pinned to a concrete `Checkpoint` type, with an
    /// optional checkpoint value. Useful from parser-internal helpers that
    /// receive an `Option<C>` from the wire format.
    #[must_use]
    pub const fn new(table_id: TableId, checkpoint: Option<C>) -> Self {
        Self {
            table_id,
            checkpoint,
        }
    }
}

impl<C: Checkpoint> WalEvent<C> {
    /// Event kind.
    #[must_use]
    pub const fn kind(&self) -> EventKind {
        match self {
            Self::Insert { .. } => EventKind::Insert,
            Self::Update { .. } => EventKind::Update,
            Self::Delete { .. } => EventKind::Delete,
            Self::Truncate { .. } => EventKind::Truncate,
        }
    }

    /// Table ID associated with this event.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        match self {
            Self::Insert { table_id, .. }
            | Self::Update { table_id, .. }
            | Self::Delete { table_id, .. }
            | Self::Truncate { table_id, .. } => *table_id,
        }
    }

    /// Source position of this event in its CDC stream, when known.
    ///
    /// `None` when the parser could not determine a position (e.g. wal2json
    /// messages with no `lsn` field, synthetic test events). Engines + oplogs
    /// propagate this through to notifications.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&C> {
        match self {
            Self::Insert { checkpoint, .. }
            | Self::Update { checkpoint, .. }
            | Self::Delete { checkpoint, .. }
            | Self::Truncate { checkpoint, .. } => checkpoint.as_ref(),
        }
    }

    /// Primary key for row-level events. Truncate returns an empty key.
    #[must_use]
    pub fn pk(&self) -> &PrimaryKey {
        static EMPTY_PK: spin::Lazy<PrimaryKey> = spin::Lazy::new(PrimaryKey::empty);
        match self {
            Self::Insert { pk, .. } | Self::Update { pk, .. } | Self::Delete { pk, .. } => pk,
            Self::Truncate { .. } => &EMPTY_PK,
        }
    }

    /// Replace this event's checkpoint with one of a different type.
    ///
    /// Useful when a parser produces events anchored to one checkpoint
    /// type (e.g. [`crate::NoCheckpoint`] for parsers that read only the
    /// plugin payload) and an outer transport supplies the actual
    /// position out-of-band (e.g. the LSN carried in a pgoutput
    /// `XLogData` frame header). The event's structural content
    /// (`table_id`, `pk`, row images, `changed_columns`) is preserved
    /// byte-for-byte. Only the checkpoint type and value change.
    #[must_use]
    pub fn set_checkpoint<C2: Checkpoint>(self, new_checkpoint: Option<C2>) -> WalEvent<C2> {
        match self {
            Self::Insert {
                table_id,
                pk,
                new_row,
                checkpoint: _,
            } => WalEvent::Insert {
                table_id,
                pk,
                new_row,
                checkpoint: new_checkpoint,
            },
            Self::Update {
                table_id,
                pk,
                old_row,
                new_row,
                changed_columns,
                checkpoint: _,
            } => WalEvent::Update {
                table_id,
                pk,
                old_row,
                new_row,
                changed_columns,
                checkpoint: new_checkpoint,
            },
            Self::Delete {
                table_id,
                pk,
                old_row,
                checkpoint: _,
            } => WalEvent::Delete {
                table_id,
                pk,
                old_row,
                checkpoint: new_checkpoint,
            },
            Self::Truncate {
                table_id,
                checkpoint: _,
            } => WalEvent::Truncate {
                table_id,
                checkpoint: new_checkpoint,
            },
        }
    }

    /// Old row image if present.
    #[must_use]
    pub const fn old_row(&self) -> Option<&RowImage> {
        match self {
            Self::Update { old_row, .. } => old_row.as_ref(),
            Self::Delete { old_row, .. } => Some(old_row),
            Self::Insert { .. } | Self::Truncate { .. } => None,
        }
    }

    /// New row image if present.
    #[must_use]
    pub const fn new_row(&self) -> Option<&RowImage> {
        match self {
            Self::Insert { new_row, .. } | Self::Update { new_row, .. } => Some(new_row),
            Self::Delete { .. } | Self::Truncate { .. } => None,
        }
    }

    /// Columns changed by the update.
    #[must_use]
    pub fn changed_columns(&self) -> &[ColumnId] {
        match self {
            Self::Update {
                changed_columns, ..
            } => changed_columns,
            Self::Insert { .. } | Self::Delete { .. } | Self::Truncate { .. } => &[],
        }
    }
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

/// Eviction policy applied when the registry cap is hit.
///
/// Default is `Reject`: registrations past the cap fail with
/// [`crate::RegisterError::RegistryFull`]. Other variants make room for
/// the incoming subscription by removing an existing one and surface the
/// evicted [`SubscriptionId`]s in [`RegisterResult::evicted`].
///
/// For closure-based policies that pick the victim per-call (e.g. fair
/// share across tenants, custom heuristics that read live activity
/// counters), use
/// [`SubscriptionEngine::with_custom_eviction`](crate::SubscriptionEngine::with_custom_eviction)
/// instead of this enum.
///
/// ```
/// use std::sync::Arc;
///
/// use sql_traits::structs::ParserDB;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use subql::{
///     DefaultIds, EvictionPolicy, RegisterError, SubscriptionEngine,
///     SubscriptionRequest,
/// };
///
/// let database = Arc::new(
///     ParserDB::parse::<PostgreSqlDialect>(
///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
///     )?,
/// );
///
/// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
///     SubscriptionEngine::new(database, PostgreSqlDialect {})
///         .with_max_subscriptions(1, EvictionPolicy::EvictOldest);
///
/// let first = engine.register(SubscriptionRequest::new(
///     1u64,
///     "SELECT * FROM orders WHERE amount > 1",
/// ))?;
/// let second = engine.register(SubscriptionRequest::new(
///     2u64,
///     "SELECT * FROM orders WHERE amount > 2",
/// ))?;
///
/// // The cap was already at 1 when `second` was registered, so the
/// // oldest subscription got evicted to make room.
/// assert_eq!(second.evicted, vec![first.subscription_id]);
/// assert_eq!(engine.subscription_count(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EvictionPolicy {
    /// Hard cap: reject the registration when the registry is full.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     DefaultIds, EvictionPolicy, RegisterError, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    ///     )?,
    /// );
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(1, EvictionPolicy::Reject);
    ///
    /// engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    ///
    /// match engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount > 2",
    /// )) {
    ///     Err(RegisterError::RegistryFull { cap }) => assert_eq!(cap, 1),
    ///     other => panic!("expected RegistryFull, got {other:?}"),
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[default]
    Reject,
    /// Evict the oldest subscription (lowest `SubscriptionId`) and proceed.
    /// Reports the evicted id via [`RegisterResult::evicted`].
    ///
    /// See the [enum-level doctest](EvictionPolicy) for a runnable
    /// example.
    EvictOldest,
    /// Evict the subscription with the oldest `last_dispatch_at`.
    /// Subscriptions never matched by an event are considered "least
    /// active" (their `last_dispatch_at` is `None`, treated as -infinity)
    /// and are evicted first. Ties resolve by oldest `SubscriptionId`.
    ///
    /// Requires activity stamping: configuring this policy makes
    /// [`SubscriptionEngine::consumers`](crate::SubscriptionEngine::consumers)
    /// record the subscriptions that contributed to each match.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     catalog_helpers, Cell, ClockHandle, DefaultIds, EvictionPolicy, ManualClock,
    ///     PrimaryKey, RowImage, SubscriptionEngine, SubscriptionRequest, WalEvent,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    ///     )?,
    /// );
    /// let orders_id = catalog_helpers::table_id(&*database, "orders").unwrap();
    /// let clock = Arc::new(ManualClock::new(0));
    /// let handle: ClockHandle = clock.clone();
    ///
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(2, EvictionPolicy::EvictLeastActive)
    ///         .with_activity_clock(handle);
    ///
    /// let make_event = |id: i64, amount: i64| -> Result<_, Box<dyn std::error::Error>> {
    ///     Ok(WalEvent::builder(orders_id)
    ///         .insert()
    ///         .pk(PrimaryKey::new(Arc::from([0u16]), Arc::from([Cell::Int(id)]))?)
    ///         .new_row(RowImage {
    ///             cells: Arc::from([Cell::Int(id), Cell::Int(amount)]),
    ///         })
    ///         .build()?)
    /// };
    ///
    /// // Register and stamp `first` early. Predicate matches amount = 5.
    /// let first = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount = 5",
    /// ))?;
    /// clock.advance(Duration::from_micros(10));
    /// engine.consumers(&make_event(1, 5)?)?;
    ///
    /// // Register `second` and stamp it later. Predicate matches amount = 10
    /// // exclusively, so `first` is not re-stamped.
    /// let _second = engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount = 10",
    /// ))?;
    /// clock.advance(Duration::from_micros(20));
    /// engine.consumers(&make_event(2, 10)?)?;
    ///
    /// // Third registration hits the cap. `first` was stamped at t=10
    /// // and `_second` at t=30. `first` is the least active and is
    /// // evicted to make room.
    /// let third = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount = 999",
    /// ))?;
    /// assert_eq!(third.evicted, vec![first.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictLeastActive,
    /// Evict the subscription with the lowest `dispatch_count` (the
    /// "coldest" subscription, with the fewest matches over its
    /// lifetime). Ties resolve by oldest `SubscriptionId`.
    ///
    /// Requires activity stamping (see [`EvictLeastActive`](Self::EvictLeastActive)).
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     catalog_helpers, Cell, DefaultIds, EvictionPolicy, PrimaryKey, RowImage,
    ///     SubscriptionEngine, SubscriptionRequest, WalEvent,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    ///     )?,
    /// );
    /// let orders_id = catalog_helpers::table_id(&*database, "orders").unwrap();
    ///
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(2, EvictionPolicy::EvictColdest);
    ///
    /// let _hot = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount = 5",
    /// ))?;
    /// let cold = engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount = 9999",
    /// ))?;
    /// for i in 0..3 {
    ///     let event = WalEvent::builder(orders_id)
    ///         .insert()
    ///         .pk(PrimaryKey::new(Arc::from([0u16]), Arc::from([Cell::Int(i)]))?)
    ///         .new_row(RowImage {
    ///             cells: Arc::from([Cell::Int(i), Cell::Int(5)]),
    ///         })
    ///         .build()?;
    ///     engine.consumers(&event)?;
    /// }
    /// let third = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount = 7",
    /// ))?;
    /// assert_eq!(third.evicted, vec![cold.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictColdest,
    /// Prefer evicting session-scoped subscriptions before durable ones.
    /// Among session subscriptions, the oldest `SubscriptionId` is
    /// evicted first. If no session subscriptions exist, falls back to
    /// [`EvictOldest`](Self::EvictOldest).
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     DefaultIds, EvictionPolicy, SubscriptionEngine, SubscriptionRequest,
    ///     SubscriptionScope,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    ///     )?,
    /// );
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(2, EvictionPolicy::EvictBySession);
    ///
    /// let _durable = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    /// let session = engine.register(
    ///     SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE amount > 2")
    ///         .scope(SubscriptionScope::Session(7_777)),
    /// )?;
    ///
    /// let third = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount > 3",
    /// ))?;
    /// assert_eq!(third.evicted, vec![session.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictBySession,
    /// Evict from the consumer currently holding the most live
    /// subscriptions ("the biggest hog"). Among that consumer's
    /// subscriptions, the oldest `SubscriptionId` is evicted first.
    /// Ties between consumers (same count) resolve by lowest consumer id.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     DefaultIds, EvictionPolicy, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    ///     )?,
    /// );
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(3, EvictionPolicy::EvictByConsumer);
    ///
    /// let hog_a = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    /// let _hog_b = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 2",
    /// ))?;
    /// let _other = engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount > 3",
    /// ))?;
    /// let fourth = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount > 4",
    /// ))?;
    /// // Consumer 1 was the biggest hog (2 subs). The oldest of its
    /// // subscriptions is evicted.
    /// assert_eq!(fourth.evicted, vec![hog_a.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictByConsumer,
}

/// Snapshot of a single subscription's metadata.
///
/// Surfaced through [`SubscriptionsView`] to the closure passed to
/// [`SubscriptionEngine::with_custom_eviction`](crate::SubscriptionEngine::with_custom_eviction).
/// Activity-aware policies use `last_dispatch_at` / `dispatch_count` to
/// pick a victim. Scope/consumer-aware policies use `scope` / `consumer_id`.
/// Subscriptions never matched by an event have `last_dispatch_at = None`
/// and `dispatch_count = 0`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SubscriptionMetadata<I: IdTypes> {
    /// Engine-assigned subscription identifier (monotonic. Lower is older).
    pub subscription_id: SubscriptionId,
    /// Consumer who owns this subscription.
    pub consumer_id: I::ConsumerId,
    /// Lifetime scope: durable or session-bound.
    pub scope: SubscriptionScope<I>,
    /// Microsecond timestamp from the engine's activity clock at the most
    /// recent dispatch that matched this subscription, or `None` if it
    /// has never matched. Compare with other entries within the same
    /// view, never with absolute wall-clock time.
    pub last_dispatch_at: Option<u64>,
    /// Number of dispatch events that matched this subscription since
    /// registration. Saturates at `u64::MAX`.
    pub dispatch_count: u64,
}

impl<I: IdTypes> SubscriptionMetadata<I> {
    /// Construct a [`SubscriptionMetadata`] with the canonical fields.
    /// The struct is `#[non_exhaustive]`. Use this constructor (rather
    /// than a struct literal) so callers compile across future field
    /// additions.
    #[must_use]
    pub const fn new(
        subscription_id: SubscriptionId,
        consumer_id: I::ConsumerId,
        scope: SubscriptionScope<I>,
        last_dispatch_at: Option<u64>,
        dispatch_count: u64,
    ) -> Self {
        Self {
            subscription_id,
            consumer_id,
            scope,
            last_dispatch_at,
            dispatch_count,
        }
    }
}

/// Borrowed read-only view of every live subscription, used by custom
/// eviction closures.
///
/// Cheap to construct (borrows the engine's internal HashMap entries).
/// The closure must not retain the borrow past the call. Lifetime is
/// bounded to the single call into the custom eviction function.
///
/// ```
/// use subql::{DefaultIds, SubscriptionMetadata, SubscriptionScope, SubscriptionsView};
///
/// let entries: Vec<SubscriptionMetadata<DefaultIds>> = vec![
///     SubscriptionMetadata::new(1, 10, SubscriptionScope::Durable, Some(100), 5),
///     SubscriptionMetadata::new(2, 10, SubscriptionScope::Durable, None, 0),
/// ];
/// let view = SubscriptionsView::<DefaultIds>::new(&entries);
/// let coldest = view.iter().min_by_key(|m| m.dispatch_count).unwrap();
/// assert_eq!(coldest.subscription_id, 2);
/// ```
pub struct SubscriptionsView<'a, I: IdTypes> {
    entries: &'a [SubscriptionMetadata<I>],
}

impl<'a, I: IdTypes> SubscriptionsView<'a, I> {
    /// Build a view from a slice of metadata. Public so eviction closures
    /// can be unit-tested with hand-rolled views.
    #[must_use]
    pub const fn new(entries: &'a [SubscriptionMetadata<I>]) -> Self {
        Self { entries }
    }

    /// Iterator over the metadata entries.
    pub fn iter(&self) -> core::slice::Iter<'_, SubscriptionMetadata<I>> {
        self.entries.iter()
    }

    /// Number of live subscriptions in the view.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no live subscriptions are present.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Borrow the underlying metadata slice.
    #[must_use]
    pub const fn as_slice(&self) -> &[SubscriptionMetadata<I>] {
        self.entries
    }
}

impl<'a, I: IdTypes> IntoIterator for &'a SubscriptionsView<'_, I> {
    type Item = &'a SubscriptionMetadata<I>;
    type IntoIter = core::slice::Iter<'a, SubscriptionMetadata<I>>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
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
    /// Subscriptions evicted to make room for this registration.
    ///
    /// Empty under the default policy ([`EvictionPolicy::Reject`]) and
    /// when the registry is below cap. Populated when an eviction policy
    /// freed space. The caller may use this to notify the affected
    /// clients (e.g. send an "evicted" signal over their transport).
    pub evicted: Vec<SubscriptionId>,
}

/// Durability policy for registration writes when storage is enabled.
#[cfg(feature = "std")]
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
#[cfg(feature = "std")]
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
    /// Get interested consumers for a WAL event.
    ///
    /// Returns view-relative notifications: each consumer sees INSERT/DELETE/UPDATE
    /// relative to their own result set.
    fn consumers(
        &mut self,
        event: &WalEvent,
    ) -> Result<ConsumerNotifications<I>, crate::DispatchError>;
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
#[cfg(feature = "std")]
pub trait DurableShardStore: Send {
    /// Snapshot a table partition to durable storage.
    fn snapshot_table(&self, table_id: TableId) -> Result<(), crate::StorageError>;
}

/// Column value type for aggregate validation at registration time.
///
/// Returned by [`crate::catalog_helpers::column_type`] when the live database
/// exposes type information for the column. SUM/AVG over `Bool`/`String`
/// columns is rejected at registration time, while `Unknown` is permissive.
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
    /// Unknown or mixed type (treated as potentially numeric, no error)
    Unknown,
}

/// Typed signed delta from an aggregate subscription.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AggDelta {
    /// COUNT(*) / COUNT(column) delta: always +/-1 per matching (non-NULL) row.
    Count(i64),
    /// SUM(column) delta: signed change in the column sum.
    Sum(f64),
    /// AVG(column) delta: both components needed to update a running average.
    ///
    /// Caller maintains `running_sum` and `running_count` separately:
    /// ```text
    /// running_sum   += sum_delta
    /// running_count += count_delta
    /// avg            = running_sum / running_count  (when running_count > 0)
    /// ```
    Avg { sum_delta: f64, count_delta: i64 },
    /// VAR_POP / VAR_SAMP / STDDEV_POP / STDDEV_SAMP delta. Carries the
    /// three components needed to update a running variance or standard
    /// deviation.
    ///
    /// The kernel is the same for all four functions. The consumer
    /// applies the appropriate derivation to the running tuple:
    /// ```text
    /// running_sum    += sum_delta
    /// running_sum_sq += sum_sq_delta
    /// running_count  += count_delta
    ///
    /// // population variance:
    /// var_pop  = running_sum_sq / N - (running_sum / N)^2
    /// // sample variance (N >= 2):
    /// var_samp = (running_sum_sq - (running_sum)^2 / N) / (N - 1)
    /// stddev_*  = sqrt(var_*)
    /// ```
    /// where `N = running_count`.
    Stats {
        sum_delta: f64,
        sum_sq_delta: f64,
        count_delta: i64,
    },
}

/// Per-consumer notification classification from `consumers()`.
///
/// Each consumer sees events **relative to their own result set** (view-relative
/// deltas), not the base-table operation.  A single base-table UPDATE may
/// produce `Inserted` for one consumer, `Deleted` for another, and `Updated`
/// for a third.
///
/// Carries the originating event's [`Checkpoint`] so downstream replay /
/// oplog code can correlate notifications with positions in the source
/// stream. Default `C = NoCheckpoint` preserves the original API for
/// callers that do not care about positions.
pub struct ConsumerNotifications<I: IdTypes, C: Checkpoint = NoCheckpoint> {
    /// Consumers for whom a row appeared in their result set.
    /// (Base INSERT, or base UPDATE where new row matches but old didn't.)
    pub(crate) inserted: Vec<I::ConsumerId>,
    /// Consumers for whom a row disappeared from their result set.
    /// (Base DELETE, or base UPDATE where old row matched but new doesn't.)
    pub(crate) deleted: Vec<I::ConsumerId>,
    /// Consumers for whom a row changed but remained in their result set.
    /// (Base UPDATE where both old and new rows match.)
    pub(crate) updated: Vec<I::ConsumerId>,
    /// Position of the originating event, when known.
    pub(crate) checkpoint: Option<C>,
}

impl<I: IdTypes, C: Checkpoint> ConsumerNotifications<I, C> {
    /// Create empty notifications with no checkpoint.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            inserted: Vec::new(),
            deleted: Vec::new(),
            updated: Vec::new(),
            checkpoint: None,
        }
    }

    /// Construct notifications from explicit buckets.
    #[must_use]
    pub(crate) const fn from_parts(
        inserted: Vec<I::ConsumerId>,
        deleted: Vec<I::ConsumerId>,
        updated: Vec<I::ConsumerId>,
    ) -> Self {
        Self {
            inserted,
            deleted,
            updated,
            checkpoint: None,
        }
    }

    /// Attach a checkpoint to these notifications.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: Option<C>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Consumers notified as inserted.
    #[must_use]
    pub fn inserted(&self) -> &[I::ConsumerId] {
        &self.inserted
    }

    /// Consumers notified as deleted.
    #[must_use]
    pub fn deleted(&self) -> &[I::ConsumerId] {
        &self.deleted
    }

    /// Consumers notified as updated.
    #[must_use]
    pub fn updated(&self) -> &[I::ConsumerId] {
        &self.updated
    }

    /// Position of the originating event, when the parser provided one.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&C> {
        self.checkpoint.as_ref()
    }

    /// Decompose into `(inserted, deleted, updated)`. The checkpoint is
    /// dropped. Use [`checkpoint`](Self::checkpoint) first if needed.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(self) -> (Vec<I::ConsumerId>, Vec<I::ConsumerId>, Vec<I::ConsumerId>) {
        (self.inserted, self.deleted, self.updated)
    }
}

impl<I: IdTypes, C: Checkpoint> core::fmt::Debug for ConsumerNotifications<I, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConsumerNotifications")
            .field("inserted", &self.inserted)
            .field("deleted", &self.deleted)
            .field("updated", &self.updated)
            .field("checkpoint", &self.checkpoint)
            .finish()
    }
}

/// Iterator over `inserted` and `updated` consumers: those who should see
/// the current row state.
pub struct ConsumerNotificationsIter<I: IdTypes> {
    inserted: alloc::vec::IntoIter<I::ConsumerId>,
    updated: alloc::vec::IntoIter<I::ConsumerId>,
}

impl<I: IdTypes> Iterator for ConsumerNotificationsIter<I> {
    type Item = I::ConsumerId;

    fn next(&mut self) -> Option<Self::Item> {
        self.inserted.next().or_else(|| self.updated.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.inserted.len() + self.updated.len();
        (remaining, Some(remaining))
    }
}

impl<I: IdTypes, C: Checkpoint> IntoIterator for ConsumerNotifications<I, C> {
    type Item = I::ConsumerId;
    type IntoIter = ConsumerNotificationsIter<I>;

    fn into_iter(self) -> Self::IntoIter {
        ConsumerNotificationsIter {
            inserted: self.inserted.into_iter(),
            updated: self.updated.into_iter(),
        }
    }
}

/// Aggregate dispatch: delivers typed signed deltas for aggregate subscriptions.
///
/// Separate from [`SubscriptionDispatch`] because:
/// - UPDATE events require evaluating **both** the old row and the new row.
/// - Returns signed deltas, not consumer bitmaps.
/// - Aggregate predicates are **never** included in `consumers()` results.
///
/// # Caller contract
///
/// The engine handles only WAL-driven deltas. Callers must:
/// 1. **Bootstrap**: query the DB for the initial aggregate **before** subscribing.
/// 2. **Accumulate**: `running_value += delta` on each call.
/// 3. **Reset on policy change**: RLS/ACL changes produce no WAL events.
///    Re-query the DB and replace the stored value.
/// 4. **Reset on TRUNCATE**: engine returns `Err(TruncateRequiresReset)`.
///    Re-query and replace the stored value.
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
#[cfg(feature = "std")]
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
