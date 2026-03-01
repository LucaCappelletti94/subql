//! Core type definitions for subql

use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeSet;
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

impl std::fmt::Display for PrimaryKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "primary key columns/values length mismatch: columns={}, values={}",
            self.columns_len, self.values_len
        )
    }
}

impl std::error::Error for PrimaryKeyError {}

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

/// WAL event from PostgreSQL CDC
#[derive(Clone, Debug)]
pub enum WalEvent {
    /// Row insertion.
    Insert {
        /// Table this event belongs to.
        table_id: TableId,
        /// Primary key of affected row.
        pk: PrimaryKey,
        /// New row image.
        new_row: RowImage,
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
    },
    /// Row deletion.
    Delete {
        /// Table this event belongs to.
        table_id: TableId,
        /// Primary key of affected row.
        pk: PrimaryKey,
        /// Old row image.
        old_row: RowImage,
    },
    /// Table truncate.
    Truncate {
        /// Table this event belongs to.
        table_id: TableId,
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

impl std::fmt::Display for WalEventBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingNewRow => write!(f, "missing required field: new_row"),
            Self::MissingOldRow => write!(f, "missing required field: old_row"),
            Self::MismatchedPkLengths {
                columns_len,
                values_len,
            } => {
                write!(
                    f,
                    "primary key columns/values length mismatch: columns={}, values={}",
                    columns_len, values_len
                )
            }
            Self::DuplicatePkColumn(col) => write!(f, "duplicate primary key column: {col}"),
            Self::FieldNotAllowedForKind(field) => {
                write!(f, "field '{field}' is not allowed for this event kind")
            }
        }
    }
}

impl std::error::Error for WalEventBuildError {}

/// Start of fluent `WalEvent` construction.
#[derive(Clone, Debug)]
pub struct WalEventBuilderStart {
    table_id: TableId,
}

/// Fluent builder for [`WalEvent::Insert`].
#[derive(Clone, Debug)]
pub struct InsertEventBuilder {
    table_id: TableId,
    pk_columns: Vec<ColumnId>,
    pk_values: Vec<Cell>,
    new_row: Option<RowImage>,
}

/// Fluent builder for [`WalEvent::Update`].
#[derive(Clone, Debug)]
pub struct UpdateEventBuilder {
    table_id: TableId,
    pk_columns: Vec<ColumnId>,
    pk_values: Vec<Cell>,
    old_row: Option<RowImage>,
    new_row: Option<RowImage>,
    changed_columns: Arc<[ColumnId]>,
}

/// Fluent builder for [`WalEvent::Delete`].
#[derive(Clone, Debug)]
pub struct DeleteEventBuilder {
    table_id: TableId,
    pk_columns: Vec<ColumnId>,
    pk_values: Vec<Cell>,
    old_row: Option<RowImage>,
}

/// Fluent builder for [`WalEvent::Truncate`].
#[derive(Clone, Debug)]
pub struct TruncateEventBuilder {
    table_id: TableId,
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

fn apply_pk(columns: &mut Vec<ColumnId>, values: &mut Vec<Cell>, pk: PrimaryKey) {
    columns.clear();
    values.clear();
    for (&col, value) in pk.columns().iter().zip(pk.values().iter()) {
        columns.push(col);
        values.push(value.clone());
    }
}

impl WalEventBuilderStart {
    /// Start building an INSERT event.
    #[must_use]
    pub fn insert(self) -> InsertEventBuilder {
        InsertEventBuilder {
            table_id: self.table_id,
            pk_columns: Vec::new(),
            pk_values: Vec::new(),
            new_row: None,
        }
    }

    /// Start building an UPDATE event.
    #[must_use]
    pub fn update(self) -> UpdateEventBuilder {
        UpdateEventBuilder {
            table_id: self.table_id,
            pk_columns: Vec::new(),
            pk_values: Vec::new(),
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        }
    }

    /// Start building a DELETE event.
    #[must_use]
    pub fn delete(self) -> DeleteEventBuilder {
        DeleteEventBuilder {
            table_id: self.table_id,
            pk_columns: Vec::new(),
            pk_values: Vec::new(),
            old_row: None,
        }
    }

    /// Start building a TRUNCATE event.
    #[must_use]
    pub fn truncate(self) -> TruncateEventBuilder {
        TruncateEventBuilder {
            table_id: self.table_id,
        }
    }
}

impl InsertEventBuilder {
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

    /// Build the insert event.
    pub fn build(self) -> Result<WalEvent, WalEventBuildError> {
        let new_row = self.new_row.ok_or(WalEventBuildError::MissingNewRow)?;
        let pk = build_primary_key(self.pk_columns, self.pk_values)?;
        Ok(WalEvent::Insert {
            table_id: self.table_id,
            pk,
            new_row,
        })
    }
}

impl UpdateEventBuilder {
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

    /// Build the update event.
    pub fn build(self) -> Result<WalEvent, WalEventBuildError> {
        let new_row = self.new_row.ok_or(WalEventBuildError::MissingNewRow)?;
        let pk = build_primary_key(self.pk_columns, self.pk_values)?;
        Ok(WalEvent::Update {
            table_id: self.table_id,
            pk,
            old_row: self.old_row,
            new_row,
            changed_columns: self.changed_columns,
        })
    }
}

impl DeleteEventBuilder {
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

    /// Build the delete event.
    pub fn build(self) -> Result<WalEvent, WalEventBuildError> {
        let old_row = self.old_row.ok_or(WalEventBuildError::MissingOldRow)?;
        let pk = build_primary_key(self.pk_columns, self.pk_values)?;
        Ok(WalEvent::Delete {
            table_id: self.table_id,
            pk,
            old_row,
        })
    }
}

impl TruncateEventBuilder {
    /// Build the truncate event.
    pub fn build(self) -> Result<WalEvent, WalEventBuildError> {
        Ok(WalEvent::Truncate {
            table_id: self.table_id,
        })
    }
}

impl WalEvent {
    /// Start fluent construction for a WAL event.
    #[must_use]
    pub fn builder(table_id: TableId) -> WalEventBuilderStart {
        WalEventBuilderStart { table_id }
    }

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
            | Self::Truncate { table_id } => *table_id,
        }
    }

    /// Primary key for row-level events. Truncate returns an empty key.
    #[must_use]
    pub fn pk(&self) -> &PrimaryKey {
        static EMPTY_PK: std::sync::LazyLock<PrimaryKey> =
            std::sync::LazyLock::new(PrimaryKey::empty);
        match self {
            Self::Insert { pk, .. } | Self::Update { pk, .. } | Self::Delete { pk, .. } => pk,
            Self::Truncate { .. } => &EMPTY_PK,
        }
    }

    /// Old row image if present.
    #[must_use]
    pub fn old_row(&self) -> Option<&RowImage> {
        match self {
            Self::Update { old_row, .. } => old_row.as_ref(),
            Self::Delete { old_row, .. } => Some(old_row),
            Self::Insert { .. } | Self::Truncate { .. } => None,
        }
    }

    /// New row image if present.
    #[must_use]
    pub fn new_row(&self) -> Option<&RowImage> {
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

/// Per-consumer notification classification from `consumers()`.
///
/// Each consumer sees events **relative to their own result set** (view-relative
/// deltas), not the base-table operation.  A single base-table UPDATE may
/// produce `Inserted` for one consumer, `Deleted` for another, and `Updated`
/// for a third.
pub struct ConsumerNotifications<I: IdTypes> {
    /// Consumers for whom a row appeared in their result set.
    /// (Base INSERT, or base UPDATE where new row matches but old didn't.)
    pub(crate) inserted: Vec<I::ConsumerId>,
    /// Consumers for whom a row disappeared from their result set.
    /// (Base DELETE, or base UPDATE where old row matched but new doesn't.)
    pub(crate) deleted: Vec<I::ConsumerId>,
    /// Consumers for whom a row changed but remained in their result set.
    /// (Base UPDATE where both old and new rows match.)
    pub(crate) updated: Vec<I::ConsumerId>,
}

impl<I: IdTypes> ConsumerNotifications<I> {
    /// Create empty notifications.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            inserted: Vec::new(),
            deleted: Vec::new(),
            updated: Vec::new(),
        }
    }

    /// Construct notifications from explicit buckets.
    #[must_use]
    pub(crate) fn from_parts(
        inserted: Vec<I::ConsumerId>,
        deleted: Vec<I::ConsumerId>,
        updated: Vec<I::ConsumerId>,
    ) -> Self {
        Self {
            inserted,
            deleted,
            updated,
        }
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

    /// Decompose into `(inserted, deleted, updated)`.
    #[must_use]
    pub fn into_parts(self) -> (Vec<I::ConsumerId>, Vec<I::ConsumerId>, Vec<I::ConsumerId>) {
        (self.inserted, self.deleted, self.updated)
    }
}

impl<I: IdTypes> std::fmt::Debug for ConsumerNotifications<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerNotifications")
            .field("inserted", &self.inserted)
            .field("deleted", &self.deleted)
            .field("updated", &self.updated)
            .finish()
    }
}

/// Iterator over `inserted ∪ updated` consumers — those who should see the
/// current row state.
pub struct ConsumerNotificationsIter<I: IdTypes> {
    inserted: std::vec::IntoIter<I::ConsumerId>,
    updated: std::vec::IntoIter<I::ConsumerId>,
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

impl<I: IdTypes> IntoIterator for ConsumerNotifications<I> {
    type Item = I::ConsumerId;
    type IntoIter = ConsumerNotificationsIter<I>;

    fn into_iter(self) -> Self::IntoIter {
        ConsumerNotificationsIter {
            inserted: self.inserted.into_iter(),
            updated: self.updated.into_iter(),
        }
    }
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
