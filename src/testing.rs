//! Test-only fixtures shared across unit tests, doctests, and
//! integration tests.
//!
//! [`TestEvent<B>`] is the concrete [`CdcEvent`] tests build to drive
//! the engine when no real WAL parser is in the loop. It stays in the
//! production build (rather than under `#[cfg(test)]`) so doctests and
//! downstream integration tests can construct it the same way as unit
//! tests. The type is small and shape-stable, so leaving it always
//! compiled keeps every test path unified.

use alloc::vec::Vec;

use crate::backend::{Backend, CdcEvent, Presence, RowKind, Value};
use crate::checkpoint::NoCheckpoint;
use crate::{ColumnId, EventKind, TableId};

/// Concrete [`CdcEvent`] fixture for tests.
///
/// Fields are public so tests can mutate individual row images without
/// going through the builder API. The scalar accessors dispatch on the
/// requested [`RowKind`] and return the matching scalar variant.
///
/// # Semantics
///
/// * Column index out of range for the requested view returns
///   [`Presence::Missing`].
/// * [`Value::Missing`] at that index returns [`Presence::Missing`].
/// * [`Value::Null`] at that index returns [`Presence::Null`].
/// * Wrong scalar variant vs. accessor returns [`Presence::Missing`].
///   In a real event this would be a compiler bug. In a test it means
///   the fixture and the compiled program disagree on a column's
///   scalar shape.
/// * [`RowKind::Pk`] on a `col` not listed in `pk_columns` returns
///   [`Presence::Missing`], matching the design contract.
#[derive(Clone, Debug)]
pub struct TestEvent<B: Backend> {
    /// Event kind ([`EventKind::Insert`], [`EventKind::Update`],
    /// [`EventKind::Delete`], [`EventKind::Truncate`]).
    pub kind: EventKind,
    /// Table id the event applies to.
    pub table_id: TableId,
    /// Primary key column ordinals for this row.
    pub pk_columns: Vec<ColumnId>,
    /// Columns changed by an update. Empty for insert / delete /
    /// truncate.
    pub changed_columns: Vec<ColumnId>,
    /// Post-image values, index-aligned with [`ColumnId`]. Empty for
    /// delete / truncate.
    pub new_row: Vec<Value<B>>,
    /// Pre-image values, index-aligned with [`ColumnId`]. Empty for
    /// insert / truncate.
    pub old_row: Vec<Value<B>>,
}

impl<B: Backend> TestEvent<B> {
    /// Build an [`EventKind::Insert`] event for `table_id` carrying
    /// `new_row`. `pk_columns` defaults to empty (chain
    /// [`with_pk_columns`](Self::with_pk_columns) when the test needs a
    /// PK view).
    #[must_use]
    pub fn insert(table_id: TableId, new_row: Vec<Value<B>>) -> Self {
        Self {
            kind: EventKind::Insert,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row,
            old_row: Vec::new(),
        }
    }

    /// Build an [`EventKind::Update`] event for `table_id`.
    #[must_use]
    pub fn update(table_id: TableId, old_row: Vec<Value<B>>, new_row: Vec<Value<B>>) -> Self {
        Self {
            kind: EventKind::Update,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row,
            old_row,
        }
    }

    /// Build an [`EventKind::Delete`] event for `table_id` carrying
    /// `old_row`.
    #[must_use]
    pub fn delete(table_id: TableId, old_row: Vec<Value<B>>) -> Self {
        Self {
            kind: EventKind::Delete,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row: Vec::new(),
            old_row,
        }
    }

    /// Build an [`EventKind::Truncate`] event for `table_id`.
    #[must_use]
    pub fn truncate(table_id: TableId) -> Self {
        Self {
            kind: EventKind::Truncate,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row: Vec::new(),
            old_row: Vec::new(),
        }
    }

    /// Set the PK column ordinals.
    #[must_use]
    pub fn with_pk_columns(mut self, pk_columns: impl IntoIterator<Item = ColumnId>) -> Self {
        self.pk_columns = pk_columns.into_iter().collect();
        self
    }

    /// Set the changed column ordinals (only meaningful for an
    /// [`EventKind::Update`]).
    #[must_use]
    pub fn with_changed_columns(
        mut self,
        changed_columns: impl IntoIterator<Item = ColumnId>,
    ) -> Self {
        self.changed_columns = changed_columns.into_iter().collect();
        self
    }

    fn read_view(&self, row: RowKind) -> &[Value<B>] {
        match row {
            RowKind::New => &self.new_row,
            RowKind::Old => &self.old_row,
            // PK values live in whichever image the event carries. For
            // Insert / Update we read from `new_row`. For Delete we
            // read from `old_row`. Truncate has no row so the caller
            // gets an empty slice.
            RowKind::Pk => match self.kind {
                EventKind::Delete => &self.old_row,
                EventKind::Insert | EventKind::Update => &self.new_row,
                EventKind::Truncate => &[],
            },
        }
    }

    fn cell(&self, row: RowKind, col: ColumnId) -> Option<&Value<B>> {
        if row == RowKind::Pk && !self.pk_columns.contains(&col) {
            return None;
        }
        self.read_view(row).get(col as usize)
    }

    fn read<T>(
        &self,
        row: RowKind,
        col: ColumnId,
        extract: impl FnOnce(&Value<B>) -> Option<&T>,
    ) -> Presence<&T> {
        match self.cell(row, col) {
            None => Presence::Missing,
            Some(Value::Missing) => Presence::Missing,
            Some(Value::Null) => Presence::Null,
            Some(v) => extract(v).map_or(Presence::Missing, Presence::Present),
        }
    }
}

impl<B: Backend> CdcEvent for TestEvent<B> {
    type Backend = B;
    type Checkpoint = NoCheckpoint;

    fn kind(&self) -> EventKind {
        self.kind
    }
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn checkpoint(&self) -> Option<&NoCheckpoint> {
        None
    }
    fn pk_columns(&self) -> &[ColumnId] {
        &self.pk_columns
    }
    fn changed_columns(&self) -> &[ColumnId] {
        &self.changed_columns
    }

    fn bool_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Bool> {
        self.read(row, col, |v| match v {
            Value::Bool(x) => Some(x),
            _ => None,
        })
    }
    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Int> {
        self.read(row, col, |v| match v {
            Value::Int(x) => Some(x),
            _ => None,
        })
    }
    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Float> {
        self.read(row, col, |v| match v {
            Value::Float(x) => Some(x),
            _ => None,
        })
    }
    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::String> {
        self.read(row, col, |v| match v {
            Value::String(x) => Some(x),
            _ => None,
        })
    }
    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Bytes> {
        self.read(row, col, |v| match v {
            Value::Bytes(x) => Some(x),
            _ => None,
        })
    }
    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Uuid> {
        self.read(row, col, |v| match v {
            Value::Uuid(x) => Some(x),
            _ => None,
        })
    }
    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Timestamp> {
        self.read(row, col, |v| match v {
            Value::Timestamp(x) => Some(x),
            _ => None,
        })
    }
    fn timestamp_tz_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::TimestampTz> {
        self.read(row, col, |v| match v {
            Value::TimestampTz(x) => Some(x),
            _ => None,
        })
    }
    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Date> {
        self.read(row, col, |v| match v {
            Value::Date(x) => Some(x),
            _ => None,
        })
    }
    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Time> {
        self.read(row, col, |v| match v {
            Value::Time(x) => Some(x),
            _ => None,
        })
    }
    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Decimal> {
        self.read(row, col, |v| match v {
            Value::Decimal(x) => Some(x),
            _ => None,
        })
    }
    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Json> {
        self.read(row, col, |v| match v {
            Value::Json(x) => Some(x),
            _ => None,
        })
    }
    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&B::Jsonb> {
        self.read(row, col, |v| match v {
            Value::Jsonb(x) => Some(x),
            _ => None,
        })
    }
}
