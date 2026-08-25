#![allow(clippy::match_same_arms)]
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

use crate::backend::{Backend, CdcEvent, RowKind, Value};
use crate::checkpoint::{Checkpoint, NoCheckpoint};
use crate::{ColumnId, EventKind, TableId};
use sql_traits::prelude::DatabaseLike;

/// Concrete [`CdcEvent`] fixture for tests.
///
/// Fields are public so tests can mutate individual row images without
/// going through the builder API. [`value_at`](CdcEvent::value_at) reads
/// the requested [`RowKind`] and returns the stored [`Value`].
///
/// # Semantics
///
/// * A column index out of range for the requested view returns
///   [`Value::Missing`].
/// * A stored [`Value::Missing`] or [`Value::Null`] returns itself.
/// * [`RowKind::Pk`] on a `col` not listed in `pk_columns` returns
///   [`Value::Missing`], matching the design contract.
#[derive(Clone, Debug)]
pub struct TestEvent<B: Backend, C: Checkpoint = NoCheckpoint> {
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
    /// Position in the change stream. `None` unless
    /// [`with_checkpoint`](Self::with_checkpoint) set one, which is what
    /// the default `C = NoCheckpoint` leaves it at.
    pub checkpoint: Option<C>,
}

impl<B: Backend, C: Checkpoint> TestEvent<B, C> {
    /// Build an [`EventKind::Insert`] event for `table_id` carrying
    /// `new_row`. `pk_columns` defaults to empty (chain
    /// [`with_pk_columns`](Self::with_pk_columns) when the test needs a
    /// PK view).
    #[must_use]
    pub const fn insert(table_id: TableId, new_row: Vec<Value<B>>) -> Self {
        Self {
            kind: EventKind::Insert,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row,
            old_row: Vec::new(),
            checkpoint: None,
        }
    }

    /// Build an [`EventKind::Update`] event for `table_id`.
    #[must_use]
    pub const fn update(table_id: TableId, old_row: Vec<Value<B>>, new_row: Vec<Value<B>>) -> Self {
        Self {
            kind: EventKind::Update,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row,
            old_row,
            checkpoint: None,
        }
    }

    /// Build an [`EventKind::Delete`] event for `table_id` carrying
    /// `old_row`.
    #[must_use]
    pub const fn delete(table_id: TableId, old_row: Vec<Value<B>>) -> Self {
        Self {
            kind: EventKind::Delete,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row: Vec::new(),
            old_row,
            checkpoint: None,
        }
    }

    /// Build an [`EventKind::Truncate`] event for `table_id`.
    #[must_use]
    pub const fn truncate(table_id: TableId) -> Self {
        Self {
            kind: EventKind::Truncate,
            table_id,
            pk_columns: Vec::new(),
            changed_columns: Vec::new(),
            new_row: Vec::new(),
            old_row: Vec::new(),
            checkpoint: None,
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

    /// Place the event at `checkpoint` in the change stream.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: C) -> Self {
        self.checkpoint = Some(checkpoint);
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
}

impl<B: Backend, C: Checkpoint> CdcEvent for TestEvent<B, C> {
    type Backend = B;
    type Checkpoint = C;

    fn kind(&self) -> EventKind {
        self.kind
    }
    fn table_id<DB: DatabaseLike>(&self, _db: &DB) -> TableId {
        self.table_id
    }
    fn checkpoint(&self) -> Option<C> {
        self.checkpoint.clone()
    }
    fn pk_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
        self.pk_columns.clone()
    }
    fn changed_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
        self.changed_columns.clone()
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        _db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        Ok(self.cell(row, col).cloned().unwrap_or(Value::Missing))
    }
}
