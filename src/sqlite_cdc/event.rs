#![allow(clippy::match_same_arms)]
//! Typed CDC event surfaced by the SQLite changeset parser.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::backend::{CdcEvent, RowKind, SQLite, Value};
use crate::{ColumnId, EventKind, NoCheckpoint, TableId};
use sql_traits::prelude::DatabaseLike;

/// One row change decoded from a SQLite session-extension changeset.
///
/// Backed by the [`SQLite`] backend. Changesets carry no wire-level
/// position, so the checkpoint type is [`NoCheckpoint`].
///
/// The row images are eagerly decoded at construction. Each row image
/// slot holds a [`Value<SQLite>`] per column ordinal, with
/// [`Value::Missing`] standing in for cells the wire did not carry on
/// that side. Changeset semantics:
///
/// * `Insert`: `new_row` populated for every column; `old_row` absent.
/// * `Update`: both `new_row` and `old_row` carry the primary-key
///   columns plus every changed non-PK column. Unchanged non-PK
///   columns are marked [`Value::Missing`] on both sides. This mirrors
///   the `sqlite3_changeset_old` / `_new` pairing per row.
/// * `Delete`: `old_row` carries the full old-row image (`Value::Null`
///   for cells the row itself held as SQL NULL); `new_row` absent.
///
/// `changed_columns` is authoritative for `Update` events (columns
/// where the changeset's old and new values differ, PK or not),
/// authoritative-empty for `Insert` / `Delete` / `Truncate`.
#[derive(Clone, Debug)]
pub struct SqliteChangesetEvent {
    pub(super) kind: EventKind,
    pub(super) table_id: TableId,
    pub(super) pk_columns: Arc<[ColumnId]>,
    pub(super) changed_columns: Arc<[ColumnId]>,
    pub(super) new_row: Option<Box<[Value<SQLite>]>>,
    pub(super) old_row: Option<Box<[Value<SQLite>]>>,
}

impl SqliteChangesetEvent {
    fn row_view(&self, row: RowKind, col: ColumnId) -> Option<&Value<SQLite>> {
        if row == RowKind::Pk && !self.pk_columns.contains(&col) {
            return None;
        }
        let cells = match (self.kind, row) {
            (EventKind::Truncate, _) => return None,
            (EventKind::Insert, RowKind::New | RowKind::Pk) => self.new_row.as_deref(),
            (EventKind::Delete, RowKind::Old | RowKind::Pk) => self.old_row.as_deref(),
            (EventKind::Update, RowKind::New) => self.new_row.as_deref(),
            (EventKind::Update, RowKind::Old | RowKind::Pk) => self.old_row.as_deref(),
            _ => None,
        }?;
        cells.get(col as usize)
    }
}

impl CdcEvent for SqliteChangesetEvent {
    type Backend = SQLite;
    type Checkpoint = NoCheckpoint;

    fn kind(&self) -> EventKind {
        self.kind
    }
    fn table_id<DB: DatabaseLike>(&self, _db: &DB) -> TableId {
        self.table_id
    }
    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        None
    }
    fn pk_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
        self.pk_columns.to_vec()
    }
    fn changed_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
        self.changed_columns.to_vec()
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        _db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Value<Self::Backend> {
        self.row_view(row, col).cloned().unwrap_or(Value::Missing)
    }
}
