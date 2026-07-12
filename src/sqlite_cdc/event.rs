#![allow(clippy::match_same_arms)]
//! Typed CDC event surfaced by the SQLite changeset parser.

use alloc::boxed::Box;
use alloc::sync::Arc;

use crate::backend::{CdcEvent, Presence, RowKind, SQLite, Value};
use crate::{ColumnId, EventKind, NoCheckpoint, TableId};

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

/// Present the row lookup + variant match as a single macro so each
/// scalar accessor stays a one-liner.
macro_rules! changeset_scalar_accessor {
    ($self:ident, $row:ident, $col:ident, $variant:ident) => {{
        let Some(v) = $self.row_view($row, $col) else {
            return Presence::Missing;
        };
        match v {
            Value::$variant(x) => Presence::Present(x),
            Value::Null => Presence::Null,
            _ => Presence::Missing,
        }
    }};
}

impl CdcEvent for SqliteChangesetEvent {
    type Backend = SQLite;
    type Checkpoint = NoCheckpoint;

    fn kind(&self) -> EventKind {
        self.kind
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn checkpoint(&self) -> Option<&Self::Checkpoint> {
        None
    }

    fn pk_columns(&self) -> &[ColumnId] {
        &self.pk_columns
    }

    fn changed_columns(&self) -> &[ColumnId] {
        &self.changed_columns
    }

    fn bool_at(&self, row: RowKind, col: ColumnId) -> Presence<&i64> {
        changeset_scalar_accessor!(self, row, col, Bool)
    }

    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&i64> {
        changeset_scalar_accessor!(self, row, col, Int)
    }

    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&f64> {
        changeset_scalar_accessor!(self, row, col, Float)
    }

    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        changeset_scalar_accessor!(self, row, col, String)
    }

    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::vec::Vec<u8>> {
        changeset_scalar_accessor!(self, row, col, Bytes)
    }

    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        changeset_scalar_accessor!(self, row, col, Uuid)
    }

    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDateTime> {
        changeset_scalar_accessor!(self, row, col, Timestamp)
    }

    fn timestamp_tz_at(
        &self,
        row: RowKind,
        col: ColumnId,
    ) -> Presence<&chrono::DateTime<chrono::Utc>> {
        changeset_scalar_accessor!(self, row, col, TimestampTz)
    }

    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDate> {
        changeset_scalar_accessor!(self, row, col, Date)
    }

    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveTime> {
        changeset_scalar_accessor!(self, row, col, Time)
    }

    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&bigdecimal::BigDecimal> {
        changeset_scalar_accessor!(self, row, col, Decimal)
    }

    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        changeset_scalar_accessor!(self, row, col, Json)
    }

    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        changeset_scalar_accessor!(self, row, col, Jsonb)
    }
}
