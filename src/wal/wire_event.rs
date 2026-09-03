//! The shape the four wire readers share, and the one place their
//! [`CdcEvent`](crate::backend::CdcEvent) impl is written.
//!
//! A wire event knows its table only by name, so every read of it resolves
//! that name first. [`WireEvent`] therefore takes an already-resolved
//! [`TableId`] on each accessor, and the macro below is what pairs those
//! accessors with the public trait: the public methods resolve the table once
//! and hand it down, and the doc-hidden resolved hooks pass through the id
//! their caller already holds.

use crate::backend::{Backend, RowKind, Value};
use crate::types::{ColumnId, EventKind, TableId};
use crate::Checkpoint;
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;

/// One CDC wire format, read against an already-resolved table.
pub trait WireEvent {
    /// The database this format observes.
    type Backend: Backend;
    /// The position this format carries, if any.
    type Checkpoint: Checkpoint;

    /// Which flavour of event this is.
    fn wire_kind(&self) -> EventKind;

    /// The table the event names, resolved against `db`.
    fn wire_table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId;

    /// The position the event carries, if any.
    fn wire_checkpoint(&self) -> Option<Self::Checkpoint>;

    /// One cell of the `row` image.
    fn wire_value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError>;

    /// The columns an UPDATE moved, empty for every other kind.
    fn wire_changed_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId>;

    /// The primary key, which every format reads from the catalog.
    fn wire_pk_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        crate::catalog_helpers::primary_key_columns(db, table_id).unwrap_or_default()
    }

    /// The image a primary-key read takes, or `None` for an event with no row.
    fn pk_row(&self) -> Option<RowKind> {
        match self.wire_kind() {
            EventKind::Insert => Some(RowKind::New),
            EventKind::Update | EventKind::Delete => Some(RowKind::Old),
            EventKind::Truncate => None,
        }
    }

    /// One cell of a column the caller already knows is part of the key, so
    /// the key membership is not looked up again.
    fn wire_value_at_known_pk<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        let Some(row) = self.pk_row() else {
            return Ok(Value::Missing);
        };
        self.wire_value_at(db, table_id, row, col)
    }
}

/// Write the [`CdcEvent`](crate::backend::CdcEvent) impl for a [`WireEvent`].
///
/// Every wire format's impl is the same delegation, so it is spelled once
/// here rather than four times over.
macro_rules! wire_cdc_event {
    ($wire:ty, $backend:ty, $checkpoint:ty) => {
        impl $crate::backend::CdcEvent for $wire {
            type Backend = $backend;
            type Checkpoint = $checkpoint;

            fn kind(&self) -> $crate::types::EventKind {
                $crate::wal::wire_event::WireEvent::wire_kind(self)
            }

            fn table_id<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
            ) -> $crate::types::TableId {
                $crate::wal::wire_event::WireEvent::wire_table_id(self, db)
            }

            fn checkpoint(&self) -> Option<Self::Checkpoint> {
                $crate::wal::wire_event::WireEvent::wire_checkpoint(self)
            }

            fn pk_columns<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
            ) -> alloc::vec::Vec<$crate::types::ColumnId> {
                let table_id = $crate::wal::wire_event::WireEvent::wire_table_id(self, db);
                $crate::wal::wire_event::WireEvent::wire_pk_columns(self, db, table_id)
            }

            fn pk_columns_resolved<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
                table_id: $crate::types::TableId,
            ) -> alloc::vec::Vec<$crate::types::ColumnId> {
                $crate::wal::wire_event::WireEvent::wire_pk_columns(self, db, table_id)
            }

            fn changed_columns<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
            ) -> alloc::vec::Vec<$crate::types::ColumnId> {
                let table_id = $crate::wal::wire_event::WireEvent::wire_table_id(self, db);
                $crate::wal::wire_event::WireEvent::wire_changed_columns(self, db, table_id)
            }

            fn changed_columns_resolved<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
                table_id: $crate::types::TableId,
            ) -> alloc::vec::Vec<$crate::types::ColumnId> {
                $crate::wal::wire_event::WireEvent::wire_changed_columns(self, db, table_id)
            }

            fn value_at<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
                row: $crate::backend::RowKind,
                col: $crate::types::ColumnId,
            ) -> Result<$crate::backend::Value<Self::Backend>, $crate::ValueError> {
                let table_id = $crate::wal::wire_event::WireEvent::wire_table_id(self, db);
                $crate::wal::wire_event::WireEvent::wire_value_at(self, db, table_id, row, col)
            }

            fn value_at_resolved<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
                table_id: $crate::types::TableId,
                row: $crate::backend::RowKind,
                col: $crate::types::ColumnId,
            ) -> Result<$crate::backend::Value<Self::Backend>, $crate::ValueError> {
                $crate::wal::wire_event::WireEvent::wire_value_at(self, db, table_id, row, col)
            }

            fn value_at_known_pk<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
                col: $crate::types::ColumnId,
            ) -> Result<$crate::backend::Value<Self::Backend>, $crate::ValueError> {
                let table_id = $crate::wal::wire_event::WireEvent::wire_table_id(self, db);
                $crate::wal::wire_event::WireEvent::wire_value_at_known_pk(self, db, table_id, col)
            }

            fn value_at_known_pk_resolved<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                db: &DB,
                table_id: $crate::types::TableId,
                col: $crate::types::ColumnId,
            ) -> Result<$crate::backend::Value<Self::Backend>, $crate::ValueError> {
                $crate::wal::wire_event::WireEvent::wire_value_at_known_pk(self, db, table_id, col)
            }
        }
    };
}

pub(crate) use wire_cdc_event;
