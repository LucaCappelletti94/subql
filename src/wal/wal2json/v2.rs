use alloc::vec::Vec;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;
use wal2json_events::{Action, Column, MessageV2, RowV2};

use crate::backend::{CdcEvent, Postgres, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};
use crate::wal::{changed_columns_by_name, resolve_table};

use super::decode_helpers::{column_value, decode_cell, IndexedName};
use super::parse_helpers::v2_row_kind;

/// The row payload, for the row actions that carry one.
const fn v2_row(msg: &MessageV2) -> Option<&RowV2> {
    match msg {
        MessageV2::Insert(row) | MessageV2::Update(row) | MessageV2::Delete(row) => Some(row),
        MessageV2::Begin(_)
        | MessageV2::Commit(_)
        | MessageV2::Truncate(_)
        | MessageV2::Message(_) => None,
    }
}

fn v2_image(msg: &MessageV2, row: RowKind) -> Option<&[Column]> {
    let payload = v2_row(msg)?;
    match (msg.action(), row) {
        (Action::Insert, RowKind::New | RowKind::Pk) | (Action::Update, RowKind::New) => {
            payload.columns.as_deref()
        }
        (Action::Delete | Action::Update, RowKind::Old | RowKind::Pk) => {
            payload.identity.as_deref()
        }
        _ => None,
    }
}

fn v2_index(columns: &[Column]) -> HashMap<IndexedName<'_>, &Column> {
    let mut index = HashMap::with_capacity(columns.len());
    for column in columns {
        index
            .entry(IndexedName::new(&column.name))
            .or_insert(column);
    }
    index
}

fn v2_table_id<DB: DatabaseLike>(msg: &MessageV2, db: &DB) -> Option<TableId> {
    let schema = msg.schema().unwrap_or("");
    let table = msg.table()?;
    resolve_table(schema, table, db).ok()
}

impl CdcEvent for MessageV2 {
    type Backend = Postgres;
    type Checkpoint = crate::PgLsn;

    fn kind(&self) -> EventKind {
        v2_row_kind(self.action()).expect(
            "CdcEvent::kind called on a non-row wal2json v2 message. Filter with parse_wal2json_v2 first",
        )
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        v2_table_id(self, db).unwrap_or(TableId::MAX)
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        let lsn = match self {
            Self::Insert(row) | Self::Update(row) | Self::Delete(row) => row.lsn.as_deref(),
            Self::Truncate(truncate) => truncate.lsn.as_deref(),
            Self::Begin(_) | Self::Commit(_) | Self::Message(_) => None,
        };
        lsn.and_then(crate::PgLsn::parse)
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        self.pk_columns_resolved(db, self.table_id(db))
    }

    fn pk_columns_resolved<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        if self.action() == Action::Truncate {
            return Vec::new();
        }
        catalog_helpers::primary_key_columns(db, table_id).unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        self.changed_columns_resolved(db, self.table_id(db))
    }

    fn changed_columns_resolved<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
    ) -> Vec<ColumnId> {
        if self.action() != Action::Update {
            return Vec::new();
        }
        let Some(payload) = v2_row(self) else {
            return Vec::new();
        };
        let (Some(new_cols), Some(old_cols)) =
            (payload.columns.as_deref(), payload.identity.as_deref())
        else {
            return Vec::new();
        };
        let Ok(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        if new_cols.len() != arity || old_cols.len() != arity {
            return Vec::new();
        }
        let old = v2_index(old_cols);
        let new = v2_index(new_cols);
        changed_columns_by_name(db, table_id, arity, |name| {
            (
                old.get(&IndexedName::new(name))
                    .and_then(|column| column.value.as_ref()),
                new.get(&IndexedName::new(name))
                    .and_then(|column| column.value.as_ref()),
            )
        })
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        self.value_at_resolved(db, self.table_id(db), row, col)
    }

    fn value_at_resolved<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        if row == RowKind::Pk
            && !catalog_helpers::primary_key_columns(db, table_id)
                .is_ok_and(|columns| columns.contains(&col))
        {
            return Ok(Value::Missing);
        }
        let Some(columns) = v2_image(self, row) else {
            return Ok(Value::Missing);
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            return Ok(Value::Missing);
        };
        decode_cell(column_value(columns, &name), db, table_id, col)
    }

    fn value_at_known_pk_resolved<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        let row = match self.kind() {
            EventKind::Insert => RowKind::New,
            EventKind::Update | EventKind::Delete => RowKind::Old,
            EventKind::Truncate => return Ok(Value::Missing),
        };
        self.value_at_resolved(db, table_id, row, col)
    }
}
