use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;
use wal2json_events::ChangeV1;

use crate::backend::{CdcEvent, Postgres, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};
use crate::wal::{changed_columns_by_name, resolve_table};

use super::decode_helpers::{decode_cell, IndexedName};
use super::parse_helpers::v1_row_kind;

/// The `(names, values)` parallel arrays `row` selects for `change`, or
/// `None` when the change does not carry that image.
const fn v1_image(change: &ChangeV1, row: RowKind) -> Option<(&[String], &[serde_json::Value])> {
    match (change, row) {
        (ChangeV1::Insert { columns, .. }, RowKind::New | RowKind::Pk)
        | (ChangeV1::Update { columns, .. }, RowKind::New) => Some((
            columns.columnnames.as_slice(),
            columns.columnvalues.as_slice(),
        )),
        (
            ChangeV1::Update { oldkeys, .. } | ChangeV1::Delete { oldkeys, .. },
            RowKind::Old | RowKind::Pk,
        ) => Some((oldkeys.keynames.as_slice(), oldkeys.keyvalues.as_slice())),
        _ => None,
    }
}

/// The `(schema, table)` naming of a v1 row change, for catalog resolution.
/// The schema is empty when `include-schemas=false` left it off the wire.
fn v1_naming(change: &ChangeV1) -> Option<(&str, &str)> {
    Some((change.schema().unwrap_or(""), change.table()?))
}

fn v1_value<'a>(
    names: &[String],
    values: &'a [serde_json::Value],
    name: &str,
) -> Option<&'a serde_json::Value> {
    names
        .iter()
        .position(|n| n == name)
        .and_then(|i| values.get(i))
}

fn v1_index<'a>(
    names: &'a [String],
    values: &'a [serde_json::Value],
) -> HashMap<IndexedName<'a>, &'a serde_json::Value> {
    let mut index = HashMap::with_capacity(names.len().min(values.len()));
    for (name, value) in names.iter().zip(values) {
        index.entry(IndexedName::new(name)).or_insert(value);
    }
    index
}

impl CdcEvent for ChangeV1 {
    type Backend = Postgres;
    type Checkpoint = crate::NoCheckpoint;

    fn kind(&self) -> EventKind {
        v1_row_kind(self).expect(
            "CdcEvent::kind called on a non-row wal2json v1 change. Filter with parse_wal2json_v1 first",
        )
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        v1_naming(self)
            .and_then(|(schema, table)| resolve_table(schema, table, db).ok())
            .unwrap_or(TableId::MAX)
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        None
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        self.pk_columns_resolved(db, self.table_id(db))
    }

    fn pk_columns_resolved<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
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
        let Self::Update {
            columns, oldkeys, ..
        } = self
        else {
            return Vec::new();
        };
        let Ok(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        if columns.columnnames.len() != arity || oldkeys.keynames.len() != arity {
            return Vec::new();
        }
        let old = v1_index(&oldkeys.keynames, &oldkeys.keyvalues);
        let new = v1_index(&columns.columnnames, &columns.columnvalues);
        changed_columns_by_name(db, table_id, arity, |name| {
            (
                old.get(&IndexedName::new(name)).copied(),
                new.get(&IndexedName::new(name)).copied(),
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
        let Some((names, values)) = v1_image(self, row) else {
            return Ok(Value::Missing);
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            return Ok(Value::Missing);
        };
        decode_cell(v1_value(names, values, &name), db, table_id, col)
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
