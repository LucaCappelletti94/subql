//! [`CdcEvent`] for [`pg_walstream::ChangeEvent`].
//!
//! subql's Postgres CDC sources yield the ecosystem [`ChangeEvent`] type
//! directly, decoded by [`pg_walstream::PgOutputDecoder`]. This module is
//! the subql-side view over it: it resolves the event's table and column
//! names to catalog ordinals and decodes each cell against the catalog
//! scalar kind on demand. It replaces the former bespoke `PgOutputParser`
//! and `PgOutputEvent` pair, which re-implemented the relation caching and
//! tuple assembly that `pg_walstream` already performs.
//!
//! [`ChangeEvent`] carries more than the row events subql dispatches
//! (transaction boundaries, relation definitions, streaming markers). The
//! sources reduce a raw stream to row events with [`into_engine_events`]
//! before handing anything to the engine, so [`CdcEvent::kind`] is only
//! ever called on an Insert, Update, Delete, or Truncate.

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
use alloc::sync::Arc;
use alloc::vec::Vec;

use pg_walstream::{ChangeEvent, ColumnValue, EventType, RowData};
use sql_traits::prelude::DatabaseLike;

use super::pg_type::text_to_pg_value_by_kind;
use super::resolve_table;
use crate::backend::{Postgres, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};
use crate::wal::wire_event::{wire_cdc_event, WireEvent};

/// The DML or truncate kind of `event`, or `None` for the non-row events
/// (`Begin`, `Commit`, `Relation`, streaming and two-phase markers) that a
/// pg_walstream stream also carries.
const fn dml_kind(event: &ChangeEvent) -> Option<EventKind> {
    match event.event_type {
        EventType::Insert { .. } => Some(EventKind::Insert),
        EventType::Update { .. } => Some(EventKind::Update),
        EventType::Delete { .. } => Some(EventKind::Delete),
        EventType::Truncate(_) => Some(EventKind::Truncate),
        _ => None,
    }
}

/// Reduce a decoded [`ChangeEvent`] to the row events subql's engine
/// consumes. Insert, Update, and Delete pass through unchanged. A
/// `Truncate` naming several tables fans out into one single-table
/// `Truncate` per table, since a subql event is always about one table.
/// Every other variant yields nothing.
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
pub fn into_engine_events(event: ChangeEvent) -> Vec<ChangeEvent> {
    match &event.event_type {
        EventType::Insert { .. } | EventType::Update { .. } | EventType::Delete { .. } => {
            vec![event]
        }
        EventType::Truncate(names) => names
            .iter()
            .map(|name| ChangeEvent {
                event_type: EventType::Truncate(vec![Arc::clone(name)]),
                lsn: event.lsn,
                metadata: event.metadata.clone(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Resolve the observed table for `event` to a subql [`TableId`], or
/// `None` when the event carries no table (a non-row event) or the name is
/// not in `db`.
fn event_table_id<DB: DatabaseLike>(event: &ChangeEvent, db: &DB) -> Option<TableId> {
    match &event.event_type {
        EventType::Insert { schema, table, .. }
        | EventType::Update { schema, table, .. }
        | EventType::Delete { schema, table, .. } => resolve_table(schema, table, db).ok(),
        EventType::Truncate(names) => {
            let full = names.first()?.as_ref();
            let (schema, table) = full.rsplit_once('.').unwrap_or(("", full));
            resolve_table(schema, table, db).ok()
        }
        _ => None,
    }
}

/// The row image `row` selects for `event`, or `None` when the event does
/// not carry that image. Matches the pgoutput access rules: Insert exposes
/// its row as New and Pk, Delete its old row as Old and Pk, and Update its
/// new row as New and its old row as Old and Pk.
const fn image_for(event: &ChangeEvent, row: RowKind) -> Option<&RowData> {
    match (&event.event_type, row) {
        (EventType::Insert { data, .. }, RowKind::New | RowKind::Pk) => Some(data),
        (EventType::Delete { old_data, .. }, RowKind::Old | RowKind::Pk) => Some(old_data),
        (EventType::Update { new_data, .. }, RowKind::New) => Some(new_data),
        (EventType::Update { old_data, .. }, RowKind::Old | RowKind::Pk) => old_data.as_ref(),
        _ => None,
    }
}

wire_cdc_event!(ChangeEvent, Postgres, crate::PgLsn);

impl WireEvent for ChangeEvent {
    type Backend = Postgres;
    type Checkpoint = crate::PgLsn;

    fn wire_kind(&self) -> EventKind {
        dml_kind(self).expect(
            "CdcEvent::kind called on a non-row ChangeEvent. Sources must reduce the stream with into_engine_events first",
        )
    }

    fn wire_table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        // Infallible in the trait, so an unresolved name yields the
        // `u32` sentinel, which the engine reports as an unknown table.
        event_table_id(self, db).unwrap_or(TableId::MAX)
    }

    fn wire_checkpoint(&self) -> Option<Self::Checkpoint> {
        Some(crate::PgLsn(self.lsn.value()))
    }

    fn wire_pk_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        match &self.event_type {
            EventType::Insert { .. } | EventType::Update { .. } | EventType::Delete { .. } => {
                catalog_helpers::primary_key_columns(db, table_id).unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    fn wire_changed_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        let EventType::Update {
            old_data: Some(old),
            new_data,
            ..
        } = &self.event_type
        else {
            return Vec::new();
        };
        let Ok(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        if old.len() != arity || new_data.len() != arity {
            return Vec::new();
        }
        super::changed_columns_by_name(db, table_id, arity, |name| {
            (old.get(name), new_data.get(name))
        })
    }

    fn wire_value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        if row == RowKind::Pk && !WireEvent::wire_pk_columns(self, db, table_id).contains(&col) {
            return Ok(Value::Missing);
        }
        let Some(image) = image_for(self, row) else {
            return Ok(Value::Missing);
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            return Ok(Value::Missing);
        };
        match image.get(&name) {
            // A column the wire did not carry, and binary-format cells
            // (which do not appear on subql's text-mode proto v1 streams),
            // are both Missing here: they escalate to re-execution rather
            // than surfacing as a decode error.
            None | Some(ColumnValue::Binary(_)) => Ok(Value::Missing),
            Some(ColumnValue::Null) => Ok(Value::Null),
            Some(ColumnValue::Text(bytes)) => {
                catalog_helpers::column_scalar_kind::<Postgres, DB>(db, table_id, col).map_or(
                    Ok(Value::Missing),
                    |kind| {
                        // Non-UTF-8 bytes fail before any kind is consulted,
                        // so they are reported against the kind the column
                        // declares, custom or not.
                        let Ok(text) = core::str::from_utf8(bytes) else {
                            return Err(kind.family().map_or_else(
                                || crate::ValueError::Custom {
                                    column: col,
                                    custom: alloc::format!("{kind:?}"),
                                },
                                |builtin| crate::ValueError::Builtin {
                                    column: col,
                                    kind: builtin,
                                },
                            ));
                        };
                        crate::backend::decode_cell(col, kind, |builtin| {
                            text_to_pg_value_by_kind(text, builtin)
                        })
                    },
                )
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::CdcEvent;
    use crate::PgLsn;
    use pg_walstream::{Lsn, ReplicaIdentity};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn orders() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);",
        )
        .expect("parse DDL")
    }

    fn row(pairs: Vec<(&str, ColumnValue)>) -> RowData {
        RowData::from_pairs(pairs)
    }

    #[test]
    fn insert_exposes_pk_and_typed_cells() {
        let db = orders();
        let ev = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: row(vec![
                    ("id", ColumnValue::text("7")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("250")),
                    ("status", ColumnValue::text("paid")),
                ]),
            },
            lsn: Lsn::new(0x10),
            metadata: None,
        };
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(&db), vec![0u16]);
        assert!(
            ev.changed_columns(&db).is_empty(),
            "insert has no old image so no changed columns arise"
        );
        assert_eq!(ev.checkpoint(), Some(PgLsn(0x10)));
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(7));
        assert_eq!(
            ev.value_at(&db, RowKind::New, 3).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
        // Non-PK column read through Pk is Missing.
        assert_eq!(ev.value_at(&db, RowKind::Pk, 2).unwrap(), Value::Missing);
        // Insert carries no old image.
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Missing);
    }

    #[test]
    fn update_full_identity_derives_changed_columns() {
        let db = orders();
        let ev = ChangeEvent {
            event_type: EventType::Update {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                old_data: Some(row(vec![
                    ("id", ColumnValue::text("7")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("100")),
                    ("status", ColumnValue::text("pending")),
                ])),
                new_data: row(vec![
                    ("id", ColumnValue::text("8")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("250")),
                    ("status", ColumnValue::text("paid")),
                ]),
                replica_identity: ReplicaIdentity::Full,
                key_columns: vec!["id".into()],
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        assert_eq!(ev.kind(), EventKind::Update);
        assert_eq!(ev.pk_columns(&db), vec![0u16]);
        let mut changed = ev.changed_columns(&db);
        changed.sort_unstable();
        assert_eq!(changed, vec![0u16, 2u16, 3u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 2).unwrap(), Value::Int(100));
        assert_eq!(ev.value_at(&db, RowKind::New, 2).unwrap(), Value::Int(250));
        let resolved = crate::backend::ResolvedEvent::new(&ev, &db);
        assert_eq!(
            resolved.value_at_known_pk(&db, 0).expect("old primary key"),
            Value::Int(7)
        );
    }

    #[test]
    fn update_sparse_old_image_leaves_changed_empty() {
        let db = orders();
        let ev = ChangeEvent {
            event_type: EventType::Update {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                // REPLICA IDENTITY DEFAULT: old image carries the key only.
                old_data: Some(row(vec![("id", ColumnValue::text("7"))])),
                new_data: row(vec![
                    ("id", ColumnValue::text("7")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("250")),
                    ("status", ColumnValue::text("paid")),
                ]),
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec!["id".into()],
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        assert!(
            ev.changed_columns(&db).is_empty(),
            "old image has only the pk so non-pk changes are indeterminate"
        );
        // Pre-update PK identifies the row through the old image.
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
    }

    #[test]
    fn delete_pk_from_key_columns_and_old_image() {
        let db = orders();
        let ev = ChangeEvent {
            event_type: EventType::Delete {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                old_data: row(vec![("id", ColumnValue::text("9"))]),
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec!["id".into()],
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        assert_eq!(ev.kind(), EventKind::Delete);
        assert_eq!(ev.pk_columns(&db), vec![0u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Missing);
    }

    #[test]
    fn null_is_distinct_from_missing() {
        let db = orders();
        let ev = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: row(vec![
                    ("id", ColumnValue::text("7")),
                    ("status", ColumnValue::Null),
                ]),
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        // Present-but-NULL decodes to Null.
        assert_eq!(ev.value_at(&db, RowKind::New, 3).unwrap(), Value::Null);
        // Column the wire did not carry decodes to Missing.
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Missing);
    }

    #[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
    #[test]
    fn truncate_fans_out_and_non_row_events_drop() {
        let truncate = ChangeEvent {
            event_type: EventType::Truncate(vec!["public.orders".into(), "public.items".into()]),
            lsn: Lsn::new(5),
            metadata: None,
        };
        let out = into_engine_events(truncate);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|e| dml_kind(e) == Some(EventKind::Truncate)));

        let marker = ChangeEvent {
            event_type: EventType::StreamStop,
            lsn: Lsn::new(0),
            metadata: None,
        };
        assert!(into_engine_events(marker).is_empty());
    }
}
