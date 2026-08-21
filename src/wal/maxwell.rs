//! [`CdcEvent`] for the `sqlite-diff-rs` Maxwell message type.
//!
//! subql parses Maxwell JSON with `sqlite_diff_rs::maxwell::parse` and views
//! the resulting [`Message`] as a [`CdcEvent`], resolving table and column
//! names to catalog ordinals and decoding each cell against the catalog on
//! demand. This replaces the former bespoke `MaxwellParser` and `MaxwellEvent`.
//!
//! [`parse_messages`] adapts the two Maxwell shapes `sqlite-diff-rs` does not
//! model: control messages (`ddl`, `bootstrap-start`, and friends) carry no
//! row `data` and are dropped, and `bootstrap-insert` is normalized to a
//! plain insert.

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use serde::Deserialize;
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::maxwell::{Message, OpType};

use super::pg_type::json_value_to_mysql_value_by_kind;
use super::{resolve_table, WalParseError};
use crate::backend::{CdcEvent, MySql, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};

/// Minimal peek at a Maxwell message's `type`, so control and bootstrap
/// messages can be classified before the full row parse.
#[derive(Deserialize)]
struct MaxwellType {
    #[serde(rename = "type")]
    type_name: String,
}

/// Parse one Maxwell JSON message into the row events subql dispatches.
///
/// Returns an empty vector for control messages (`ddl`, `table-create`,
/// `bootstrap-start`, and friends) that carry no row data. `bootstrap-insert`
/// is normalized to an insert. Insert, update, and delete each yield a single
/// [`Message`].
///
/// # Errors
///
/// [`WalParseError::InvalidUtf8`] for non-UTF-8 input and
/// [`WalParseError::JsonError`] for malformed JSON.
pub fn parse_messages(bytes: &[u8]) -> Result<Vec<Message>, WalParseError> {
    let text =
        core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    let peek: MaxwellType =
        serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
    match peek.type_name.as_str() {
        "insert" | "update" | "delete" => {
            let msg = sqlite_diff_rs::maxwell::parse(text)
                .map_err(|e| WalParseError::JsonError(e.to_string()))?;
            Ok(alloc::vec![msg])
        }
        // `sqlite-diff-rs`'s `OpType` has no bootstrap variant, so rewrite the
        // type to a plain insert before the row parse.
        "bootstrap-insert" => {
            let mut value: serde_json::Value =
                serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
            if let Some(obj) = value.as_object_mut() {
                obj.insert("type".to_string(), serde_json::Value::from("insert"));
            }
            let msg: Message = serde_json::from_value(value)
                .map_err(|e| WalParseError::JsonError(e.to_string()))?;
            Ok(alloc::vec![msg])
        }
        // ddl, table-*, database-*, bootstrap-start, bootstrap-complete, ...
        _ => Ok(Vec::new()),
    }
}

const fn op_kind(op: OpType) -> EventKind {
    match op {
        OpType::Insert => EventKind::Insert,
        OpType::Update => EventKind::Update,
        OpType::Delete => EventKind::Delete,
    }
}

/// The row map `row` selects for `msg`, or `None` when the message does not
/// carry that image. Insert exposes `data` as New and Pk, Delete exposes
/// `data` (Maxwell puts the deleted row there) as Old and Pk, and Update
/// exposes `data` as New and `old` as Old and Pk.
const fn image_for(msg: &Message, row: RowKind) -> Option<&BTreeMap<String, serde_json::Value>> {
    match (msg.op_type, row) {
        (OpType::Insert, RowKind::New | RowKind::Pk)
        | (OpType::Delete, RowKind::Old | RowKind::Pk)
        | (OpType::Update, RowKind::New) => Some(&msg.data),
        (OpType::Update, RowKind::Old | RowKind::Pk) => msg.old.as_ref(),
        _ => None,
    }
}

impl CdcEvent for Message {
    type Backend = MySql;
    type Checkpoint = crate::NoCheckpoint;

    fn kind(&self) -> EventKind {
        op_kind(self.op_type)
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        // Infallible in the trait, so an unresolved name yields the `u32`
        // sentinel, which the engine reports as an unknown table.
        resolve_table(&self.database, &self.table, db).unwrap_or(TableId::MAX)
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        None
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        // The primary key is the row identity subql matches follows and PK
        // projections against, so it comes from the catalog.
        resolve_table(&self.database, &self.table, db)
            .ok()
            .and_then(|table_id| catalog_helpers::primary_key_columns(db, table_id))
            .unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        if self.op_type != OpType::Update {
            return Vec::new();
        }
        let Some(old) = self.old.as_ref() else {
            return Vec::new();
        };
        let Ok(table_id) = resolve_table(&self.database, &self.table, db) else {
            return Vec::new();
        };
        // Maxwell's `old` carries exactly the columns whose value changed, so
        // it is an authoritative changed-column set.
        old.keys()
            .filter_map(|name| catalog_helpers::column_id(db, table_id, name))
            .collect()
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<MySql>, crate::ValueError> {
        let Ok(table_id) = resolve_table(&self.database, &self.table, db) else {
            return Ok(Value::Missing);
        };
        if row == RowKind::Pk
            && !catalog_helpers::primary_key_columns(db, table_id)
                .unwrap_or_default()
                .contains(&col)
        {
            return Ok(Value::Missing);
        }
        let Some(image) = image_for(self, row) else {
            return Ok(Value::Missing);
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            return Ok(Value::Missing);
        };
        match image.get(name.as_str()) {
            None => Ok(Value::Missing),
            Some(value) if value.is_null() => Ok(Value::Null),
            Some(value) => catalog_helpers::column_scalar_kind::<MySql, DB>(db, table_id, col)
                .map_or(Ok(Value::Missing), |kind| {
                    crate::backend::decode_cell(col, kind, |builtin| {
                        json_value_to_mysql_value_by_kind(value, builtin)
                    })
                }),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::MySqlDialect;

    fn orders() -> ParserDB {
        ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("parse DDL")
    }

    fn one(bytes: &[u8]) -> Message {
        let mut msgs = parse_messages(bytes).expect("parse succeeds");
        assert_eq!(msgs.len(), 1);
        msgs.remove(0)
    }

    #[test]
    fn insert_exposes_pk_and_typed_cells() {
        let db = orders();
        let ev = one(br#"{"database":"test","table":"orders","type":"insert",
                 "data":{"id":7,"amount":250,"status":"paid"}}"#);
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(&db), alloc::vec![0u16]);
        assert!(
            ev.changed_columns(&db).is_empty(),
            "insert has no old image so no changed columns arise"
        );
        assert_eq!(ev.checkpoint(), None);
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Int(250));
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 1).unwrap(), Value::Missing);
    }

    #[test]
    fn update_old_field_is_authoritative_changed_columns() {
        let db = orders();
        let ev = one(br#"{"database":"test","table":"orders","type":"update",
                 "data":{"id":7,"amount":250,"status":"paid"},
                 "old":{"amount":100,"status":"pending"}}"#);
        assert_eq!(ev.kind(), EventKind::Update);
        let mut changed = ev.changed_columns(&db);
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![1u16, 2u16]);
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Int(250));
        assert_eq!(ev.value_at(&db, RowKind::Old, 1).unwrap(), Value::Int(100));
    }

    #[test]
    fn delete_reads_pk_and_old_from_data() {
        let db = orders();
        let ev = one(br#"{"database":"test","table":"orders","type":"delete",
                 "data":{"id":9,"amount":10,"status":"paid"}}"#);
        assert_eq!(ev.kind(), EventKind::Delete);
        assert_eq!(ev.pk_columns(&db), alloc::vec![0u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Missing);
    }

    #[test]
    fn control_messages_drop_and_bootstrap_insert_is_an_insert() {
        assert!(
            parse_messages(br#"{"database":"test","table":"orders","type":"ddl"}"#)
                .expect("ddl parses")
                .is_empty()
        );
        assert!(parse_messages(
            br#"{"type":"bootstrap-start","database":"test","table":"orders"}"#
        )
        .expect("bootstrap-start parses")
        .is_empty());
        let db = orders();
        let ev = one(
            br#"{"database":"test","table":"orders","type":"bootstrap-insert",
                 "data":{"id":42,"amount":100,"status":"open"}}"#,
        );
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(42));
    }

    #[test]
    fn null_is_distinct_from_missing() {
        let db = orders();
        let ev = one(br#"{"database":"test","table":"orders","type":"insert",
                 "data":{"id":7,"status":null}}"#);
        assert_eq!(ev.value_at(&db, RowKind::New, 2).unwrap(), Value::Null);
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Missing);
    }

    #[test]
    fn value_at_errors_on_unsigned_bigint_above_i64_max() {
        // A MySQL BIGINT UNSIGNED cell above i64::MAX cannot be
        // represented as subql's i64-backed Int, so value_at surfaces a
        // decode error rather than silently dropping the cell.
        let db = ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE t (id INT PRIMARY KEY, big BIGINT UNSIGNED);",
        )
        .expect("parse DDL");
        let ev = one(br#"{"database":"test","table":"t","type":"insert",
                 "data":{"id":1,"big":18446744073709551615}}"#);
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(1));
        let err = ev.value_at(&db, RowKind::New, 1).unwrap_err();
        assert_eq!(
            err,
            crate::ValueError::Builtin {
                column: 1,
                kind: crate::backend::ScalarKind::Int
            }
        );
    }
}
