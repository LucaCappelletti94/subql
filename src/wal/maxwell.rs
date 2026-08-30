//! [`CdcEvent`] for the `maxwell-cdc` message type.
//!
//! subql parses Maxwell JSON with `maxwell_cdc::parse` and views the resulting
//! [`Message`] as a [`CdcEvent`], resolving table and column names to catalog
//! ordinals and decoding each cell against the catalog on demand. The same
//! type is what `sqlite_diff_rs::maxwell` digests into a patchset, so a parsed
//! message serves both paths.
//!
//! [`parse_messages`] keeps only the messages that carry a row. Control
//! messages (bootstrap boundaries, table and database DDL) are dropped, and so
//! is a message whose `type` the model does not know, which is how a tag added
//! by a newer Maxwell skips instead of ending the stream. `bootstrap-insert`
//! reads as a plain insert.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use maxwell_cdc::{Message, OpType, ParseError, RowChange};
use serde_json::{Map, Value as JsonValue};
use sql_traits::prelude::DatabaseLike;

use super::pg_type::json_value_to_mysql_value_by_kind;
use super::{resolve_table, WalParseError};
use crate::backend::{CdcEvent, MySql, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};

/// Parse one Maxwell JSON message into the row events subql dispatches.
///
/// Returns an empty vector for a message that carries no row: a control
/// message (`bootstrap-start`, `table-create`, and friends) or one whose
/// `type` the model does not know. Insert, update, delete, and
/// bootstrap-insert each yield a single [`Message`].
///
/// # Errors
///
/// [`WalParseError::InvalidUtf8`] for non-UTF-8 input and
/// [`WalParseError::JsonError`] for malformed JSON.
pub fn parse_messages(bytes: &[u8]) -> Result<Vec<Message>, WalParseError> {
    let text =
        core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    match maxwell_cdc::parse(text) {
        Ok(msg) if row(&msg).is_some() => Ok(alloc::vec![msg]),
        Ok(_) | Err(ParseError::UnknownMessageType(_)) => Ok(Vec::new()),
        Err(e @ ParseError::Json(_)) => Err(WalParseError::JsonError(e.to_string())),
    }
}

/// The row payload, for the message types that carry one.
const fn row(msg: &Message) -> Option<&RowChange> {
    match msg {
        Message::Insert(row)
        | Message::Update(row)
        | Message::Delete(row)
        | Message::BootstrapInsert(row) => Some(row),
        // Control messages and DDL carry no row. `Message` is non-exhaustive,
        // so a type a newer model adds reads as one of those until subql says
        // otherwise.
        _ => None,
    }
}

/// The event kind `msg` dispatches as, or `None` when it carries no row.
/// A bootstrap insert is an insert: the snapshot row is new to every consumer.
const fn kind_of(msg: &Message) -> Option<EventKind> {
    match msg.op_type() {
        Some(OpType::Insert | OpType::BootstrapInsert) => Some(EventKind::Insert),
        Some(OpType::Update) => Some(EventKind::Update),
        Some(OpType::Delete) => Some(EventKind::Delete),
        // No op type at all is a control message, and an op type the model
        // gained later carries a row shape subql has not been taught.
        Some(_) | None => None,
    }
}

/// The row map `image` selects for `msg`, or `None` when the message does not
/// carry that image. Insert exposes `data` as New and Pk, Delete exposes
/// `data` (Maxwell puts the deleted row there) as Old and Pk, and Update
/// exposes `data` as New and `old` as Old and Pk.
fn image_for(msg: &Message, image: RowKind) -> Option<&Map<String, JsonValue>> {
    let payload = row(msg)?;
    match (kind_of(msg)?, image) {
        (EventKind::Insert, RowKind::New | RowKind::Pk)
        | (EventKind::Delete, RowKind::Old | RowKind::Pk)
        | (EventKind::Update, RowKind::New) => Some(&payload.data),
        (EventKind::Update, RowKind::Old | RowKind::Pk) => payload.old.as_ref(),
        _ => None,
    }
}

/// The catalog table `msg` names, when it carries a row that resolves.
fn table_of<DB: DatabaseLike>(msg: &Message, db: &DB) -> Option<TableId> {
    let payload = row(msg)?;
    resolve_table(&payload.database, &payload.table, db).ok()
}

impl CdcEvent for Message {
    type Backend = MySql;
    type Checkpoint = crate::NoCheckpoint;

    fn kind(&self) -> EventKind {
        kind_of(self).expect(
            "CdcEvent::kind called on a Maxwell message with no row. Filter with parse_messages first",
        )
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        // Infallible in the trait, so an unresolved name yields the `u32`
        // sentinel, which the engine reports as an unknown table.
        table_of(self, db).unwrap_or(TableId::MAX)
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        None
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        // The primary key is the row identity subql matches follows and PK
        // projections against, so it comes from the catalog.
        table_of(self, db)
            .and_then(|table_id| catalog_helpers::primary_key_columns(db, table_id))
            .unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        let Self::Update(payload) = self else {
            return Vec::new();
        };
        let Some(old) = payload.old.as_ref() else {
            return Vec::new();
        };
        let Some(table_id) = table_of(self, db) else {
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
        let Some(table_id) = table_of(self, db) else {
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
        // A `type` the model does not know skips, so a tag a newer Maxwell
        // adds does not end the stream.
        assert_eq!(
            parse_messages(br#"{"database":"test","table":"orders","type":"ddl"}"#)
                .expect("an unknown type is not an error"),
            []
        );
        assert_eq!(
            parse_messages(
                br#"{"type":"bootstrap-start","database":"test","table":"orders","data":{}}"#
            )
            .expect("bootstrap-start parses"),
            []
        );
        let db = orders();
        let ev = one(
            br#"{"database":"test","table":"orders","type":"bootstrap-insert",
                 "data":{"id":42,"amount":100,"status":"open"}}"#,
        );
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(42));
    }

    #[test]
    fn malformed_input_is_an_error_not_a_silent_drop() {
        // Skipping is reserved for a type the model does not know. A message
        // of a type it does know that will not parse is a real failure, and
        // so is input that is not JSON at all.
        assert!(matches!(
            parse_messages(br#"{"type":"insert","database":"test","table":"orders"}"#),
            Err(WalParseError::JsonError(_))
        ));
        assert!(matches!(
            parse_messages(b"{not json"),
            Err(WalParseError::JsonError(_))
        ));
        assert!(matches!(
            parse_messages(&[0xff, 0xfe]),
            Err(WalParseError::InvalidUtf8(_))
        ));
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
                kind: crate::backend::BuiltinKind::Int
            }
        );
    }
}
