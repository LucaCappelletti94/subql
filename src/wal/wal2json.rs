//! [`CdcEvent`] for the `sqlite-diff-rs` wal2json message types.
//!
//! subql parses wal2json JSON with `sqlite_diff_rs::wal2json::{parse_v2,
//! parse_v1}` and views the resulting [`MessageV2`] and [`ChangeV1`] as
//! [`CdcEvent`]s, resolving table and column names to catalog ordinals and
//! decoding each cell against the catalog on demand. This replaces the former
//! bespoke `Wal2JsonV{1,2}Parser` and `Wal2JsonV{1,2}Event`.
//!
//! v2 carries the stream LSN (with `include-lsn=true`) and surfaces it as a
//! [`PgLsn`](crate::PgLsn) checkpoint. v1 batches a transaction and has no
//! per-change LSN, so it uses [`NoCheckpoint`](crate::NoCheckpoint).

use alloc::string::ToString;
use alloc::vec::Vec;

use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::wal2json::{Action, ChangeV1, Column, MessageV2};

use super::pg_type::json_value_to_pg_value_by_kind;
use super::{changed_columns_by_name, resolve_table, WalParseError};
use crate::backend::{CdcEvent, Postgres, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};

// ---------------------------------------------------------------------------
// Parse helpers
// ---------------------------------------------------------------------------

const fn v2_row_kind(action: Action) -> Option<EventKind> {
    match action {
        Action::I => Some(EventKind::Insert),
        Action::U => Some(EventKind::Update),
        Action::D => Some(EventKind::Delete),
        Action::T => Some(EventKind::Truncate),
        // Begin, Commit, and Message are transaction boundaries, not rows.
        Action::B | Action::C | Action::M => None,
    }
}

fn v1_row_kind(kind: &str) -> Option<EventKind> {
    match kind {
        "insert" => Some(EventKind::Insert),
        "update" => Some(EventKind::Update),
        "delete" => Some(EventKind::Delete),
        "truncate" => Some(EventKind::Truncate),
        _ => None,
    }
}

/// Parse one wal2json v2 line into the row events subql dispatches.
///
/// Returns an empty vector for a transaction boundary (`B`, `C`, `M`) and a
/// single [`MessageV2`] for a row action (`I`, `U`, `D`, `T`).
///
/// # Errors
///
/// [`WalParseError::InvalidUtf8`] for non-UTF-8 input and
/// [`WalParseError::JsonError`] for malformed JSON.
pub fn parse_wal2json_v2(bytes: &[u8]) -> Result<Vec<MessageV2>, WalParseError> {
    let text =
        core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    let msg = sqlite_diff_rs::wal2json::parse_v2(text)
        .map_err(|e| WalParseError::JsonError(e.to_string()))?;
    Ok(if v2_row_kind(msg.action).is_some() {
        alloc::vec![msg]
    } else {
        Vec::new()
    })
}

/// Parse a wal2json v1 transaction into one [`ChangeV1`] per row change.
///
/// Non-row changes are dropped.
///
/// # Errors
///
/// [`WalParseError::InvalidUtf8`] for non-UTF-8 input and
/// [`WalParseError::JsonError`] for malformed JSON.
pub fn parse_wal2json_v1(bytes: &[u8]) -> Result<Vec<ChangeV1>, WalParseError> {
    let text =
        core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    let txn = sqlite_diff_rs::wal2json::parse_v1(text)
        .map_err(|e| WalParseError::JsonError(e.to_string()))?;
    Ok(txn
        .change
        .into_iter()
        .filter(|c| v1_row_kind(&c.kind).is_some())
        .collect())
}

// ---------------------------------------------------------------------------
// Shared decode helpers
// ---------------------------------------------------------------------------

/// Decode one wal2json JSON cell against the catalog's declared type.
/// `None` (the wire did not carry the column) yields `Ok(Value::Missing)`,
/// a JSON null yields `Ok(Value::Null)`, and a carried cell of a known
/// kind that will not decode yields `Err`.
fn decode_cell<DB: DatabaseLike>(
    value: Option<&serde_json::Value>,
    db: &DB,
    table_id: TableId,
    col: ColumnId,
) -> Result<Value<Postgres>, crate::ValueError> {
    match value {
        None => Ok(Value::Missing),
        Some(v) if v.is_null() => Ok(Value::Null),
        Some(v) => catalog_helpers::column_scalar_kind::<Postgres, DB>(db, table_id, col).map_or(
            Ok(Value::Missing),
            |kind| {
                crate::backend::decode_cell(col, kind, |builtin| {
                    json_value_to_pg_value_by_kind(v, builtin)
                })
            },
        ),
    }
}

fn column_value<'a>(columns: &'a [Column], name: &str) -> Option<&'a serde_json::Value> {
    columns.iter().find(|c| c.name == name).map(|c| &c.value)
}

// ---------------------------------------------------------------------------
// wal2json v2
// ---------------------------------------------------------------------------

fn v2_image(msg: &MessageV2, row: RowKind) -> Option<&[Column]> {
    match (msg.action, row) {
        (Action::I, RowKind::New | RowKind::Pk) | (Action::U, RowKind::New) => {
            msg.columns.as_deref()
        }
        (Action::D | Action::U, RowKind::Old | RowKind::Pk) => msg.identity.as_deref(),
        _ => None,
    }
}

fn v2_table_id<DB: DatabaseLike>(msg: &MessageV2, db: &DB) -> Option<TableId> {
    let schema = msg.schema.as_deref().unwrap_or("");
    let table = msg.table.as_deref()?;
    resolve_table(schema, table, db).ok()
}

impl CdcEvent for MessageV2 {
    type Backend = Postgres;
    type Checkpoint = crate::PgLsn;

    fn kind(&self) -> EventKind {
        v2_row_kind(self.action).expect(
            "CdcEvent::kind called on a non-row wal2json v2 message. Filter with parse_wal2json_v2 first",
        )
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        v2_table_id(self, db).unwrap_or(TableId::MAX)
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        self.lsn.as_deref().and_then(crate::PgLsn::parse)
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        if self.action == Action::T {
            return Vec::new();
        }
        v2_table_id(self, db)
            .and_then(|table_id| catalog_helpers::primary_key_columns(db, table_id))
            .unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        if self.action != Action::U {
            return Vec::new();
        }
        let (Some(new_cols), Some(old_cols)) = (self.columns.as_deref(), self.identity.as_deref())
        else {
            return Vec::new();
        };
        let Some(table_id) = v2_table_id(self, db) else {
            return Vec::new();
        };
        let Some(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        // Derive only when both images cover every column (REPLICA IDENTITY
        // FULL). A sparser identity leaves the result empty.
        if new_cols.len() != arity || old_cols.len() != arity {
            return Vec::new();
        }
        changed_columns_by_name(db, table_id, arity, |name| {
            (column_value(old_cols, name), column_value(new_cols, name))
        })
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        let Some(table_id) = v2_table_id(self, db) else {
            return Ok(Value::Missing);
        };
        if row == RowKind::Pk && !self.pk_columns(db).contains(&col) {
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
}

// ---------------------------------------------------------------------------
// wal2json v1
// ---------------------------------------------------------------------------

/// The `(names, values)` parallel arrays `row` selects for `change`, or
/// `None` when the change does not carry that image.
fn v1_image(
    change: &ChangeV1,
    row: RowKind,
) -> Option<(&[alloc::string::String], &[serde_json::Value])> {
    let kind = v1_row_kind(&change.kind)?;
    match (kind, row) {
        (EventKind::Insert, RowKind::New | RowKind::Pk) | (EventKind::Update, RowKind::New) => {
            Some((&change.columnnames, &change.columnvalues))
        }
        (EventKind::Delete | EventKind::Update, RowKind::Old | RowKind::Pk) => change
            .oldkeys
            .as_ref()
            .map(|ok| (ok.keynames.as_slice(), ok.keyvalues.as_slice())),
        _ => None,
    }
}

fn v1_value<'a>(
    names: &[alloc::string::String],
    values: &'a [serde_json::Value],
    name: &str,
) -> Option<&'a serde_json::Value> {
    names
        .iter()
        .position(|n| n == name)
        .and_then(|i| values.get(i))
}

impl CdcEvent for ChangeV1 {
    type Backend = Postgres;
    type Checkpoint = crate::NoCheckpoint;

    fn kind(&self) -> EventKind {
        v1_row_kind(&self.kind).expect(
            "CdcEvent::kind called on a non-row wal2json v1 change. Filter with parse_wal2json_v1 first",
        )
    }

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        resolve_table(&self.schema, &self.table, db).unwrap_or(TableId::MAX)
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        None
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        if v1_row_kind(&self.kind) == Some(EventKind::Truncate) {
            return Vec::new();
        }
        resolve_table(&self.schema, &self.table, db)
            .ok()
            .and_then(|table_id| catalog_helpers::primary_key_columns(db, table_id))
            .unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        if v1_row_kind(&self.kind) != Some(EventKind::Update) {
            return Vec::new();
        }
        let Some(oldkeys) = self.oldkeys.as_ref() else {
            return Vec::new();
        };
        let Ok(table_id) = resolve_table(&self.schema, &self.table, db) else {
            return Vec::new();
        };
        let Some(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        if self.columnnames.len() != arity || oldkeys.keynames.len() != arity {
            return Vec::new();
        }
        changed_columns_by_name(db, table_id, arity, |name| {
            (
                v1_value(&oldkeys.keynames, &oldkeys.keyvalues, name),
                v1_value(&self.columnnames, &self.columnvalues, name),
            )
        })
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        let Ok(table_id) = resolve_table(&self.schema, &self.table, db) else {
            return Ok(Value::Missing);
        };
        if row == RowKind::Pk && !self.pk_columns(db).contains(&col) {
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::PgLsn;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn orders() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);",
        )
        .expect("parse DDL")
    }

    fn one_v2(bytes: &[u8]) -> MessageV2 {
        let mut msgs = parse_wal2json_v2(bytes).expect("parse succeeds");
        assert_eq!(msgs.len(), 1);
        msgs.remove(0)
    }

    #[test]
    fn v2_insert_pk_and_cells_and_lsn() {
        let db = orders();
        let ev = one_v2(
            br#"{"action":"I","schema":"public","table":"orders","lsn":"0/16B2270",
                 "columns":[{"name":"id","type":"integer","value":7},
                            {"name":"amount","type":"integer","value":250},
                            {"name":"status","type":"text","value":"paid"}]}"#,
        );
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(&db), alloc::vec![0u16]);
        assert_eq!(ev.checkpoint(), PgLsn::parse("0/16B2270"));
        assert!(ev.checkpoint().is_some());
        assert_eq!(ev.value_at(&db, RowKind::New, 2).unwrap(), Value::Int(250));
        assert_eq!(
            ev.value_at(&db, RowKind::New, 3).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 2).unwrap(), Value::Missing);
    }

    #[test]
    fn v2_full_identity_derives_changed_columns() {
        let db = orders();
        let ev = one_v2(
            br#"{"action":"U","schema":"public","table":"orders",
                 "columns":[{"name":"id","type":"integer","value":7},
                            {"name":"customer","type":"integer","value":3},
                            {"name":"amount","type":"integer","value":250},
                            {"name":"status","type":"text","value":"paid"}],
                 "identity":[{"name":"id","type":"integer","value":7},
                             {"name":"customer","type":"integer","value":3},
                             {"name":"amount","type":"integer","value":100},
                             {"name":"status","type":"text","value":"pending"}]}"#,
        );
        let mut changed = ev.changed_columns(&db);
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![2u16, 3u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 2).unwrap(), Value::Int(100));
        assert_eq!(ev.value_at(&db, RowKind::New, 2).unwrap(), Value::Int(250));
    }

    #[test]
    fn v2_boundary_messages_drop() {
        assert!(parse_wal2json_v2(br#"{"action":"B"}"#).unwrap().is_empty());
        assert!(parse_wal2json_v2(br#"{"action":"C"}"#).unwrap().is_empty());
    }

    #[test]
    fn v1_delete_reads_oldkeys() {
        let db = orders();
        let mut changes = parse_wal2json_v1(
            br#"{"change":[{"kind":"delete","schema":"public","table":"orders",
                 "oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[42]}}]}"#,
        )
        .expect("parse");
        assert_eq!(changes.len(), 1);
        let ev = changes.remove(0);
        assert_eq!(ev.kind(), EventKind::Delete);
        assert_eq!(ev.pk_columns(&db), alloc::vec![0u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Int(42));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(42));
        assert_eq!(ev.checkpoint(), None);
    }

    #[test]
    fn v1_multi_change_transaction_splits() {
        let db = orders();
        let changes = parse_wal2json_v1(
            br#"{"change":[
                 {"kind":"insert","schema":"public","table":"orders",
                  "columnnames":["id","amount"],"columntypes":["integer","integer"],
                  "columnvalues":[7,250]},
                 {"kind":"delete","schema":"public","table":"orders",
                  "oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[9]}}]}"#,
        )
        .expect("parse");
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].kind(), EventKind::Insert);
        assert_eq!(
            changes[0].value_at(&db, RowKind::New, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(changes[1].kind(), EventKind::Delete);
        assert_eq!(
            changes[1].value_at(&db, RowKind::Old, 0).unwrap(),
            Value::Int(9)
        );
    }
}
