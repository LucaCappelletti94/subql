//! [`CdcEvent`] for the `wal2json-events` message types.
//!
//! subql parses wal2json JSON with `wal2json_events::{parse_v2,
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
use wal2json_events::{Action, ChangeV1, Column, MessageV2, RowV2};

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
        Action::Insert => Some(EventKind::Insert),
        Action::Update => Some(EventKind::Update),
        Action::Delete => Some(EventKind::Delete),
        Action::Truncate => Some(EventKind::Truncate),
        // Begin, Commit, and Message are transaction boundaries, not rows.
        Action::Begin | Action::Commit | Action::Message => None,
    }
}

const fn v1_row_kind(change: &ChangeV1) -> Option<EventKind> {
    match change {
        ChangeV1::Insert { .. } => Some(EventKind::Insert),
        ChangeV1::Update { .. } => Some(EventKind::Update),
        ChangeV1::Delete { .. } => Some(EventKind::Delete),
        // Not a row change.
        ChangeV1::Message { .. } => None,
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
    let msg =
        wal2json_events::parse_v2(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
    Ok(if v2_row_kind(msg.action()).is_some() {
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
    let txn =
        wal2json_events::parse_v1(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
    Ok(txn
        .change
        .into_iter()
        .filter(|c| v1_row_kind(c).is_some())
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

/// The cell `name` carries in `columns`, if any. An entry without a value
/// (as in the `pk` listing) reads as an absent cell.
fn column_value<'a>(columns: &'a [Column], name: &str) -> Option<&'a serde_json::Value> {
    columns
        .iter()
        .find(|c| c.name == name)
        .and_then(|c| c.value.as_ref())
}

// ---------------------------------------------------------------------------
// wal2json v2
// ---------------------------------------------------------------------------

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
        if self.action() == Action::Truncate {
            return Vec::new();
        }
        v2_table_id(self, db)
            .and_then(|table_id| catalog_helpers::primary_key_columns(db, table_id))
            .unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
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
const fn v1_image(
    change: &ChangeV1,
    row: RowKind,
) -> Option<(&[alloc::string::String], &[serde_json::Value])> {
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
        v1_naming(self)
            .and_then(|(schema, table)| resolve_table(schema, table, db).ok())
            .and_then(|table_id| catalog_helpers::primary_key_columns(db, table_id))
            .unwrap_or_default()
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        let Self::Update {
            columns, oldkeys, ..
        } = self
        else {
            return Vec::new();
        };
        let Some(table_id) =
            v1_naming(self).and_then(|(schema, table)| resolve_table(schema, table, db).ok())
        else {
            return Vec::new();
        };
        let Some(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        if columns.columnnames.len() != arity || oldkeys.keynames.len() != arity {
            return Vec::new();
        }
        changed_columns_by_name(db, table_id, arity, |name| {
            (
                v1_value(&oldkeys.keynames, &oldkeys.keyvalues, name),
                v1_value(&columns.columnnames, &columns.columnvalues, name),
            )
        })
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        let Some(table_id) =
            v1_naming(self).and_then(|(schema, table)| resolve_table(schema, table, db).ok())
        else {
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
        assert_eq!(parse_wal2json_v2(br#"{"action":"B"}"#).unwrap(), []);
        assert_eq!(parse_wal2json_v2(br#"{"action":"C"}"#).unwrap(), []);
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
