use alloc::string::ToString;
use alloc::vec::Vec;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;
use wal2json_events::{Action, Column, MessageV2, RowV2, TransactionBoundary};

use crate::backend::{Postgres, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};
use crate::wal::transaction_order::{TransactionOrder, TransactionOrderError};
use crate::wal::wire_event::{wire_cdc_event, WireEvent};
use crate::wal::{changed_columns_by_name, resolve_table, WalParseError};
use crate::{PgCommitPosition, PgLsn};

use super::decode_helpers::{column_value, decode_cell, IndexedName};
use super::parse_helpers::v2_row_kind;

/// A wal2json v2 row message and where it falls in commit order.
///
/// The message's own `lsn` is the position of its WAL record, which does not
/// follow commit order when transactions interleave. The checkpoint this
/// event carries is its [`PgCommitPosition`], or none when the stream was
/// decoded without `include-lsn`.
#[derive(Clone, Debug)]
pub struct Wal2JsonV2Event {
    message: MessageV2,
    position: Option<PgCommitPosition>,
}

impl Wal2JsonV2Event {
    /// The row message `message` at `position`.
    ///
    /// `message` is an insert, update, delete, or truncate. Any other action
    /// is not a row event, and reading its kind panics.
    #[must_use]
    pub const fn new(message: MessageV2, position: Option<PgCommitPosition>) -> Self {
        Self { message, position }
    }

    /// Where the message falls in commit order, when the stream names commit
    /// positions.
    #[must_use]
    pub const fn position(&self) -> Option<PgCommitPosition> {
        self.position
    }

    /// The parsed message.
    #[must_use]
    pub const fn message(&self) -> &MessageV2 {
        &self.message
    }

    /// The parsed message, without its position.
    #[must_use]
    pub fn into_message(self) -> MessageV2 {
        self.message
    }
}

/// Reads a wal2json v2 stream, one line at a time, into row events placed in
/// commit order.
///
/// The stream must carry its transaction boundaries (`include-transaction`,
/// on by default), since a row is placed by the begin before it. The begin
/// names the transaction's commit position under `include-lsn=true`, and
/// without it the events carry no position.
///
/// # Examples
///
/// ```
/// use subql::backend::CdcEvent;
/// use subql::{PgCommitPosition, PgLsn, Wal2JsonV2Reader};
///
/// let mut reader = Wal2JsonV2Reader::new();
/// let lines: [&[u8]; 4] = [
///     br#"{"action":"B","lsn":"0/5DC"}"#,
///     br#"{"action":"I","schema":"public","table":"orders","lsn":"0/3E8","columns":[{"name":"id","type":"integer","value":1}]}"#,
///     br#"{"action":"I","schema":"public","table":"orders","lsn":"0/44C","columns":[{"name":"id","type":"integer","value":2}]}"#,
///     br#"{"action":"C","lsn":"0/5DC"}"#,
/// ];
/// let mut checkpoints = Vec::new();
/// for line in lines {
///     if let Some(event) = reader.parse(line)? {
///         checkpoints.push(event.checkpoint());
///     }
/// }
/// assert_eq!(
///     checkpoints,
///     [
///         Some(PgCommitPosition::new(PgLsn(0x5DC), 1)),
///         Some(PgCommitPosition::new(PgLsn(0x5DC), 2)),
///     ]
/// );
/// # Ok::<(), subql::WalParseError>(())
/// ```
pub struct Wal2JsonV2Reader {
    order: TransactionOrder<Option<PgLsn>>,
}

impl Default for Wal2JsonV2Reader {
    fn default() -> Self {
        Self::new()
    }
}

impl Wal2JsonV2Reader {
    /// A reader expecting the start of a transaction.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            order: TransactionOrder::new(),
        }
    }

    /// Read one wal2json v2 line, returning the row event it carries, or
    /// `None` for a transaction boundary or a logical message.
    ///
    /// # Errors
    ///
    /// [`WalParseError::InvalidUtf8`] for non-UTF-8 input,
    /// [`WalParseError::JsonError`] for malformed JSON,
    /// [`WalParseError::MalformedPayload`] for a boundary `lsn` that is not
    /// one, and [`WalParseError::TransactionOrder`] for a boundary or row out
    /// of place.
    pub fn parse(&mut self, bytes: &[u8]) -> Result<Option<Wal2JsonV2Event>, WalParseError> {
        let text =
            core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
        let message =
            wal2json_events::parse_v2(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
        match &message {
            MessageV2::Begin(boundary) => {
                self.order.begin(boundary_lsn(boundary)?)?;
                Ok(None)
            }
            MessageV2::Commit(boundary) => {
                let (began, _) = self.order.commit()?;
                if let (Some(began), Some(committed)) = (began, boundary_lsn(boundary)?) {
                    if began != committed {
                        return Err(
                            TransactionOrderError::CommitMismatch { began, committed }.into()
                        );
                    }
                }
                Ok(None)
            }
            MessageV2::Message(_) => Ok(None),
            MessageV2::Insert(_)
            | MessageV2::Update(_)
            | MessageV2::Delete(_)
            | MessageV2::Truncate(_) => {
                let (commit, ordinal) = self.order.next_row()?;
                let position = commit.map(|commit| PgCommitPosition::new(commit, ordinal));
                Ok(Some(Wal2JsonV2Event::new(message, position)))
            }
        }
    }
}

/// The commit position a boundary names, under `include-lsn=true`.
fn boundary_lsn(boundary: &TransactionBoundary) -> Result<Option<PgLsn>, WalParseError> {
    boundary
        .lsn
        .as_deref()
        .map(|text| {
            PgLsn::parse(text)
                .ok_or_else(|| WalParseError::MalformedPayload(alloc::format!("lsn {text:?}")))
        })
        .transpose()
}

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
    resolve_table::<crate::backend::Postgres, DB>(schema, table, db).ok()
}

wire_cdc_event!(Wal2JsonV2Event, Postgres, PgCommitPosition);

impl WireEvent for Wal2JsonV2Event {
    type Backend = Postgres;
    type Checkpoint = PgCommitPosition;

    fn wire_kind(&self) -> EventKind {
        v2_row_kind(self.message.action()).expect(
            "CdcEvent::kind called on a non-row wal2json v2 message. Build one from an insert, update, delete, or truncate",
        )
    }

    fn wire_table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        v2_table_id(&self.message, db).unwrap_or(TableId::MAX)
    }

    fn wire_checkpoint(&self) -> Option<Self::Checkpoint> {
        self.position
    }

    fn wire_pk_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        if self.message.action() == Action::Truncate {
            return Vec::new();
        }
        catalog_helpers::primary_key_columns(db, table_id).unwrap_or_default()
    }

    fn wire_changed_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        if self.message.action() != Action::Update {
            return Vec::new();
        }
        let Some(payload) = v2_row(&self.message) else {
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

    fn wire_value_at<DB: DatabaseLike>(
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
        let Some(columns) = v2_image(&self.message, row) else {
            return Ok(Value::Missing);
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            return Ok(Value::Missing);
        };
        decode_cell(column_value(columns, &name), db, table_id, col)
    }
}
