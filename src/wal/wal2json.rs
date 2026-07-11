#![allow(clippy::match_same_arms)]
//! wal2json v1 and v2 WAL parsers.
//!
//! [wal2json](https://github.com/eulerto/wal2json) is a PostgreSQL logical
//! decoding output plugin that emits changes as JSON. Version 1 batches all
//! changes in a transaction into a single message. Version 2 emits one
//! message per change.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use serde::Deserialize;

use super::pg_type::json_value_to_pg_value;
use super::{parse_event_kind, resolve_table, WalParseError, WalParser};
use crate::backend::{CdcEvent, Postgres, Presence, RowKind, Value};
use crate::{catalog_helpers, ColumnId, EventKind, TableId};
use sql_traits::prelude::DatabaseLike;
use spin::Once;

// ============================================================================
// Serde structs: v1
// ============================================================================

#[derive(Deserialize)]
struct Wal2JsonV1Message {
    #[allow(dead_code)]
    pub xid: Option<u64>,
    pub change: Vec<Wal2JsonV1Change>,
}

#[derive(Deserialize)]
struct Wal2JsonV1Change {
    pub kind: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub columnnames: Vec<String>,
    #[serde(default)]
    pub columntypes: Vec<String>,
    #[serde(default)]
    pub columnvalues: Vec<serde_json::Value>,
    pub oldkeys: Option<Wal2JsonV1OldKeys>,
}

#[derive(Deserialize)]
struct Wal2JsonV1OldKeys {
    pub keynames: Vec<String>,
    pub keytypes: Vec<String>,
    pub keyvalues: Vec<serde_json::Value>,
}

// ============================================================================
// Serde structs: v2
// ============================================================================

#[derive(Deserialize)]
struct Wal2JsonV2Message {
    pub action: String,
    /// Schema name (absent on transaction boundary messages: B, C, M).
    #[serde(default)]
    pub schema: Option<String>,
    /// Table name (absent on transaction boundary messages: B, C, M).
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<Wal2JsonV2Column>>,
    #[serde(default)]
    pub identity: Option<Vec<Wal2JsonV2Column>>,
    #[serde(default)]
    pub pk: Option<Vec<Wal2JsonV2PkColumn>>,
    /// PostgreSQL LSN in `"hi/lo"` hex notation when wal2json was invoked
    /// with `include-lsn=true`. Absent otherwise. `convert_v2_message`
    /// surfaces `Some(PgLsn)` when present and parsable.
    #[serde(default)]
    pub lsn: Option<String>,
}

#[derive(Deserialize)]
struct Wal2JsonV2Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub value: serde_json::Value,
}

/// v2 PK column entry: `{"name": "col_name", "type": "col_type"}`
#[derive(Deserialize)]
struct Wal2JsonV2PkColumn {
    pub name: String,
    #[allow(dead_code)]
    #[serde(rename = "type")]
    pub type_name: String,
}

// ============================================================================
// Parsers
// ============================================================================

/// wal2json **v1** parser (batched: one message per transaction).
pub struct Wal2JsonV1Parser;

/// wal2json **v2** parser (per-change: one message per row change).
pub struct Wal2JsonV2Parser;

impl<DB: DatabaseLike> WalParser<DB> for Wal2JsonV1Parser {
    type Checkpoint = crate::NoCheckpoint;
    type Event = Wal2JsonV1Event;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        let Some(msg): Option<Wal2JsonV1Message> = super::parse_json_message_or_tombstone(data)?
        else {
            return Ok(Vec::new());
        };
        let mut events = Vec::with_capacity(msg.change.len());
        for change in &msg.change {
            match convert_v1_change_typed(change, database) {
                Ok(event) => events.push(event),
                Err(WalParseError::UnknownEventKind(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(events)
    }
}

impl<DB: DatabaseLike> WalParser<DB> for Wal2JsonV2Parser {
    type Checkpoint = crate::PgLsn;
    type Event = Wal2JsonV2Event;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        let Some(msg): Option<Wal2JsonV2Message> = super::parse_json_message_or_tombstone(data)?
        else {
            return Ok(Vec::new());
        };
        // Skip non-row transactional metadata messages.
        if matches!(msg.action.as_str(), "B" | "C" | "M") {
            return Ok(Vec::new());
        }
        match convert_v2_message_typed(msg, database) {
            Ok(event) => Ok(alloc::vec![event]),
            Err(WalParseError::UnknownEventKind(_)) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }
}

// ============================================================================
// Shared helpers
// ============================================================================

/// Parse event kind from v1 string.
fn parse_v1_kind(kind: &str) -> Result<EventKind, WalParseError> {
    parse_event_kind(kind, &["insert"], &["update"], &["delete"], &["truncate"])
}

/// Parse event kind from v2 single-char action.
fn parse_v2_kind(action: &str) -> Result<EventKind, WalParseError> {
    parse_event_kind(action, &["I"], &["U"], &["D"], &["T"])
}


// ============================================================================
// Wal2JsonV2Event: typed [`CdcEvent`] output of the v2 parser
// ============================================================================

/// Typed CDC event surfaced by [`Wal2JsonV2Parser::parse_wal_message`].
///
/// Owns the wal2json v2 wire payload for one row change plus lazily
/// populated caches of decoded [`Value<Postgres>`] cells. Scalar accessors
/// on the [`CdcEvent`] impl decode each cell on first access through
/// `json_value_to_pg_value` and return references into the cache.
///
/// Backed by the Postgres [`Backend`](crate::backend::Backend). Anchored on
/// [`PgLsn`](crate::PgLsn) when `wal2json` was invoked with
/// `include-lsn=true`.
pub struct Wal2JsonV2Event {
    kind: EventKind,
    table_id: TableId,
    pk_columns: alloc::sync::Arc<[ColumnId]>,
    changed_columns: alloc::sync::Arc<[ColumnId]>,
    checkpoint: Option<crate::PgLsn>,
    new_image: Option<V2RowImage>,
    old_image: Option<V2RowImage>,
}

/// One row image (new or old) inside a [`Wal2JsonV2Event`].
struct V2RowImage {
    /// Wire entries in the order the message carried them.
    entries: alloc::boxed::Box<[V2WireCell]>,
    /// `ColumnId -> entries` position. Arity-sized; `None` for columns
    /// the source did not carry in this image.
    by_col: alloc::boxed::Box<[Option<u16>]>,
    /// Lazily populated decoded values (arity-sized). Uninitialised
    /// [`Once`] for cells the caller never touched. Decode failures are
    /// materialised as [`Value::Missing`] so accessors return
    /// [`Presence::Missing`] and the engine escalates to re-execution.
    cache: alloc::boxed::Box<[Once<Value<Postgres>>]>,
}

/// One decoded wire cell inside a [`V2RowImage`].
struct V2WireCell {
    col_id: ColumnId,
    pg_type: alloc::sync::Arc<str>,
    value: serde_json::Value,
}

impl V2RowImage {
    fn from_wire_columns(
        columns: Vec<Wal2JsonV2Column>,
        table_id: TableId,
        arity: usize,
        database: &impl DatabaseLike,
        context: &'static str,
    ) -> Result<Self, WalParseError> {
        let mut entries = Vec::with_capacity(columns.len());
        let mut by_col = alloc::vec![None; arity].into_boxed_slice();
        let mut seen = hashbrown::HashSet::with_capacity(columns.len());

        for col in columns {
            let Wal2JsonV2Column {
                name,
                type_name,
                value,
            } = col;
            let col_id =
                catalog_helpers::column_id(database, table_id, name.as_str()).ok_or_else(|| {
                    WalParseError::UnknownColumn {
                        table_id,
                        column: name.clone(),
                    }
                })?;
            if !seen.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} contains duplicate column '{name}' (id {col_id})"
                )));
            }
            if (col_id as usize) >= arity {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} column '{name}' resolved to out-of-range id {col_id} for table {table_id} (arity {arity})"
                )));
            }
            let idx = u16::try_from(entries.len()).map_err(|_| {
                WalParseError::MalformedPayload(format!(
                    "{context} has more than {} entries",
                    u16::MAX
                ))
            })?;
            by_col[col_id as usize] = Some(idx);
            entries.push(V2WireCell {
                col_id,
                pg_type: alloc::sync::Arc::from(type_name),
                value,
            });
        }

        let cache = (0..arity)
            .map(|_| Once::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            entries: entries.into_boxed_slice(),
            by_col,
            cache,
        })
    }

    /// Resolve the decoded [`Value<Postgres>`] for `col` in this image, or
    /// `None` when the wire did not carry that column.
    fn value_at(&self, col: ColumnId) -> Option<&Value<Postgres>> {
        let idx = usize::from(col);
        let wire_idx = usize::from((*self.by_col.get(idx)?)?);
        let entry = self.entries.get(wire_idx)?;
        let cache_slot = self.cache.get(idx)?;
        Some(cache_slot.call_once(|| {
            json_value_to_pg_value(
                &entry.value,
                entry.pg_type.as_ref(),
                entry.col_id_field_name(),
            )
            .unwrap_or(Value::Missing)
        }))
    }
}

impl V2WireCell {
    #[allow(clippy::unused_self)]
    const fn col_id_field_name(&self) -> &'static str {
        "wal2json.v2.column"
    }
}

impl Wal2JsonV2Event {
    /// Backend this event observes.
    ///
    /// Chosen so the [`CdcEvent`] impl compiles cleanly under the Postgres
    /// scalar bounds. Wal2json only ever wraps a PG source.
    const fn kind_matches_pk_source(&self, row: RowKind) -> Option<&V2RowImage> {
        match (self.kind, row) {
            (EventKind::Truncate, _) => None,
            (EventKind::Insert, RowKind::New | RowKind::Pk) => self.new_image.as_ref(),
            (EventKind::Delete, RowKind::Old | RowKind::Pk) => self.old_image.as_ref(),
            (EventKind::Update, RowKind::New) => self.new_image.as_ref(),
            (EventKind::Update, RowKind::Old | RowKind::Pk) => self.old_image.as_ref(),
            _ => None,
        }
    }

    fn value_at(&self, row: RowKind, col: ColumnId) -> Option<&Value<Postgres>> {
        if row == RowKind::Pk && !self.pk_columns.contains(&col) {
            return None;
        }
        self.kind_matches_pk_source(row)
            .and_then(|image| image.value_at(col))
    }
}

/// Look up a decoded value and match it against the requested scalar
/// shape. Missing wire, `Value::Missing`, or scalar-shape mismatch all
/// surface as [`Presence::Missing`]; `Value::Null` surfaces as
/// [`Presence::Null`].
macro_rules! v2_scalar_accessor {
    ($self:ident, $row:ident, $col:ident, $variant:ident) => {{
        let Some(v) = $self.value_at($row, $col) else {
            return Presence::Missing;
        };
        match v {
            Value::$variant(x) => Presence::Present(x),
            Value::Null => Presence::Null,
            _ => Presence::Missing,
        }
    }};
}

impl CdcEvent for Wal2JsonV2Event {
    type Backend = Postgres;
    type Checkpoint = crate::PgLsn;

    fn kind(&self) -> EventKind {
        self.kind
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn checkpoint(&self) -> Option<&Self::Checkpoint> {
        self.checkpoint.as_ref()
    }

    fn pk_columns(&self) -> &[ColumnId] {
        &self.pk_columns
    }

    fn changed_columns(&self) -> &[ColumnId] {
        &self.changed_columns
    }

    fn bool_at(&self, row: RowKind, col: ColumnId) -> Presence<&bool> {
        v2_scalar_accessor!(self, row, col, Bool)
    }

    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&i64> {
        v2_scalar_accessor!(self, row, col, Int)
    }

    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&f64> {
        v2_scalar_accessor!(self, row, col, Float)
    }

    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        v2_scalar_accessor!(self, row, col, String)
    }

    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::vec::Vec<u8>> {
        v2_scalar_accessor!(self, row, col, Bytes)
    }

    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&uuid::Uuid> {
        v2_scalar_accessor!(self, row, col, Uuid)
    }

    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDateTime> {
        v2_scalar_accessor!(self, row, col, Timestamp)
    }

    fn timestamp_tz_at(
        &self,
        row: RowKind,
        col: ColumnId,
    ) -> Presence<&chrono::DateTime<chrono::Utc>> {
        v2_scalar_accessor!(self, row, col, TimestampTz)
    }

    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDate> {
        v2_scalar_accessor!(self, row, col, Date)
    }

    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveTime> {
        v2_scalar_accessor!(self, row, col, Time)
    }

    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&bigdecimal::BigDecimal> {
        v2_scalar_accessor!(self, row, col, Decimal)
    }

    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        v2_scalar_accessor!(self, row, col, Json)
    }

    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        v2_scalar_accessor!(self, row, col, Jsonb)
    }
}


#[allow(clippy::too_many_lines)]
fn convert_v2_message_typed<DB: DatabaseLike>(
    msg: Wal2JsonV2Message,
    database: &DB,
) -> Result<Wal2JsonV2Event, WalParseError> {
    let kind = parse_v2_kind(&msg.action)?;

    let schema = msg.schema.as_deref().unwrap_or("");
    let table_name = msg
        .table
        .as_deref()
        .ok_or_else(|| {
            WalParseError::JsonError("data message (I/U/D) missing 'table' field".to_string())
        })?
        .to_string();
    let table_id = resolve_table(schema, &table_name, database)?;
    let arity = catalog_helpers::table_arity(database, table_id).ok_or_else(|| {
        WalParseError::UnknownTable {
            schema: schema.to_string(),
            table: table_name.clone(),
        }
    })?;

    let checkpoint = msg.lsn.as_deref().and_then(crate::PgLsn::parse);
    let Wal2JsonV2Message {
        columns,
        identity,
        pk,
        ..
    } = msg;

    // Presence rules match convert_v2_message.
    if matches!(kind, EventKind::Insert | EventKind::Update)
        && columns.as_ref().is_none_or(std::vec::Vec::is_empty)
    {
        return Err(WalParseError::MissingField("columns".to_string()));
    }
    if kind == EventKind::Delete && identity.as_ref().is_none_or(std::vec::Vec::is_empty) {
        return Err(WalParseError::MissingField("identity".to_string()));
    }

    let new_image = match kind {
        EventKind::Insert | EventKind::Update => Some(V2RowImage::from_wire_columns(
            columns.expect("columns presence checked above"),
            table_id,
            arity,
            database,
            "v2 columns",
        )?),
        EventKind::Delete | EventKind::Truncate => None,
    };
    let old_image = match kind {
        EventKind::Delete => Some(V2RowImage::from_wire_columns(
            identity.expect("identity presence checked above"),
            table_id,
            arity,
            database,
            "v2 identity",
        )?),
        EventKind::Update => identity
            .filter(|identity| !identity.is_empty())
            .map(|identity| {
                V2RowImage::from_wire_columns(
                    identity,
                    table_id,
                    arity,
                    database,
                    "v2 identity",
                )
            })
            .transpose()?,
        EventKind::Insert | EventKind::Truncate => None,
    };

    // PK column ids follow the same fallback chain as convert_v2_message,
    // but only the ids matter here (values reach callers through
    // `RowKind::Pk` scalar accessors, not through a stored PrimaryKey).
    let pk_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Truncate {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    } else if let Some(pk_meta) = pk.as_ref() {
        let names: Vec<alloc::string::String> =
            pk_meta.iter().map(|c| c.name.clone()).collect();
        let mut ids = Vec::with_capacity(names.len());
        let mut seen = hashbrown::HashSet::with_capacity(names.len());
        for name in &names {
            let col_id = catalog_helpers::column_id(database, table_id, name.as_str())
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
            if !seen.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "v2 pk contains duplicate column '{name}' (id {col_id})"
                )));
            }
            ids.push(col_id);
        }
        alloc::sync::Arc::from(ids)
    } else if let Some(image) = old_image.as_ref() {
        alloc::sync::Arc::from(
            image
                .entries
                .iter()
                .map(|e| e.col_id)
                .collect::<Vec<_>>(),
        )
    } else if let Some(image) = new_image.as_ref() {
        catalog_helpers::primary_key_columns(database, table_id).map_or_else(
            || {
                alloc::sync::Arc::from(
                    image
                        .entries
                        .iter()
                        .map(|e| e.col_id)
                        .collect::<Vec<_>>(),
                )
            },
            alloc::sync::Arc::from,
        )
    } else {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    };

    // Derive changed_columns by wire-level comparison of new vs old when
    // both images cover every column. That signals REPLICA IDENTITY FULL;
    // sparse identity (PK-only) leaves the slice empty (safe: consumers
    // treat empty as "no hint, re-evaluate everything"). No decode-cache
    // slots are touched by the comparison.
    let changed_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Update {
        alloc::sync::Arc::from(v2_derive_changed_columns(
            new_image.as_ref(),
            old_image.as_ref(),
            arity,
        ))
    } else {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    };

    Ok(Wal2JsonV2Event {
        kind,
        table_id,
        pk_columns,
        changed_columns,
        checkpoint,
        new_image,
        old_image,
    })
}

/// Wire-level `changed_columns` derivation for wal2json v2 Update events.
///
/// Runs only when both images cover the table's arity (source is running
/// with `REPLICA IDENTITY FULL`). Returns an empty vector otherwise, per
/// the [`CdcEvent`] contract's "hint, not authoritative diff" clause.
fn v2_derive_changed_columns(
    new_image: Option<&V2RowImage>,
    old_image: Option<&V2RowImage>,
    arity: usize,
) -> Vec<ColumnId> {
    let (Some(new), Some(old)) = (new_image, old_image) else {
        return Vec::new();
    };
    if new.entries.len() != arity || old.entries.len() != arity {
        return Vec::new();
    }
    let mut changed = Vec::new();
    for col in 0..arity {
        let (Some(new_idx), Some(old_idx)) = (new.by_col[col], old.by_col[col]) else {
            continue;
        };
        let new_val = &new.entries[new_idx as usize].value;
        let old_val = &old.entries[old_idx as usize].value;
        if new_val != old_val {
            #[allow(clippy::cast_possible_truncation)]
            changed.push(col as ColumnId);
        }
    }
    changed
}

// ============================================================================
// Wal2JsonV1Event: typed [`CdcEvent`] output of the v1 parser
// ============================================================================

/// Typed CDC event surfaced by [`Wal2JsonV1Parser::parse_wal_message`].
///
/// Owns the wal2json v1 wire payload for one row change plus lazily
/// populated caches of decoded [`Value<Postgres>`] cells. Scalar accessors
/// on the [`CdcEvent`] impl decode each cell on first access through
/// `json_value_to_pg_value` and return references into the cache.
///
/// Backed by the Postgres [`Backend`](crate::backend::Backend). V1 does not
/// carry an LSN, so [`crate::Checkpoint`] is [`crate::NoCheckpoint`].
pub struct Wal2JsonV1Event {
    kind: EventKind,
    table_id: TableId,
    pk_columns: alloc::sync::Arc<[ColumnId]>,
    changed_columns: alloc::sync::Arc<[ColumnId]>,
    checkpoint: Option<crate::NoCheckpoint>,
    new_image: Option<V1RowImage>,
    old_image: Option<V1RowImage>,
}

/// One row image (new or old) inside a [`Wal2JsonV1Event`].
struct V1RowImage {
    entries: alloc::boxed::Box<[V1WireCell]>,
    by_col: alloc::boxed::Box<[Option<u16>]>,
    cache: alloc::boxed::Box<[Once<Value<Postgres>>]>,
}

/// One decoded wire cell inside a [`V1RowImage`].
struct V1WireCell {
    #[allow(dead_code)]
    col_id: ColumnId,
    pg_type: alloc::sync::Arc<str>,
    value: serde_json::Value,
}

impl V1RowImage {
    fn from_parallel_arrays(
        names: &[String],
        types: &[String],
        values: Vec<serde_json::Value>,
        table_id: TableId,
        arity: usize,
        database: &impl DatabaseLike,
        context: &'static str,
    ) -> Result<Self, WalParseError> {
        if names.len() != types.len() || names.len() != values.len() {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} parallel arrays have mismatched lengths: {} names, {} types, {} values",
                names.len(),
                types.len(),
                values.len()
            )));
        }

        let mut entries = Vec::with_capacity(names.len());
        let mut by_col = alloc::vec![None; arity].into_boxed_slice();
        let mut seen = hashbrown::HashSet::with_capacity(names.len());

        for ((name, pg_type), value) in names.iter().zip(types.iter()).zip(values) {
            let col_id =
                catalog_helpers::column_id(database, table_id, name.as_str()).ok_or_else(|| {
                    WalParseError::UnknownColumn {
                        table_id,
                        column: name.clone(),
                    }
                })?;
            if !seen.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} contains duplicate column '{name}' (id {col_id})"
                )));
            }
            if (col_id as usize) >= arity {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} column '{name}' resolved to out-of-range id {col_id} for table {table_id} (arity {arity})"
                )));
            }
            let idx = u16::try_from(entries.len()).map_err(|_| {
                WalParseError::MalformedPayload(format!(
                    "{context} has more than {} entries",
                    u16::MAX
                ))
            })?;
            by_col[col_id as usize] = Some(idx);
            entries.push(V1WireCell {
                col_id,
                pg_type: alloc::sync::Arc::from(pg_type.as_str()),
                value,
            });
        }

        let cache = (0..arity)
            .map(|_| Once::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            entries: entries.into_boxed_slice(),
            by_col,
            cache,
        })
    }

    fn value_at(&self, col: ColumnId) -> Option<&Value<Postgres>> {
        let idx = usize::from(col);
        let wire_idx = usize::from((*self.by_col.get(idx)?)?);
        let entry = self.entries.get(wire_idx)?;
        let cache_slot = self.cache.get(idx)?;
        Some(cache_slot.call_once(|| {
            json_value_to_pg_value(
                &entry.value,
                entry.pg_type.as_ref(),
                entry.col_id_field_name(),
            )
            .unwrap_or(Value::Missing)
        }))
    }
}

impl V1WireCell {
    #[allow(clippy::unused_self)]
    const fn col_id_field_name(&self) -> &'static str {
        "wal2json.v1.column"
    }
}

impl Wal2JsonV1Event {
    const fn kind_matches_pk_source(&self, row: RowKind) -> Option<&V1RowImage> {
        match (self.kind, row) {
            (EventKind::Truncate, _) => None,
            (EventKind::Insert, RowKind::New | RowKind::Pk) => self.new_image.as_ref(),
            (EventKind::Delete, RowKind::Old | RowKind::Pk) => self.old_image.as_ref(),
            (EventKind::Update, RowKind::New) => self.new_image.as_ref(),
            (EventKind::Update, RowKind::Old | RowKind::Pk) => self.old_image.as_ref(),
            _ => None,
        }
    }

    fn value_at(&self, row: RowKind, col: ColumnId) -> Option<&Value<Postgres>> {
        if row == RowKind::Pk && !self.pk_columns.contains(&col) {
            return None;
        }
        self.kind_matches_pk_source(row)
            .and_then(|image| image.value_at(col))
    }
}

macro_rules! v1_scalar_accessor {
    ($self:ident, $row:ident, $col:ident, $variant:ident) => {{
        let Some(v) = $self.value_at($row, $col) else {
            return Presence::Missing;
        };
        match v {
            Value::$variant(x) => Presence::Present(x),
            Value::Null => Presence::Null,
            _ => Presence::Missing,
        }
    }};
}

impl CdcEvent for Wal2JsonV1Event {
    type Backend = Postgres;
    type Checkpoint = crate::NoCheckpoint;

    fn kind(&self) -> EventKind {
        self.kind
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn checkpoint(&self) -> Option<&Self::Checkpoint> {
        self.checkpoint.as_ref()
    }

    fn pk_columns(&self) -> &[ColumnId] {
        &self.pk_columns
    }

    fn changed_columns(&self) -> &[ColumnId] {
        &self.changed_columns
    }

    fn bool_at(&self, row: RowKind, col: ColumnId) -> Presence<&bool> {
        v1_scalar_accessor!(self, row, col, Bool)
    }

    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&i64> {
        v1_scalar_accessor!(self, row, col, Int)
    }

    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&f64> {
        v1_scalar_accessor!(self, row, col, Float)
    }

    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        v1_scalar_accessor!(self, row, col, String)
    }

    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::vec::Vec<u8>> {
        v1_scalar_accessor!(self, row, col, Bytes)
    }

    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&uuid::Uuid> {
        v1_scalar_accessor!(self, row, col, Uuid)
    }

    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDateTime> {
        v1_scalar_accessor!(self, row, col, Timestamp)
    }

    fn timestamp_tz_at(
        &self,
        row: RowKind,
        col: ColumnId,
    ) -> Presence<&chrono::DateTime<chrono::Utc>> {
        v1_scalar_accessor!(self, row, col, TimestampTz)
    }

    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDate> {
        v1_scalar_accessor!(self, row, col, Date)
    }

    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveTime> {
        v1_scalar_accessor!(self, row, col, Time)
    }

    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&bigdecimal::BigDecimal> {
        v1_scalar_accessor!(self, row, col, Decimal)
    }

    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        v1_scalar_accessor!(self, row, col, Json)
    }

    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        v1_scalar_accessor!(self, row, col, Jsonb)
    }
}


fn convert_v1_change_typed<DB: DatabaseLike>(
    change: &Wal2JsonV1Change,
    database: &DB,
) -> Result<Wal2JsonV1Event, WalParseError> {
    let kind = parse_v1_kind(&change.kind)?;
    let table_id = resolve_table(&change.schema, &change.table, database)?;
    let arity = catalog_helpers::table_arity(database, table_id).ok_or_else(|| {
        WalParseError::UnknownTable {
            schema: change.schema.clone(),
            table: change.table.clone(),
        }
    })?;

    if kind == EventKind::Truncate {
        return Ok(Wal2JsonV1Event {
            kind,
            table_id,
            pk_columns: alloc::sync::Arc::from(Vec::<ColumnId>::new()),
            changed_columns: alloc::sync::Arc::from(Vec::<ColumnId>::new()),
            checkpoint: None,
            new_image: None,
            old_image: None,
        });
    }

    if matches!(kind, EventKind::Insert | EventKind::Update)
        && change.columnnames.is_empty()
        && change.columntypes.is_empty()
        && change.columnvalues.is_empty()
    {
        return Err(WalParseError::MissingField(
            "columnnames/columntypes/columnvalues".to_string(),
        ));
    }

    if kind == EventKind::Delete && change.oldkeys.is_none() {
        return Err(WalParseError::MissingField("oldkeys".to_string()));
    }

    let new_image = match kind {
        EventKind::Insert | EventKind::Update => Some(V1RowImage::from_parallel_arrays(
            &change.columnnames,
            &change.columntypes,
            change.columnvalues.clone(),
            table_id,
            arity,
            database,
            "v1 columns",
        )?),
        EventKind::Delete | EventKind::Truncate => None,
    };

    let old_image = match &change.oldkeys {
        Some(oldkeys) => Some(V1RowImage::from_parallel_arrays(
            &oldkeys.keynames,
            &oldkeys.keytypes,
            oldkeys.keyvalues.clone(),
            table_id,
            arity,
            database,
            "v1 oldkeys",
        )?),
        None => None,
    };

    let pk_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Truncate {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    } else if let Some(ref oldkeys) = change.oldkeys {
        let mut ids = Vec::with_capacity(oldkeys.keynames.len());
        let mut seen = hashbrown::HashSet::with_capacity(oldkeys.keynames.len());
        for name in &oldkeys.keynames {
            let col_id = catalog_helpers::column_id(database, table_id, name.as_str())
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
            if !seen.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "v1 oldkeys contains duplicate column '{name}' (id {col_id})"
                )));
            }
            ids.push(col_id);
        }
        alloc::sync::Arc::from(ids)
    } else {
        catalog_helpers::primary_key_columns(database, table_id).map_or_else(|| alloc::sync::Arc::from(Vec::<ColumnId>::new()), alloc::sync::Arc::from)
    };

    let changed_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Update {
        alloc::sync::Arc::from(v1_derive_changed_columns(
            new_image.as_ref(),
            old_image.as_ref(),
            arity,
        ))
    } else {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    };

    Ok(Wal2JsonV1Event {
        kind,
        table_id,
        pk_columns,
        changed_columns,
        checkpoint: None,
        new_image,
        old_image,
    })
}

/// Wire-level `changed_columns` derivation for wal2json v1 Update events.
///
/// wal2json v1's `oldkeys` is almost always sparse (PK columns only), so
/// this returns an empty vector in the common case. It fires only when
/// the source is running with `REPLICA IDENTITY FULL`, where `oldkeys`
/// covers the arity.
fn v1_derive_changed_columns(
    new_image: Option<&V1RowImage>,
    old_image: Option<&V1RowImage>,
    arity: usize,
) -> Vec<ColumnId> {
    let (Some(new), Some(old)) = (new_image, old_image) else {
        return Vec::new();
    };
    if new.entries.len() != arity || old.entries.len() != arity {
        return Vec::new();
    }
    let mut changed = Vec::new();
    for col in 0..arity {
        let (Some(new_idx), Some(old_idx)) = (new.by_col[col], old.by_col[col]) else {
            continue;
        };
        let new_val = &new.entries[new_idx as usize].value;
        let old_val = &old.entries[old_idx as usize].value;
        if new_val != old_val {
            #[allow(clippy::cast_possible_truncation)]
            changed.push(col as ColumnId);
        }
    }
    changed
}
// ============================================================================

#[cfg(test)]
mod tests {
    // Phase 10 note: the exhaustive `#[cfg(any())]` legacy
    // parser test suite (~ tens of tests spelled against the
    // retired Cell/RowImage/PrimaryKey API) was dropped rather
    // than migrated. The typed-event round-trip is exercised
    // by the live `typed_<parser>_*` unit tests in this same
    // module; Phase 11 restores per-scenario coverage against
    // the typed CdcEvent surface.

    use super::super::test_support::{
        orders_customer_catalog as orders_catalog,
        orders_customer_no_pk_catalog as orders_no_pk_catalog,
    };
    use super::*;

    // NOTE: Tests `error_no_arity_v1`/`v2` were removed in the migration from
    // the `SchemaCatalog` trait to `DatabaseLike`. With `DatabaseLike` a
    // resolved table always exposes its column count, so the "arity missing"
    // failure mode is no longer reachable.

    // -- v1 INSERT -----------------------------------------------------------

    
    // -- v1 UPDATE -----------------------------------------------------------

    
    // -- v1 DELETE -----------------------------------------------------------

    
    // -- v1 multi-change transaction -----------------------------------------

    
    // -- v2 INSERT -----------------------------------------------------------

    
    
    // -- v2 UPDATE -----------------------------------------------------------

    
    // -- v2 DELETE -----------------------------------------------------------

    
    // -- Error paths ---------------------------------------------------------

    
    
    
    
    
    
    
    
    
    
    // -- B1: v1 unknown kind is skipped -------------------------------------

    
    // -- v1 unknown kind in multi-change transaction is isolated skip --------

    
    // -- B2: v2 missing table on data action ---------------------------------

    
    // -- Trait object safety --------------------------------------------------

    
    // -- v1 UPDATE with full old row (REPLICA IDENTITY FULL) ----------------

    
    // -- v1 UPDATE without oldkeys (changed_columns branch: None old_row) ----

    
    
    // -- v1 INSERT without catalog PK columns --------------------------------

    
    // -- v2 UPDATE with identity but no pk metadata --------------------------

    
    // -- v2 INSERT without pk metadata AND without catalog PK ----------------

    
    // -- v2 UPDATE without identity (changed_columns branch: None old_row) ---

    
    // -- v2 unknown column error ---------------------------------------------

    
    // -- Null handling -------------------------------------------------------

    
    // -- INSERT without PK metadata (catalog fallback) -----------------------

    
    // -- Error: unknown column in oldkeys ------------------------------------

    
    
    
    
    
    
    
    
    // -- Direct test: build_pk_from_key_arrays with unknown column -----------

    
    
    
    
    // ------------------------------------------------------------------
    // Phase 7A: Wal2JsonV2Event typed CdcEvent smoke tests
    // ------------------------------------------------------------------

    #[test]
    fn typed_v2_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Wal2JsonV2Event>();
    }

    #[test]
    fn typed_v2_event_dispatches_through_engine() {
        let database = orders_catalog();
        let mut engine: crate::SubscriptionEngine<
            Wal2JsonV2Event,
            crate::DefaultIds,
            sql_traits::structs::ParserDB,
        > = crate::SubscriptionEngine::new(database, sqlparser::dialect::PostgreSqlDialect {});

        engine
            .register(
                crate::SubscriptionRequest::new(42u64, "SELECT * FROM orders WHERE amount > 100")
                    .updated_at_unix_ms(1_704_067_200_000),
            )
            .expect("register subscription");

        let msg = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 7},
                {"name": "customer", "type": "integer", "value": 3},
                {"name": "amount", "type": "integer", "value": 250},
                {"name": "status", "type": "text", "value": "paid"}
            ],
            "pk": [{"name": "id", "type": "integer"}]
        }"#;

        let events = Wal2JsonV2Parser
            .parse_wal_message(msg.as_bytes(), engine.database())
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Insert);
        assert_eq!(event.pk_columns(), &[0u16]);

        // Typed scalar accessors: integer round-trip on New.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Present(&250)
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 3),
            crate::backend::Presence::Present(&alloc::string::String::from("paid"))
        );
        // Shape mismatch: id is int, asking for bool returns Missing.
        assert_eq!(
            event.bool_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
        // Non-PK column via Pk returns Missing (not Null).
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 2),
            crate::backend::Presence::Missing
        );
        // PK column via Pk returns the value.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&7)
        );
        // Old image is absent on Insert.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Missing
        );

        // End-to-end: engine dispatch reads the typed accessors internally.
        let notifs = engine.consumers(event).expect("dispatch");
        assert_eq!(notifs.inserted(), alloc::vec![42u64]);
        assert!(notifs.updated().is_empty());
        assert!(notifs.deleted().is_empty());
    }

    #[test]
    fn typed_v2_delete_pk_reads_from_identity() {
        let database = orders_catalog();
        let msg = r#"{
            "action": "D",
            "schema": "public",
            "table": "orders",
            "identity": [{"name": "id", "type": "integer", "value": 9}],
            "pk": [{"name": "id", "type": "integer"}]
        }"#;
        let events = Wal2JsonV2Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Delete);
        assert_eq!(event.pk_columns(), &[0u16]);
        // Old image populated from identity.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Present(&9)
        );
        // Pk aliases Old on a Delete.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&9)
        );
        // New image absent on Delete.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_v2_full_identity_derives_changed_columns() {
        // Update with `identity` covering every column (REPLICA IDENTITY FULL).
        // amount and status change; id and customer stay the same.
        let database = orders_catalog();
        let msg = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 7},
                {"name": "customer", "type": "integer", "value": 3},
                {"name": "amount", "type": "integer", "value": 250},
                {"name": "status", "type": "text", "value": "paid"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 7},
                {"name": "customer", "type": "integer", "value": 3},
                {"name": "amount", "type": "integer", "value": 100},
                {"name": "status", "type": "text", "value": "pending"}
            ],
            "pk": [{"name": "id", "type": "integer"}]
        }"#;
        let events = Wal2JsonV2Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        let mut changed = ev.changed_columns().to_vec();
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![2u16, 3u16]);
    }

    #[test]
    fn typed_v2_sparse_identity_leaves_changed_columns_empty() {
        // Update with `identity` covering only PK (REPLICA IDENTITY DEFAULT):
        // derivation is unsafe, so changed_columns() must be empty.
        let database = orders_catalog();
        let msg = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 7},
                {"name": "customer", "type": "integer", "value": 3},
                {"name": "amount", "type": "integer", "value": 250},
                {"name": "status", "type": "text", "value": "paid"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 7}
            ],
            "pk": [{"name": "id", "type": "integer"}]
        }"#;
        let events = Wal2JsonV2Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let ev = &events[0];
        assert!(ev.changed_columns().is_empty());
    }

    #[test]
    fn typed_v2_null_vs_missing_are_distinct() {
        let database = orders_catalog();
        let msg = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 11},
                {"name": "amount", "type": "integer", "value": null}
            ],
            "pk": [{"name": "id", "type": "integer"}]
        }"#;
        let events = Wal2JsonV2Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        // amount is explicitly SQL NULL on the wire.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Null
        );
        // status was omitted from the columns array entirely.
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 3),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_v2_checkpoint_from_lsn() {
        let database = orders_catalog();
        let msg = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "lsn": "0/16B2270",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "amount", "type": "integer", "value": 10}
            ],
            "pk": [{"name": "id", "type": "integer"}]
        }"#;
        let events = Wal2JsonV2Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert!(events[0].checkpoint().is_some());
    }

    #[test]
    fn typed_v2_skips_transaction_metadata_messages() {
        let database = orders_catalog();
        for tag in ["B", "C", "M"] {
            let msg = format!(r#"{{"action": "{tag}"}}"#);
            let events = Wal2JsonV2Parser
                .parse_wal_message(msg.as_bytes(), &database)
                .expect("parse succeeds");
            assert!(
                events.is_empty(),
                "tag {tag} should produce no events"
            );
        }
    }

    #[test]
    fn typed_v2_tombstone_is_empty() {
        let database = orders_catalog();
        let events = Wal2JsonV2Parser
            .parse_wal_message(b"null", &database)
            .expect("tombstone");
        assert!(events.is_empty());
    }
    // ------------------------------------------------------------------
    // Phase 7B: Wal2JsonV1Event typed CdcEvent smoke tests
    // ------------------------------------------------------------------

    #[test]
    fn typed_v1_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Wal2JsonV1Event>();
    }

    #[test]
    fn typed_v1_event_dispatches_through_engine() {
        let database = orders_catalog();
        let mut engine: crate::SubscriptionEngine<
            Wal2JsonV1Event,
            crate::DefaultIds,
            sql_traits::structs::ParserDB,
        > = crate::SubscriptionEngine::new(database, sqlparser::dialect::PostgreSqlDialect {});

        engine
            .register(
                crate::SubscriptionRequest::new(42u64, "SELECT * FROM orders WHERE amount > 100")
                    .updated_at_unix_ms(1_704_067_200_000),
            )
            .expect("register subscription");

        let msg = r#"{
            "xid": 100,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "integer", "integer", "text"],
                "columnvalues": [7, 3, 250, "paid"]
            }]
        }"#;

        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), engine.database())
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Insert);
        assert_eq!(event.pk_columns(), &[0u16]);

        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Present(&250)
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 3),
            crate::backend::Presence::Present(&alloc::string::String::from("paid"))
        );
        assert_eq!(
            event.bool_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 2),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&7)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Missing
        );

        let notifs = engine.consumers(event).expect("dispatch");
        assert_eq!(notifs.inserted(), alloc::vec![42u64]);
        assert!(notifs.updated().is_empty());
        assert!(notifs.deleted().is_empty());
    }

    #[test]
    fn typed_v1_pk_via_rowkind_pk_returns_missing_for_non_pk_column() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 100,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "integer", "integer", "text"],
                "columnvalues": [1, 2, 3, "ok"]
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 1),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_v1_pk_via_rowkind_pk_returns_present_for_pk_column() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 100,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "integer", "integer", "text"],
                "columnvalues": [1, 2, 3, "ok"]
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&1)
        );
    }

    #[test]
    fn typed_v1_null_vs_missing_are_distinct() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 100,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "amount"],
                "columntypes": ["integer", "integer"],
                "columnvalues": [11, null]
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Null
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 3),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_v1_tombstone_is_empty() {
        let database = orders_catalog();
        let events = Wal2JsonV1Parser
            .parse_wal_message(b"null", &database)
            .expect("tombstone");
        assert!(events.is_empty());
    }

    #[test]
    fn typed_v1_full_oldkeys_derives_changed_columns() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 200,
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "integer", "integer", "text"],
                "columnvalues": [7, 3, 250, "paid"],
                "oldkeys": {
                    "keynames": ["id", "customer", "amount", "status"],
                    "keytypes": ["integer", "integer", "integer", "text"],
                    "keyvalues": [7, 3, 100, "pending"]
                }
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        let mut changed = ev.changed_columns().to_vec();
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![2u16, 3u16]);
    }

    #[test]
    fn typed_v1_sparse_oldkeys_leaves_changed_columns_empty() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 201,
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "integer", "integer", "text"],
                "columnvalues": [7, 3, 250, "paid"],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [7]
                }
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert!(events[0].changed_columns().is_empty());
    }

    #[test]
    fn typed_v1_delete_oldkeys_provides_pk_and_old_image() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 102,
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders",
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [42]
                }
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Delete);
        assert_eq!(event.pk_columns(), &[0u16]);
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Present(&42)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&42)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_v1_truncate_has_no_images() {
        let database = orders_catalog();
        let msg = r#"{
            "xid": 103,
            "change": [{
                "kind": "truncate",
                "schema": "public",
                "table": "orders"
            }]
        }"#;
        let events = Wal2JsonV1Parser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Truncate);
        assert!(event.pk_columns().is_empty());
        assert!(event.changed_columns().is_empty());
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Missing
        );
    }
}
