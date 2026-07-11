//! wal2json v1 and v2 WAL parsers.
//!
//! [wal2json](https://github.com/eulerto/wal2json) is a PostgreSQL logical
//! decoding output plugin that emits changes as JSON. Version 1 batches all
//! changes in a transaction into a single message. Version 2 emits one
//! message per change.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use serde::Deserialize;

use super::map_cdc::parse_event_kind;
use super::pg_type::json_value_to_cell_strict;
use super::row_build::{
    build_pk_from_typed_arrays_with, build_row_from_named_typed_values_with,
    build_row_from_typed_arrays_with,
};
use super::{
    build_event_from_rows, build_pk_from_resolved, pk_from_catalog_or_empty, resolve_table,
    strict_pk_column_ids_from_names, truncate_event, WalParseError, WalParser,
};
use crate::backend::{CdcEvent, Postgres, Presence, RowKind, Value};
use crate::{catalog_helpers, Cell, ColumnId, EventKind, PrimaryKey, RowImage, TableId, WalEvent};
use spin::Once;

use super::pg_type::json_value_to_pg_value;
use sql_traits::prelude::DatabaseLike;

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

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<WalEvent>, WalParseError> {
        let msg: Option<Wal2JsonV1Message> = super::parse_json_message_or_tombstone(data)?;
        let Some(msg) = msg else {
            // wal2json topics may emit tombstone messages ("null") for compaction.
            return Ok(Vec::new());
        };

        let mut events = Vec::with_capacity(msg.change.len());
        for change in &msg.change {
            if let Some(event) = super::skip_unknown_event_kind(
                convert_v1_change(change, database),
                "wal2json v1",
                "kind",
            )? {
                events.push(event);
            }
        }
        Ok(events)
    }
}

impl<DB: DatabaseLike> WalParser<DB> for Wal2JsonV2Parser {
    type Checkpoint = crate::PgLsn;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<WalEvent<Self::Checkpoint>>, WalParseError> {
        super::parse_single_json_event::<Wal2JsonV2Message, _, crate::PgLsn>(data, |msg| {
            // Skip non-row transactional metadata messages.
            if matches!(msg.action.as_str(), "B" | "C" | "M") {
                return Ok(None);
            }
            super::skip_unknown_event_kind(
                convert_v2_message(msg, database),
                "wal2json v2",
                "action",
            )
        })
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

/// Build a [`RowImage`] from parallel name/type/value arrays.
///
/// Returns `(row_image, Vec<(ColumnId, Cell)>)`. The vec is used for PK
/// extraction.
fn build_row_from_arrays<DB: DatabaseLike>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    database: &DB,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
    build_row_from_typed_arrays_with(
        names,
        types,
        values,
        table_id,
        database,
        "column",
        |value, ty, name| json_value_to_cell_strict(value, ty, &format!("wal2json.column.{name}")),
    )
}

/// Build a [`RowImage`] from v2 column structs.
fn build_row_from_v2_columns<DB: DatabaseLike>(
    columns: &[Wal2JsonV2Column],
    table_id: TableId,
    database: &DB,
) -> Result<(RowImage, Vec<(ColumnId, Cell)>), WalParseError> {
    let typed_columns: Vec<(&str, &str, &serde_json::Value)> = columns
        .iter()
        .map(|col| (col.name.as_str(), col.type_name.as_str(), &col.value))
        .collect();
    build_row_from_named_typed_values_with(
        &typed_columns,
        table_id,
        database,
        "v2 columns",
        |value, ty, name| json_value_to_cell_strict(value, ty, &format!("wal2json.column.{name}")),
    )
}

/// Build a [`PrimaryKey`] from the old-keys section (names resolved through database).
fn build_pk_from_key_arrays<DB: DatabaseLike>(
    names: &[String],
    types: &[String],
    values: &[serde_json::Value],
    table_id: TableId,
    database: &DB,
) -> Result<PrimaryKey, WalParseError> {
    build_pk_from_typed_arrays_with(
        names,
        types,
        values,
        table_id,
        database,
        "oldkeys",
        |value, ty, name| json_value_to_cell_strict(value, ty, &format!("wal2json.oldkeys.{name}")),
    )
}

// ============================================================================
// v1 conversion
// ============================================================================

fn convert_v1_change<DB: DatabaseLike>(
    change: &Wal2JsonV1Change,
    database: &DB,
) -> Result<WalEvent, WalParseError> {
    let kind = parse_v1_kind(&change.kind)?;
    let table_id = resolve_table(&change.schema, &change.table, database)?;

    if kind == EventKind::Truncate {
        return truncate_event(table_id, None);
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

    let (new_row, new_resolved) = if kind == EventKind::Insert || kind == EventKind::Update {
        let (row, resolved) = build_row_from_arrays(
            &change.columnnames,
            &change.columntypes,
            &change.columnvalues,
            table_id,
            database,
        )?;
        (Some(row), resolved)
    } else {
        (None, Vec::new())
    };

    let (old_row, pk) = if let Some(ref oldkeys) = change.oldkeys {
        // Build old row from oldkeys (sparse: only key columns)
        let (row, _) = build_row_from_arrays(
            &oldkeys.keynames,
            &oldkeys.keytypes,
            &oldkeys.keyvalues,
            table_id,
            database,
        )?;

        let pk = build_pk_from_key_arrays(
            &oldkeys.keynames,
            &oldkeys.keytypes,
            &oldkeys.keyvalues,
            table_id,
            database,
        )?;

        (Some(row), pk)
    } else {
        // INSERT without oldkeys: extract PK from new row using database metadata
        let pk = pk_from_catalog_or_empty(&new_resolved, table_id, database)?;
        (None, pk)
    };
    let old_row_complete = super::old_row_is_complete(old_row.as_ref());
    build_event_from_rows(
        kind,
        table_id,
        pk,
        old_row,
        new_row,
        old_row_complete,
        "columnnames/columntypes/columnvalues",
        "oldkeys",
        None,
    )
}

// ============================================================================
// v2 conversion
// ============================================================================

fn convert_v2_message<DB: DatabaseLike>(
    msg: &Wal2JsonV2Message,
    database: &DB,
) -> Result<WalEvent<crate::PgLsn>, WalParseError> {
    let kind = parse_v2_kind(&msg.action)?;

    let schema = msg.schema.as_deref().unwrap_or("");
    let table = msg.table.as_deref().ok_or_else(|| {
        WalParseError::JsonError("data message (I/U/D) missing 'table' field".to_string())
    })?;
    let table_id = resolve_table(schema, table, database)?;

    // Parse PG LSN if wal2json was invoked with include-lsn=true. Malformed
    // strings are dropped silently (the parser does not gate event emission
    // on a present checkpoint - oplog / replay can fall back to None).
    let checkpoint = msg.lsn.as_deref().and_then(crate::PgLsn::parse);

    let (new_row, new_resolved) = match kind {
        EventKind::Insert | EventKind::Update => {
            let columns = msg
                .columns
                .as_ref()
                .filter(|columns| !columns.is_empty())
                .ok_or_else(|| WalParseError::MissingField("columns".to_string()))?;
            let (row, resolved) = build_row_from_v2_columns(columns, table_id, database)?;
            (Some(row), resolved)
        }
        EventKind::Delete | EventKind::Truncate => (None, Vec::new()),
    };

    let (old_row, identity_resolved) = match kind {
        EventKind::Delete => {
            let identity = msg
                .identity
                .as_ref()
                .filter(|identity| !identity.is_empty())
                .ok_or_else(|| WalParseError::MissingField("identity".to_string()))?;
            let (row, resolved) = build_row_from_v2_columns(identity, table_id, database)?;
            (Some(row), resolved)
        }
        EventKind::Update => {
            if let Some(identity) = msg
                .identity
                .as_ref()
                .filter(|identity| !identity.is_empty())
            {
                let (row, resolved) = build_row_from_v2_columns(identity, table_id, database)?;
                (Some(row), resolved)
            } else {
                (None, Vec::new())
            }
        }
        EventKind::Insert | EventKind::Truncate => (None, Vec::new()),
    };
    let old_row_complete = super::old_row_is_complete(old_row.as_ref());

    // Build PK: prefer identity columns, then pk metadata, then database.
    // The three-way branching is clearer as if-let chains than map_or_else.
    #[allow(clippy::option_if_let_else)]
    let pk = if kind == EventKind::Truncate {
        PrimaryKey::empty()
    } else if !identity_resolved.is_empty() {
        // Use identity columns as PK source (UPDATE/DELETE)
        if let Some(ref pk_cols) = msg.pk {
            let pk_names: Vec<String> = pk_cols.iter().map(|c| c.name.clone()).collect();
            let pk_col_ids = strict_pk_column_ids_from_names(
                table_id,
                &pk_names,
                &identity_resolved,
                database,
                "pk",
            )?;
            build_pk_from_resolved(&identity_resolved, &pk_col_ids)
        } else {
            // No pk metadata: use all identity columns as PK
            let cols: Vec<ColumnId> = identity_resolved.iter().map(|(c, _)| *c).collect();
            let vals: Vec<Cell> = identity_resolved.iter().map(|(_, v)| v.clone()).collect();
            PrimaryKey::new(Arc::from(cols), Arc::from(vals))
                .expect("identity columns and values are built in lockstep")
        }
    } else if let Some(ref pk_cols) = msg.pk {
        // INSERT: extract PK from new row using pk metadata
        let pk_names: Vec<String> = pk_cols.iter().map(|c| c.name.clone()).collect();
        let pk_col_ids =
            strict_pk_column_ids_from_names(table_id, &pk_names, &new_resolved, database, "pk")?;
        build_pk_from_resolved(&new_resolved, &pk_col_ids)
    } else {
        pk_from_catalog_or_empty(&new_resolved, table_id, database)?
    };

    build_event_from_rows(
        kind,
        table_id,
        pk,
        old_row,
        new_row,
        old_row_complete,
        "columns",
        "identity",
        checkpoint,
    )
}

// ============================================================================
// Wal2JsonV2Event: typed [`CdcEvent`] output of the v2 parser
// ============================================================================

/// Typed CDC event surfaced by [`Wal2JsonV2Parser::parse_wal_message_typed`].
///
/// Owns the wal2json v2 wire payload for one row change plus lazily
/// populated caches of decoded [`Value<Postgres>`] cells. Scalar accessors
/// on the [`CdcEvent`] impl decode each cell on first access through
/// [`json_value_to_pg_value`] and return references into the cache.
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
    fn col_id_field_name(&self) -> &str {
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

impl Wal2JsonV2Parser {
    /// Typed variant of [`WalParser::parse_wal_message`] returning
    /// [`Wal2JsonV2Event`] instances that implement
    /// [`crate::backend::CdcEvent`] directly.
    ///
    /// Retained alongside the legacy [`WalEvent`]-producing trait method
    /// during Phase 7. The trait swap happens in Phase 7F.
    pub fn parse_wal_message_typed<DB: DatabaseLike>(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Wal2JsonV2Event>, WalParseError> {
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
        && columns.as_ref().is_none_or(|c| c.is_empty())
    {
        return Err(WalParseError::MissingField("columns".to_string()));
    }
    if kind == EventKind::Delete && identity.as_ref().is_none_or(|i| i.is_empty()) {
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
        catalog_helpers::primary_key_columns(database, table_id)
            .map(alloc::sync::Arc::from)
            .unwrap_or_else(|| {
                alloc::sync::Arc::from(
                    image
                        .entries
                        .iter()
                        .map(|e| e.col_id)
                        .collect::<Vec<_>>(),
                )
            })
    } else {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    };

    // `changed_columns` derivation is deferred to a follow-up: it requires
    // decoding paired old / new cells, which conflicts with decode-on-
    // demand. Consumers treat the slice as a hint, not an authoritative
    // diff, so an empty slice is safe (over-notification, never under-
    // notification).
    let changed_columns: alloc::sync::Arc<[ColumnId]> =
        alloc::sync::Arc::from(Vec::<ColumnId>::new());

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


// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
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

    #[test]
    fn v1_insert() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 100,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 99.95, "pending"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.table_id(), 1);

        // New row present
        let new = ev.new_row().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));
        assert_eq!(new.get(2), Some(&Cell::Float(99.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("pending"))));

        // No old row
        assert!(ev.old_row().is_none());

        // PK extracted from new row via catalog
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);

        // No changed columns for INSERT
        assert!(ev.changed_columns().is_empty());
    }

    // -- v1 UPDATE -----------------------------------------------------------

    #[test]
    fn v1_update() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 101,
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 149.95, "shipped"],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [1]
                }
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);

        // New row
        let new = ev.new_row().expect("UPDATE should have new_row");
        assert_eq!(new.get(2), Some(&Cell::Float(149.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("shipped"))));

        // Old row (sparse: only key columns)
        let old = ev.old_row().expect("UPDATE should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(1)));
        assert_eq!(old.get(1), Some(&Cell::Missing)); // not in oldkeys

        // PK from oldkeys
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v1 DELETE -----------------------------------------------------------

    #[test]
    fn v1_delete() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
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

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert!(ev.new_row().is_none());
        assert!(ev.old_row().is_some());

        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(42)]);

        assert!(ev.changed_columns().is_empty());
    }

    // -- v1 multi-change transaction -----------------------------------------

    #[test]
    fn v1_multi_change() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "xid": 200,
            "change": [
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "orders",
                    "columnnames": ["id", "customer", "amount", "status"],
                    "columntypes": ["integer", "text", "numeric", "text"],
                    "columnvalues": [10, "bob", 50.0, "new"]
                },
                {
                    "kind": "update",
                    "schema": "public",
                    "table": "orders",
                    "columnnames": ["id", "customer", "amount", "status"],
                    "columntypes": ["integer", "text", "numeric", "text"],
                    "columnvalues": [11, "carol", 75.0, "confirmed"],
                    "oldkeys": {
                        "keynames": ["id"],
                        "keytypes": ["integer"],
                        "keyvalues": [11]
                    }
                },
                {
                    "kind": "delete",
                    "schema": "public",
                    "table": "orders",
                    "oldkeys": {
                        "keynames": ["id"],
                        "keytypes": ["integer"],
                        "keyvalues": [12]
                    }
                }
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind(), EventKind::Insert);
        assert_eq!(events[1].kind(), EventKind::Update);
        assert_eq!(events[2].kind(), EventKind::Delete);
    }

    // -- v2 INSERT -----------------------------------------------------------

    #[test]
    fn v2_insert() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 99.95},
                {"name": "status", "type": "text", "value": "pending"}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.table_id(), 1);

        let new = ev.new_row().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));

        assert!(ev.old_row().is_none());

        // PK from pk metadata + new row
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    #[test]
    fn v2_insert_duplicate_columns_returns_error() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "id", "type": "integer", "value": 2}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("duplicate v2 columns should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- v2 UPDATE -----------------------------------------------------------

    #[test]
    fn v2_update() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 149.95},
                {"name": "status", "type": "text", "value": "shipped"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 1}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);

        assert!(ev.new_row().is_some());
        assert!(ev.old_row().is_some());

        // PK from identity
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v2 DELETE -----------------------------------------------------------

    #[test]
    fn v2_delete() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "D",
            "schema": "public",
            "table": "orders",
            "identity": [
                {"name": "id", "type": "integer", "value": 42}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert!(ev.new_row().is_none());
        assert!(ev.old_row().is_some());

        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(42)]);
    }

    // -- Error paths ---------------------------------------------------------

    #[test]
    fn error_invalid_utf8() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn error_malformed_json() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn error_unknown_table() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "nonexistent",
                "columnnames": ["id"],
                "columntypes": ["integer"],
                "columnvalues": [1]
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    #[test]
    fn error_unknown_column() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "nonexistent_col"],
                "columntypes": ["integer", "text"],
                "columnvalues": [1, "value"]
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn v1_truncate_kind_emits_truncate_event() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "truncate",
                "schema": "public",
                "table": "orders",
                "columnnames": [],
                "columntypes": [],
                "columnvalues": []
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("truncate should parse");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Truncate);
        assert_eq!(ev.table_id(), 1);
        assert!(ev.pk().is_empty());
        assert!(ev.old_row().is_none());
        assert!(ev.new_row().is_none());
        assert!(ev.changed_columns().is_empty());
    }

    #[test]
    fn v2_non_data_actions_skipped() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        // Begin (B), Commit (C), Message (M) are silently skipped.
        for action in &["B", "C", "M"] {
            let json = format!(r#"{{"action": "{action}"}}"#);
            let events = parser
                .parse_wal_message(json.as_bytes(), &catalog)
                .expect("non-data messages should not error");
            assert!(
                events.is_empty(),
                "action '{action}' should return empty vec"
            );
        }
    }

    #[test]
    fn v2_truncate_action_emits_truncate_event() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "T",
            "schema": "public",
            "table": "orders"
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("truncate action should parse");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Truncate);
        assert_eq!(ev.table_id(), 1);
        assert!(ev.pk().is_empty());
        assert!(ev.old_row().is_none());
        assert!(ev.new_row().is_none());
        assert!(ev.changed_columns().is_empty());
    }

    #[test]
    fn v1_tombstone_null_is_ignored() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;
        let events = parser
            .parse_wal_message(b"null", &catalog)
            .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[test]
    fn v2_tombstone_null_is_ignored() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;
        let events = parser
            .parse_wal_message(b"null", &catalog)
            .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[test]
    fn v2_unknown_action_is_skipped() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        // "X" is not a known v2 action. It must be skipped, not error.
        let json = r#"{"action":"X","schema":"public","table":"orders"}"#;
        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("unknown action should be skipped, not error");
        assert!(events.is_empty(), "unknown action should produce no output");
    }

    // -- B1: v1 unknown kind is skipped -------------------------------------

    #[test]
    fn v1_unknown_kind_is_skipped() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;
        // "schema_change" is not insert/update/delete. It must be skipped, not error.
        let json = r#"{"change":[{"kind":"schema_change","schema":"public","table":"orders","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}]}"#;
        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("unknown kind should be skipped, not error");
        assert!(events.is_empty(), "unknown kind should produce no output");
    }

    // -- v1 unknown kind in multi-change transaction is isolated skip --------

    #[test]
    fn v1_unknown_kind_in_multi_change_skips_only_that_change() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;
        // Two changes: the first is unknown (skipped), the second is a valid insert.
        let json = r#"{
            "change": [
                {"kind":"schema_change","schema":"public","table":"orders","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]},
                {"kind":"insert","schema":"public","table":"orders","columnnames":["id","customer","amount","status"],"columntypes":["integer","text","numeric","text"],"columnvalues":[42,"alice",99.0,"new"]}
            ]
        }"#;
        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("mixed changes should not error");
        assert_eq!(events.len(), 1, "only the valid insert should be emitted");
        assert_eq!(events[0].kind(), EventKind::Insert);
    }

    // -- B2: v2 missing table on data action ---------------------------------

    #[test]
    fn v2_missing_table_on_data_action() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;
        // "action": "I" with no table should fail
        let json = r#"{"action":"I","schema":"public","columnnames":["id"],"columntypes":["integer"],"columnvalues":[1]}"#;
        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("missing table should fail");
        assert!(
            matches!(err, WalParseError::MissingField(_))
                || matches!(err, WalParseError::JsonError(_))
                || matches!(err, WalParseError::UnknownTable { .. }),
            "Expected error for missing table, got: {err:?}"
        );
    }

    // -- Trait object safety --------------------------------------------------

    #[test]
    fn trait_object_compiles() {
        let catalog = orders_catalog();
        let parser: &dyn WalParser<
            sql_traits::structs::ParserDB,
            Checkpoint = crate::NoCheckpoint,
        > = &Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "test", 10.0, "new"]
            }]
        }"#;

        let result = parser.parse_wal_message(json.as_bytes(), &catalog);
        assert!(result.is_ok());
    }

    // -- v1 UPDATE with full old row (REPLICA IDENTITY FULL) ----------------

    #[test]
    fn v1_update_with_changed_columns() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        // Simulate REPLICA IDENTITY FULL: oldkeys contains all columns
        let json = r#"{
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 149.95, "shipped"],
                "oldkeys": {
                    "keynames": ["id", "customer", "amount", "status"],
                    "keytypes": ["integer", "text", "numeric", "text"],
                    "keyvalues": [1, "alice", 99.95, "pending"]
                }
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);

        // amount (col 2) and status (col 3) changed
        let changed: Vec<ColumnId> = ev.changed_columns().to_vec();
        assert!(changed.contains(&2), "amount should be changed");
        assert!(changed.contains(&3), "status should be changed");
        assert!(!changed.contains(&0), "id should NOT be changed");
        assert!(!changed.contains(&1), "customer should NOT be changed");
    }

    // -- v1 UPDATE without oldkeys (changed_columns branch: None old_row) ----

    #[test]
    fn v1_update_without_oldkeys() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 149.95, "shipped"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        // No oldkeys, no old_row, changed_columns is empty
        assert!(ev.old_row().is_none());
        assert!(ev.changed_columns().is_empty());
    }

    #[test]
    fn v1_update_with_partial_oldkeys_keeps_changed_columns_empty_for_safety() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        // oldkeys includes only the PK. New row changes both PK and amount.
        // The old row is partial, so changed-columns must be treated as unknown.
        let json = r#"{
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [2, "alice", 149.95, "shipped"],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [1]
                }
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert!(ev.old_row().is_some());
        assert!(
            ev.changed_columns().is_empty(),
            "partial oldkeys must disable changed-columns pruning"
        );
    }

    // -- v1 INSERT without catalog PK columns --------------------------------

    #[test]
    fn v1_insert_no_catalog_pk() {
        let catalog = orders_no_pk_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 99.95, "pending"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // No oldkeys and no catalog PK: empty PK
        assert!(ev.pk().columns.is_empty());
        assert!(ev.pk().values.is_empty());
    }

    // -- v2 UPDATE with identity but no pk metadata --------------------------

    #[test]
    fn v2_update_identity_no_pk_metadata() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 149.95},
                {"name": "status", "type": "text", "value": "shipped"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 1}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        // PK from all identity columns (no pk metadata to filter)
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    // -- v2 INSERT without pk metadata AND without catalog PK ----------------

    #[test]
    fn v2_insert_no_pk_no_catalog() {
        let catalog = orders_no_pk_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 5},
                {"name": "customer", "type": "text", "value": "eve"},
                {"name": "amount", "type": "numeric", "value": 10.0},
                {"name": "status", "type": "text", "value": "new"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // No pk metadata and no catalog PK: empty PK
        assert!(ev.pk().columns.is_empty());
        assert!(ev.pk().values.is_empty());
    }

    // -- v2 UPDATE without identity (changed_columns branch: None old_row) ---

    #[test]
    fn v2_update_without_identity() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 149.95},
                {"name": "status", "type": "text", "value": "shipped"}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        // No identity, no old_row, changed_columns empty
        assert!(ev.old_row().is_none());
        assert!(ev.changed_columns().is_empty());
    }

    // -- v2 unknown column error ---------------------------------------------

    #[test]
    fn error_unknown_column_v2() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "bogus_col", "type": "text", "value": "x"}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    // -- Null handling -------------------------------------------------------

    #[test]
    fn v1_null_values() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, null, null, "active"]
            }]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let new = events[0].new_row().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Null));
        assert_eq!(new.get(2), Some(&Cell::Null));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("active"))));
    }

    // -- INSERT without PK metadata (catalog fallback) -----------------------

    #[test]
    fn v2_insert_no_pk_metadata() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 7},
                {"name": "customer", "type": "text", "value": "dave"},
                {"name": "amount", "type": "numeric", "value": 25.0},
                {"name": "status", "type": "text", "value": "new"}
            ]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // PK should come from catalog.primary_key_columns()
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(7)]);
    }

    // -- Error: unknown column in oldkeys ------------------------------------

    #[test]
    fn error_unknown_column_in_oldkeys() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders",
                "oldkeys": {
                    "keynames": ["nonexistent_key"],
                    "keytypes": ["integer"],
                    "keyvalues": [1]
                }
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_v1_delete_missing_oldkeys() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders"
            }]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("delete without oldkeys should fail");
        assert!(matches!(err, WalParseError::MissingField(field) if field == "oldkeys"));
    }

    #[test]
    fn error_v2_insert_missing_columns() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders"
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("insert without columns should fail");
        assert!(matches!(err, WalParseError::MissingField(field) if field == "columns"));
    }

    #[test]
    fn error_v2_update_missing_columns() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "identity": [
                {"name": "id", "type": "integer", "value": 1}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("update without columns should fail");
        assert!(matches!(err, WalParseError::MissingField(field) if field == "columns"));
    }

    #[test]
    fn error_v2_delete_missing_identity() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "D",
            "schema": "public",
            "table": "orders"
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("delete without identity should fail");
        assert!(matches!(err, WalParseError::MissingField(field) if field == "identity"));
    }

    #[test]
    fn error_v2_numeric_overflow() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "bigint", "value": 18446744073709551615}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("overflow should fail");
        assert!(matches!(err, WalParseError::NumericOverflow { .. }));
    }

    #[test]
    fn error_v2_pk_metadata_unknown_column() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "bigint", "value": 1},
                {"name": "amount", "type": "numeric", "value": 12.5}
            ],
            "pk": [
                {"name": "id", "type": "bigint"},
                {"name": "missing_pk_col", "type": "bigint"}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("unknown PK metadata column should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_v2_pk_metadata_column_missing_in_row() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV2Parser;

        let json = r#"{
            "action": "I",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "amount", "type": "numeric", "value": 12.5}
            ],
            "pk": [
                {"name": "id", "type": "bigint"}
            ]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("missing PK value in row should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- Direct test: build_pk_from_key_arrays with unknown column -----------

    #[test]
    fn error_build_pk_unknown_column() {
        // Exercise build_pk_from_key_arrays directly (normally preempted
        // by build_row_from_arrays checking the same columns first).
        let catalog = orders_catalog();
        let names = vec!["nonexistent".to_string()];
        let types = vec!["integer".to_string()];
        let values = vec![serde_json::json!(1)];

        let err = build_pk_from_key_arrays(&names, &types, &values, 1, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[test]
    fn error_mismatched_column_array_lengths_v1_does_not_panic() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer"],
                "columntypes": ["integer"],
                "columnvalues": [1, "alice"]
            }]
        }"#;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(json.as_bytes(), &catalog)
        }));

        assert!(result.is_ok(), "parser must not panic on malformed payload");
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn error_mismatched_oldkeys_array_lengths_v1_does_not_panic() {
        let catalog = orders_catalog();
        let parser = Wal2JsonV1Parser;

        let json = r#"{
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "orders",
                "oldkeys": {
                    "keynames": ["id", "customer"],
                    "keytypes": ["integer"],
                    "keyvalues": [1, "alice"]
                }
            }]
        }"#;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(json.as_bytes(), &catalog)
        }));

        assert!(result.is_ok(), "parser must not panic on malformed payload");
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[test]
    fn update_partial_old_rows_keep_changed_columns_empty_in_v1_and_v2() {
        let catalog = orders_catalog();
        let v1 = Wal2JsonV1Parser;
        let v2 = Wal2JsonV2Parser;

        let v1_json = r#"{
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "orders",
                "columnnames": ["id", "customer", "amount", "status"],
                "columntypes": ["integer", "text", "numeric", "text"],
                "columnvalues": [1, "alice", 75.0, "open"],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [1]
                }
            }]
        }"#;
        let v1_events = v1
            .parse_wal_message(v1_json.as_bytes(), &catalog)
            .expect("v1 update should parse");
        assert_eq!(v1_events.len(), 1);
        assert_eq!(v1_events[0].kind(), EventKind::Update);
        assert!(v1_events[0].old_row().is_some());
        assert!(
            v1_events[0].changed_columns().is_empty(),
            "v1 partial old row must not compute changed_columns"
        );

        let v2_json = r#"{
            "action": "U",
            "schema": "public",
            "table": "orders",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "customer", "type": "text", "value": "alice"},
                {"name": "amount", "type": "numeric", "value": 75.0},
                {"name": "status", "type": "text", "value": "open"}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 1}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        }"#;
        let v2_events = v2
            .parse_wal_message(v2_json.as_bytes(), &catalog)
            .expect("v2 update should parse");
        assert_eq!(v2_events.len(), 1);
        assert_eq!(v2_events[0].kind(), EventKind::Update);
        assert!(v2_events[0].old_row().is_some());
        assert!(
            v2_events[0].changed_columns().is_empty(),
            "v2 partial old row must not compute changed_columns"
        );
    }

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
            .parse_wal_message_typed(msg.as_bytes(), engine.database())
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
            .parse_wal_message_typed(msg.as_bytes(), &database)
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
            .parse_wal_message_typed(msg.as_bytes(), &database)
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
            .parse_wal_message_typed(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert!(events[0].checkpoint().is_some());
    }

    #[test]
    fn typed_v2_skips_transaction_metadata_messages() {
        let database = orders_catalog();
        for tag in ["B", "C", "M"] {
            let msg = format!(r#"{{"action": "{tag}"}}"#);
            let events = Wal2JsonV2Parser
                .parse_wal_message_typed(msg.as_bytes(), &database)
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
            .parse_wal_message_typed(b"null", &database)
            .expect("tombstone");
        assert!(events.is_empty());
    }
}
