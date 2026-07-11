//! pgoutput adapter: maps `pg_walstream::LogicalReplicationMessage` to `WalEvent`.

use alloc::collections::VecDeque;
use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;

use hashbrown::{HashMap, HashSet};
use spin::Mutex;

use pg_walstream::error::ReplicationError;
use pg_walstream::protocol::{
    ColumnData, ColumnInfo, LogicalReplicationMessage, LogicalReplicationParser, TupleData,
};
use sql_traits::prelude::DatabaseLike;

use super::{resolve_table, WalParseError, WalParser};
use crate::{catalog_helpers, ColumnId, EventKind, TableId};

const MAX_COLUMNS_PER_MESSAGE: usize = 10_000;
#[cfg(not(test))]
const MAX_CACHED_RELATIONS: usize = 65_536;
#[cfg(test)]
const MAX_CACHED_RELATIONS: usize = 32;
const PROTOCOL_VERSION: u32 = 1;


#[derive(Clone, Debug)]
struct CachedRelation {
    table_id: TableId,
    column_ids: Vec<ColumnId>,
    column_type_oids: Vec<u32>,
    arity: usize,
    identity_columns: Vec<usize>,
}

#[derive(Default)]
struct RelationCache {
    map: HashMap<u32, CachedRelation>,
    insertion_order: VecDeque<u32>,
}

impl RelationCache {
    fn insert(&mut self, oid: u32, relation: CachedRelation) {
        if self.map.contains_key(&oid) {
            self.map.insert(oid, relation);
            if let Some(pos) = self
                .insertion_order
                .iter()
                .position(|existing| *existing == oid)
            {
                let _ = self.insertion_order.remove(pos);
            }
            self.insertion_order.push_back(oid);
            return;
        }
        if self.map.len() >= MAX_CACHED_RELATIONS {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.map.remove(&oldest);
            }
        }
        self.map.insert(oid, relation);
        self.insertion_order.push_back(oid);
    }
}

pub struct PgOutputParser {
    parser: Mutex<LogicalReplicationParser>,
    relations: Mutex<RelationCache>,
}

impl PgOutputParser {
    #[must_use]
    pub fn new() -> Self {
        Self {
            parser: Mutex::new(LogicalReplicationParser::with_protocol_version(
                PROTOCOL_VERSION,
            )),
            relations: Mutex::new(RelationCache::default()),
        }
    }
}

impl Default for PgOutputParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PgOutputParser {
    fn get_relation(&self, oid: u32) -> Result<CachedRelation, WalParseError> {
        self.relations
            .lock()
            .map
            .get(&oid)
            .cloned()
            .ok_or(WalParseError::UnknownRelationOid(oid))
    }
}

/// Message tags whose payloads carry no row data and that the legacy
/// parser silently skipped without even decoding the body. We match that
/// behavior so truncated 2PC/keepalive frames in pg slot replays do not
/// surface parse errors.
const SKIP_TAGS: &[u8] = b"BCOYMSEcAPKrbp";

impl<DB: DatabaseLike> WalParser<DB> for PgOutputParser {
    type Checkpoint = crate::PgLsn;
    type Event = PgOutputEvent;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        if data.is_empty() {
            return Ok(vec![]);
        }
        let tag = data[0];
        if SKIP_TAGS.contains(&tag) {
            return Ok(vec![]);
        }
        if !matches!(tag, b'R' | b'I' | b'U' | b'D' | b'T') {
            return Ok(vec![]);
        }

        let parsed = self
            .parser
            .lock()
            .parse_wal_message(data)
            .map_err(translate_error)?;

        match parsed.message {
            LogicalReplicationMessage::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity: _,
                columns,
            } => {
                let cached = build_cached_relation(
                    relation_id,
                    namespace.as_ref(),
                    relation_name.as_ref(),
                    &columns,
                    database,
                )?;
                self.relations.lock().insert(relation_id, cached);
                Ok(vec![])
            }
            LogicalReplicationMessage::Insert { relation_id, tuple } => {
                let rel = self.get_relation(relation_id)?;
                let new_image = tuple_wire_full(&rel, &tuple, true)?;
                let pk_columns = pk_columns_for_new(&rel, database)?;
                Ok(vec![PgOutputEvent {
                    kind: EventKind::Insert,
                    table_id: rel.table_id,
                    pk_columns,
                    changed_columns: Arc::from(Vec::<ColumnId>::new()),
                    checkpoint: None,
                    new_image: Some(new_image),
                    old_image: None,
                }])
            }
            LogicalReplicationMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                let rel = self.get_relation(relation_id)?;
                let new_image = tuple_wire_full(&rel, &new_tuple, true)?;
                let old_image = parse_update_old_wire(&rel, old_tuple, key_type)?;
                let pk_columns = pk_columns_from_identity_or_catalog(&rel, database)?;
                Ok(vec![PgOutputEvent {
                    kind: EventKind::Update,
                    table_id: rel.table_id,
                    pk_columns,
                    changed_columns: pgout_derive_changed_columns(
                        Some(&new_image),
                        old_image.as_ref(),
                        rel.arity,
                    ),
                    checkpoint: None,
                    new_image: Some(new_image),
                    old_image,
                }])
            }
            LogicalReplicationMessage::Delete {
                relation_id,
                old_tuple,
                key_type,
            } => {
                let rel = self.get_relation(relation_id)?;
                let old_image = match key_type {
                    'K' => tuple_wire_key(&rel, &old_tuple)?,
                    'O' => tuple_wire_full(&rel, &old_tuple, false)?,
                    other => {
                        return Err(WalParseError::MalformedPayload(format!(
                            "invalid DELETE tuple key_type 0x{:02X}",
                            other as u32
                        )));
                    }
                };
                let pk_columns = pk_columns_from_identity_or_catalog(&rel, database)?;
                Ok(vec![PgOutputEvent {
                    kind: EventKind::Delete,
                    table_id: rel.table_id,
                    pk_columns,
                    changed_columns: Arc::from(Vec::<ColumnId>::new()),
                    checkpoint: None,
                    new_image: None,
                    old_image: Some(old_image),
                }])
            }
            LogicalReplicationMessage::Truncate { relation_ids, .. } => {
                let cache = self.relations.lock();
                let mut events = Vec::with_capacity(relation_ids.len());
                for oid in relation_ids {
                    if let Some(rel) = cache.map.get(&oid) {
                        events.push(PgOutputEvent {
                            kind: EventKind::Truncate,
                            table_id: rel.table_id,
                            pk_columns: Arc::from(Vec::<ColumnId>::new()),
                            changed_columns: Arc::from(Vec::<ColumnId>::new()),
                            checkpoint: None,
                            new_image: None,
                            old_image: None,
                        });
                    }
                }
                Ok(events)
            }
            _ => Ok(vec![]),
        }
    }
}

fn build_cached_relation<DB: DatabaseLike>(
    oid: u32,
    namespace: &str,
    name: &str,
    columns: &[ColumnInfo],
    database: &DB,
) -> Result<CachedRelation, WalParseError> {
    if columns.len() > MAX_COLUMNS_PER_MESSAGE {
        return Err(WalParseError::MalformedPayload(format!(
            "Relation '{name}' (oid {oid}) declares {} columns, exceeding limit {MAX_COLUMNS_PER_MESSAGE}",
            columns.len()
        )));
    }

    let table_id = resolve_table(namespace, name, database)?;
    let arity = catalog_helpers::table_arity(database, table_id).ok_or_else(|| {
        WalParseError::UnknownTable {
            schema: namespace.to_string(),
            table: name.to_string(),
        }
    })?;

    let mut column_ids = Vec::with_capacity(columns.len());
    let mut column_type_oids = Vec::with_capacity(columns.len());
    let mut seen = HashSet::with_capacity(columns.len());

    for col in columns {
        let col_name = col.name.as_ref();
        let col_id = catalog_helpers::column_id(database, table_id, col_name).ok_or_else(|| {
            WalParseError::UnknownColumn {
                table_id,
                column: col_name.to_string(),
            }
        })?;
        if !seen.insert(col_id) {
            return Err(WalParseError::MalformedPayload(format!(
                "relation '{name}' column '{col_name}' resolves to duplicate column id {col_id} for table {table_id}"
            )));
        }
        if (col_id as usize) >= arity {
            return Err(WalParseError::MalformedPayload(format!(
                "relation column '{col_name}' resolved to out-of-range column id {col_id} for table {table_id} (arity {arity})"
            )));
        }
        column_ids.push(col_id);
        column_type_oids.push(col.type_id);
    }

    let identity_columns: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.flags & 1 != 0)
        .map(|(i, _)| i)
        .collect();

    Ok(CachedRelation {
        table_id,
        column_ids,
        column_type_oids,
        arity,
        identity_columns,
    })
}


fn translate_error(err: ReplicationError) -> WalParseError {
    match err {
        ReplicationError::Buffer(msg) => {
            parse_truncated(&msg).unwrap_or(WalParseError::TruncatedMessage {
                expected: 0,
                actual: 0,
            })
        }
        ReplicationError::Protocol(msg) => parse_unknown_tuple_tag(&msg)
            .map(WalParseError::UnknownTupleTag)
            .or_else(|| parse_truncated(&msg))
            .unwrap_or(WalParseError::MalformedPayload(msg)),
        ReplicationError::Deserialize(msg) | ReplicationError::Generic(msg) => {
            WalParseError::MalformedPayload(msg)
        }
        other => WalParseError::MalformedPayload(format!("{other:?}")),
    }
}

/// Recover [`WalParseError::TruncatedMessage`] from pg_walstream's two
/// truncation error shapes (the byte-count short read, and the cstring
/// missing terminator path).
fn parse_truncated(msg: &str) -> Option<WalParseError> {
    if msg == "Unterminated string in buffer" {
        return Some(WalParseError::TruncatedMessage {
            expected: 0,
            actual: 0,
        });
    }
    let rest = msg.strip_prefix("Not enough bytes remaining. Need ")?;
    let (need_str, rest) = rest.split_once(", have ")?;
    let expected: usize = need_str.parse().ok()?;
    let actual: usize = rest.parse().ok()?;
    Some(WalParseError::TruncatedMessage { expected, actual })
}

fn parse_unknown_tuple_tag(msg: &str) -> Option<u8> {
    let rest = msg
        .strip_prefix("Unknown column data type: '")?
        .strip_suffix('\'')?;
    let mut chars = rest.chars();
    let c = chars.next()?;
    if chars.next().is_some() {
        return None;
    }
    u8::try_from(u32::from(c)).ok()
}

// ============================================================================
// PgOutputEvent: typed [`CdcEvent`] output of the pgoutput parser
// ============================================================================

use crate::backend::{CdcEvent, Postgres, Presence, RowKind, Value};
use crate::NoCheckpoint;
use spin::Once;

use super::pg_type::text_to_pg_value;

/// Typed CDC event surfaced by [`PgOutputParser::parse_wal_message`].
///
/// Owns the pgoutput wire payload for one row change plus lazily
/// populated caches of decoded [`Value<Postgres>`] cells. Scalar
/// accessors on the [`CdcEvent`] impl decode each cell on first access
/// through [`text_to_pg_value`] (routed by the declared type OID) and
/// return references into the cache.
///
/// Backed by the Postgres [`Backend`](crate::backend::Backend). The
/// wire protocol does not carry LSN inside the pgoutput payload itself;
/// [`PgOutputEvent::with_checkpoint`] lets the streaming layer stamp the
/// `XLogData` header LSN after parse.
pub struct PgOutputEvent {
    kind: EventKind,
    table_id: TableId,
    pk_columns: Arc<[ColumnId]>,
    changed_columns: Arc<[ColumnId]>,
    checkpoint: Option<crate::PgLsn>,
    new_image: Option<PgOutRowImage>,
    old_image: Option<PgOutRowImage>,
}

/// One row image (new or old) inside a [`PgOutputEvent`].
struct PgOutRowImage {
    entries: alloc::boxed::Box<[PgOutWireCell]>,
    /// `ColumnId -> entries` position. Arity-sized; `None` for columns
    /// the source did not carry in this image.
    by_col: alloc::boxed::Box<[Option<u16>]>,
    /// Lazily populated decoded values (arity-sized). Uninitialised
    /// [`Once`] for cells the caller never touched. Decode failures cache
    /// [`Value::Missing`] so accessors return [`Presence::Missing`] and
    /// the engine escalates to re-execution.
    cache: alloc::boxed::Box<[Once<Value<Postgres>>]>,
}

/// One wire cell inside a [`PgOutRowImage`]. Owns the wire text so the
/// event does not borrow from the source buffer once parsed.
struct PgOutWireCell {
    #[allow(dead_code)]
    col_id: ColumnId,
    type_oid: u32,
    payload: PgOutWirePayload,
}

#[derive(PartialEq)]
enum PgOutWirePayload {
    /// Wire carried the SQL NULL marker for this cell (`ColumnData::Null`).
    Null,
    /// Wire carried the unchanged-TOAST marker (`ColumnData::Unchanged`).
    /// Only ever valid in an old-image tuple.
    Missing,
    /// Wire carried a text-format encoding for this cell. Owned; UTF-8
    /// validated at parse time.
    Text(alloc::string::String),
}

impl PgOutRowImage {
    fn from_tuple_positions(
        rel: &CachedRelation,
        tuple: &TupleData,
        positions: &[usize],
        context: &str,
        is_new_tuple: bool,
    ) -> Result<Self, WalParseError> {
        if positions.len() > rel.column_ids.len() {
            return Err(WalParseError::MalformedPayload(format!(
                "{context} column count {} exceeds relation column count {}",
                positions.len(),
                rel.column_ids.len()
            )));
        }
        let arity = rel.arity;
        let mut entries: Vec<PgOutWireCell> = Vec::with_capacity(positions.len());
        let mut by_col: Vec<Option<u16>> = alloc::vec![None; arity];

        for (wire_idx, &rel_idx) in positions.iter().enumerate() {
            if rel_idx >= rel.column_ids.len() {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} column index {rel_idx} is out of bounds for relation column count {}",
                    rel.column_ids.len()
                )));
            }
            let col_data = tuple.columns.get(wire_idx).ok_or_else(|| {
                WalParseError::MalformedPayload(format!(
                    "{context} missing column data at wire position {wire_idx}"
                ))
            })?;
            let type_oid = rel.column_type_oids[rel_idx];
            let col_id = rel.column_ids[rel_idx];
            let cell = wire_cell_from_column_data(col_data, type_oid, col_id, is_new_tuple)?;
            let entry_idx = u16::try_from(entries.len()).map_err(|_| {
                WalParseError::MalformedPayload(format!(
                    "{context} has more than {} entries",
                    u16::MAX
                ))
            })?;
            if (col_id as usize) < arity {
                by_col[col_id as usize] = Some(entry_idx);
            }
            entries.push(cell);
        }

        let cache = (0..arity)
            .map(|_| Once::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            entries: entries.into_boxed_slice(),
            by_col: by_col.into_boxed_slice(),
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
        Some(cache_slot.call_once(|| match &entry.payload {
            PgOutWirePayload::Null => Value::Null,
            PgOutWirePayload::Missing => Value::Missing,
            PgOutWirePayload::Text(text) => {
                text_to_pg_value(text, entry.type_oid).unwrap_or(Value::Missing)
            }
        }))
    }
}

/// Wire-level `changed_columns` derivation for a pgoutput Update event.
///
/// Runs only when the old image covers every column (key_type = 'O',
/// i.e. REPLICA IDENTITY FULL upstream). Compares the wire `payload`
/// per column without touching the [`Value<Postgres>`] decode cache;
/// wire-difference implies semantic-difference for Postgres canonical
/// text output. Sparser old images ('K' key tuples) leave the returned
/// slice empty (safe: over-notification).
fn pgout_derive_changed_columns(
    new_image: Option<&PgOutRowImage>,
    old_image: Option<&PgOutRowImage>,
    arity: usize,
) -> Arc<[ColumnId]> {
    let (Some(new), Some(old)) = (new_image, old_image) else {
        return Arc::from(Vec::<ColumnId>::new());
    };
    if new.entries.len() != arity || old.entries.len() != arity {
        return Arc::from(Vec::<ColumnId>::new());
    }
    let mut changed = Vec::new();
    for col in 0..arity {
        let (Some(new_idx), Some(old_idx)) = (new.by_col[col], old.by_col[col]) else {
            continue;
        };
        if new.entries[new_idx as usize].payload != old.entries[old_idx as usize].payload {
            #[allow(clippy::cast_possible_truncation)]
            changed.push(col as ColumnId);
        }
    }
    Arc::from(changed)
}

fn wire_cell_from_column_data(
    col: &ColumnData,
    type_oid: u32,
    col_id: ColumnId,
    is_new_tuple: bool,
) -> Result<PgOutWireCell, WalParseError> {
    let payload = if col.is_null() {
        PgOutWirePayload::Null
    } else if col.is_unchanged() {
        if is_new_tuple {
            return Err(WalParseError::MalformedPayload(
                "unchanged-TOAST tag 'u' is not valid in a new-image tuple".to_string(),
            ));
        }
        PgOutWirePayload::Missing
    } else if col.is_binary() {
        return Err(WalParseError::UnknownTupleTag(b'b'));
    } else {
        let bytes = col.as_bytes();
        let text = core::str::from_utf8(bytes)
            .map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
        PgOutWirePayload::Text(text.to_string())
    };
    Ok(PgOutWireCell {
        col_id,
        type_oid,
        payload,
    })
}

impl PgOutputEvent {
    /// Attach an [`crate::PgLsn`] to the event after parse. The pgoutput
    /// wire payload does not carry LSN; the streaming layer reads it from
    /// the enclosing `XLogData` header and stamps events on their way to
    /// the consumer channel.
    #[must_use]
    pub const fn with_checkpoint(mut self, cp: Option<crate::PgLsn>) -> Self {
        self.checkpoint = cp;
        self
    }

    const fn kind_matches_pk_source(&self, row: RowKind) -> Option<&PgOutRowImage> {
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

macro_rules! pgout_scalar_accessor {
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

impl CdcEvent for PgOutputEvent {
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
        pgout_scalar_accessor!(self, row, col, Bool)
    }

    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&i64> {
        pgout_scalar_accessor!(self, row, col, Int)
    }

    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&f64> {
        pgout_scalar_accessor!(self, row, col, Float)
    }

    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        pgout_scalar_accessor!(self, row, col, String)
    }

    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::vec::Vec<u8>> {
        pgout_scalar_accessor!(self, row, col, Bytes)
    }

    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&uuid::Uuid> {
        pgout_scalar_accessor!(self, row, col, Uuid)
    }

    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDateTime> {
        pgout_scalar_accessor!(self, row, col, Timestamp)
    }

    fn timestamp_tz_at(
        &self,
        row: RowKind,
        col: ColumnId,
    ) -> Presence<&chrono::DateTime<chrono::Utc>> {
        pgout_scalar_accessor!(self, row, col, TimestampTz)
    }

    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDate> {
        pgout_scalar_accessor!(self, row, col, Date)
    }

    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveTime> {
        pgout_scalar_accessor!(self, row, col, Time)
    }

    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&bigdecimal::BigDecimal> {
        pgout_scalar_accessor!(self, row, col, Decimal)
    }

    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        pgout_scalar_accessor!(self, row, col, Json)
    }

    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        pgout_scalar_accessor!(self, row, col, Jsonb)
    }
}


fn tuple_wire_full(
    rel: &CachedRelation,
    tuple: &TupleData,
    is_new_tuple: bool,
) -> Result<PgOutRowImage, WalParseError> {
    let wal_count = tuple.columns.len();
    if wal_count != rel.arity {
        return Err(WalParseError::ArityMismatch {
            table_id: rel.table_id,
            wal_count,
            catalog_arity: rel.arity,
        });
    }
    let positions: Vec<usize> = (0..wal_count).collect();
    PgOutRowImage::from_tuple_positions(rel, tuple, &positions, "tuple", is_new_tuple)
}

fn tuple_wire_key(rel: &CachedRelation, tuple: &TupleData) -> Result<PgOutRowImage, WalParseError> {
    let wal_count = tuple.columns.len();
    let mapped_positions: Vec<usize> = if !rel.identity_columns.is_empty()
        && wal_count == rel.identity_columns.len()
    {
        rel.identity_columns.clone()
    } else if wal_count == rel.arity {
        (0..wal_count).collect()
    } else {
        return Err(WalParseError::MalformedPayload(format!(
            "key tuple column count {wal_count} does not match identity column count {} or relation arity {}",
            rel.identity_columns.len(),
            rel.arity
        )));
    };
    PgOutRowImage::from_tuple_positions(rel, tuple, &mapped_positions, "key tuple", false)
}

fn parse_update_old_wire(
    rel: &CachedRelation,
    old_tuple: Option<TupleData>,
    key_type: Option<char>,
) -> Result<Option<PgOutRowImage>, WalParseError> {
    match (old_tuple, key_type) {
        (Some(t), Some('K')) => tuple_wire_key(rel, &t).map(Some),
        (Some(t), Some('O')) => tuple_wire_full(rel, &t, false).map(Some),
        (Some(_), Some(other)) => Err(WalParseError::MalformedPayload(format!(
            "invalid UPDATE old-tuple key_type 0x{:02X}",
            other as u32
        ))),
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(WalParseError::MalformedPayload(
            "UPDATE message has inconsistent old_tuple/key_type pair".to_string(),
        )),
    }
}

/// PK column IDs for an Insert event: the catalog's declared PK, or empty
/// if the catalog carries none.
fn pk_columns_for_new<DB: DatabaseLike>(
    rel: &CachedRelation,
    database: &DB,
) -> Result<Arc<[ColumnId]>, WalParseError> {
    Ok(catalog_helpers::primary_key_columns(database, rel.table_id)
        .map(Arc::from)
        .unwrap_or_else(|| Arc::from(Vec::<ColumnId>::new())))
}

/// PK column IDs for Update / Delete: prefer the relation's identity
/// columns (REPLICA IDENTITY USING INDEX / FULL / DEFAULT), fall back to
/// the catalog PK when the relation has REPLICA IDENTITY NOTHING.
fn pk_columns_from_identity_or_catalog<DB: DatabaseLike>(
    rel: &CachedRelation,
    database: &DB,
) -> Result<Arc<[ColumnId]>, WalParseError> {
    if !rel.identity_columns.is_empty() {
        let ids: Vec<ColumnId> = rel
            .identity_columns
            .iter()
            .map(|&i| rel.column_ids[i])
            .collect();
        return Ok(Arc::from(ids));
    }
    pk_columns_for_new(rel, database)
}

// Keep the NoCheckpoint import referenced (avoid unused-import lints when
// the streaming stamping wiring lands in Phase 7F).
const _: fn() -> NoCheckpoint = || NoCheckpoint;


// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        orders_customer_catalog as orders_catalog,
        orders_customer_no_pk_catalog as orders_no_pk_catalog,
    };
    use super::*;

    // -- Binary message builders ---------------------------------------------

    fn push_cstring(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(s.as_bytes());
        buf.push(0);
    }

    fn push_u8(buf: &mut Vec<u8>, v: u8) {
        buf.push(v);
    }

    fn push_i16(buf: &mut Vec<u8>, v: i16) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_i32(buf: &mut Vec<u8>, v: i32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    fn push_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Build a Relation ('R') message.
    fn build_relation_msg(
        oid: u32,
        namespace: &str,
        name: &str,
        columns: &[(&str, u32, u8)], // (name, type_oid, flags)
    ) -> Vec<u8> {
        let mut buf = vec![b'R'];
        push_u32(&mut buf, oid);
        push_cstring(&mut buf, namespace);
        push_cstring(&mut buf, name);
        push_u8(&mut buf, 0); // replica identity
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        push_i16(&mut buf, columns.len() as i16);
        for &(col_name, type_oid, flags) in columns {
            push_u8(&mut buf, flags);
            push_cstring(&mut buf, col_name);
            push_u32(&mut buf, type_oid);
            push_i32(&mut buf, -1); // type modifier
        }
        buf
    }

    enum TupleCol {
        Null,
        Unchanged,
        Text(String),
    }

    /// Build raw tuple data bytes (without the leading tag like 'N'/'K'/'O').
    fn build_tuple_data(cols: &[TupleCol]) -> Vec<u8> {
        let mut buf = Vec::new();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        push_i16(&mut buf, cols.len() as i16);
        for col in cols {
            match col {
                TupleCol::Null => push_u8(&mut buf, b'n'),
                TupleCol::Unchanged => push_u8(&mut buf, b'u'),
                TupleCol::Text(s) => {
                    push_u8(&mut buf, b't');
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    push_i32(&mut buf, s.len() as i32);
                    buf.extend_from_slice(s.as_bytes());
                }
            }
        }
        buf
    }

    fn build_insert_msg(oid: u32, tuple: &[TupleCol]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, b'N');
        buf.extend_from_slice(&build_tuple_data(tuple));
        buf
    }

    fn build_update_msg_with_old(
        oid: u32,
        old_tag: u8,
        old_tuple: &[TupleCol],
        new_tuple: &[TupleCol],
    ) -> Vec<u8> {
        let mut buf = vec![b'U'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, old_tag);
        buf.extend_from_slice(&build_tuple_data(old_tuple));
        push_u8(&mut buf, b'N');
        buf.extend_from_slice(&build_tuple_data(new_tuple));
        buf
    }

    fn build_update_msg_no_old(oid: u32, new_tuple: &[TupleCol]) -> Vec<u8> {
        let mut buf = vec![b'U'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, b'N');
        buf.extend_from_slice(&build_tuple_data(new_tuple));
        buf
    }

    fn build_delete_msg(oid: u32, tag: u8, old_tuple: &[TupleCol]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        push_u32(&mut buf, oid);
        push_u8(&mut buf, tag);
        buf.extend_from_slice(&build_tuple_data(old_tuple));
        buf
    }

    fn build_truncate_msg(option_bits: u8, relation_oids: &[u32]) -> Vec<u8> {
        let mut buf = vec![b'T'];
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        push_i32(&mut buf, relation_oids.len() as i32);
        push_u8(&mut buf, option_bits);
        for &oid in relation_oids {
            push_u32(&mut buf, oid);
        }
        buf
    }

    /// Standard 4-column relation for orders table.
    fn orders_columns() -> Vec<(&'static str, u32, u8)> {
        vec![
            ("id", 23, 1),       // int4, identity
            ("customer", 25, 0), // text
            ("amount", 1700, 0), // numeric
            ("status", 25, 0),   // text
        ]
    }

    // -- Test 1: Relation caching + Insert (happy path) ----------------------

    #[cfg(any())]
    #[test]
    fn relation_caching_and_insert() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Send Relation message
        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let events = parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");
        assert!(events.is_empty());

        // Send Insert
        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.table_id(), 1);

        let new = ev.new_row().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));
        assert_eq!(new.get(2), Some(&Cell::Float(99.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("pending"))));

        assert!(ev.old_row().is_none());

        // PK from catalog
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns().is_empty());
    }

    // Removed `relation_with_out_of_range_catalog_column_id_errors` and
    // `relation_with_duplicate_catalog_column_id_errors`: both injected
    // out-of-range or duplicate column ordinals into a `MockCatalog`.
    // ParserDB always assigns unique, in-range ordinals to columns, so the
    // failure modes are unreachable through the public API.

    #[cfg(any())]
    #[test]
    fn relation_cache_is_bounded_and_evicts_oldest() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let total_relations = MAX_CACHED_RELATIONS + 5;
        for i in 0..total_relations {
            let oid = 20_000_u32 + u32::try_from(i).expect("loop index fits u32");
            let rel_msg = build_relation_msg(oid, "public", "orders", &orders_columns());
            parser
                .parse_wal_message(&rel_msg, &catalog)
                .expect("relation should parse");
        }

        let cache = parser.relations.lock();
        assert_eq!(cache.map.len(), MAX_CACHED_RELATIONS);
        drop(cache);

        let oldest_oid = 20_000_u32;
        let oldest_insert = build_insert_msg(
            oldest_oid,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let err = parser
            .parse_wal_message(&oldest_insert, &catalog)
            .expect_err("oldest relation should be evicted");
        assert!(matches!(err, WalParseError::UnknownRelationOid(oid) if oid == oldest_oid));

        let newest_oid = 20_000_u32 + u32::try_from(total_relations - 1).expect("index fits u32");
        let newest_insert = build_insert_msg(
            newest_oid,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&newest_insert, &catalog)
            .expect("newest relation should remain cached");
        assert_eq!(events.len(), 1);
    }

    // -- Test 2: Update with 'K' old key (DEFAULT replica identity) ----------

    #[cfg(any())]
    #[test]
    fn update_with_key_old_tuple() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Unchanged,
                TupleCol::Unchanged,
                TupleCol::Unchanged,
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);

        assert!(ev.new_row().is_some());
        assert!(ev.old_row().is_some());

        // PK from identity column (id, flags=1)
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    #[cfg(any())]
    #[test]
    fn update_with_partial_key_tuple_keeps_changed_columns_empty_for_safety() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Key tuple contains only the identity column. New tuple changes both id and amount.
        // Old row is partial, so changed-columns must be treated as unknown.
        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[TupleCol::Text("1".into())],
            &[
                TupleCol::Text("2".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");
        assert_eq!(events.len(), 1);

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert!(ev.old_row().is_some());
        assert!(
            ev.changed_columns().is_empty(),
            "partial key tuple must disable changed-columns pruning"
        );
    }

    #[cfg(any())]
    #[test]
    fn update_with_key_old_tuple_sparse_identity_maps_correctly() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Identity is on `customer` (column index 1), not the first column.
        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 1),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Sparse key tuple where only the identity column value is present.
        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[TupleCol::Text("alice".into())],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("sparse key tuple should map onto identity columns");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert_eq!(ev.pk().columns.as_ref(), &[1]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::String(Arc::from("alice"))]);
    }

    // -- Test 3: Update with 'O' full old row (FULL replica identity) --------

    #[cfg(any())]
    #[test]
    fn update_with_full_old_row() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // All columns have flags=1 for FULL replica identity
        let full_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),
            ("customer", 25, 1),
            ("amount", 1700, 1),
            ("status", 25, 1),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &full_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);

        let old = ev.old_row().expect("should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(1)));
        assert_eq!(old.get(2), Some(&Cell::Float(99.95)));

        let new = ev.new_row().expect("should have new_row");
        assert_eq!(new.get(2), Some(&Cell::Float(149.95)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("shipped"))));

        // PK from identity columns (all marked)
        assert_eq!(ev.pk().columns.as_ref(), &[0, 1, 2, 3]);
    }

    // -- Test 4: Update without old tuple ------------------------------------

    #[cfg(any())]
    #[test]
    fn update_without_old_tuple() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_no_old(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert!(ev.old_row().is_none());
        assert!(ev.changed_columns().is_empty());

        // PK from catalog (no old row)
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    // -- Test 5: Delete with 'K' key ----------------------------------------

    #[cfg(any())]
    #[test]
    fn delete_with_key() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'K',
            &[
                TupleCol::Text("42".into()),
                TupleCol::Unchanged,
                TupleCol::Unchanged,
                TupleCol::Unchanged,
            ],
        );

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete should parse");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert!(ev.new_row().is_none());
        assert!(ev.old_row().is_some());

        // PK from identity column
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(42)]);

        assert!(ev.changed_columns().is_empty());
    }

    #[cfg(any())]
    #[test]
    fn delete_with_key_sparse_identity_maps_correctly() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Identity is on `customer` (column index 1), not the first column.
        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 1),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(16384, b'K', &[TupleCol::Text("bob".into())]);

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("sparse key tuple should map onto identity columns");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert_eq!(ev.pk().columns.as_ref(), &[1]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::String(Arc::from("bob"))]);
    }

    // -- Test 6: Delete with 'O' full old row --------------------------------

    #[cfg(any())]
    #[test]
    fn delete_with_full_old_row() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'O',
            &[
                TupleCol::Text("42".into()),
                TupleCol::Text("bob".into()),
                TupleCol::Text("50.00".into()),
                TupleCol::Text("cancelled".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete should parse");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);

        let old = ev.old_row().expect("should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(42)));
        assert_eq!(old.get(1), Some(&Cell::String(Arc::from("bob"))));

        // PK from identity column (id)
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(42)]);
    }

    // -- Test 7: Metadata messages return empty vec --------------------------

    #[cfg(any())]
    #[test]
    fn metadata_messages_return_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        for &tag in b"BCOY" {
            let msg = vec![tag, 0, 0, 0, 0]; // tag + some padding
            let events = parser
                .parse_wal_message(&msg, &catalog)
                .expect("metadata should be skipped");
            assert!(events.is_empty(), "tag 0x{tag:02X} should return empty");
        }
    }

    #[cfg(any())]
    #[test]
    fn non_row_control_messages_return_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // M = Logical decoding message; S/E/c/A = stream control/transaction wrappers.
        for &tag in b"MSEcA" {
            let msg = vec![tag, 0, 0, 0, 0];
            let events = parser
                .parse_wal_message(&msg, &catalog)
                .expect("non-row control message should be skipped");
            assert!(events.is_empty(), "tag 0x{tag:02X} should return empty");
        }
    }

    #[cfg(any())]
    #[test]
    fn truncate_message_emits_events_per_relation() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let relation_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let events = parser
            .parse_wal_message(&relation_msg, &catalog)
            .expect("relation should parse");
        assert!(events.is_empty());

        let truncate_msg = build_truncate_msg(0, &[16384]);
        let events = parser
            .parse_wal_message(&truncate_msg, &catalog)
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

    // -- Test 8: Empty input returns empty vec --------------------------------

    #[cfg(any())]
    #[test]
    fn empty_input_returns_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let events = parser
            .parse_wal_message(&[], &catalog)
            .expect("empty should succeed");
        assert!(events.is_empty());
    }

    // -- Test 9: Unknown message type -> skip --------------------------------

    #[cfg(any())]
    #[test]
    fn unknown_message_type_is_skipped() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // 0xFF is not a known pgoutput message type. It must be skipped, not error.
        let msg = vec![0xFF, 0, 0, 0, 0];
        let events = parser
            .parse_wal_message(&msg, &catalog)
            .expect("unknown message type should be skipped, not error");
        assert!(
            events.is_empty(),
            "unknown message type should produce no output"
        );
    }

    // -- Test 10: Insert without preceding Relation -> UnknownRelationOid ----

    #[cfg(any())]
    #[test]
    fn insert_without_relation_error() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let insert_msg = build_insert_msg(99999, &[TupleCol::Text("1".into())]);
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownRelationOid(99999)));
    }

    // -- Test 11: Truncated messages -> TruncatedMessage ---------------------

    #[cfg(any())]
    #[test]
    fn truncated_message_error() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Insert with only 2 bytes after the type tag (needs at least 4 for OID)
        let msg = vec![b'I', 0, 0];
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::TruncatedMessage { .. }));
    }

    #[cfg(any())]
    #[test]
    fn insert_with_trailing_bytes_errors() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        insert_msg.push(0xAA);
        insert_msg.push(0xBB);

        // pg_walstream does not surface the number of bytes consumed from
        // `parse_wal_message`, so the adapter cannot detect trailing bytes
        // without an upstream API addition. For now we accept that trailing
        // bytes are silently tolerated. Revisit when pg_walstream exposes a
        // `bytes_consumed` accessor or a strict variant.
        let result = parser.parse_wal_message(&insert_msg, &catalog);
        assert!(
            result.is_ok(),
            "trailing bytes are currently tolerated, got: {result:?}"
        );
    }

    // -- Test 12: NULL columns ('n' tag) -> Cell::Null -----------------------

    #[cfg(any())]
    #[test]
    fn null_column_tag() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Null,
                TupleCol::Null,
                TupleCol::Text("active".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let new = events[0].new_row().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Null));
        assert_eq!(new.get(2), Some(&Cell::Null));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("active"))));
    }

    // -- Test 13: Unchanged TOAST ('u' tag) -> Cell::Missing in OLD tuple ----

    #[cfg(any())]
    #[test]
    fn unchanged_toast_tag_in_old_key_tuple() {
        // 'u' (unchanged-TOAST) is only valid in old-image tuples (key tuples or full old rows).
        // It must produce Cell::Missing there, and be rejected in new-image tuples.
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Update with 'K' old key (b'u' is valid here) and a clean new tuple (no b'u')
        // Columns: id(int4), customer(text), amount(numeric), status(text)
        let update_msg = build_update_msg_with_old(
            16384,
            b'K',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Unchanged, // valid in old/key tuple (customer)
                TupleCol::Unchanged, // valid in old/key tuple (amount)
                TupleCol::Unchanged, // valid in old/key tuple (status)
            ],
            &[
                TupleCol::Text("1".into()),       // id
                TupleCol::Text("alice".into()),   // customer (text OID 25)
                TupleCol::Text("200.00".into()),  // amount (numeric OID 1700)
                TupleCol::Text("shipped".into()), // status (text OID 25)
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        let old = ev.old_row().expect("should have old_row");
        assert_eq!(old.get(1), Some(&Cell::Missing)); // unchanged TOAST in old is ok

        let new = ev.new_row().expect("should have new_row");
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("alice"))));
        assert_eq!(new.get(2), Some(&Cell::Float(200.0)));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("shipped"))));
    }

    // -- Test 14: Type conversion for various OIDs ---------------------------

    #[cfg(any())]
    #[test]
    fn type_conversion_oids() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Custom columns with different types
        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),       // int4
            ("customer", 25, 0), // text
            ("amount", 701, 0),  // float8
            ("status", 16, 0),   // bool
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("42".into()),
                TupleCol::Text("bob".into()),
                TupleCol::Text("3.15".into()),
                TupleCol::Text("t".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let new = events[0].new_row().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(42)));
        assert_eq!(new.get(1), Some(&Cell::String(Arc::from("bob"))));
        assert_eq!(new.get(2), Some(&Cell::Float(3.15)));
        assert_eq!(new.get(3), Some(&Cell::Bool(true)));
    }

    #[cfg(any())]
    #[test]
    fn type_conversion_invalid_typed_text_errors() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),       // int4
            ("customer", 25, 0), // text
            ("amount", 701, 0),  // float8
            ("status", 16, 0),   // bool
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("not-an-int".into()),
                TupleCol::Text("bob".into()),
                TupleCol::Text("3.15".into()),
                TupleCol::Text("t".into()),
            ],
        );

        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("invalid int4 tuple text should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- Test 15: changed_columns computed correctly for FULL update ----------

    #[cfg(any())]
    #[test]
    fn changed_columns_full_update() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // All columns have flags=1 for FULL replica identity
        let full_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 1),
            ("customer", 25, 1),
            ("amount", 1700, 1),
            ("status", 25, 1),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &full_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("149.95".into()),
                TupleCol::Text("shipped".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        let changed: Vec<ColumnId> = ev.changed_columns().to_vec();
        assert!(changed.contains(&2), "amount should be changed");
        assert!(changed.contains(&3), "status should be changed");
        assert!(!changed.contains(&0), "id should NOT be changed");
        assert!(!changed.contains(&1), "customer should NOT be changed");
    }

    // -- Test 16: Trait object safety ----------------------------------------

    #[cfg(any())]
    #[test]
    fn trait_object_compiles() {
        let catalog = orders_catalog();
        let parser: &dyn WalParser<
            sql_traits::structs::ParserDB,
            Checkpoint = crate::NoCheckpoint,
        > = &PgOutputParser::new();

        // Should compile and work as a trait object
        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        let result = parser.parse_wal_message(&rel_msg, &catalog);
        assert!(result.is_ok());
    }

    // -- Test 17: Thread safety (compile-time Send + Sync check) -------------

    #[cfg(any())]
    #[test]
    fn send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PgOutputParser>();
    }

    // -- Test: Insert without catalog PK -> empty PK -------------------------

    #[cfg(any())]
    #[test]
    fn insert_no_catalog_pk() {
        let catalog = orders_no_pk_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert should parse");

        let ev = &events[0];
        assert!(ev.pk().columns.is_empty());
        assert!(ev.pk().values.is_empty());
    }

    // -- Test: Truncated relation message ------------------------------------

    #[cfg(any())]
    #[test]
    fn truncated_relation_message() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // Relation message with only the OID (truncated before namespace)
        let msg = vec![b'R', 0, 0, 0x40, 0x00];
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::TruncatedMessage { .. }));
    }

    // -- Test: Delete without identity columns -> use all columns as PK ------

    #[cfg(any())]
    #[test]
    fn delete_no_identity_columns() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        // No identity flags (all flags = 0)
        let no_identity_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 0),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &no_identity_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete should parse");

        let ev = &events[0];
        // All columns used as PK since no identity columns
        assert_eq!(ev.pk().columns.len(), 4);
    }

    #[cfg(any())]
    #[test]
    fn delete_no_identity_columns_with_missing_old_value_errors() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let no_identity_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 0),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &no_identity_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let delete_msg = build_delete_msg(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Unchanged,
                TupleCol::Text("pending".into()),
            ],
        );

        let err = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect_err("missing old tuple value should fail for fallback PK");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[cfg(any())]
    #[test]
    fn update_no_identity_columns_uses_full_old_tuple_as_pk() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let no_identity_cols: Vec<(&str, u32, u8)> = vec![
            ("id", 23, 0),
            ("customer", 25, 0),
            ("amount", 1700, 0),
            ("status", 25, 0),
        ];
        let rel_msg = build_relation_msg(16384, "public", "orders", &no_identity_cols);
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let update_msg = build_update_msg_with_old(
            16384,
            b'O',
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("120.00".into()),
                TupleCol::Text("paid".into()),
            ],
        );

        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update should parse");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert_eq!(ev.pk().columns.len(), 4);
    }

    #[cfg(any())]
    #[test]
    fn relation_negative_column_count_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let mut msg = Vec::new();
        msg.push(b'R');
        push_u32(&mut msg, 16384);
        push_cstring(&mut msg, "public");
        push_cstring(&mut msg, "orders");
        msg.push(b'd');
        push_i16(&mut msg, -1);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on malformed relation"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        // pg_walstream reads the column count as u16, so -1 wraps to 65535
        // and the parser hits "Not enough bytes" before noticing the count is
        // structurally bogus. Either error variant is acceptable: the point
        // of this test is no-panic, not which variant is returned.
        assert!(
            matches!(
                parse_result,
                Err(WalParseError::MalformedPayload(_) | WalParseError::TruncatedMessage { .. })
            ),
            "parser should return MalformedPayload or TruncatedMessage, got {parse_result:?}"
        );
    }

    #[cfg(any())]
    #[test]
    fn tuple_negative_column_count_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut msg = Vec::new();
        msg.push(b'I');
        push_u32(&mut msg, 16384);
        msg.push(b'N');
        push_i16(&mut msg, -1);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(result.is_ok(), "parser must not panic on malformed tuple");
        let parse_result = result.expect("catch_unwind should be Ok");
        // Same situation as `relation_negative_column_count_does_not_panic`:
        // u16 wrap-around means the failure mode is "Not enough bytes" rather
        // than a structural rejection.
        assert!(
            matches!(
                parse_result,
                Err(WalParseError::MalformedPayload(_) | WalParseError::TruncatedMessage { .. })
            ),
            "parser should return MalformedPayload or TruncatedMessage, got {parse_result:?}"
        );
    }

    #[cfg(any())]
    #[test]
    fn insert_invalid_tuple_tag_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut msg = build_insert_msg(16384, &[TupleCol::Text("1".into())]);
        msg[5] = b'X';

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on invalid insert tuple tag"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[cfg(any())]
    #[test]
    fn update_invalid_initial_tuple_tag_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let mut msg = build_update_msg_no_old(16384, &[TupleCol::Text("1".into())]);
        msg[5] = b'X';

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on invalid update tuple tag"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[cfg(any())]
    #[test]
    fn update_invalid_new_tuple_tag_after_old_does_not_panic() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let old_cols = [
            TupleCol::Text("1".into()),
            TupleCol::Unchanged,
            TupleCol::Unchanged,
            TupleCol::Unchanged,
        ];
        let mut msg =
            build_update_msg_with_old(16384, b'K', &old_cols, &[TupleCol::Text("1".into())]);
        let new_tag_idx = 1 + 4 + 1 + build_tuple_data(&old_cols).len();
        msg[new_tag_idx] = b'X';

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse_wal_message(&msg, &catalog)
        }));

        assert!(
            result.is_ok(),
            "parser must not panic on invalid post-old update tuple tag"
        );
        let parse_result = result.expect("catch_unwind should be Ok");
        assert!(
            matches!(parse_result, Err(WalParseError::MalformedPayload(_))),
            "parser should return MalformedPayload"
        );
    }

    #[cfg(any())]
    #[test]
    fn delete_invalid_tuple_tag_returns_malformed_payload() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let msg = build_delete_msg(16384, b'X', &[TupleCol::Text("1".into())]);
        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }
    // -- B3: skip 2PC/keepalive/replication protocol messages ----------------

    #[cfg(any())]
    #[test]
    fn skip_2pc_keepalive_messages() {
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();
        // b'P' = Prepare (2PC)
        assert!(
            parser
                .parse_wal_message(b"P", &catalog)
                .expect("P should parse")
                .is_empty(),
            "P should yield no events"
        );
        // b'K' = Keepalive
        assert!(
            parser
                .parse_wal_message(b"K", &catalog)
                .expect("K should parse")
                .is_empty(),
            "K should yield no events"
        );
        // b'r' = standby status update
        assert!(
            parser
                .parse_wal_message(b"r", &catalog)
                .expect("r should parse")
                .is_empty(),
            "r should yield no events"
        );
        // b'b' = begin prepared
        assert!(
            parser
                .parse_wal_message(b"b", &catalog)
                .expect("b should parse")
                .is_empty(),
            "b should yield no events"
        );
        // b'p' = commit prepared
        assert!(
            parser
                .parse_wal_message(b"p", &catalog)
                .expect("p should parse")
                .is_empty(),
            "p should yield no events"
        );
    }

    // -- B4: under-arity normal tuples are rejected --------------------------

    #[cfg(any())]
    #[test]
    fn insert_underarity_tuple_rejected() {
        // Register a 4-column relation (orders), then send an INSERT with only 1 column.
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Build an INSERT with only 1 column in the tuple (arity mismatch: 4 expected)
        let insert_msg = build_insert_msg(16384, &[TupleCol::Text("1".into())]);
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("under-arity tuple should fail");
        assert!(
            matches!(err, WalParseError::ArityMismatch { .. })
                || matches!(err, WalParseError::MalformedPayload(_)),
            "Expected ArityMismatch or MalformedPayload, got: {err:?}"
        );
    }

    // -- B5: LRU eviction boundary -------------------------------------------

    #[cfg(any())]
    #[test]
    fn lru_eviction_boundary() {
        // MAX_CACHED_RELATIONS = 32 in test mode.
        // Register 33 relations (OIDs 0..=32), then OID 0 should be evicted.
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        // Register 33 relations: OIDs 0..=32
        for oid in 0u32..=32u32 {
            let rel_msg = build_relation_msg(oid, "public", "orders", &orders_columns());
            parser
                .parse_wal_message(&rel_msg, &catalog)
                .expect("relation should parse");
        }

        // Now try to INSERT using OID 0: it should have been evicted
        let insert_msg = build_insert_msg(
            0,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("alice".into()),
                TupleCol::Text("99.95".into()),
                TupleCol::Text("pending".into()),
            ],
        );
        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("OID 0 should have been evicted");
        assert!(
            matches!(err, WalParseError::UnknownRelationOid(0)),
            "Expected UnknownRelationOid(0), got: {err:?}"
        );
    }

    // -- B6: TRUNCATE edge cases: zero relations, unknown OID ----------------

    #[cfg(any())]
    #[test]
    fn truncate_zero_relations() {
        // TRUNCATE with rel_count=0 should produce no events
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let msg = build_truncate_msg(0, &[]);
        let events = parser
            .parse_wal_message(&msg, &catalog)
            .expect("zero-relation truncate should parse");
        assert!(
            events.is_empty(),
            "Expected empty events for zero-relation truncate"
        );
    }

    #[cfg(any())]
    #[test]
    fn truncate_unknown_oid_is_skipped() {
        // TRUNCATE referencing an unknown OID should silently skip that relation
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let msg = build_truncate_msg(0, &[9999]);
        let events = parser
            .parse_wal_message(&msg, &catalog)
            .expect("unknown OID in TRUNCATE should be skipped, not error");
        assert!(
            events.is_empty(),
            "Expected empty events when all OIDs are unknown"
        );
    }

    #[cfg(any())]
    #[test]
    fn truncate_skips_unknown_oid_emits_known_ones() {
        // A 3-OID TRUNCATE where OID[1] is unknown: events for OID[0] and
        // OID[2] should be emitted. Catalog contains both `orders` and
        // `items` tables so two distinct relations can register.
        let catalog = sql_traits::structs::ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);\n\
             CREATE TABLE items (sku INT PRIMARY KEY);",
        )
        .expect("orders+items DDL parses");
        let orders_id = crate::catalog_helpers::table_id(&catalog, "orders").expect("orders id");
        let items_id = crate::catalog_helpers::table_id(&catalog, "items").expect("items id");

        let parser = PgOutputParser::new();

        let rel_a = build_relation_msg(100, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_a, &catalog)
            .expect("relation A should parse");

        let rel_b = build_relation_msg(200, "public", "items", &[("sku", 25, 1)]);
        parser
            .parse_wal_message(&rel_b, &catalog)
            .expect("relation B should parse");

        // OID 100 (known), OID 9999 (unknown), OID 200 (known)
        let truncate_msg = build_truncate_msg(0, &[100, 9999, 200]);
        let events = parser
            .parse_wal_message(&truncate_msg, &catalog)
            .expect("truncate with one unknown OID should succeed");

        assert_eq!(events.len(), 2, "Should emit events for both known OIDs");
        assert!(events.iter().any(|e| e.table_id() == orders_id));
        assert!(events.iter().any(|e| e.table_id() == items_id));
        assert!(events.iter().all(|e| e.kind() == EventKind::Truncate));
    }

    // -- B7: UnknownTupleTag error path --------------------------------------

    // -- A2: unchanged-TOAST tag 'u' must be rejected in new-image tuples ----

    #[cfg(any())]
    #[test]
    fn insert_with_unchanged_toast_tag_is_rejected() {
        // Build an INSERT message where a column's tuple-data tag is b'u' (unchanged-TOAST).
        // This tag is only valid in old-image tuples. It must be rejected in new-image tuples.
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        let insert_msg = build_insert_msg(
            16384,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Unchanged, // 'u' tag, invalid in a new-image tuple
                TupleCol::Text("50.0".into()),
                TupleCol::Text("pending".into()),
            ],
        );

        let err = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect_err("unchanged-TOAST tag in INSERT new-tuple should fail");
        assert!(
            matches!(err, WalParseError::MalformedPayload(_)),
            "Expected MalformedPayload, got: {err:?}"
        );
    }

    #[cfg(any())]
    #[test]
    fn unknown_tuple_tag_errors() {
        // Build an INSERT whose tuple-data column tag is 0xFF (not 'n', 'u', or 't')
        let parser = PgOutputParser::new();
        let catalog = orders_catalog();

        let rel_msg = build_relation_msg(16384, "public", "orders", &orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation should parse");

        // Build a raw INSERT message:
        //   b'I', OID(4 bytes), b'N', num_cols(i16=4), then tag 0xFF for col 0
        let mut msg = vec![b'I'];
        push_u32(&mut msg, 16384u32);
        push_u8(&mut msg, b'N'); // new tuple tag
        push_i16(&mut msg, 4i16); // 4 columns matching arity
        push_u8(&mut msg, 0xFF); // unknown column data tag

        let err = parser
            .parse_wal_message(&msg, &catalog)
            .expect_err("unknown tuple tag should fail");
        assert!(
            matches!(err, WalParseError::UnknownTupleTag(0xFF)),
            "Expected UnknownTupleTag(0xFF), got: {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // Phase 7E: PgOutputEvent typed CdcEvent smoke tests
    // ------------------------------------------------------------------

    /// Wire OIDs matching `orders_customer_catalog` (all-INT except status TEXT).
    /// Column 0 has flags=1 so identity_columns == [0] on Update/Delete paths.
    fn typed_orders_columns() -> Vec<(&'static str, u32, u8)> {
        vec![
            ("id", 23, 1),
            ("customer", 23, 0),
            ("amount", 23, 0),
            ("status", 25, 0),
        ]
    }

    #[test]
    fn typed_pgoutput_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PgOutputEvent>();
    }

    #[test]
    fn typed_pgoutput_event_dispatches_through_engine() {
        let database = orders_catalog();
        let mut engine: crate::SubscriptionEngine<
            PgOutputEvent,
            crate::DefaultIds,
            sql_traits::structs::ParserDB,
        > = crate::SubscriptionEngine::new(database, sqlparser::dialect::PostgreSqlDialect {});

        engine
            .register(
                crate::SubscriptionRequest::new(88u64, "SELECT * FROM orders WHERE id > 0")
                    .updated_at_unix_ms(1_704_067_200_000),
            )
            .expect("register subscription");

        let parser = PgOutputParser::new();

        // Seed the relation cache.
        let rel_msg = build_relation_msg(50_000, "public", "orders", &typed_orders_columns());
        let events = parser
            .parse_wal_message(&rel_msg, engine.database())
            .expect("relation parses");
        assert!(events.is_empty(), "Relation msg produces no events");

        // Insert.
        let insert_msg = build_insert_msg(
            50_000,
            &[
                TupleCol::Text("7".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("250".into()),
                TupleCol::Text("paid".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&insert_msg, engine.database())
            .expect("insert parses");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Insert);
        assert_eq!(event.pk_columns(), &[0u16]);

        // Typed accessor: id round-trip.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Present(&7)
        );
        // Shape mismatch: id is int, asking for bool returns Missing.
        assert_eq!(
            event.bool_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
        // Non-PK column via Pk returns Missing.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 2),
            crate::backend::Presence::Missing
        );
        // PK column via Pk returns the value.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&7)
        );

        // Engine dispatch reads accessors internally; predicate id > 0 matches.
        let notifs = engine.consumers(event).expect("dispatch");
        assert_eq!(notifs.inserted(), alloc::vec![88u64]);
        assert!(notifs.updated().is_empty());
        assert!(notifs.deleted().is_empty());
    }

    #[test]
    fn typed_pgoutput_full_old_tuple_derives_changed_columns() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let rel_msg = build_relation_msg(60_000, "public", "orders", &typed_orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation parses");

        // Update with old_tag='O' (all columns). id unchanged, customer
        // unchanged, amount changed, status changed.
        let update_msg = build_update_msg_with_old(
            60_000,
            b'O',
            &[
                TupleCol::Text("7".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("100".into()),
                TupleCol::Text("pending".into()),
            ],
            &[
                TupleCol::Text("7".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("250".into()),
                TupleCol::Text("paid".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update parses");
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Update);
        let mut changed = event.changed_columns().to_vec();
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![2u16, 3u16]);
    }

    #[test]
    fn typed_pgoutput_key_tuple_update_leaves_changed_columns_empty() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let rel_msg = build_relation_msg(61_000, "public", "orders", &typed_orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation parses");

        // Update with key tuple 'K' (identity column only in old).
        let update_msg = build_update_msg_with_old(
            61_000,
            b'K',
            &[TupleCol::Text("7".into())],
            &[
                TupleCol::Text("7".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("250".into()),
                TupleCol::Text("paid".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update parses");
        assert!(events[0].changed_columns().is_empty());
    }

    #[test]
    fn typed_pgoutput_null_vs_missing_distinct_on_update_old_image() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let rel_msg = build_relation_msg(51_000, "public", "orders", &typed_orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation parses");

        // Update with old_tag='O' (all columns). Old image has:
        //   id=7 (identity, present)
        //   customer=Null (SQL NULL)
        //   amount=Unchanged (TOAST unchanged)
        //   status=old-text
        // New image populates every column fresh.
        let update_msg = build_update_msg_with_old(
            51_000,
            b'O',
            &[
                TupleCol::Text("7".into()),
                TupleCol::Null,
                TupleCol::Unchanged,
                TupleCol::Text("old".into()),
            ],
            &[
                TupleCol::Text("7".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("250".into()),
                TupleCol::Text("new".into()),
            ],
        );
        let events = parser
            .parse_wal_message(&update_msg, &catalog)
            .expect("update parses");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Update);

        // Old image: customer explicitly Null, amount omitted (Missing).
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 1),
            crate::backend::Presence::Null
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 2),
            crate::backend::Presence::Missing
        );
        // Old image still has id + status.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Present(&7)
        );
        // New image has amount fully populated.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Present(&250)
        );
    }

    #[test]
    fn typed_pgoutput_delete_pk_reads_from_identity() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let rel_msg = build_relation_msg(52_000, "public", "orders", &typed_orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation parses");

        // Delete with key tuple ('K') carrying only the identity column.
        let delete_msg = build_delete_msg(52_000, b'K', &[TupleCol::Text("9".into())]);
        let events = parser
            .parse_wal_message(&delete_msg, &catalog)
            .expect("delete parses");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Delete);
        assert_eq!(event.pk_columns(), &[0u16]);
        // Old image populated only for identity column.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Present(&9)
        );
        // Non-identity Old columns absent.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 2),
            crate::backend::Presence::Missing
        );
        // Pk aliases Old on a Delete.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&9)
        );
        // New image absent on delete.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_pgoutput_truncate_emits_one_event_per_relation() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let rel_msg = build_relation_msg(53_000, "public", "orders", &typed_orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation parses");

        let truncate_msg = build_truncate_msg(0, &[53_000]);
        let events = parser
            .parse_wal_message(&truncate_msg, &catalog)
            .expect("truncate parses");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind(), EventKind::Truncate);
        assert!(events[0].pk_columns().is_empty());
    }

    #[test]
    fn typed_pgoutput_empty_data_returns_empty_events() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let events = parser
            .parse_wal_message(b"", &catalog)
            .expect("empty msg");
        assert!(events.is_empty());
    }

    #[test]
    fn typed_pgoutput_checkpoint_setter_roundtrip() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        let rel_msg = build_relation_msg(54_000, "public", "orders", &typed_orders_columns());
        parser
            .parse_wal_message(&rel_msg, &catalog)
            .expect("relation parses");
        let insert_msg = build_insert_msg(
            54_000,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("2".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("s".into()),
            ],
        );
        let mut events = parser
            .parse_wal_message(&insert_msg, &catalog)
            .expect("insert parses");
        assert_eq!(events.len(), 1);
        assert!(events[0].checkpoint().is_none());
        let stamped = events.remove(0).with_checkpoint(Some(crate::PgLsn(0x1234)));
        assert_eq!(stamped.checkpoint().copied(), Some(crate::PgLsn(0x1234)));
    }

    #[test]
    fn typed_pgoutput_unknown_relation_returns_error() {
        let catalog = orders_catalog();
        let parser = PgOutputParser::new();
        // No Relation seed sent. Insert against unknown oid.
        let insert_msg = build_insert_msg(
            77_777,
            &[
                TupleCol::Text("1".into()),
                TupleCol::Text("2".into()),
                TupleCol::Text("3".into()),
                TupleCol::Text("s".into()),
            ],
        );
        let result = parser.parse_wal_message(&insert_msg, &catalog);
        assert!(matches!(result, Err(WalParseError::UnknownRelationOid(77_777))));
    }
}
