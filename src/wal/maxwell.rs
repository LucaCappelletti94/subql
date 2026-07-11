//! Maxwell's Daemon CDC parser.
//!
//! [Maxwell](https://maxwells-daemon.io/) reads MySQL binlogs and emits
//! one JSON message per row change. Unlike wal2json, Maxwell provides no
//! column type information. Values are bare JSON primitives, so we use
//! type inference via [`infer_cell_from_json`].

use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::HashMap;
use serde::Deserialize;

use super::{parse_event_kind, resolve_table, WalParseError, WalParser};
use crate::backend::{CdcEvent, MySql, Presence, RowKind, Value};
use crate::{catalog_helpers, ColumnId, EventKind, TableId};
use spin::Once;
use sql_traits::prelude::DatabaseLike;

// ============================================================================
// Serde structs
// ============================================================================

#[derive(Deserialize)]
struct MaxwellMessage {
    database: String,
    table: String,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    data: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    old: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    primary_key_columns: Option<Vec<String>>,
    #[allow(dead_code)]
    #[serde(default)]
    ts: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    xid: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    commit: Option<bool>,
}

// ============================================================================
// Parser
// ============================================================================

/// Maxwell's Daemon CDC parser (per-change: one JSON message per row change).
pub struct MaxwellParser;

impl<DB: DatabaseLike> WalParser<DB> for MaxwellParser {
    type Checkpoint = crate::NoCheckpoint;
    type Event = MaxwellEvent;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        let Some(msg): Option<MaxwellMessage> = super::parse_json_message_or_tombstone(data)?
        else {
            return Ok(Vec::new());
        };
        match convert_maxwell_typed(msg, database) {
            Ok(Some(event)) => Ok(alloc::vec![event]),
            Ok(None) => Ok(Vec::new()),
            Err(WalParseError::UnknownEventKind(_)) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }
}

// ============================================================================
// Conversion logic
// ============================================================================

fn parse_maxwell_kind(event_type: &str) -> Result<EventKind, WalParseError> {
    parse_event_kind(
        event_type,
        &["insert", "bootstrap-insert"],
        &["update"],
        &["delete"],
        &[],
    )
}


// ============================================================================
// MaxwellEvent: typed [`CdcEvent`] output of the Maxwell parser
// ============================================================================

/// Typed CDC event surfaced by [`MaxwellParser::parse_wal_message`].
///
/// Owns the Maxwell wire payload for one row change plus lazily populated
/// caches of decoded [`Value<MySql>`] cells. Scalar accessors on the
/// [`CdcEvent`] impl decode each cell on first access through
/// [`super::pg_type::infer_mysql_value_from_json_strict`] and return
/// references into the cache.
///
/// Backed by the MySQL [`Backend`](crate::backend::MySql).
pub struct MaxwellEvent {
    kind: EventKind,
    table_id: TableId,
    pk_columns: alloc::sync::Arc<[ColumnId]>,
    changed_columns: alloc::sync::Arc<[ColumnId]>,
    checkpoint: Option<crate::NoCheckpoint>,
    new_image: Option<MaxwellRowImage>,
    old_image: Option<MaxwellRowImage>,
}

/// One row image (new or old) inside a [`MaxwellEvent`].
struct MaxwellRowImage {
    /// Wire entries in the order the message carried them.
    entries: alloc::boxed::Box<[MaxwellWireCell]>,
    /// `ColumnId -> entries` position. Arity-sized; `None` for columns
    /// the source did not carry in this image.
    by_col: alloc::boxed::Box<[Option<u16>]>,
    /// Lazily populated decoded values (arity-sized). Uninitialised
    /// [`Once`] for cells the caller never touched. Decode failures are
    /// materialised as [`Value::Missing`] so accessors return
    /// [`Presence::Missing`] and the engine escalates to re-execution.
    cache: alloc::boxed::Box<[Once<Value<MySql>>]>,
}

/// One decoded wire cell inside a [`MaxwellRowImage`].
struct MaxwellWireCell {
    field_name: alloc::sync::Arc<str>,
    value: serde_json::Value,
}

impl MaxwellRowImage {
    fn from_hashmap(
        map: HashMap<String, serde_json::Value>,
        table_id: TableId,
        arity: usize,
        database: &impl DatabaseLike,
        context: &'static str,
    ) -> Result<Self, WalParseError> {
        let mut entries = Vec::with_capacity(map.len());
        let mut by_col = alloc::vec![None; arity].into_boxed_slice();
        let mut seen = hashbrown::HashSet::with_capacity(map.len());

        for (name, value) in map {
            let col_id = catalog_helpers::column_id(database, table_id, name.as_str())
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
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
            entries.push(MaxwellWireCell {
                field_name: alloc::sync::Arc::from(name),
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

    /// Resolve the decoded [`Value<MySql>`] for `col` in this image, or
    /// `None` when the wire did not carry that column.
    fn value_at(&self, col: ColumnId) -> Option<&Value<MySql>> {
        let idx = usize::from(col);
        let wire_idx = usize::from((*self.by_col.get(idx)?)?);
        let entry = self.entries.get(wire_idx)?;
        let cache_slot = self.cache.get(idx)?;
        Some(cache_slot.call_once(|| {
            super::pg_type::infer_mysql_value_from_json_strict(
                &entry.value,
                entry.field_name.as_ref(),
            )
            .unwrap_or(Value::Missing)
        }))
    }
}

impl MaxwellEvent {
    fn kind_matches_pk_source(&self, row: RowKind) -> Option<&MaxwellRowImage> {
        match (self.kind, row) {
            (EventKind::Truncate, _) => None,
            (EventKind::Insert, RowKind::New | RowKind::Pk) => self.new_image.as_ref(),
            (EventKind::Delete, RowKind::Old | RowKind::Pk) => self.old_image.as_ref(),
            (EventKind::Update, RowKind::New) => self.new_image.as_ref(),
            (EventKind::Update, RowKind::Old | RowKind::Pk) => self.old_image.as_ref(),
            _ => None,
        }
    }

    fn value_at(&self, row: RowKind, col: ColumnId) -> Option<&Value<MySql>> {
        if row == RowKind::Pk && !self.pk_columns.contains(&col) {
            return None;
        }
        self.kind_matches_pk_source(row)
            .and_then(|image| image.value_at(col))
    }
}

/// Look up a decoded value and match it against the requested scalar
/// shape. Missing wire, [`Value::Missing`], or scalar-shape mismatch all
/// surface as [`Presence::Missing`]; [`Value::Null`] surfaces as
/// [`Presence::Null`].
macro_rules! maxwell_scalar_accessor {
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

impl CdcEvent for MaxwellEvent {
    type Backend = MySql;
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
        maxwell_scalar_accessor!(self, row, col, Bool)
    }

    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&i64> {
        maxwell_scalar_accessor!(self, row, col, Int)
    }

    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&f64> {
        maxwell_scalar_accessor!(self, row, col, Float)
    }

    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        maxwell_scalar_accessor!(self, row, col, String)
    }

    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::vec::Vec<u8>> {
        maxwell_scalar_accessor!(self, row, col, Bytes)
    }

    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&alloc::string::String> {
        maxwell_scalar_accessor!(self, row, col, Uuid)
    }

    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDateTime> {
        maxwell_scalar_accessor!(self, row, col, Timestamp)
    }

    fn timestamp_tz_at(
        &self,
        row: RowKind,
        col: ColumnId,
    ) -> Presence<&chrono::DateTime<chrono::Utc>> {
        maxwell_scalar_accessor!(self, row, col, TimestampTz)
    }

    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveDate> {
        maxwell_scalar_accessor!(self, row, col, Date)
    }

    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&chrono::NaiveTime> {
        maxwell_scalar_accessor!(self, row, col, Time)
    }

    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&bigdecimal::BigDecimal> {
        maxwell_scalar_accessor!(self, row, col, Decimal)
    }

    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        maxwell_scalar_accessor!(self, row, col, Json)
    }

    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&serde_json::Value> {
        maxwell_scalar_accessor!(self, row, col, Jsonb)
    }
}


fn convert_maxwell_typed<DB: DatabaseLike>(
    msg: MaxwellMessage,
    database: &DB,
) -> Result<Option<MaxwellEvent>, WalParseError> {
    // Skip DDL/bootstrap events
    if matches!(
        msg.event_type.as_str(),
        "ddl"
            | "table-create"
            | "table-drop"
            | "table-alter"
            | "database-create"
            | "database-drop"
            | "bootstrap-start"
            | "bootstrap-complete"
    ) {
        return Ok(None);
    }

    // Only handle insert, update, delete (including bootstrap-insert which
    // maps to insert via parse_maxwell_kind). Truncate is not a Maxwell event.
    let kind = parse_maxwell_kind(&msg.event_type)?;
    if matches!(kind, EventKind::Truncate) {
        return Ok(None);
    }

    let table_id = resolve_table(&msg.database, &msg.table, database)?;
    let arity = catalog_helpers::table_arity(database, table_id).ok_or_else(|| {
        WalParseError::UnknownTable {
            schema: msg.database.clone(),
            table: msg.table.clone(),
        }
    })?;

    // PK column ids
    let pk_columns: alloc::sync::Arc<[ColumnId]> = if let Some(pk_names) = &msg.primary_key_columns {
        let mut ids = Vec::with_capacity(pk_names.len());
        let mut seen = hashbrown::HashSet::with_capacity(pk_names.len());
        for name in pk_names {
            let col_id = catalog_helpers::column_id(database, table_id, name.as_str())
                .ok_or_else(|| WalParseError::UnknownColumn {
                    table_id,
                    column: name.clone(),
                })?;
            if !seen.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "primary_key_columns contains duplicate column '{}' (id {})",
                    name, col_id
                )));
            }
            ids.push(col_id);
        }
        alloc::sync::Arc::from(ids)
    } else {
        catalog_helpers::primary_key_columns(database, table_id)
            .map(alloc::sync::Arc::from)
            .unwrap_or_else(|| alloc::sync::Arc::from(Vec::<ColumnId>::new()))
    };

    // Extract data maps before matching on kind (from_hashmap takes ownership).
    let data_map = msg.data;
    let old_map = msg.old;

    // New/old image routing per event kind.
    // Insert: new from data, old None.
    // Update: new from data, old from old field.
    // Delete: old from data (Maxwell puts deleted row in data), new None.
    let (new_image, old_image): (Option<MaxwellRowImage>, Option<MaxwellRowImage>) = match kind {
        EventKind::Insert => {
            let data = data_map.ok_or_else(|| {
                WalParseError::MissingField("data".to_string())
            })?;
            (
                Some(MaxwellRowImage::from_hashmap(
                    data,
                    table_id,
                    arity,
                    database,
                    "maxwell data",
                )?),
                None,
            )
        }
        EventKind::Update => {
            let data = data_map.ok_or_else(|| {
                WalParseError::MissingField("data".to_string())
            })?;
            let new = MaxwellRowImage::from_hashmap(
                data,
                table_id,
                arity,
                database,
                "maxwell data",
            )?;
            let old = old_map.map(|old| {
                MaxwellRowImage::from_hashmap(
                    old,
                    table_id,
                    arity,
                    database,
                    "maxwell old",
                )
            }).transpose()?;
            (Some(new), old)
        }
        EventKind::Delete => {
            // Maxwell puts the deleted row in `data` for a delete event.
            let data = data_map.ok_or_else(|| {
                WalParseError::MissingField("data".to_string())
            })?;
            (
                None,
                Some(MaxwellRowImage::from_hashmap(
                    data,
                    table_id,
                    arity,
                    database,
                    "maxwell data",
                )?),
            )
        }
        EventKind::Truncate => (None, None),
    };

    // Maxwell's `old` field is by-convention only the columns whose
    // value changed on this update. Every column present in the old
    // image is therefore in `changed_columns`, and the derivation is
    // authoritative (not a hint). See [`MapCdcConfig::old_is_changed_columns_only`]
    // in the legacy convert path for the same statement of intent.
    let changed_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Update {
        alloc::sync::Arc::from(maxwell_derive_changed_columns(old_image.as_ref()))
    } else {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    };

    Ok(Some(MaxwellEvent {
        kind,
        table_id,
        pk_columns,
        changed_columns,
        checkpoint: None,
        new_image,
        old_image,
    }))
}

/// Wire-level `changed_columns` for a Maxwell Update event.
///
/// Maxwell's `old` field only carries columns whose value differed on
/// the update, so every column present in the old image belongs in
/// `changed_columns`. Authoritative (not a hint) for a well-behaved
/// Maxwell producer.
fn maxwell_derive_changed_columns(old_image: Option<&MaxwellRowImage>) -> Vec<ColumnId> {
    let Some(old) = old_image else {
        return Vec::new();
    };
    let mut changed = Vec::new();
    for (col, slot) in old.by_col.iter().enumerate() {
        if slot.is_some() {
            #[allow(clippy::cast_possible_truncation)]
            changed.push(col as ColumnId);
        }
    }
    changed
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};

    // -- Test catalog --------------------------------------------------------

    /// Maxwell test table: table="e", columns: id=0, m=1, c=2, comment=3, PK=[id].
    /// A leading `_maxwell_pad` table keeps `e`'s table id stable at 1.
    fn maxwell_e_catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE _maxwell_pad (id INT);\n\
             CREATE TABLE e (id INT PRIMARY KEY, m DOUBLE PRECISION, c TEXT, comment TEXT);",
        )
        .expect("maxwell e DDL parses")
    }

    fn maxwell_e_no_pk_catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE _maxwell_pad (id INT);\n\
             CREATE TABLE e (id INT, m DOUBLE PRECISION, c TEXT, comment TEXT);",
        )
        .expect("maxwell e (no-PK) DDL parses")
    }

    fn maxwell_e_table_id() -> crate::TableId {
        crate::catalog_helpers::table_id(&maxwell_e_catalog(), "e").expect("e table id")
    }

    // -- INSERT tests -------------------------------------------------------

    #[cfg(any())]
    #[test]
    fn maxwell_insert() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert","ts":1477053217,
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.table_id(), maxwell_e_table_id());

        let new = ev.new_row().expect("INSERT should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Float(4.2341)));
        assert_eq!(
            new.get(2),
            Some(&Cell::String(Arc::from("2016-10-21 05:33:37")))
        );
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("hello"))));

        assert!(ev.old_row().is_none());

        // PK from catalog
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns().is_empty());
    }

    #[cfg(any())]
    #[test]
    fn maxwell_insert_with_pk_columns() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert","ts":1477053217,
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"},
            "primary_key_columns":["id","c"]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // PK from message: both id and c
        assert_eq!(ev.pk().columns.len(), 2);
        assert!(ev.pk().columns.contains(&0)); // id
        assert!(ev.pk().columns.contains(&2)); // c
    }

    // -- UPDATE tests -------------------------------------------------------

    #[cfg(any())]
    #[test]
    fn maxwell_update() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"update",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"},
            "old":{"m":4.2341,"c":"2016-10-21 05:33:37"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);

        // New row present
        let new = ev.new_row().expect("UPDATE should have new_row");
        assert_eq!(new.get(1), Some(&Cell::Float(5.444)));

        // Old row (sparse: only changed columns)
        let old = ev.old_row().expect("UPDATE should have old_row");
        assert_eq!(old.get(1), Some(&Cell::Float(4.2341)));
        assert_eq!(
            old.get(2),
            Some(&Cell::String(Arc::from("2016-10-21 05:33:37")))
        );
        // Columns not in `old` are Missing
        assert_eq!(old.get(0), Some(&Cell::Missing));
        assert_eq!(old.get(3), Some(&Cell::Missing));

        // Maxwell's `old` contains only changed columns; Missing means "not changed".
        // changed_columns() skips Missing cells, so the result is exactly the
        // columns that differed: m (col 1) and c (col 2).
        let changed: Vec<u16> = ev.changed_columns().to_vec();
        assert_eq!(changed, vec![1, 2]);
    }

    #[cfg(any())]
    #[test]
    fn maxwell_update_without_old() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"update",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert!(ev.new_row().is_some());
        assert!(ev.old_row().is_none());
        assert!(ev.changed_columns().is_empty());
    }

    // -- DELETE tests -------------------------------------------------------

    #[cfg(any())]
    #[test]
    fn maxwell_delete() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"delete",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert!(ev.new_row().is_none());

        let old = ev.old_row().expect("DELETE should have old_row");
        assert_eq!(old.get(0), Some(&Cell::Int(1)));

        // PK from catalog
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);

        assert!(ev.changed_columns().is_empty());
    }

    #[cfg(any())]
    #[test]
    fn maxwell_delete_with_pk_columns() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"delete",
            "data":{"id":1,"m":5.444,"c":"2016-10-21 05:33:54","comment":"hello"},
            "primary_key_columns":["id"]
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        assert_eq!(ev.pk().columns.as_ref(), &[0]);
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
    }

    // -- Edge cases ----------------------------------------------------------

    #[cfg(any())]
    #[test]
    fn maxwell_null_values() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":null,"c":null,"comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let new = events[0].new_row().expect("should have new_row");
        assert_eq!(new.get(0), Some(&Cell::Int(1)));
        assert_eq!(new.get(1), Some(&Cell::Null));
        assert_eq!(new.get(2), Some(&Cell::Null));
        assert_eq!(new.get(3), Some(&Cell::String(Arc::from("hello"))));
    }

    #[cfg(any())]
    #[test]
    fn maxwell_insert_no_catalog_pk() {
        let catalog = maxwell_e_no_pk_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        let ev = &events[0];
        // No PK source: empty PK
        assert!(ev.pk().columns.is_empty());
        assert!(ev.pk().values.is_empty());
    }

    // -- Error paths ---------------------------------------------------------

    #[cfg(any())]
    #[test]
    fn error_invalid_utf8() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0xFD];

        let err = parser
            .parse_wal_message(bad_bytes, &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[cfg(any())]
    #[test]
    fn error_malformed_json() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let err = parser
            .parse_wal_message(b"not json at all", &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[cfg(any())]
    #[test]
    fn maxwell_tombstone_null_is_ignored() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let events = parser
            .parse_wal_message(b"null", &catalog)
            .expect("tombstone should be ignored");
        assert!(events.is_empty());
    }

    #[cfg(any())]
    #[test]
    fn error_unknown_table() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"other","table":"nonexistent","type":"insert",
            "data":{"id":1}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownTable { .. }));
    }

    #[cfg(any())]
    #[test]
    fn error_unknown_column() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"nonexistent_col":"value"}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[cfg(any())]
    #[test]
    fn unknown_event_kind_is_skipped() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        // "truncate" is not a known Maxwell event type. It must be skipped, not error.
        let json = r#"{
            "database":"test","table":"e","type":"truncate",
            "data":{"id":1}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("unknown event kind should be skipped, not error");
        assert!(
            events.is_empty(),
            "unknown event kind should produce no output"
        );
    }

    #[cfg(any())]
    #[test]
    fn error_missing_data() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert"
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("should fail");
        assert!(matches!(err, WalParseError::MissingField(_)));
    }

    #[cfg(any())]
    #[test]
    fn error_numeric_overflow() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":18446744073709551615}
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("overflow should fail");
        assert!(matches!(err, WalParseError::NumericOverflow { .. }));
    }

    #[cfg(any())]
    #[test]
    fn error_pk_metadata_unknown_column() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":4.2,"c":"2020-01-01","comment":"hello"},
            "primary_key_columns":["id","does_not_exist"]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("unknown PK metadata column should fail");
        assert!(matches!(err, WalParseError::UnknownColumn { .. }));
    }

    #[cfg(any())]
    #[test]
    fn error_pk_metadata_column_missing_in_row() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"m":4.2,"c":"2020-01-01","comment":"hello"},
            "primary_key_columns":["id"]
        }"#;

        let err = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect_err("missing PK value in row should fail");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    // -- Trait checks -------------------------------------------------------

    #[cfg(any())]
    #[test]
    fn trait_object_compiles() {
        let catalog = maxwell_e_catalog();
        let parser: &dyn WalParser<ParserDB, Checkpoint = crate::NoCheckpoint> = &MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"insert",
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
        }"#;

        let result = parser.parse_wal_message(json.as_bytes(), &catalog);
        assert!(result.is_ok());
    }

    #[cfg(any())]
    #[test]
    fn send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MaxwellParser>();
    }
    // -- B8: Maxwell DDL events are skipped ---------------------------------

    #[cfg(any())]
    #[test]
    fn maxwell_ddl_event_is_skipped() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{"type":"ddl","database":"test","table":"e","def":"ALTER TABLE e ADD COLUMN x INT"}"#;
        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("DDL event should be skipped, not errored");
        assert!(events.is_empty(), "DDL events should produce no output");
    }

    #[cfg(any())]
    #[test]
    fn maxwell_table_create_is_skipped() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{"type":"table-create","database":"test","table":"e","def":"CREATE TABLE e (id INT)"}"#;
        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("table-create event should be skipped");
        assert!(
            events.is_empty(),
            "table-create events should produce no output"
        );
    }

    // -- A3: UPDATE PK change must use pre-update PK -------------------------

    #[cfg(any())]
    #[test]
    fn update_pk_change_uses_pre_update_pk() {
        // When a PK column changes (id: 1 -> 2), the emitted PK must be the
        // pre-update value (1), not the post-update value (2).
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"update",
            "data":{"id":2,"m":200.0,"c":"2016-10-22","comment":"after"},
            "old":{"id":1,"m":100.0,"c":"2016-10-21","comment":"before"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("parse should succeed");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        // PK must come from the pre-update (old) row: id = 1
        assert_eq!(ev.pk().values.as_ref(), &[Cell::Int(1)]);
        assert_eq!(ev.pk().columns.as_ref(), &[0u16]);
    }

    // -- A4: bootstrap-insert events must be treated as normal inserts -------

    #[cfg(any())]
    #[test]
    fn bootstrap_insert_is_treated_as_insert() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        let json = r#"{
            "database":"test","table":"e","type":"bootstrap-insert","ts":1477053217,
            "data":{"id":1,"m":4.2341,"c":"2016-10-21 05:33:37","comment":"hello"}
        }"#;

        let events = parser
            .parse_wal_message(json.as_bytes(), &catalog)
            .expect("bootstrap-insert should parse as insert");

        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.table_id(), maxwell_e_table_id());
        assert!(ev.new_row().is_some());
        assert!(ev.old_row().is_none());
    }

    #[cfg(any())]
    #[test]
    fn bootstrap_start_and_complete_are_skipped() {
        let catalog = maxwell_e_catalog();
        let parser = MaxwellParser;

        for event_type in &["bootstrap-start", "bootstrap-complete"] {
            let json = format!(r#"{{"database":"test","table":"e","type":"{event_type}"}}"#);
            let events = parser
                .parse_wal_message(json.as_bytes(), &catalog)
                .unwrap_or_else(|e| panic!("{event_type} should be skipped, not error: {e:?}"));
            assert!(events.is_empty(), "{event_type} should produce no output");
        }
    }
    // ------------------------------------------------------------------
    // Phase 7C: MaxwellEvent typed CdcEvent smoke tests
    // ------------------------------------------------------------------

    /// Test catalog for typed Maxwell tests: table="orders",
    /// columns: id=0, amount=1, status=2, PK=[id].
    fn typed_maxwell_catalog() -> ParserDB {
        ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE _pad (id INT);\n\
             CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("typed maxwell DDL parses")
    }

    /// 1. Send + Sync compile-time check.
    #[test]
    fn typed_maxwell_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MaxwellEvent>();
    }

    /// 2. End-to-end register + parse + engine.consumers roundtrip.
    #[test]
    fn typed_maxwell_dispatches_through_engine() {
        let database = typed_maxwell_catalog();
        let mut engine: crate::SubscriptionEngine<
            MaxwellEvent,
            crate::DefaultIds,
            ParserDB,
        > = crate::SubscriptionEngine::new(database, MySqlDialect {});

        engine
            .register(
                crate::SubscriptionRequest::new(55u64, "SELECT * FROM orders WHERE amount > 100")
                    .updated_at_unix_ms(1_704_067_200_000),
            )
            .expect("register subscription");

        let msg = r#"{
            "database":"test","table":"orders","type":"insert",
            "data":{"id":7,"amount":250,"status":"paid"}
        }"#;

        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), engine.database())
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Insert);
        assert_eq!(event.pk_columns(), &[0u16]);

        // Typed scalar accessors: integer round-trip on New.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 1),
            crate::backend::Presence::Present(&250)
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Present(&alloc::string::String::from("paid"))
        );

        // End-to-end: engine dispatch reads the typed accessors internally.
        let notifs = engine.consumers(event).expect("dispatch");
        assert_eq!(notifs.inserted(), alloc::vec![55u64]);
        assert!(notifs.updated().is_empty());
        assert!(notifs.deleted().is_empty());
    }

    /// 3. Non-PK column via RowKind::Pk returns Presence::Missing.
    #[test]
    fn typed_maxwell_pk_non_pk_column_missing() {
        let database = typed_maxwell_catalog();
        let msg = r#"{
            "database":"test","table":"orders","type":"insert",
            "data":{"id":1,"amount":50,"status":"pending"}
        }"#;

        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];

        // amount (col 1) is NOT a PK column.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 1),
            crate::backend::Presence::Missing
        );
    }

    /// 4. PK column via RowKind::Pk returns Presence::Present.
    #[test]
    fn typed_maxwell_pk_column_present() {
        let database = typed_maxwell_catalog();
        let msg = r#"{
            "database":"test","table":"orders","type":"insert",
            "data":{"id":42,"amount":50,"status":"pending"}
        }"#;

        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];

        // id (col 0) IS a PK column.
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&42)
        );
    }

    /// 5. Value::Null vs Value::Missing distinction.
    #[test]
    fn typed_maxwell_null_vs_missing() {
        let database = typed_maxwell_catalog();
        // amount is explicitly null, status is omitted entirely.
        let msg = r#"{
            "database":"test","table":"orders","type":"insert",
            "data":{"id":1,"amount":null}
        }"#;

        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];

        // amount is explicitly SQL NULL on the wire.
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 1),
            crate::backend::Presence::Null
        );
        // status was omitted from the data entirely.
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_maxwell_old_field_populates_changed_columns() {
        let database = typed_maxwell_catalog();
        // Maxwell puts every changed column into `old`. amount and status
        // are the two columns that changed here.
        let msg = r#"{
            "database":"test","table":"orders","type":"update",
            "data":{"id":7,"amount":250,"status":"paid"},
            "old":{"amount":100,"status":"pending"}
        }"#;
        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Update);
        let mut changed = event.changed_columns().to_vec();
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![1u16, 2u16]);
    }

    #[test]
    fn typed_maxwell_insert_leaves_changed_columns_empty() {
        let database = typed_maxwell_catalog();
        let msg = r#"{
            "database":"test","table":"orders","type":"insert",
            "data":{"id":9,"amount":10,"status":"new"}
        }"#;
        let events = MaxwellParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert!(events[0].changed_columns().is_empty());
    }

    /// 6. Tombstone (null JSON) parses to empty Vec.
    #[test]
    fn typed_maxwell_tombstone_is_empty() {
        let database = typed_maxwell_catalog();
        let events = MaxwellParser
            .parse_wal_message(b"null", &database)
            .expect("tombstone");
        assert!(events.is_empty());
    }

    /// 7. Parser-specific skip semantics: DDL and bootstrap events.
    #[test]
    fn typed_maxwell_skips_control_events() {
        let database = typed_maxwell_catalog();
        for event_type in &["ddl", "table-create", "bootstrap-start", "bootstrap-complete"] {
            let msg = format!(r#"{{"database":"test","table":"orders","type":"{event_type}"}}"#);
            let events = MaxwellParser
                .parse_wal_message(msg.as_bytes(), &database)
                .unwrap_or_else(|e| panic!("{event_type} should be skipped, not error: {e:?}"));
            assert!(events.is_empty(), "{event_type} should produce no output");
        }
    }
}
