//! Debezium CDC parser.
//!
//! [Debezium](https://debezium.io/) captures row-level changes from databases
//! and emits them as JSON envelope messages (typically via Kafka Connect).
//! Like Maxwell, Debezium provides bare JSON values without column type
//! metadata, so we use type inference via [`infer_cell_from_json`].

use alloc::string::String;
#[cfg(test)]
use alloc::sync::Arc;
use alloc::vec::Vec;
use hashbrown::HashMap;

use serde::Deserialize;

use super::{parse_event_kind, resolve_table, WalParseError, WalParser};
use crate::{ColumnId, EventKind, TableId};
use sql_traits::prelude::DatabaseLike;

// ============================================================================
// Serde structs
// ============================================================================

#[derive(Deserialize)]
struct DebeziumEnvelope {
    before: Option<HashMap<String, serde_json::Value>>,
    after: Option<HashMap<String, serde_json::Value>>,
    source: DebeziumSource,
    op: String,
    #[allow(dead_code)]
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct DebeziumSource {
    #[allow(dead_code)]
    connector: Option<String>,
    #[serde(default)]
    db: String,
    #[serde(default)]
    schema: String,
    table: String,
}

// ============================================================================
// Parser
// ============================================================================

/// Debezium CDC parser (per-change: one JSON envelope per row change).
pub struct DebeziumParser;

impl<DB: DatabaseLike> WalParser<DB> for DebeziumParser {
    type Checkpoint = crate::NoCheckpoint;
    type Event = DebeziumEvent;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        let Some(msg): Option<DebeziumEnvelope> = super::parse_json_message_or_tombstone(data)?
        else {
            return Ok(Vec::new());
        };
        if msg.op == "m" {
            return Ok(Vec::new());
        }
        match convert_debezium_message_typed(msg, database) {
            Ok(event) => Ok(alloc::vec![event]),
            Err(WalParseError::UnknownEventKind(_)) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }
}

// ============================================================================
// Conversion logic
// ============================================================================

fn parse_debezium_op(op: &str) -> Result<EventKind, WalParseError> {
    parse_event_kind(op, &["c", "r"], &["u"], &["d"], &["t"])
}


// ============================================================================
// DebeziumEvent: typed [`CdcEvent`] output of the typed parser
// ============================================================================

use crate::backend::{Postgres, Value};
use crate::catalog_helpers;
use spin::Once;

/// Typed CDC event surfaced by [`DebeziumParser::parse_wal_message`].
pub struct DebeziumEvent {
    kind: EventKind,
    table_id: TableId,
    pk_columns: alloc::sync::Arc<[ColumnId]>,
    changed_columns: alloc::sync::Arc<[ColumnId]>,
    new_image: Option<DebeziumRowImage>,
    old_image: Option<DebeziumRowImage>,
}

struct DebeziumRowImage {
    entries: alloc::boxed::Box<[DebeziumWireCell]>,
    by_col: alloc::boxed::Box<[Option<u16>]>,
    cache: alloc::boxed::Box<[Once<Value<Postgres>>]>,
}

#[allow(dead_code)]
struct DebeziumWireCell {
    col_id: ColumnId,
    field_name: alloc::sync::Arc<str>,
    value: serde_json::Value,
}

impl DebeziumRowImage {
    fn from_hashmap(
        map: &HashMap<String, serde_json::Value>,
        table_id: TableId,
        arity: usize,
        database: &impl DatabaseLike,
        context: &'static str,
    ) -> Result<Self, WalParseError> {
        let mut entries = Vec::with_capacity(map.len());
        let mut by_col = alloc::vec![None; arity].into_boxed_slice();
        let mut seen = hashbrown::HashSet::with_capacity(map.len());

        for (field_name, value) in map {
            let col_id = catalog_helpers::column_id(database, table_id, field_name.as_str()).ok_or_else(|| {
                WalParseError::UnknownColumn {
                    table_id,
                    column: field_name.clone(),
                }
            })?;
            if !seen.insert(col_id) {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} contains duplicate column '{field_name}' (id {col_id})"
                )));
            }
            if (col_id as usize) >= arity {
                return Err(WalParseError::MalformedPayload(format!(
                    "{context} column '{field_name}' resolved to out-of-range id {col_id} for table {table_id} (arity {arity})"
                )));
            }
            let idx = u16::try_from(entries.len()).map_err(|_| {
                WalParseError::MalformedPayload(format!(
                    "{context} has more than {} entries",
                    u16::MAX
                ))
            })?;
            by_col[col_id as usize] = Some(idx);
            entries.push(DebeziumWireCell {
                col_id,
                field_name: alloc::sync::Arc::from(field_name.as_str()),
                value: value.clone(),
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
            super::pg_type::infer_pg_value_from_json_strict(
                &entry.value,
                entry.field_name.as_ref(),
            )
            .unwrap_or(Value::Missing)
        }))
    }
}

impl DebeziumEvent {
    const fn kind_matches_pk_source(&self, row: crate::backend::RowKind) -> Option<&DebeziumRowImage> {
        match (self.kind, row) {
            (EventKind::Truncate, _) => None,
            (EventKind::Insert, crate::backend::RowKind::New | crate::backend::RowKind::Pk) => self.new_image.as_ref(),
            (EventKind::Delete, crate::backend::RowKind::Old | crate::backend::RowKind::Pk) => self.old_image.as_ref(),
            (EventKind::Update, crate::backend::RowKind::New) => self.new_image.as_ref(),
            (EventKind::Update, crate::backend::RowKind::Old | crate::backend::RowKind::Pk) => self.old_image.as_ref(),
            _ => None,
        }
    }

    fn value_at(&self, row: crate::backend::RowKind, col: ColumnId) -> Option<&Value<Postgres>> {
        if row == crate::backend::RowKind::Pk && !self.pk_columns.contains(&col) {
            return None;
        }
        self.kind_matches_pk_source(row)
            .and_then(|image| image.value_at(col))
    }
}

macro_rules! debezium_scalar_accessor {
    ($self:ident, $row:ident, $col:ident, $variant:ident) => {{
        let Some(v) = $self.value_at($row, $col) else {
            return crate::backend::Presence::Missing;
        };
        match v {
            Value::$variant(x) => crate::backend::Presence::Present(x),
            Value::Null => crate::backend::Presence::Null,
            _ => crate::backend::Presence::Missing,
        }
    }};
}

impl crate::backend::CdcEvent for DebeziumEvent {
    type Backend = Postgres;
    type Checkpoint = crate::NoCheckpoint;

    fn kind(&self) -> EventKind {
        self.kind
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn checkpoint(&self) -> Option<&Self::Checkpoint> {
        None
    }

    fn pk_columns(&self) -> &[ColumnId] {
        &self.pk_columns
    }

    fn changed_columns(&self) -> &[ColumnId] {
        &self.changed_columns
    }

    fn bool_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&bool> {
        debezium_scalar_accessor!(self, row, col, Bool)
    }

    fn int_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&i64> {
        debezium_scalar_accessor!(self, row, col, Int)
    }

    fn float_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&f64> {
        debezium_scalar_accessor!(self, row, col, Float)
    }

    fn string_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&alloc::string::String> {
        debezium_scalar_accessor!(self, row, col, String)
    }

    fn bytes_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&alloc::vec::Vec<u8>> {
        debezium_scalar_accessor!(self, row, col, Bytes)
    }

    fn uuid_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&uuid::Uuid> {
        debezium_scalar_accessor!(self, row, col, Uuid)
    }

    fn timestamp_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&chrono::NaiveDateTime> {
        debezium_scalar_accessor!(self, row, col, Timestamp)
    }

    fn timestamp_tz_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&chrono::DateTime<chrono::Utc>> {
        debezium_scalar_accessor!(self, row, col, TimestampTz)
    }

    fn date_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&chrono::NaiveDate> {
        debezium_scalar_accessor!(self, row, col, Date)
    }

    fn time_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&chrono::NaiveTime> {
        debezium_scalar_accessor!(self, row, col, Time)
    }

    fn decimal_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&bigdecimal::BigDecimal> {
        debezium_scalar_accessor!(self, row, col, Decimal)
    }

    fn json_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&serde_json::Value> {
        debezium_scalar_accessor!(self, row, col, Json)
    }

    fn jsonb_at(&self, row: crate::backend::RowKind, col: ColumnId) -> crate::backend::Presence<&serde_json::Value> {
        debezium_scalar_accessor!(self, row, col, Jsonb)
    }
}


fn convert_debezium_message_typed<DB: DatabaseLike>(
    msg: DebeziumEnvelope,
    database: &DB,
) -> Result<DebeziumEvent, WalParseError> {
    let kind = parse_debezium_op(&msg.op)?;

    let table_id = resolve_table(&msg.source.schema, &msg.source.table, database)
        .or_else(|e| {
            if matches!(e, WalParseError::UnknownTable { .. }) {
                resolve_table(&msg.source.db, &msg.source.table, database)
            } else {
                Err(e)
            }
        })?;
    let arity = catalog_helpers::table_arity(database, table_id).ok_or_else(|| {
        WalParseError::UnknownTable {
            schema: msg.source.schema.clone(),
            table: msg.source.table.clone(),
        }
    })?;

    let pk_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Truncate {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    } else {
        catalog_helpers::primary_key_columns(database, table_id).map_or_else(|| alloc::sync::Arc::from(Vec::<ColumnId>::new()), alloc::sync::Arc::from)
    };

    let new_image = if matches!(kind, EventKind::Insert | EventKind::Update) {
        msg.after.as_ref().map(|m| {
            DebeziumRowImage::from_hashmap(m, table_id, arity, database, "debezium after")
        }).transpose()?
    } else {
        None
    };
    let old_image = if matches!(kind, EventKind::Update | EventKind::Delete) {
        msg.before.as_ref().map(|m| {
            DebeziumRowImage::from_hashmap(m, table_id, arity, database, "debezium before")
        }).transpose()?
    } else {
        None
    };

    // Derive changed_columns by wire-level comparison of `after` vs
    // `before` when both cover the arity (source is running under
    // REPLICA IDENTITY FULL). Sparser identities leave the slice empty,
    // which is safe (over-notification).
    let changed_columns: alloc::sync::Arc<[ColumnId]> = if kind == EventKind::Update {
        alloc::sync::Arc::from(debezium_derive_changed_columns(
            new_image.as_ref(),
            old_image.as_ref(),
            arity,
        ))
    } else {
        alloc::sync::Arc::from(Vec::<ColumnId>::new())
    };

    Ok(DebeziumEvent {
        kind,
        table_id,
        pk_columns,
        changed_columns,
        new_image,
        old_image,
    })
}

/// Wire-level `changed_columns` derivation for a Debezium Update event.
///
/// Runs only when both images cover every column (`REPLICA IDENTITY
/// FULL` upstream). Sparser replica identity settings leave the slice
/// empty (safe: over-notification, per the [`CdcEvent`] contract).
fn debezium_derive_changed_columns(
    new_image: Option<&DebeziumRowImage>,
    old_image: Option<&DebeziumRowImage>,
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
// Tests
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

    use super::super::test_support::{orders_catalog, orders_no_pk_catalog};
    use super::*;
    use crate::backend::CdcEvent;

    // -- INSERT tests -------------------------------------------------------

    
    
    // -- UPDATE tests -------------------------------------------------------

    
    
    // -- DELETE tests -------------------------------------------------------

    
    
    // -- Edge cases ----------------------------------------------------------

    
    
    // -- Error paths ---------------------------------------------------------

    
    
    
    
    // Removed `error_ambiguous_table_does_not_fallback_to_db_name`: this
    // test relied on a `MockCatalog` holding both `orders` and
    // `public.orders` as distinct table entries. `ParserDB` rejects this
    // shape at DDL parse time (the bare `orders` is treated as
    // `public.orders` implicitly), so the ambiguity path is no longer
    // reachable from a single `ParserDB`. Coverage for `AmbiguousTable`
    // is provided by `src/wal/mod.rs::test_resolve_table_conflicting_matches_errors`.

    
    
    
    
    
    // -- Trait checks -------------------------------------------------------

    
        // -- B8: Debezium message op is skipped ----------------------------------

    
    // -- A3: UPDATE PK change must use pre-update PK -------------------------

    
    // -- Phase 7D: DebeziumEvent typed CdcEvent smoke tests -----------------

    #[test]
    fn typed_debezium_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DebeziumEvent>();
    }

    #[test]
    fn typed_debezium_event_dispatches_through_engine() {
        let database = orders_catalog();
        let mut engine: crate::SubscriptionEngine<
            DebeziumEvent,
            crate::DefaultIds,
            sql_traits::structs::ParserDB,
        > = crate::SubscriptionEngine::new(database, sqlparser::dialect::PostgreSqlDialect {});

        engine
            .register(
                crate::SubscriptionRequest::new(66u64, "SELECT * FROM orders WHERE amount > 100")
                    .updated_at_unix_ms(1_704_067_200_000),
            )
            .expect("register subscription");

        let msg = r#"{
            "before": null,
            "after": {"id": 7, "amount": 250, "status": "paid", "comment": "test"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;

        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), engine.database())
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Insert);
        assert_eq!(event.pk_columns(), &[0u16]);

        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Present(&7)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 1),
            crate::backend::Presence::Present(&250)
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Present(&alloc::string::String::from("paid"))
        );
        assert_eq!(
            event.bool_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 1),
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
        assert_eq!(notifs.inserted(), alloc::vec![66u64]);
        assert!(notifs.updated().is_empty());
        assert!(notifs.deleted().is_empty());
    }

    #[test]
    fn typed_debezium_pk_non_pk_distinction() {
        let database = orders_catalog();
        let msg = r#"{
            "before": null,
            "after": {"id": 5, "amount": 50, "status": "new", "comment": "ok"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 1),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&5)
        );
    }

    #[test]
    fn typed_debezium_null_vs_missing_are_distinct() {
        let database = orders_catalog();
        let msg = r#"{
            "before": null,
            "after": {"id": 1, "amount": null},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "c",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 1),
            crate::backend::Presence::Null
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 2),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.string_at(crate::backend::RowKind::New, 3),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_debezium_tombstone_is_empty() {
        let database = orders_catalog();
        let events = DebeziumParser
            .parse_wal_message(b"null", &database)
            .expect("tombstone");
        assert!(events.is_empty());
    }

    #[test]
    fn typed_debezium_message_op_is_skipped() {
        let database = orders_catalog();
        let msg = r#"{"before":null,"after":null,"source":{"db":"testdb","schema":"public","table":"orders"},"op":"m","ts_ms":1000}"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("message op should be skipped");
        assert!(events.is_empty(), "Message ops should produce no output");
    }

    #[test]
    fn typed_debezium_delete_pk_from_before() {
        let database = orders_catalog();
        let msg = r#"{
            "before": {"id": 9, "amount": 100, "status": "old", "comment": "gone"},
            "after": null,
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "d",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Delete);
        assert_eq!(event.pk_columns(), &[0u16]);
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Present(&9)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&9)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
    }

    #[test]
    fn typed_debezium_update_pk_uses_before() {
        let database = orders_catalog();
        let msg = r#"{
            "before": {"id": 1, "amount": 100, "status": "old", "comment": "before"},
            "after":  {"id": 2, "amount": 200, "status": "new", "comment": "after"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "u",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Update);
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Present(&1)
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Present(&2)
        );
    }

    #[test]
    fn typed_debezium_full_before_derives_changed_columns() {
        let database = orders_catalog();
        // Full REPLICA IDENTITY (before covers every column). id stays,
        // amount and status differ, comment stays.
        let msg = r#"{
            "before": {"id": 1, "amount": 100, "status": "old", "comment": "same"},
            "after":  {"id": 1, "amount": 200, "status": "new", "comment": "same"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "u",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        let event = &events[0];
        let mut changed = event.changed_columns().to_vec();
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![1u16, 2u16]);
    }

    #[test]
    fn typed_debezium_sparse_before_leaves_changed_columns_empty() {
        let database = orders_catalog();
        // Sparse before (PK-only replica identity).
        let msg = r#"{
            "before": {"id": 1},
            "after":  {"id": 1, "amount": 200, "status": "new", "comment": "same"},
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "u",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert!(events[0].changed_columns().is_empty());
    }

    #[test]
    fn typed_debezium_truncate_no_images() {
        let database = orders_catalog();
        let msg = r#"{
            "before": null,
            "after": null,
            "source": {"connector": "postgresql", "db": "mydb", "schema": "public", "table": "orders"},
            "op": "t",
            "ts_ms": 1234567890
        }"#;
        let events = DebeziumParser
            .parse_wal_message(msg.as_bytes(), &database)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.kind(), EventKind::Truncate);
        assert!(event.pk_columns().is_empty());
        assert_eq!(
            event.int_at(crate::backend::RowKind::New, 0),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Old, 0),
            crate::backend::Presence::Missing
        );
        assert_eq!(
            event.int_at(crate::backend::RowKind::Pk, 0),
            crate::backend::Presence::Missing
        );
    }
}
