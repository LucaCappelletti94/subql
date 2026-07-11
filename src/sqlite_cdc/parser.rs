//! Parser: SQLite session-extension patchset bytes into typed
//! [`SqlitePatchsetEvent`] instances.
//!
//! Turns [`sqlite_diff_rs::ParsedDiffSet::Patchset`] into a stream of
//! typed events by:
//!
//! 1. Parsing the wire bytes via `sqlite_diff_rs::ParsedDiffSet::parse`.
//! 2. Rejecting a changeset-marker payload up front (subql only
//!    consumes patchset for now).
//! 3. Resolving each op's table name against the catalog, computing PK
//!    column ordinals from the wire's `pk_flags`, and looking up the
//!    per-column [`crate::backend::ScalarKind`] from the catalog to
//!    route the wire's raw `Value<String, Vec<u8>>` into the correct
//!    [`Value<SQLite>`] variant.
//! 4. Materialising each row image as an arity-sized
//!    `Box<[Value<SQLite>]>` with [`Value::Missing`] for cells the wire
//!    did not carry on that side.

use alloc::boxed::Box;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;

use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{ParseError, ParsedDiffSet, PatchsetOp, TableSchema, Value as WireValue};

use super::event::SqlitePatchsetEvent;
use crate::backend::{ScalarKind, Value, SQLite};
use crate::wal::{resolve_table, WalParseError, WalParser};
use crate::{catalog_helpers, ColumnId, EventKind, TableId};

/// Parser marker type. Zero-sized; safe to construct freely.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqlitePatchsetParser;

impl<DB: DatabaseLike> WalParser<DB> for SqlitePatchsetParser {
    type Checkpoint = crate::NoCheckpoint;
    type Event = SqlitePatchsetEvent;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let parsed = ParsedDiffSet::parse(data).map_err(convert_parse_error)?;
        let ParsedDiffSet::Patchset(diffset) = parsed else {
            return Err(WalParseError::MalformedPayload(
                "expected SQLite patchset marker 'P', got changeset marker 'T'".to_string(),
            ));
        };
        let mut events = Vec::new();
        for op in diffset.iter() {
            if let Some(ev) = op_to_event(op, database)? {
                events.push(ev);
            }
        }
        Ok(events)
    }
}

fn convert_parse_error(err: ParseError) -> WalParseError {
    match err {
        ParseError::UnexpectedEof(pos) => WalParseError::TruncatedMessage {
            expected: 0,
            actual: pos,
        },
        ParseError::InvalidTableName(_) | ParseError::UnterminatedTableName => {
            WalParseError::InvalidUtf8(err.to_string())
        }
        _ => WalParseError::MalformedPayload(err.to_string()),
    }
}

fn op_to_event<DB: DatabaseLike>(
    op: PatchsetOp<'_, TableSchema<alloc::string::String>, alloc::string::String, Vec<u8>>,
    database: &DB,
) -> Result<Option<SqlitePatchsetEvent>, WalParseError> {
    let schema = op.table();
    let table_name = schema.name();
    let table_id = resolve_table("", table_name.as_str(), database)?;
    let arity =
        catalog_helpers::table_arity(database, table_id).ok_or_else(|| WalParseError::UnknownTable {
            schema: alloc::string::String::new(),
            table: table_name.clone(),
        })?;
    if schema.pk_flags().len() != arity {
        return Err(WalParseError::ArityMismatch {
            table_id,
            wal_count: schema.pk_flags().len(),
            catalog_arity: arity,
        });
    }
    let pk_columns = pk_columns_from_flags(schema.pk_flags());
    let scalar_kinds = column_scalar_kinds(database, table_id, arity);

    let (kind, new_row, old_row, changed_columns) = match op {
        PatchsetOp::Insert { values, .. } => {
            if values.len() != arity {
                return Err(WalParseError::ArityMismatch {
                    table_id,
                    wal_count: values.len(),
                    catalog_arity: arity,
                });
            }
            let mut row: Vec<Value<SQLite>> = Vec::with_capacity(arity);
            for (col, wire) in values.iter().enumerate() {
                row.push(decode_wire_value(wire.clone(), scalar_kind_for(&scalar_kinds, col)));
            }
            (
                EventKind::Insert,
                Some(row.into_boxed_slice()),
                None,
                Arc::from(Vec::<ColumnId>::new()),
            )
        }
        PatchsetOp::Update { pk, entries, .. } => {
            if entries.len() != arity {
                return Err(WalParseError::ArityMismatch {
                    table_id,
                    wal_count: entries.len(),
                    catalog_arity: arity,
                });
            }
            let mut new_row: Vec<Value<SQLite>> = Vec::with_capacity(arity);
            let mut changed = Vec::new();
            for (col, ((), maybe_new)) in entries.iter().enumerate() {
                if let Some(wire) = maybe_new {
                    #[allow(clippy::cast_possible_truncation)]
                    changed.push(col as ColumnId);
                    new_row.push(decode_wire_value(
                        wire.clone(),
                        scalar_kind_for(&scalar_kinds, col),
                    ));
                } else {
                    new_row.push(Value::Missing);
                }
            }
            let old_row = old_row_from_pk(pk, &pk_columns, arity, &scalar_kinds);
            (
                EventKind::Update,
                Some(new_row.into_boxed_slice()),
                Some(old_row),
                Arc::from(changed),
            )
        }
        PatchsetOp::Delete { pk, .. } => {
            let old_row = old_row_from_pk(pk, &pk_columns, arity, &scalar_kinds);
            (
                EventKind::Delete,
                None,
                Some(old_row),
                Arc::from(Vec::<ColumnId>::new()),
            )
        }
    };

    Ok(Some(SqlitePatchsetEvent {
        kind,
        table_id,
        pk_columns,
        changed_columns,
        new_row,
        old_row,
    }))
}

/// Build an arity-sized old-row image populated only for PK columns.
///
/// `pk_values` holds the PK column values in PK-declaration order (as
/// carried by the wire). We route each PK value to its ColumnId ordinal
/// so the row image looks like every other one: index-aligned with
/// [`ColumnId`], [`Value::Missing`] for non-PK cells.
fn old_row_from_pk(
    pk_values: &[WireValue<alloc::string::String, Vec<u8>>],
    pk_columns: &[ColumnId],
    arity: usize,
    scalar_kinds: &[Option<ScalarKind>],
) -> Box<[Value<SQLite>]> {
    let mut old_row: Vec<Value<SQLite>> = (0..arity).map(|_| Value::Missing).collect();
    for (i, wire) in pk_values.iter().enumerate() {
        let Some(&col) = pk_columns.get(i) else {
            break;
        };
        if let Some(slot) = old_row.get_mut(col as usize) {
            *slot = decode_wire_value(wire.clone(), scalar_kind_for(scalar_kinds, col as usize));
        }
    }
    old_row.into_boxed_slice()
}

fn pk_columns_from_flags(pk_flags: &[u8]) -> Arc<[ColumnId]> {
    let mut pk_with_ord: Vec<(ColumnId, u8)> = pk_flags
        .iter()
        .enumerate()
        .filter_map(|(i, &ord)| {
            if ord == 0 {
                return None;
            }
            #[allow(clippy::cast_possible_truncation)]
            Some((i as ColumnId, ord))
        })
        .collect();
    pk_with_ord.sort_by_key(|(_, ord)| *ord);
    Arc::from(pk_with_ord.into_iter().map(|(c, _)| c).collect::<Vec<_>>())
}

fn column_scalar_kinds<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    arity: usize,
) -> Vec<Option<ScalarKind>> {
    (0..arity)
        .map(|i| {
            #[allow(clippy::cast_possible_truncation)]
            let col_id = i as ColumnId;
            catalog_helpers::column_scalar_kind(database, table_id, col_id)
        })
        .collect()
}

fn scalar_kind_for(kinds: &[Option<ScalarKind>], col: usize) -> Option<ScalarKind> {
    kinds.get(col).copied().flatten()
}

/// Route a wire value into its typed [`Value<SQLite>`] variant using
/// the catalog-declared [`ScalarKind`] as the disambiguator. Shape
/// mismatches (e.g. wire says `Text` but the catalog declared `Int`)
/// resolve to [`Value::Missing`], mirroring the "wrong-shape accessor"
/// contract from the [`crate::backend::CdcEvent`] trait.
fn decode_wire_value(
    wire: WireValue<alloc::string::String, Vec<u8>>,
    kind: Option<ScalarKind>,
) -> Value<SQLite> {
    match wire {
        WireValue::Null => Value::Null,
        WireValue::Integer(i) => match kind {
            Some(ScalarKind::Bool) => Value::Bool(i),
            Some(ScalarKind::Int) | None => Value::Int(i),
            _ => Value::Missing,
        },
        WireValue::Real(f) => match kind {
            Some(ScalarKind::Float) | None => Value::Float(f),
            _ => Value::Missing,
        },
        WireValue::Text(s) => match kind {
            Some(ScalarKind::String) | None => Value::String(s),
            Some(ScalarKind::Uuid) => Value::Uuid(s),
            Some(ScalarKind::Timestamp) => parse_naive_datetime(&s)
                .map_or(Value::Missing, Value::Timestamp),
            Some(ScalarKind::TimestampTz) => parse_datetime_utc(&s)
                .map_or(Value::Missing, Value::TimestampTz),
            Some(ScalarKind::Date) => chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .ok()
                .map_or(Value::Missing, Value::Date),
            Some(ScalarKind::Time) => parse_naive_time(&s).map_or(Value::Missing, Value::Time),
            Some(ScalarKind::Decimal) => <bigdecimal::BigDecimal as core::str::FromStr>::from_str(&s)
                .ok()
                .map_or(Value::Missing, Value::Decimal),
            Some(ScalarKind::Json) => serde_json::from_str(&s)
                .ok()
                .map_or(Value::Missing, Value::Json),
            Some(ScalarKind::Jsonb) => serde_json::from_str(&s)
                .ok()
                .map_or(Value::Missing, Value::Jsonb),
            _ => Value::Missing,
        },
        WireValue::Blob(b) => match kind {
            Some(ScalarKind::Bytes) | None => Value::Bytes(b),
            _ => Value::Missing,
        },
    }
}

fn parse_naive_datetime(s: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}

fn parse_datetime_utc(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

fn parse_naive_time(s: &str) -> Option<chrono::NaiveTime> {
    chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{CdcEvent, Presence, RowKind};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{DiffOps, Insert, PatchDelete, PatchSet, SimpleTable, Update};

    fn orders_db() -> ParserDB {
        ParserDB::parse::<sqlparser::dialect::SQLiteDialect>(
            "CREATE TABLE _pad (id INT);\n\
             CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("orders DDL parses")
    }

    fn orders_table() -> SimpleTable {
        SimpleTable::new("orders", &["id", "amount", "status"], &[0])
    }

    #[test]
    fn typed_sqlite_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SqlitePatchsetEvent>();
    }

    #[test]
    fn typed_sqlite_insert_roundtrip() {
        let db = orders_db();
        let orders = orders_table();
        let patchset = PatchSet::<_, alloc::string::String, Vec<u8>>::new().insert(
            Insert::from(orders)
                .set(0, 7_i64)
                .unwrap()
                .set(1, 250_i64)
                .unwrap()
                .set(2, "paid")
                .unwrap(),
        );
        let bytes: Vec<u8> = patchset.into();
        let events = SqlitePatchsetParser
            .parse_wal_message(&bytes, &db)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(), &[0u16]);
        assert!(ev.changed_columns().is_empty());
        assert_eq!(ev.int_at(RowKind::New, 0), Presence::Present(&7));
        assert_eq!(ev.int_at(RowKind::New, 1), Presence::Present(&250));
        assert_eq!(
            ev.string_at(RowKind::New, 2),
            Presence::Present(&alloc::string::String::from("paid"))
        );
        assert_eq!(ev.int_at(RowKind::Pk, 0), Presence::Present(&7));
        assert_eq!(ev.int_at(RowKind::Pk, 1), Presence::Missing);
        assert_eq!(ev.int_at(RowKind::Old, 0), Presence::Missing);
    }

    #[test]
    fn typed_sqlite_update_carries_changed_columns_and_pk() {
        let db = orders_db();
        let orders = orders_table();
        // Patchset Update: PK must be part of the wire values so the
        // builder can extract it. Set col 0 (PK) to its unchanged value
        // and col 2 (status) to its new value; col 1 (amount) stays
        // Undefined.
        let patchset = PatchSet::<_, alloc::string::String, Vec<u8>>::new().update(
            Update::<_, sqlite_diff_rs::PatchsetFormat, alloc::string::String, Vec<u8>>::from(orders)
                .set(0, 7_i64)
                .unwrap()
                .set(2, "shipped")
                .unwrap(),
        );
        let bytes: Vec<u8> = patchset.into();
        let events = SqlitePatchsetParser
            .parse_wal_message(&bytes, &db)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert_eq!(ev.pk_columns(), &[0u16]);
        let mut changed = ev.changed_columns().to_vec();
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![0u16, 2u16]);
        assert_eq!(ev.int_at(RowKind::New, 0), Presence::Present(&7));
        assert_eq!(ev.int_at(RowKind::New, 1), Presence::Missing);
        assert_eq!(
            ev.string_at(RowKind::New, 2),
            Presence::Present(&alloc::string::String::from("shipped"))
        );
        assert_eq!(ev.int_at(RowKind::Old, 0), Presence::Present(&7));
        assert_eq!(ev.int_at(RowKind::Old, 1), Presence::Missing);
        assert_eq!(ev.int_at(RowKind::Pk, 0), Presence::Present(&7));
    }

    #[test]
    fn typed_sqlite_delete_carries_pk_only() {
        let db = orders_db();
        let orders = orders_table();
        let patchset = PatchSet::<_, alloc::string::String, Vec<u8>>::new()
            .delete(PatchDelete::new(orders, alloc::vec![WireValue::Integer(9)]));
        let bytes: Vec<u8> = patchset.into();
        let events = SqlitePatchsetParser
            .parse_wal_message(&bytes, &db)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert!(ev.changed_columns().is_empty());
        assert_eq!(ev.int_at(RowKind::Old, 0), Presence::Present(&9));
        assert_eq!(ev.int_at(RowKind::Old, 1), Presence::Missing);
        assert_eq!(ev.int_at(RowKind::Pk, 0), Presence::Present(&9));
        assert_eq!(ev.int_at(RowKind::New, 0), Presence::Missing);
    }

    #[test]
    fn typed_sqlite_empty_data_yields_empty_events() {
        let db = orders_db();
        let events = SqlitePatchsetParser
            .parse_wal_message(&[], &db)
            .expect("parse succeeds");
        assert!(events.is_empty());
    }

    #[test]
    fn typed_sqlite_changeset_marker_is_rejected() {
        let db = orders_db();
        let bytes: alloc::vec::Vec<u8> = alloc::vec![b'T', 0, 0];
        let result = SqlitePatchsetParser.parse_wal_message(&bytes, &db);
        assert!(matches!(result, Err(WalParseError::MalformedPayload(_))));
    }

    #[test]
    fn typed_sqlite_dispatches_through_engine() {
        let db = orders_db();
        let mut engine: crate::SubscriptionEngine<
            SqlitePatchsetEvent,
            crate::DefaultIds,
            ParserDB,
        > = crate::SubscriptionEngine::new(db, sqlparser::dialect::SQLiteDialect {});
        engine
            .register(
                crate::SubscriptionRequest::new(99u64, "SELECT * FROM orders WHERE amount > 100")
                    .updated_at_unix_ms(1_704_067_200_000),
            )
            .expect("register subscription");
        let orders = orders_table();
        let patchset = PatchSet::<_, alloc::string::String, Vec<u8>>::new().insert(
            Insert::from(orders)
                .set(0, 7_i64)
                .unwrap()
                .set(1, 250_i64)
                .unwrap()
                .set(2, "paid")
                .unwrap(),
        );
        let bytes: Vec<u8> = patchset.into();
        let events = SqlitePatchsetParser
            .parse_wal_message(&bytes, engine.database())
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let notifs = engine.consumers(&events[0]).expect("dispatch");
        assert_eq!(notifs.inserted(), alloc::vec![99u64]);
    }
}
