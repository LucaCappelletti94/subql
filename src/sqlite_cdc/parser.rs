//! Parser: SQLite session-extension changeset bytes into typed
//! [`SqliteChangesetEvent`] instances.
//!
//! Turns [`sqlite_diff_rs::ParsedDiffSet::Changeset`] into a stream of
//! typed events by:
//!
//! 1. Parsing the wire bytes via `sqlite_diff_rs::ParsedDiffSet::parse`.
//! 2. Rejecting a patchset-marker payload up front (subql only consumes
//!    changesets so `UPDATE` and `DELETE` events carry full old-row
//!    images, which patchsets omit).
//! 3. Resolving each op's table name against the catalog, computing PK
//!    column ordinals from the wire's `pk_flags`, and looking up the
//!    per-column [`crate::backend::ScalarKind`] from the catalog to
//!    route the wire's raw `Value<String, Vec<u8>>` into the correct
//!    [`Value<SQLite>`] variant.
//! 4. Materialising each row image as an arity-sized
//!    `Box<[Value<SQLite>]>` with [`Value::Missing`] for cells the wire
//!    did not carry on that side.

use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;

use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{
    ChangesetOp, ChangesetUpdatePair, ParseError, ParsedDiffSet, TableSchema, Value as WireValue,
};

use super::event::SqliteChangesetEvent;
use crate::backend::{SQLite, ScalarKind, Value};
use crate::wal::{resolve_table, WalParseError, WalParser};
use crate::{catalog_helpers, ColumnId, EventKind, TableId};

/// Parser marker type. Zero-sized; safe to construct freely.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteChangesetParser;

impl<DB: DatabaseLike> WalParser<DB> for SqliteChangesetParser {
    type Checkpoint = crate::NoCheckpoint;
    type Event = SqliteChangesetEvent;

    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let parsed = ParsedDiffSet::parse(data).map_err(convert_parse_error)?;
        let ParsedDiffSet::Changeset(diffset) = parsed else {
            return Err(WalParseError::MalformedPayload(
                "expected SQLite changeset marker 'T', got patchset marker 'P'".to_string(),
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

#[allow(clippy::needless_pass_by_value)]
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

#[allow(clippy::needless_pass_by_value)]
fn op_to_event<DB: DatabaseLike>(
    op: ChangesetOp<'_, TableSchema<alloc::string::String>, alloc::string::String, Vec<u8>>,
    database: &DB,
) -> Result<Option<SqliteChangesetEvent>, WalParseError> {
    let schema = op.table();
    let table_name = schema.name();
    let table_id = resolve_table("", table_name.as_str(), database)?;
    let arity = catalog_helpers::table_arity(database, table_id).ok_or_else(|| {
        WalParseError::UnknownTable {
            schema: alloc::string::String::new(),
            table: table_name.clone(),
        }
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
        ChangesetOp::Insert { values, .. } => {
            if values.len() != arity {
                return Err(WalParseError::ArityMismatch {
                    table_id,
                    wal_count: values.len(),
                    catalog_arity: arity,
                });
            }
            let mut row: Vec<Value<SQLite>> = Vec::with_capacity(arity);
            for (col, wire) in values.iter().enumerate() {
                row.push(decode_wire_value(
                    wire.clone(),
                    scalar_kind_for(&scalar_kinds, col),
                ));
            }
            (
                EventKind::Insert,
                Some(row.into_boxed_slice()),
                None,
                Arc::from(Vec::<ColumnId>::new()),
            )
        }
        ChangesetOp::Update { values, .. } => {
            if values.len() != arity {
                return Err(WalParseError::ArityMismatch {
                    table_id,
                    wal_count: values.len(),
                    catalog_arity: arity,
                });
            }
            let mut new_row: Vec<Value<SQLite>> = Vec::with_capacity(arity);
            let mut old_row: Vec<Value<SQLite>> = Vec::with_capacity(arity);
            let mut changed = Vec::new();
            for (col, pair) in values.iter().enumerate() {
                let kind = scalar_kind_for(&scalar_kinds, col);
                let (old_v, new_v) = decode_update_pair(pair, kind);
                if is_changed(pair) {
                    #[allow(clippy::cast_possible_truncation)]
                    changed.push(col as ColumnId);
                }
                old_row.push(old_v);
                new_row.push(new_v);
            }
            (
                EventKind::Update,
                Some(new_row.into_boxed_slice()),
                Some(old_row.into_boxed_slice()),
                Arc::from(changed),
            )
        }
        ChangesetOp::Delete { old_values, .. } => {
            if old_values.len() != arity {
                return Err(WalParseError::ArityMismatch {
                    table_id,
                    wal_count: old_values.len(),
                    catalog_arity: arity,
                });
            }
            let mut row: Vec<Value<SQLite>> = Vec::with_capacity(arity);
            for (col, wire) in old_values.iter().enumerate() {
                row.push(decode_wire_value(
                    wire.clone(),
                    scalar_kind_for(&scalar_kinds, col),
                ));
            }
            (
                EventKind::Delete,
                None,
                Some(row.into_boxed_slice()),
                Arc::from(Vec::<ColumnId>::new()),
            )
        }
    };

    Ok(Some(SqliteChangesetEvent {
        kind,
        table_id,
        pk_columns,
        changed_columns,
        new_row,
        old_row,
    }))
}

/// Decode a changeset UPDATE column pair into `(old, new)` typed values.
///
/// A slot marked `None` on the wire means "the changeset did not carry
/// this side", which happens for the non-diffed columns of an UPDATE
/// (both slots `None`). We surface that as [`Value::Missing`] on the
/// corresponding side.
fn decode_update_pair(
    pair: &ChangesetUpdatePair<alloc::string::String, Vec<u8>>,
    kind: Option<ScalarKind>,
) -> (Value<SQLite>, Value<SQLite>) {
    let old = pair
        .0
        .as_ref()
        .map_or(Value::Missing, |v| decode_wire_value(v.clone(), kind));
    let new = pair
        .1
        .as_ref()
        .map_or(Value::Missing, |v| decode_wire_value(v.clone(), kind));
    (old, new)
}

/// A column counts as changed when the wire distinguishes its old and
/// new values. Undefined-undefined pairs (unchanged non-PK columns)
/// return false; equal-value pairs (unchanged PK columns) also return
/// false so `changed_columns` stays semantically accurate.
fn is_changed(pair: &ChangesetUpdatePair<alloc::string::String, Vec<u8>>) -> bool {
    match (pair.0.as_ref(), pair.1.as_ref()) {
        (None, None) => false,
        (Some(a), Some(b)) => a != b,
        _ => true,
    }
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
            Some(ScalarKind::Timestamp) => {
                parse_naive_datetime(&s).map_or(Value::Missing, Value::Timestamp)
            }
            Some(ScalarKind::TimestampTz) => {
                parse_datetime_utc(&s).map_or(Value::Missing, Value::TimestampTz)
            }
            Some(ScalarKind::Date) => chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .ok()
                .map_or(Value::Missing, Value::Date),
            Some(ScalarKind::Time) => parse_naive_time(&s).map_or(Value::Missing, Value::Time),
            Some(ScalarKind::Decimal) => {
                <bigdecimal::BigDecimal as core::str::FromStr>::from_str(&s)
                    .ok()
                    .map_or(Value::Missing, Value::Decimal)
            }
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::{CdcEvent, RowKind};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{ChangeDelete, ChangeSet, DiffOps, Insert, SimpleTable, Update};

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
        assert_send_sync::<SqliteChangesetEvent>();
    }

    #[test]
    fn typed_sqlite_insert_roundtrip() {
        let db = orders_db();
        let orders = orders_table();
        let changeset = ChangeSet::<_, alloc::string::String, Vec<u8>>::new().insert(
            Insert::from(orders)
                .set(0, 7_i64)
                .unwrap()
                .set(1, 250_i64)
                .unwrap()
                .set(2, "paid")
                .unwrap(),
        );
        let bytes: Vec<u8> = changeset.into();
        let events = SqliteChangesetParser
            .parse_wal_message(&bytes, &db)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(&db), &[0u16]);
        assert!(ev.changed_columns(&db).is_empty());
        assert_eq!(ev.value_at(&db, RowKind::New, 0), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::New, 1), Value::Int(250));
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 1), Value::Missing);
        assert_eq!(ev.value_at(&db, RowKind::Old, 0), Value::Missing);
    }

    #[test]
    fn typed_sqlite_update_carries_full_old_and_new() {
        let db = orders_db();
        let orders = orders_table();
        // Changeset UPDATE: PK column and the changed non-PK column
        // both carry (old, new). The unchanged non-PK column carries
        // (None, None). `Update::set(col, old, new)` on the changeset
        // builder expresses that directly.
        let changeset = ChangeSet::<_, alloc::string::String, Vec<u8>>::new().update(
            Update::<_, sqlite_diff_rs::ChangesetFormat, alloc::string::String, Vec<u8>>::from(
                orders,
            )
            .set(0, 7_i64, 7_i64)
            .unwrap()
            .set(2, "pending", "shipped")
            .unwrap(),
        );
        let bytes: Vec<u8> = changeset.into();
        let events = SqliteChangesetParser
            .parse_wal_message(&bytes, &db)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Update);
        assert_eq!(ev.pk_columns(&db), &[0u16]);
        // Only the status column truly changed; the PK column pair
        // `(7, 7)` is unchanged and stays out of `changed_columns`.
        let changed = ev.changed_columns(&db);
        assert_eq!(changed, alloc::vec![2u16]);
        assert_eq!(ev.value_at(&db, RowKind::New, 0), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::New, 1), Value::Missing);
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2),
            Value::String("shipped".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Old, 0), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Old, 1), Value::Missing);
        assert_eq!(
            ev.value_at(&db, RowKind::Old, 2),
            Value::String("pending".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0), Value::Int(7));
    }

    #[test]
    fn typed_sqlite_delete_carries_full_old_image() {
        let db = orders_db();
        let orders = orders_table();
        let changeset = ChangeSet::<_, alloc::string::String, Vec<u8>>::new().delete(
            ChangeDelete::from(orders)
                .set(0, 9_i64)
                .unwrap()
                .set(1, 500_i64)
                .unwrap()
                .set(2, "paid")
                .unwrap(),
        );
        let bytes: Vec<u8> = changeset.into();
        let events = SqliteChangesetParser
            .parse_wal_message(&bytes, &db)
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind(), EventKind::Delete);
        assert!(ev.changed_columns(&db).is_empty());
        assert_eq!(ev.value_at(&db, RowKind::Old, 0), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::Old, 1), Value::Int(500));
        assert_eq!(
            ev.value_at(&db, RowKind::Old, 2),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::New, 0), Value::Missing);
    }

    #[test]
    fn typed_sqlite_empty_data_yields_empty_events() {
        let db = orders_db();
        let events = SqliteChangesetParser
            .parse_wal_message(&[], &db)
            .expect("parse succeeds");
        assert!(events.is_empty());
    }

    #[test]
    fn typed_sqlite_patchset_marker_is_rejected() {
        let db = orders_db();
        let bytes: alloc::vec::Vec<u8> = alloc::vec![b'P', 0, 0];
        let result = SqliteChangesetParser.parse_wal_message(&bytes, &db);
        assert!(matches!(result, Err(WalParseError::MalformedPayload(_))));
    }

    #[test]
    fn typed_sqlite_dispatches_through_engine() {
        let db = orders_db();
        let mut engine: crate::SubscriptionEngine<
            SqliteChangesetEvent,
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
        let changeset = ChangeSet::<_, alloc::string::String, Vec<u8>>::new().insert(
            Insert::from(orders)
                .set(0, 7_i64)
                .unwrap()
                .set(1, 250_i64)
                .unwrap()
                .set(2, "paid")
                .unwrap(),
        );
        let bytes: Vec<u8> = changeset.into();
        let events = SqliteChangesetParser
            .parse_wal_message(&bytes, engine.database())
            .expect("parse succeeds");
        assert_eq!(events.len(), 1);
        let notifs = engine.consumers(&events[0]).expect("dispatch");
        assert_eq!(notifs.inserted(), alloc::vec![99u64]);
    }
}
