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
//! 3. Resolving each op's table name against the catalog, asking the wire
//!    schema for its key columns in key order, and looking up the per-column
//!    [`crate::backend::ScalarKind`] from the catalog to route the wire's raw
//!    `Value<String, Vec<u8>>` into the correct [`Value<SQLite>`] variant.
//!
//!    What the wire's bytes mean is answered by `sqlite-diff-rs`, which writes
//!    them: which columns are the key and in what order through
//!    [`sqlite_diff_rs::SchemaWithPK::primary_key_columns`], and whether an
//!    update pair changed through
//!    [`sqlite_diff_rs::ChangesetUpdatePairExt::is_changed`]. This module used
//!    to re-derive both.
//! 4. Materialising each row image as an arity-sized
//!    `Box<[Value<SQLite>]>` with [`Value::Missing`] for cells the wire
//!    did not carry on that side.

use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;

use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{
    ChangesetOp, ChangesetUpdatePair, ChangesetUpdatePairExt as _, ParseError, ParsedDiffSet,
    SchemaWithPK as _, TableSchema, Value as WireValue,
};

use super::event::SqliteChangesetEvent;
use crate::backend::{
    Backend, CustomScalars, SQLite, ScalarFamily, ScalarKind, ScalarKindOf, SqliteJson, Value,
};
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
        // A table header whose nonzero key flag bytes are not the dense
        // ordinals 1 to n. Upstream refuses it rather than hand over a schema
        // it cannot represent faithfully, and for a reader that is a malformed
        // payload: the bytes are not a changeset, and no retry changes that.
        // Named rather than left to the arm below, because `ParseError` is
        // `non_exhaustive` and a future variant deserves its own decision.
        ParseError::InvalidPrimaryKeyFlags { .. } => {
            WalParseError::MalformedPayload(err.to_string())
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
    let table_id = resolve_table::<crate::backend::SQLite, DB>("", table_name.as_str(), database)?;
    let arity = catalog_helpers::table_arity(database, table_id).map_err(|_| {
        WalParseError::UnknownTable {
            schema: alloc::string::String::new(),
            table: table_name.clone(),
        }
    })?;
    // No width check here. The wire's own arity guards below, one per op, ask
    // the same question of the same number: the parser sizes every op's cell
    // list from the table header, so a header wider or narrower than the
    // catalog is exactly a cell list wider or narrower than the catalog, and
    // the refusal names the same two widths either way. Upstream validates the
    // key flags themselves, which is a separate claim and not this one.
    let pk_columns: Arc<[ColumnId]> = {
        #[allow(clippy::cast_possible_truncation)]
        let columns: Vec<ColumnId> = schema
            .primary_key_columns()
            .map(|col| col as ColumnId)
            .collect();
        Arc::from(columns)
    };
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
                row.push(decode_wire_cell(wire, scalar_kind_for(&scalar_kinds, col)));
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
                if pair.is_changed() {
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
                row.push(decode_wire_cell(wire, scalar_kind_for(&scalar_kinds, col)));
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
    kind: Option<ScalarKindOf<SQLite>>,
) -> (Value<SQLite>, Value<SQLite>) {
    let old = pair
        .0
        .as_ref()
        .map_or(Value::Missing, |v| decode_wire_cell(v, kind));
    let new = pair
        .1
        .as_ref()
        .map_or(Value::Missing, |v| decode_wire_cell(v, kind));
    (old, new)
}

fn column_scalar_kinds<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    arity: usize,
) -> Vec<Option<ScalarKindOf<SQLite>>> {
    (0..arity)
        .map(|i| {
            #[allow(clippy::cast_possible_truncation)]
            let col_id = i as ColumnId;
            catalog_helpers::column_scalar_kind::<SQLite, DB>(database, table_id, col_id)
        })
        .collect()
}

fn scalar_kind_for(
    kinds: &[Option<ScalarKindOf<SQLite>>],
    col: usize,
) -> Option<ScalarKindOf<SQLite>> {
    kinds.get(col).copied().flatten()
}

/// Decode a SQLite wire cell whose column may declare a custom type.
///
/// Mirrors [`crate::backend::decode_cell`] for a path that reports a failure
/// as [`Value::Missing`] rather than an error: this stream escalates an
/// undecodable cell to re-execution, so nothing here has an error to carry.
///
/// Borrows the cell, so only the arms that keep a payload pay for one. The
/// four temporal families and `Decimal` parse from a `&str` and drop it, and
/// every wrong-shape refusal drops it too, which is where an owning signature
/// spent an allocation per cell for nothing.
fn decode_wire_cell(
    wire: &WireValue<alloc::string::String, Vec<u8>>,
    kind: Option<ScalarKindOf<SQLite>>,
) -> Value<SQLite> {
    match kind {
        // No test reaches this arm, and none can: `SQLite`'s custom scalars
        // are `NoCustomScalars`, whose `Kind` is uninhabited, so no
        // `ScalarKind::Custom` value exists for this backend. The arm is kept
        // because it is what the shape of `ScalarKind` asks for, and because a
        // SQLite that ever declares a custom scalar needs exactly this.
        Some(ScalarKind::Custom(custom)) => {
            let carrier = <<SQLite as Backend>::Custom as CustomScalars>::carrier(custom);
            let raw = decode_wire_value(wire, Some(carrier));
            raw.as_carried()
                .and_then(|view| {
                    <<SQLite as Backend>::Custom as CustomScalars>::convert(custom, view)
                })
                .map_or(Value::Missing, Value::Custom)
        }
        Some(builtin) => decode_wire_value(wire, builtin.family()),
        None => decode_wire_value(wire, None),
    }
}

/// Route a wire value into its typed [`Value<SQLite>`] variant using
/// the catalog-declared [`ScalarKind`] as the disambiguator. Shape
/// mismatches (e.g. wire says `Text` but the catalog declared `Int`)
/// resolve to [`Value::Missing`], mirroring the "wrong-shape accessor"
/// contract from the [`crate::backend::CdcEvent`] trait.
fn decode_wire_value(
    wire: &WireValue<alloc::string::String, Vec<u8>>,
    kind: Option<ScalarFamily>,
) -> Value<SQLite> {
    match wire {
        WireValue::Null => Value::Null,
        WireValue::Integer(i) => match kind {
            Some(ScalarFamily::Bool) => Value::Bool(*i),
            Some(ScalarFamily::Int) | None => Value::Int(*i),
            Some(ScalarFamily::Json) => Value::Json(SqliteJson::integer(*i)),
            Some(ScalarFamily::Jsonb) => Value::Jsonb(SqliteJson::integer(*i)),
            _ => Value::Missing,
        },
        WireValue::Real(f) => match kind {
            Some(ScalarFamily::Float) | None => Value::Float(*f),
            Some(ScalarFamily::Json) => Value::Json(SqliteJson::real(*f)),
            Some(ScalarFamily::Jsonb) => Value::Jsonb(SqliteJson::real(*f)),
            _ => Value::Missing,
        },
        WireValue::Text(s) => match kind {
            Some(ScalarFamily::String) | None => Value::String(s.clone()),
            Some(ScalarFamily::Uuid) => Value::Uuid(s.clone()),
            Some(ScalarFamily::Timestamp) => {
                sql_scalar_text::parse_timestamp(s).map_or(Value::Missing, Value::Timestamp)
            }
            Some(ScalarFamily::TimestampTz) => {
                sql_scalar_text::parse_timestamp_tz(s).map_or(Value::Missing, Value::TimestampTz)
            }
            Some(ScalarFamily::Date) => {
                sql_scalar_text::parse_date(s).map_or(Value::Missing, Value::Date)
            }
            Some(ScalarFamily::Time) => {
                sql_scalar_text::parse_time(s).map_or(Value::Missing, Value::Time)
            }
            Some(ScalarFamily::Decimal) => {
                sql_scalar_text::parse_decimal(s).map_or(Value::Missing, Value::Decimal)
            }
            Some(ScalarFamily::Json) => Value::Json(SqliteJson::text(s.clone())),
            Some(ScalarFamily::Jsonb) => Value::Jsonb(SqliteJson::text(s.clone())),
            _ => Value::Missing,
        },
        WireValue::Blob(b) => match kind {
            Some(ScalarFamily::Bytes) | None => Value::Bytes(b.clone()),
            Some(ScalarFamily::Json) => Value::Json(SqliteJson::blob(b.clone())),
            Some(ScalarFamily::Jsonb) => Value::Jsonb(SqliteJson::blob(b.clone())),
            _ => Value::Missing,
        },
    }
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
        assert!(
            ev.changed_columns(&db).is_empty(),
            "insert has no old image so no changed columns arise"
        );
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Int(250));
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 1).unwrap(), Value::Missing);
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Missing);
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
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Missing);
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2).unwrap(),
            Value::String("shipped".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Old, 1).unwrap(), Value::Missing);
        assert_eq!(
            ev.value_at(&db, RowKind::Old, 2).unwrap(),
            Value::String("pending".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
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
        assert!(
            ev.changed_columns(&db).is_empty(),
            "delete has no new image so no changed columns arise"
        );
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::Old, 1).unwrap(), Value::Int(500));
        assert_eq!(
            ev.value_at(&db, RowKind::Old, 2).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Missing);
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

    /// The upstream parser refuses a table header whose nonzero key flag bytes
    /// are not the dense ordinals 1 to n, because such a header describes a
    /// key it cannot represent. subql reports that as a malformed payload: the
    /// bytes are not a changeset, and retrying them changes nothing.
    #[test]
    fn typed_sqlite_invalid_primary_key_flags_are_rejected() {
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
        let mut bytes: Vec<u8> = changeset.into();
        // A table header carries one flag byte per column, immediately before
        // the table name, and the key column's byte is its position within the
        // key counting from one. `orders` keys on its first column, so the
        // three bytes before the name read `[1, 0, 0]`. Rewriting the one to a
        // two leaves a key of width one whose only ordinal is two, which is
        // exactly the sequence the parser refuses.
        let name_at = bytes
            .windows(6)
            .position(|window| window == b"orders")
            .expect("the header carries the table name");
        let flags_at = name_at - 3;
        assert_eq!(
            &bytes[flags_at..name_at],
            &[1, 0, 0],
            "the flag bytes sit where this test expects them"
        );
        bytes[flags_at] = 2;

        let refused = SqliteChangesetParser
            .parse_wal_message(&bytes, &db)
            .expect_err("a header with non-dense key ordinals is refused");
        let WalParseError::MalformedPayload(message) = refused else {
            panic!("the refusal is a malformed payload, got {refused:?}");
        };
        assert!(
            message.contains("primary-key flags") && message.contains("orders"),
            "the refusal names what is wrong and where, got {message:?}"
        );
    }

    /// A composite key declared in an order the columns do not follow. The
    /// wire's flag byte carries each key column's position within the key, so
    /// `(status, id)` must read back as `[2, 0]`. Reading the flags as
    /// booleans, or sorting the result by column index, answers `[0, 2]` and
    /// pairs every key value with the wrong column.
    #[test]
    fn typed_sqlite_composite_key_keeps_its_declared_order() {
        let db = ParserDB::parse::<sqlparser::dialect::SQLiteDialect>(
            "CREATE TABLE lots (id INTEGER, amount INT, status TEXT, PRIMARY KEY (status, id));",
        )
        .expect("lots DDL parses");
        // Key order `(status, id)`, which is columns 2 then 0.
        let lots = SimpleTable::new("lots", &["id", "amount", "status"], &[2, 0]);
        let changeset = ChangeSet::<_, alloc::string::String, Vec<u8>>::new().insert(
            Insert::from(lots)
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
        let ev = &events[0];

        assert_eq!(
            ev.pk_columns(&db),
            &[2u16, 0u16],
            "the key columns come back in key order, not column order"
        );
        // The key image is addressed by column, not by key position, so these
        // read the two key columns and the non-key one between them.
        assert_eq!(
            ev.value_at(&db, RowKind::Pk, 2).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 1).unwrap(), Value::Missing);
    }

    /// A wire table wider than the catalog's is refused. The upstream parser
    /// validates the key flags but knows nothing about this catalog, so this
    /// is the check that catches a schema change between recording a session
    /// and reading it.
    #[test]
    fn typed_sqlite_wire_wider_than_the_catalog_is_refused() {
        let db = ParserDB::parse::<sqlparser::dialect::SQLiteDialect>(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INT);",
        )
        .expect("narrow orders DDL parses");
        // Three columns on the wire against two in the catalog, which is what
        // an `ALTER TABLE ADD COLUMN` after the recording looks like.
        let changeset = ChangeSet::<_, alloc::string::String, Vec<u8>>::new().insert(
            Insert::from(orders_table())
                .set(0, 7_i64)
                .unwrap()
                .set(1, 250_i64)
                .unwrap()
                .set(2, "paid")
                .unwrap(),
        );
        let bytes: Vec<u8> = changeset.into();

        let refused = SqliteChangesetParser
            .parse_wal_message(&bytes, &db)
            .expect_err("a wire table of another width is refused");
        assert!(
            matches!(
                refused,
                WalParseError::ArityMismatch {
                    wal_count: 3,
                    catalog_arity: 2,
                    ..
                }
            ),
            "the refusal names both widths, got {refused:?}"
        );
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

    /// A NULL cell is NULL whatever the column declares. It is the one wire
    /// shape every kind accepts, and the one shape that must not be confused
    /// with [`Value::Missing`], which means the wire carried nothing at all.
    #[test]
    fn a_null_cell_stays_null_whatever_the_column_declares() {
        for kind in [
            None,
            Some(ScalarFamily::Int),
            Some(ScalarFamily::String),
            Some(ScalarFamily::Json),
            Some(ScalarFamily::Timestamp),
        ] {
            assert_eq!(
                decode_wire_value(&WireValue::Null, kind),
                Value::Null,
                "a NULL cell on a {kind:?} column"
            );
        }
    }

    /// An integer cell routes by the declared kind. SQLite has no boolean
    /// storage class, so a boolean column arrives as an integer and keeps it:
    /// `Value::Bool` carries an `i64` on this backend, and truthiness is the
    /// reader's business. A column whose kind no integer can inhabit answers
    /// [`Value::Missing`], which is the wrong-shape contract rather than a
    /// silent coercion.
    #[test]
    fn an_integer_cell_routes_by_the_declared_kind() {
        assert_eq!(
            decode_wire_value(&WireValue::Integer(1), Some(ScalarFamily::Bool)),
            Value::Bool(1)
        );
        assert_eq!(
            decode_wire_value(&WireValue::Integer(0), Some(ScalarFamily::Bool)),
            Value::Bool(0)
        );
        assert_eq!(
            decode_wire_value(&WireValue::Integer(7), Some(ScalarFamily::Int)),
            Value::Int(7)
        );
        assert_eq!(
            decode_wire_value(&WireValue::Integer(7), None),
            Value::Int(7),
            "an undeclared column keeps the wire's own shape"
        );
        for refused in [
            ScalarFamily::String,
            ScalarFamily::Bytes,
            ScalarFamily::Uuid,
            ScalarFamily::Timestamp,
            ScalarFamily::Decimal,
        ] {
            assert_eq!(
                decode_wire_value(&WireValue::Integer(7), Some(refused)),
                Value::Missing,
                "an integer on a {refused:?} column is the wrong shape"
            );
        }
    }

    /// A real cell routes the same way: its own kind, or JSON's storage
    /// classes, or a refusal.
    #[test]
    fn a_real_cell_routes_by_the_declared_kind() {
        assert_eq!(
            decode_wire_value(&WireValue::Real(1.5), Some(ScalarFamily::Float)),
            Value::Float(1.5)
        );
        assert_eq!(
            decode_wire_value(&WireValue::Real(1.5), None),
            Value::Float(1.5)
        );
        for refused in [
            ScalarFamily::Int,
            ScalarFamily::Bool,
            ScalarFamily::String,
            ScalarFamily::Decimal,
        ] {
            assert_eq!(
                decode_wire_value(&WireValue::Real(1.5), Some(refused)),
                Value::Missing,
                "a real on a {refused:?} column is the wrong shape"
            );
        }
    }

    /// A text cell has the most destinations, so it has the most ways to be
    /// wrong. A UUID column keeps the text verbatim, because SQLite stores a
    /// UUID as text and this backend's UUID type IS that text: nothing here
    /// validates it. A decimal column parses, and refuses what does not parse.
    #[test]
    fn a_text_cell_routes_by_the_declared_kind() {
        assert_eq!(
            decode_wire_value(&WireValue::Text("hello".into()), Some(ScalarFamily::String)),
            Value::String("hello".into())
        );
        assert_eq!(
            decode_wire_value(&WireValue::Text("hello".into()), None),
            Value::String("hello".into())
        );
        assert_eq!(
            decode_wire_value(
                &WireValue::Text("not a uuid at all".into()),
                Some(ScalarFamily::Uuid)
            ),
            Value::Uuid("not a uuid at all".into()),
            "a UUID column carries the stored text as it stands"
        );
        assert_eq!(
            decode_wire_value(
                &WireValue::Text("12.50".into()),
                Some(ScalarFamily::Decimal)
            ),
            Value::Decimal("12.50".parse().expect("a decimal literal parses"))
        );
        assert_eq!(
            decode_wire_value(
                &WireValue::Text("twelve".into()),
                Some(ScalarFamily::Decimal)
            ),
            Value::Missing,
            "a decimal column refuses text that is not a number"
        );
        for refused in [ScalarFamily::Int, ScalarFamily::Bool, ScalarFamily::Bytes] {
            assert_eq!(
                decode_wire_value(&WireValue::Text("hello".into()), Some(refused)),
                Value::Missing,
                "text on a {refused:?} column is the wrong shape"
            );
        }
    }

    /// A blob cell routes to bytes, to JSON's blob storage class, or to a
    /// refusal.
    #[test]
    fn a_blob_cell_routes_by_the_declared_kind() {
        assert_eq!(
            decode_wire_value(&WireValue::Blob(vec![1, 2]), Some(ScalarFamily::Bytes)),
            Value::Bytes(vec![1, 2])
        );
        assert_eq!(
            decode_wire_value(&WireValue::Blob(vec![1, 2]), None),
            Value::Bytes(vec![1, 2])
        );
        for refused in [
            ScalarFamily::String,
            ScalarFamily::Int,
            ScalarFamily::Uuid,
            ScalarFamily::Decimal,
        ] {
            assert_eq!(
                decode_wire_value(&WireValue::Blob(vec![1, 2]), Some(refused)),
                Value::Missing,
                "a blob on a {refused:?} column is the wrong shape"
            );
        }
    }
    #[test]
    fn temporal_text_cell_maps_representative_values() {
        use chrono::{DateTime, Utc};
        assert!(matches!(
            decode_wire_value(
                &WireValue::Text("2026-01-01 00:00:00".into()),
                Some(ScalarFamily::Timestamp)
            ),
            Value::Timestamp(_)
        ));
        let expected: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            decode_wire_value(
                &WireValue::Text("2025-12-31 22:00:00-02".into()),
                Some(ScalarFamily::TimestampTz)
            ),
            Value::TimestampTz(expected)
        );
        assert!(matches!(
            decode_wire_value(
                &WireValue::Text("2026-01-01".into()),
                Some(ScalarFamily::Date)
            ),
            Value::Date(_)
        ));
        assert!(matches!(
            decode_wire_value(
                &WireValue::Text("12:34:56.789".into()),
                Some(ScalarFamily::Time)
            ),
            Value::Time(_)
        ));
    }

    #[test]
    fn temporal_text_cell_rejects_key_boundaries() {
        assert_eq!(
            decode_wire_value(
                &WireValue::Text("2026-01-01 00:00:00".into()),
                Some(ScalarFamily::TimestampTz)
            ),
            Value::Missing
        );
        assert_eq!(
            decode_wire_value(
                &WireValue::Text("2026-01-01 00:00:00+00".into()),
                Some(ScalarFamily::Timestamp)
            ),
            Value::Missing
        );
    }

    #[test]
    fn json_like_columns_preserve_every_sqlite_storage_class() {
        use crate::backend::SqliteJsonStorage;

        let values = [
            (
                decode_wire_value(
                    &WireValue::Text(String::from("{ \"a\": 1 }")),
                    Some(ScalarFamily::Json),
                ),
                SqliteJsonStorage::Text(String::from("{ \"a\": 1 }")),
            ),
            (
                decode_wire_value(&WireValue::Integer(1), Some(ScalarFamily::Json)),
                SqliteJsonStorage::Integer(1),
            ),
            (
                decode_wire_value(&WireValue::Real(1.5), Some(ScalarFamily::Json)),
                SqliteJsonStorage::Real(1.5),
            ),
            (
                decode_wire_value(&WireValue::Blob(vec![1, 2]), Some(ScalarFamily::Json)),
                SqliteJsonStorage::Blob(vec![1, 2]),
            ),
        ];
        for (value, expected) in values {
            let Value::Json(value) = value else {
                panic!("JSON columns keep their storage class")
            };
            assert_eq!(value.storage(), &expected);
        }
    }
}
