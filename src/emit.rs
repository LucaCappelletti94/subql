//! Outbound CDC emission: fold ecosystem CDC events into a
//! [`sqlite-diff-rs`](sqlite_diff_rs) patchset.
//!
//! This is the server-to-client half of the round trip. subql ingests
//! generic CDC and emits a [`PatchsetFormat`] patchset directed at the
//! subscribed SQLite clients, which apply it into their local replica.
//! The apply half (client-to-server) lives in [`crate::patchset`].
//!
//! # How it works
//!
//! `sqlite-diff-rs` exposes a schema-driven `digest` entry point:
//! `DiffSetBuilder::digest(event, schema, adapter)` folds one CDC event
//! into a builder, resolving the event's table through a
//! [`WireSchema`] and decoding each column payload through a
//! [`WireAdapter`](sqlite_diff_rs::WireAdapter). The schema declares one
//! [`WireType`] per column, a source-independent semantic type that
//! selects the decoder. subql's catalog is [`ScalarKind`]-based, and
//! [`scalar_kind_to_wire_type`] maps one to the other, so a single
//! [`WireCatalog`] over any [`DatabaseLike`] drives every wire source.
//!
//! # Scope
//!
//! Three vehicles are wired over one source-agnostic schema side
//! ([`WireCatalog`], [`WireTable`]): [`wal2json_patchset`] digests
//! subql-owned `sqlite_diff_rs::wal2json` events, [`maxwell_patchset`]
//! digests `sqlite_diff_rs::maxwell` events, and (behind the
//! `pgoutput-emit` feature) [`pgoutput_patchset`] digests pg_walstream
//! `ChangeEvent`s. Each differs only in its decoder registry.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::maxwell::{
    ConversionError as MaxwellConversionError, Maxwell, Message as MaxwellMessage,
};
#[cfg(feature = "pgoutput-emit")]
use sqlite_diff_rs::pg_walstream::{
    ChangeEvent as PgChangeEvent, ConversionError as PgConversionError, PgWalstream,
};
use sqlite_diff_rs::wal2json::{ConversionError, Wal2Json};
use sqlite_diff_rs::{
    ColumnNames, Digestable, DynTable, IndexableValues, NamedColumns, PatchSet, PatchsetFormat,
    SchemaWithPK, SimpleTable, TypeMap, UuidBlob16Decoder, Value as WireValue, WireColumnTypes,
    WireSchema, WireType,
};

use crate::backend::ScalarKind;
use crate::catalog_helpers;
use crate::types::{ColumnId, TableId};

/// Map a subql catalog [`ScalarKind`] to the source-independent
/// [`WireType`] that selects a decoder in `sqlite-diff-rs`'s `digest`.
///
/// The mapping is total: every `ScalarKind` has a `WireType`. subql has
/// no interval scalar, so [`WireType::Interval`] is never produced here.
#[must_use]
pub const fn scalar_kind_to_wire_type(kind: ScalarKind) -> WireType {
    match kind {
        ScalarKind::Bool => WireType::Bool,
        ScalarKind::Int => WireType::Int,
        ScalarKind::Float => WireType::Real,
        ScalarKind::String => WireType::Text,
        ScalarKind::Bytes => WireType::Bytes,
        ScalarKind::Uuid => WireType::Uuid,
        ScalarKind::Timestamp => WireType::Timestamp,
        ScalarKind::TimestampTz => WireType::TimestampTz,
        ScalarKind::Date => WireType::Date,
        ScalarKind::Time => WireType::Time,
        ScalarKind::Decimal => WireType::Decimal,
        ScalarKind::Json => WireType::Json,
        ScalarKind::Jsonb => WireType::Jsonb,
    }
}

/// One subql catalog table viewed as a `sqlite-diff-rs` schema entry.
///
/// Wraps [`SimpleTable`] for the name, column, and primary-key surface
/// and adds one [`WireType`] per column so `digest` selects a decoder
/// without any source-native type key. This is the table type a
/// [`WireCatalog`] resolves each CDC event against.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WireTable {
    inner: SimpleTable,
    wire_types: Vec<WireType>,
}

impl DynTable for WireTable {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn number_of_columns(&self) -> usize {
        self.inner.number_of_columns()
    }

    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.inner.write_pk_flags(buf);
    }
}

impl SchemaWithPK for WireTable {
    fn number_of_primary_keys(&self) -> usize {
        self.inner.number_of_primary_keys()
    }

    fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
        self.inner.primary_key_index(col_idx)
    }

    fn extract_pk<S: Clone, B: Clone>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> Vec<WireValue<S, B>> {
        self.inner.extract_pk(values)
    }
}

impl NamedColumns for WireTable {
    fn column_index(&self, column_name: &str) -> Option<usize> {
        self.inner.column_index(column_name)
    }
}

impl WireColumnTypes for WireTable {
    fn column_type(&self, column_index: usize) -> WireType {
        // The vector holds one entry per column, so an in-range index
        // always hits. The fallback only guards a caller that asks past
        // the column count, which the crate's `digest` never does.
        self.wire_types
            .get(column_index)
            .copied()
            .unwrap_or(WireType::Text)
    }
}

impl ColumnNames for WireTable {
    fn column_name(&self, index: usize) -> Option<&str> {
        self.inner.column_name(index)
    }
}

/// A subql catalog exposed as a `sqlite-diff-rs` [`WireSchema`].
///
/// Holds one owned [`WireTable`] per catalog table, keyed by bare table
/// name (the name a wal2json message carries in its `table` field).
/// Built once from a [`DatabaseLike`] catalog and reused across a batch
/// of events. When two schemas declare the same bare table name the
/// last one enumerated wins, which matches the bare-name lookup the wire
/// events perform.
#[derive(Debug, Clone)]
pub struct WireCatalog {
    tables: HashMap<String, WireTable>,
}

impl WireCatalog {
    /// Build a catalog over every table in `database`.
    ///
    /// Columns whose declared SQL type does not map to a [`ScalarKind`]
    /// fall back to [`WireType::Text`], the lossless affinity for an
    /// unmodeled type on the SQLite side.
    #[must_use]
    pub fn from_database<DB: DatabaseLike>(database: &DB) -> Self {
        let mut tables = HashMap::new();
        for index in 0..database.number_of_tables() {
            let Ok(table_id) = TableId::try_from(index) else {
                break;
            };
            let Some(wire_table) = build_wire_table(database, table_id) else {
                continue;
            };
            tables.insert(wire_table.name().to_string(), wire_table);
        }
        Self { tables }
    }

    /// Number of resolved tables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// True when no table resolved.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl WireSchema for WireCatalog {
    type Table = WireTable;

    fn get(&self, table_name: &str) -> Option<&WireTable> {
        self.tables.get(table_name)
    }
}

/// Build one [`WireTable`] for `table_id`, or `None` when the table id
/// or any of its columns cannot be resolved.
fn build_wire_table<DB: DatabaseLike>(database: &DB, table_id: TableId) -> Option<WireTable> {
    let inner = catalog_helpers::simple_table(database, table_id)?;
    let arity = inner.number_of_columns();
    let mut wire_types: Vec<WireType> = Vec::with_capacity(arity);
    for ordinal in 0..arity {
        let column_id = ColumnId::try_from(ordinal).ok()?;
        let wire_type = catalog_helpers::column_scalar_kind(database, table_id, column_id)
            .map_or(WireType::Text, scalar_kind_to_wire_type);
        wire_types.push(wire_type);
    }
    Some(WireTable { inner, wire_types })
}

/// The wal2json decoder registry subql feeds to `digest`.
///
/// `sqlite-diff-rs` keys the registry on [`WireType`], so one
/// `defaults()` table decodes every wal2json column subql models. subql
/// overrides the UUID decoder to [`UuidBlob16Decoder`] so a `UUID`
/// column lowers to a compact 16-byte `Value::Blob` rather than the
/// 36-char text form. The SQLite client stores UUIDs as BLOBs, and
/// `PgAdapter` re-binds the blob to a native Postgres `UUID` on the way
/// back.
#[must_use]
pub fn wal2json_adapter() -> TypeMap<Wal2Json, String, Vec<u8>> {
    TypeMap::defaults().with(WireType::Uuid, UuidBlob16Decoder)
}

/// Fold a batch of wal2json CDC events into one [`PatchsetFormat`]
/// patchset builder over `database`.
///
/// Each event is resolved to its table through a [`WireCatalog`] built
/// from `database` and decoded through [`wal2json_adapter`].
/// Transaction-boundary messages (begin, commit, message, truncate)
/// digest to no-ops. The builder is a [`PatchSet`], so it both serializes
/// to wire bytes via `build()` (see [`wal2json_patchset`]) and applies
/// directly through [`crate::patchset`]'s `SqliteAdapter`.
///
/// Accepts either `MessageV2` (wal2json v2) or `ChangeV1` (v1) events,
/// since both implement the wal2json `Digestable` contract.
///
/// # Errors
///
/// Propagates [`ConversionError`] when an event names a table or column
/// absent from `database`, or a decoder rejects a column payload.
pub fn wal2json_patchset_builder<DB, E>(
    database: &DB,
    events: &[E],
) -> Result<PatchSet<WireTable, String, Vec<u8>>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        PatchsetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    let catalog = WireCatalog::from_database(database);
    let adapter = wal2json_adapter();
    let mut builder = PatchSet::<WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(event, &catalog, &adapter)?;
    }
    Ok(builder)
}

/// Fold a batch of wal2json CDC events into one [`PatchsetFormat`]
/// patchset over `database`, returning the wire bytes.
///
/// A thin wrapper over [`wal2json_patchset_builder`] that serializes the
/// builder with `build()`. The returned bytes carry the SQLite
/// session-extension patchset marker and apply through any
/// `sqlite-diff-rs` consumer.
///
/// # Errors
///
/// Propagates [`ConversionError`], as [`wal2json_patchset_builder`] does.
pub fn wal2json_patchset<DB, E>(database: &DB, events: &[E]) -> Result<Vec<u8>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        PatchsetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    Ok(wal2json_patchset_builder(database, events)?.build())
}

/// The pgoutput decoder registry subql feeds to `digest`.
///
/// Requires the `pgoutput-emit` feature. Overrides UUID to
/// [`UuidBlob16Decoder`] so a `UUID` column lowers to a compact 16-byte
/// blob, matching [`wal2json_adapter`] and the SQLite client's storage.
#[cfg(feature = "pgoutput-emit")]
#[must_use]
pub fn pgoutput_adapter() -> TypeMap<PgWalstream, String, Vec<u8>> {
    TypeMap::defaults().with(WireType::Uuid, UuidBlob16Decoder)
}

/// Fold a batch of pgoutput `ChangeEvent`s into one [`PatchsetFormat`]
/// patchset builder over `database`, mirroring [`wal2json_patchset_builder`].
///
/// `sqlite-diff-rs` implements the pg `Digestable` on
/// `pg_walstream::EventType`, so each event's `event_type` folds into the
/// builder over the same source-agnostic [`WireCatalog`]. Non-row events
/// (begin, commit, relation, and similar) digest to no-ops.
///
/// # Errors
///
/// Propagates the pg source `ConversionError` when an event names a table
/// or column absent from `database`, or a decoder rejects a payload.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_patchset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[PgChangeEvent],
) -> Result<PatchSet<WireTable, String, Vec<u8>>, PgConversionError> {
    let catalog = WireCatalog::from_database(database);
    let adapter = pgoutput_adapter();
    let mut builder = PatchSet::<WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(&event.event_type, &catalog, &adapter)?;
    }
    Ok(builder)
}

/// Serialize [`pgoutput_patchset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the pg source `ConversionError`.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_patchset<DB: DatabaseLike>(
    database: &DB,
    events: &[PgChangeEvent],
) -> Result<Vec<u8>, PgConversionError> {
    Ok(pgoutput_patchset_builder(database, events)?.build())
}

/// The Maxwell decoder registry subql feeds to `digest`.
///
/// MySQL has no native UUID type, so a UUID stored as `BINARY(16)`
/// classifies as [`ScalarKind::Bytes`] and rides [`WireType::Bytes`],
/// which the default `MySqlBinaryDecoder` base64-decodes to a compact
/// 16-byte `Value::Blob`. That matches the SQLite client's blob storage
/// and the way subql's `MysqlAdapter` rebinds the blob as MySQL binary,
/// so the defaults need no override.
#[must_use]
pub fn maxwell_adapter() -> TypeMap<Maxwell, String, Vec<u8>> {
    TypeMap::defaults()
}

/// Fold a batch of Maxwell `Message`s into one [`PatchsetFormat`]
/// patchset builder over `database`, mirroring [`wal2json_patchset_builder`].
///
/// Each row message folds into the builder over the same source-agnostic
/// [`WireCatalog`]. Control messages carry no row data and should be
/// dropped before this call (subql's `parse_maxwell` does so).
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError` when a message names a
/// table or column absent from `database`, or a decoder rejects a payload.
pub fn maxwell_patchset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<PatchSet<WireTable, String, Vec<u8>>, MaxwellConversionError> {
    let catalog = WireCatalog::from_database(database);
    let adapter = maxwell_adapter();
    let mut builder = PatchSet::<WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(event, &catalog, &adapter)?;
    }
    Ok(builder)
}

/// Serialize [`maxwell_patchset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError`.
pub fn maxwell_patchset<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<Vec<u8>, MaxwellConversionError> {
    Ok(maxwell_patchset_builder(database, events)?.build())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::wal2json::parse_v2;
    use sqlite_diff_rs::{ParsedDiffSet, PatchsetOp};
    use sqlparser::dialect::PostgreSqlDialect;

    fn orders_db() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .unwrap()
    }

    fn parse_patchset(bytes: &[u8]) -> ParsedDiffSet {
        let parsed = ParsedDiffSet::parse(bytes).unwrap();
        assert!(parsed.is_patchset(), "expected the SQLite patchset marker");
        parsed
    }

    #[test]
    fn scalar_kind_maps_cover_every_kind() {
        // A change to `ScalarKind` must be reflected here. The match in
        // `scalar_kind_to_wire_type` is exhaustive, so this only pins the
        // representative mappings the wal2json decoders depend on.
        assert_eq!(scalar_kind_to_wire_type(ScalarKind::Uuid), WireType::Uuid);
        assert_eq!(scalar_kind_to_wire_type(ScalarKind::Float), WireType::Real);
        assert_eq!(scalar_kind_to_wire_type(ScalarKind::Jsonb), WireType::Jsonb);
    }

    #[test]
    fn wire_catalog_resolves_declared_table() {
        let db = orders_db();
        let catalog = WireCatalog::from_database(&db);
        assert_eq!(catalog.len(), 1);
        let table = catalog.get("orders").expect("orders resolves");
        assert_eq!(table.number_of_columns(), 3);
        assert_eq!(table.column_type(0), WireType::Int);
        assert_eq!(table.column_type(2), WireType::Text);
        assert!(catalog.get("missing").is_none());
    }

    #[test]
    fn insert_emits_patchset_insert_with_row_values() {
        let db = orders_db();
        let line = r#"{"action":"I","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}]}"#;
        let msg = parse_v2(line).unwrap();

        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let parsed = parse_patchset(&bytes);
        let ParsedDiffSet::Patchset(diff) = parsed else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Insert { table, values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        assert_eq!(table.name(), "orders");
        assert_eq!(
            values.to_vec(),
            vec![
                WireValue::Integer(1),
                WireValue::Integer(100),
                WireValue::Text("new".to_string()),
            ]
        );
    }

    #[test]
    fn update_emits_patchset_update_with_pk_and_new_values() {
        let db = orders_db();
        let line = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":250},{"name":"status","type":"text","value":"shipped"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(line).unwrap();

        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let parsed = parse_patchset(&bytes);
        let ParsedDiffSet::Patchset(diff) = parsed else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Update { table, pk, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        assert_eq!(table.name(), "orders");
        assert_eq!(pk.to_vec(), vec![WireValue::Integer(1)]);
        // The new non-PK values ride along as the patchset's new image.
        let new_values: Vec<_> = ops[0]
            .update_new_values()
            .expect("update carries new values");
        assert!(new_values
            .iter()
            .any(|v| matches!(v, Some(WireValue::Text(s)) if s == "shipped")));
    }

    #[test]
    fn delete_emits_patchset_delete_with_pk_only() {
        let db = orders_db();
        let line = r#"{"action":"D","schema":"public","table":"orders","identity":[{"name":"id","type":"integer","value":7}]}"#;
        let msg = parse_v2(line).unwrap();

        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let parsed = parse_patchset(&bytes);
        let ParsedDiffSet::Patchset(diff) = parsed else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Delete { table, pk, .. } = &ops[0] else {
            panic!("expected a delete op, got {:?}", ops[0]);
        };
        assert_eq!(table.name(), "orders");
        assert_eq!(pk.to_vec(), vec![WireValue::Integer(7)]);
    }

    #[test]
    fn transaction_boundaries_digest_to_empty_patchset() {
        let db = orders_db();
        let begin = parse_v2(r#"{"action":"B"}"#).unwrap();
        let commit = parse_v2(r#"{"action":"C"}"#).unwrap();

        // A batch of only transaction boundaries carries no row ops, so
        // the builder is empty and `build()` emits zero bytes.
        let bytes = wal2json_patchset(&db, &[begin, commit]).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn unknown_table_is_an_error() {
        let db = orders_db();
        let line = r#"{"action":"I","schema":"public","table":"absent","columns":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(line).unwrap();

        let err = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap_err();
        assert!(matches!(err, ConversionError::TableNotFound(name) if name == "absent"));
    }
}
