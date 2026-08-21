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
//! [`WireAdapter`]. The schema declares one
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
//!
//! Each vehicle also has a `*_changeset` variant (for example
//! [`wal2json_changeset`]) that emits the changeset format, which records
//! the old and new value of every changed column and so can carry a
//! primary-key-changing UPDATE that a patchset cannot.

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
    ChangeSet, ChangesetFormat, ColumnNames, DiffOps, Digestable, DynTable, IndexableValues,
    Insert, NamedColumns, PatchSet, PatchsetFormat, PgBinary, PgBinaryColumn, SchemaWithPK,
    SimpleTable, TypeMap, UuidBlob16Decoder, Value as WireValue, WireAdapter, WireColumnTypes,
    WireSchema, WireType,
};

use crate::backend::{BuiltinKind, ScalarKind};
use crate::catalog_helpers;
use crate::types::{ColumnId, TableId};

/// Map a subql catalog [`ScalarKind`] to the source-independent
/// [`WireType`] that selects a decoder in `sqlite-diff-rs`'s `digest`.
///
/// The mapping is total: every `ScalarKind` has a `WireType`. subql has
/// no interval scalar, so [`WireType::Interval`] is never produced here.
#[must_use]
pub const fn scalar_kind_to_wire_type(kind: BuiltinKind) -> WireType {
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
        let wire_type = catalog_helpers::column_builtin_kind(database, table_id, column_id)
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

/// Fold a batch of wal2json CDC events into one [`ChangesetFormat`]
/// changeset builder over `database`, the changeset counterpart to
/// [`wal2json_patchset_builder`].
///
/// A changeset records the old and new value of every changed column, so
/// unlike a patchset it can carry a primary-key-changing UPDATE. The old
/// values come from each event's old-row image (`identity`), so full
/// fidelity needs `REPLICA IDENTITY FULL`. Accepts either `MessageV2` or
/// `ChangeV1`.
///
/// # Errors
///
/// Propagates [`ConversionError`] when an event names a table or column
/// absent from `database`, or a decoder rejects a column payload.
pub fn wal2json_changeset_builder<DB, E>(
    database: &DB,
    events: &[E],
) -> Result<ChangeSet<WireTable, String, Vec<u8>>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        ChangesetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    let catalog = WireCatalog::from_database(database);
    let adapter = wal2json_adapter();
    let mut builder = ChangeSet::<WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(event, &catalog, &adapter)?;
    }
    Ok(builder)
}

/// Fold a batch of wal2json CDC events into one [`ChangesetFormat`]
/// changeset over `database`, returning the wire bytes.
///
/// A thin wrapper over [`wal2json_changeset_builder`] that serializes with
/// `build()`.
///
/// # Errors
///
/// Propagates [`ConversionError`], as [`wal2json_changeset_builder`] does.
pub fn wal2json_changeset<DB, E>(database: &DB, events: &[E]) -> Result<Vec<u8>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        ChangesetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    Ok(wal2json_changeset_builder(database, events)?.build())
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

/// Fold a batch of pgoutput `ChangeEvent`s into one [`ChangesetFormat`]
/// changeset builder over `database`, the changeset counterpart to
/// [`pgoutput_patchset_builder`].
///
/// A changeset records old and new values, so it can carry a
/// primary-key-changing UPDATE. The old values come from each event's old
/// tuple, so full fidelity needs `REPLICA IDENTITY FULL`.
///
/// # Errors
///
/// Propagates the pg source `ConversionError` when an event names a table
/// or column absent from `database`, or a decoder rejects a payload.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_changeset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[PgChangeEvent],
) -> Result<ChangeSet<WireTable, String, Vec<u8>>, PgConversionError> {
    let catalog = WireCatalog::from_database(database);
    let adapter = pgoutput_adapter();
    let mut builder = ChangeSet::<WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(&event.event_type, &catalog, &adapter)?;
    }
    Ok(builder)
}

/// Serialize [`pgoutput_changeset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the pg source `ConversionError`.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_changeset<DB: DatabaseLike>(
    database: &DB,
    events: &[PgChangeEvent],
) -> Result<Vec<u8>, PgConversionError> {
    Ok(pgoutput_changeset_builder(database, events)?.build())
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

/// Fold a batch of Maxwell `Message`s into one [`ChangesetFormat`]
/// changeset builder over `database`, the changeset counterpart to
/// [`maxwell_patchset_builder`].
///
/// A changeset records old and new values, so it can carry a
/// primary-key-changing UPDATE. The old values come from each message's
/// `old` field, which MySQL populates for the changed columns.
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError` when a message names a
/// table or column absent from `database`, or a decoder rejects a payload.
pub fn maxwell_changeset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<ChangeSet<WireTable, String, Vec<u8>>, MaxwellConversionError> {
    let catalog = WireCatalog::from_database(database);
    let adapter = maxwell_adapter();
    let mut builder = ChangeSet::<WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(event, &catalog, &adapter)?;
    }
    Ok(builder)
}

/// Serialize [`maxwell_changeset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError`.
pub fn maxwell_changeset<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<Vec<u8>, MaxwellConversionError> {
    Ok(maxwell_changeset_builder(database, events)?.build())
}

/// The `PgBinary` decoder registry for snapshot reads.
///
/// The snapshot counterpart to [`pgoutput_adapter`]. Unlike that one it
/// needs no override: the `PgBinary` `defaults()` already map
/// [`WireType::Uuid`] to [`UuidBlob16Decoder`], so a `UUID` column lowers
/// to a compact 16-byte blob, byte-identical to the pgoutput CDC path. The
/// named seam still earns its place as the single point the two policies
/// could ever diverge.
#[must_use]
pub fn pgbinary_adapter() -> TypeMap<PgBinary, String, Vec<u8>> {
    TypeMap::defaults()
}

/// Encode Postgres binary result rows for `table` into an insert-patchset
/// builder, the snapshot counterpart to [`pgoutput_patchset_builder`].
///
/// `column_names` names the result columns in order. Each row in `rows`
/// carries one `Option<&[u8]>` per result column, the raw Postgres binary
/// bytes (or `None` for SQL NULL), aligned to `column_names`. Column order
/// and primary key come from the catalog, and each column's [`WireType`] is
/// the catalog's (via [`scalar_kind_to_wire_type`]), the same source the
/// pgoutput CDC path folds through, so the patchset is byte-identical to
/// that path for the same rows (a `uuid` lands as a 16-byte blob). A
/// catalog column absent from `column_names` is stored as NULL.
///
/// A snapshot is a set of rows to insert with no old images, so this
/// assembles [`Insert`]s directly rather than using the `Digestable` fold
/// the CDC vehicles need for old-image-carrying ops.
///
/// # Errors
///
/// [`ConversionError::TableNotFound`] when `table` is not in the catalog,
/// [`ConversionError::UnsupportedType`] when a catalog column's declared
/// type has no [`ScalarKind`], [`ConversionError::MissingColumns`] when a
/// row's width does not match `column_names`, and
/// [`ConversionError::Decode`] when a column's bytes do not decode for its
/// type.
pub fn pgbinary_patchset_builder<DB: DatabaseLike>(
    database: &DB,
    table: &str,
    column_names: &[&str],
    rows: &[Vec<Option<&[u8]>>],
) -> Result<PatchSet<SimpleTable, String, Vec<u8>>, ConversionError> {
    let table_id = catalog_helpers::table_id(database, table)
        .ok_or_else(|| ConversionError::TableNotFound(table.to_string()))?;
    let simple = catalog_helpers::simple_table(database, table_id)
        .ok_or_else(|| ConversionError::TableNotFound(table.to_string()))?;

    // Catalog column list once: name plus wire type per ordinal, both read
    // from the already-resolved `simple` (as `build_wire_table` does)
    // rather than re-resolving the table per column. Same source the CDC
    // path folds through, so the two encoders agree.
    let arity = simple.number_of_columns();
    let mut columns: Vec<(String, WireType)> = Vec::with_capacity(arity);
    for ordinal in 0..arity {
        let column_id = ColumnId::try_from(ordinal)
            .map_err(|_| ConversionError::TableNotFound(table.to_string()))?;
        let name = simple
            .column_name(ordinal)
            .ok_or_else(|| ConversionError::TableNotFound(table.to_string()))?
            .to_string();
        let wire_type = catalog_helpers::column_builtin_kind(database, table_id, column_id)
            .map(scalar_kind_to_wire_type)
            .ok_or_else(|| ConversionError::UnsupportedType(name.clone()))?;
        columns.push((name, wire_type));
    }

    // Map each catalog column name to its position in the result row.
    let mut positions: HashMap<&str, usize> = HashMap::with_capacity(column_names.len());
    for (idx, name) in column_names.iter().enumerate() {
        positions.insert(*name, idx);
    }

    let adapter = pgbinary_adapter();
    let mut builder = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for row in rows {
        if row.len() != column_names.len() {
            return Err(ConversionError::MissingColumns);
        }
        let mut insert = Insert::<SimpleTable, String, Vec<u8>>::from(simple.clone());
        for (ordinal, (name, wire_type)) in columns.iter().enumerate() {
            let raw = positions.get(name.as_str()).and_then(|&idx| row[idx]);
            let value = adapter.decode(PgBinaryColumn {
                column_name: name,
                wire_type: *wire_type,
                raw,
            })?;
            // `ordinal` is bounded by the catalog arity that sized the
            // insert, so `set` cannot report an out-of-bounds column.
            insert = insert
                .set(ordinal, value)
                .map_err(|_| ConversionError::MissingColumns)?;
        }
        builder = builder.insert(insert);
    }
    Ok(builder)
}

/// Encode Postgres binary result rows for `table` into an insert-patchset,
/// the snapshot counterpart to [`pgoutput_patchset`].
///
/// A thin wrapper over [`pgbinary_patchset_builder`] that serializes with
/// `build()`. An empty `rows` yields an empty patchset (zero bytes),
/// matching the CDC vehicles.
///
/// # Errors
///
/// Propagates [`ConversionError`], as [`pgbinary_patchset_builder`] does.
pub fn pgbinary_patchset<DB: DatabaseLike>(
    database: &DB,
    table: &str,
    column_names: &[&str],
    rows: &[Vec<Option<&[u8]>>],
) -> Result<Vec<u8>, ConversionError> {
    Ok(pgbinary_patchset_builder(database, table, column_names, rows)?.build())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::wal2json::parse_v2;
    use sqlite_diff_rs::{ChangesetOp, ParsedDiffSet, PatchsetOp};
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
        assert!(
            bytes.is_empty(),
            "begin and commit carry no row ops so no bytes are emitted"
        );
    }

    #[test]
    fn unknown_table_is_an_error() {
        let db = orders_db();
        let line = r#"{"action":"I","schema":"public","table":"absent","columns":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(line).unwrap();

        let err = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap_err();
        assert!(matches!(err, ConversionError::TableNotFound(name) if name == "absent"));
    }

    fn pairs_db() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE pairs (a INT, b INT, v TEXT, PRIMARY KEY (a, b));",
        )
        .unwrap()
    }

    /// Under default replica identity the old-row image an UPDATE carries
    /// is key-only, and a DELETE carries only the key. The patchset update
    /// digest builds its WHERE from the primary key in the new image
    /// (`columns`) and never reads the old image, so a key-only old image
    /// is sufficient. This pins that the loop does not require
    /// `REPLICA IDENTITY FULL`.
    #[test]
    fn default_replica_identity_key_only_old_image_emits_update_and_delete() {
        let db = orders_db();

        // UPDATE with a full new image and a key-only identity (default RI).
        let update = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":250},{"name":"status","type":"text","value":"shipped"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(update).unwrap();
        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        let PatchsetOp::Update { pk, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        assert_eq!(
            pk.to_vec(),
            vec![WireValue::Integer(1)],
            "WHERE key present"
        );
        let new_values = ops[0]
            .update_new_values()
            .expect("update carries new values");
        assert!(new_values
            .iter()
            .any(|v| matches!(v, Some(WireValue::Text(s)) if s == "shipped")));

        // DELETE with a key-only identity (default RI).
        let delete = r#"{"action":"D","schema":"public","table":"orders","identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(delete).unwrap();
        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        let PatchsetOp::Delete { pk, .. } = &ops[0] else {
            panic!("expected a delete op, got {:?}", ops[0]);
        };
        assert_eq!(
            pk.to_vec(),
            vec![WireValue::Integer(1)],
            "delete matches key"
        );
    }

    /// A composite primary key produces an update and a delete whose pk
    /// carries every key column, in pk-ordinal order, so the apply side
    /// can match on all of them.
    #[test]
    fn composite_pk_update_and_delete_carry_all_key_columns() {
        let db = pairs_db();

        let update = r#"{"action":"U","schema":"public","table":"pairs","columns":[{"name":"a","type":"integer","value":1},{"name":"b","type":"integer","value":2},{"name":"v","type":"text","value":"y2"}],"identity":[{"name":"a","type":"integer","value":1},{"name":"b","type":"integer","value":2}]}"#;
        let msg = parse_v2(update).unwrap();
        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        let PatchsetOp::Update { pk, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        assert_eq!(
            pk.to_vec(),
            vec![WireValue::Integer(1), WireValue::Integer(2)],
            "both key columns in the update WHERE"
        );

        let delete = r#"{"action":"D","schema":"public","table":"pairs","identity":[{"name":"a","type":"integer","value":1},{"name":"b","type":"integer","value":2}]}"#;
        let msg = parse_v2(delete).unwrap();
        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        let PatchsetOp::Delete { pk, .. } = &ops[0] else {
            panic!("expected a delete op, got {:?}", ops[0]);
        };
        assert_eq!(
            pk.to_vec(),
            vec![WireValue::Integer(1), WireValue::Integer(2)],
            "both key columns in the delete WHERE"
        );
    }

    /// A primary-key-changing UPDATE cannot round-trip through a patchset:
    /// the digest builds the WHERE from the new image, so the emitted op
    /// targets the new key and the old key is lost. Applied to a replica
    /// that still holds the old key, it matches no row, so the replica
    /// diverges. This pins the documented limitation from the emit side.
    #[test]
    fn pk_changing_update_targets_the_new_key_documenting_the_limitation() {
        let db = orders_db();
        // UPDATE orders SET id = 2 WHERE id = 1: new image has id = 2, the
        // old-row identity still has id = 1.
        let line = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":2},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(line).unwrap();
        let bytes = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        let PatchsetOp::Update { pk, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        assert_eq!(
            pk.to_vec(),
            vec![WireValue::Integer(2)],
            "the emitted WHERE targets the new key, so the old key is lost"
        );
    }

    /// The changeset emit path records the old and new value of every
    /// changed column, so a primary-key-changing UPDATE carries both the
    /// old key (for the WHERE) and the new key (for the SET). A patchset
    /// emit cannot express this.
    #[test]
    fn wal2json_changeset_update_captures_old_and_new() {
        let db = orders_db();
        // A pk-changing UPDATE under REPLICA IDENTITY FULL: the new image
        // (`columns`) carries id 2, the old-row image (`identity`) carries
        // id 1.
        let line = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":2},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}],"identity":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}]}"#;
        let msg = parse_v2(line).unwrap();

        let bytes = wal2json_changeset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Changeset(diff) = ParsedDiffSet::parse(&bytes).unwrap() else {
            panic!("expected a changeset (marker T)");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let ChangesetOp::Update { values, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        // The id column carries the old key (1) and the new key (2), so a
        // changeset apply relocates the row.
        assert_eq!(
            values[0],
            (Some(WireValue::Integer(1)), Some(WireValue::Integer(2)))
        );
    }

    fn orders_uuid_db() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id UUID PRIMARY KEY, quantity BIGINT);",
        )
        .unwrap()
    }

    fn only_insert_values(bytes: &[u8]) -> Vec<WireValue<String, Vec<u8>>> {
        let ParsedDiffSet::Patchset(diff) = parse_patchset(bytes) else {
            unreachable!("marker checked above");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        values.to_vec()
    }

    #[test]
    fn pgbinary_adapter_decodes_uuid_to_16_byte_blob() {
        // The whole motivation, pinned at the subql seam: the PgBinary
        // defaults route WireType::Uuid to the 16-byte-blob decoder.
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let adapter = pgbinary_adapter();
        let value = adapter
            .decode(PgBinaryColumn {
                column_name: "id",
                wire_type: WireType::Uuid,
                raw: Some(&uuid_bytes),
            })
            .unwrap();
        assert_eq!(value, WireValue::Blob(uuid_bytes.to_vec()));
    }

    #[test]
    fn pgbinary_patchset_round_trips_uuid_and_bigint_row() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let qty = 42i64.to_be_bytes();
        let rows = vec![vec![Some(uuid_bytes.as_slice()), Some(qty.as_slice())]];
        let bytes = pgbinary_patchset(&db, "orders", &["id", "quantity"], &rows).unwrap();
        assert_eq!(
            only_insert_values(&bytes),
            vec![WireValue::Blob(uuid_bytes.to_vec()), WireValue::Integer(42),]
        );
    }

    /// The subql-level guard against the snapshot and CDC encoders drifting:
    /// the same logical uuid lands as the same 16-byte blob whether it
    /// arrives as raw binary (snapshot) or as pgoutput text (CDC).
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn pgbinary_and_pgoutput_agree_on_uuid_blob() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, RowData};

        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let uuid_str = "00010203-0405-0607-0809-0a0b0c0d0e0f";

        // Snapshot path: raw 16 binary bytes.
        let qty = 7i64.to_be_bytes();
        let rows = vec![vec![Some(uuid_bytes.as_slice()), Some(qty.as_slice())]];
        let binary_bytes = pgbinary_patchset(&db, "orders", &["id", "quantity"], &rows).unwrap();

        // CDC path: the same uuid as pgoutput text.
        let ev = PgChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text(uuid_str)),
                    ("quantity", ColumnValue::text("7")),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
        };
        let cdc_bytes = pgoutput_patchset(&db, core::slice::from_ref(&ev)).unwrap();

        assert_eq!(
            only_insert_values(&binary_bytes),
            only_insert_values(&cdc_bytes),
        );
        assert_eq!(
            only_insert_values(&binary_bytes)[0],
            WireValue::Blob(uuid_bytes.to_vec())
        );
    }

    #[test]
    fn pgbinary_null_cell_emits_null() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let rows = vec![vec![Some(uuid_bytes.as_slice()), None]];
        let bytes = pgbinary_patchset(&db, "orders", &["id", "quantity"], &rows).unwrap();
        assert_eq!(
            only_insert_values(&bytes),
            vec![WireValue::Blob(uuid_bytes.to_vec()), WireValue::Null]
        );
    }

    #[test]
    fn pgbinary_absent_column_stored_as_null() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        // The result projects only `id`; `quantity` is absent, so the
        // catalog stores it as NULL.
        let rows = vec![vec![Some(uuid_bytes.as_slice())]];
        let bytes = pgbinary_patchset(&db, "orders", &["id"], &rows).unwrap();
        assert_eq!(
            only_insert_values(&bytes),
            vec![WireValue::Blob(uuid_bytes.to_vec()), WireValue::Null]
        );
    }

    #[test]
    fn pgbinary_row_width_mismatch_is_missing_columns() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        // Two column names but a one-cell row.
        let rows = vec![vec![Some(uuid_bytes.as_slice())]];
        let err = pgbinary_patchset(&db, "orders", &["id", "quantity"], &rows).unwrap_err();
        assert!(matches!(err, ConversionError::MissingColumns));
    }

    #[test]
    fn pgbinary_unknown_table_is_an_error() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let rows = vec![vec![Some(uuid_bytes.as_slice())]];
        let err = pgbinary_patchset(&db, "absent", &["id"], &rows).unwrap_err();
        assert!(matches!(err, ConversionError::TableNotFound(name) if name == "absent"));
    }
}
