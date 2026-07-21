//! `PgSqliteEmuSource`: fake Postgres CDC source layered on SQLite.
//!
//! The wire encoding (changeset -> `pgoutput` message) lives upstream
//! in [`sqlite_diff_rs::pg_walstream_reverse`]. This module owns the
//! surrounding orchestration: PG DDL translation via [`pg2sqlite`],
//! session lifecycle, per-table [`RelationSchema`] cache, the
//! unchanged-column row lookup, and the `PgOutputDecoder` feedback loop
//! that turns the encoded frames back into typed [`crate::ChangeEvent`]s.

use alloc::collections::VecDeque;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use bytes::BytesMut;
use diesel::{
    deserialize::QueryableByName, sql_query, sql_types::Text, RunQueryDsl, SqliteConnection,
};
use diesel_sqlite_session::{Session, SqliteSessionExt};
use hashbrown::{HashMap, HashSet};
use pg2sqlite::{options::Pg2SqliteOptions, pg2sqlite::Pg2Sqlite};
use pg_walstream::{encode_message, ChangeEvent, Lsn, PgOutputDecoder};
use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::pg_walstream_reverse::{
    op_to_message, relation_message, ColumnSchema, LogicalReplicationMessage, Oid, RelationSchema,
};
use sqlite_diff_rs::{ChangesetOp, ParsedDiffSet, TableSchema, Value as WireValue};

use super::error::PgSqliteEmuError;
use crate::backend::ScalarKind;
use crate::wal::into_engine_events;
use crate::{catalog_helpers, ColumnId, TableId};

/// `pgoutput` protocol version 1: base messages only. Matches what the
/// production parser handles by default.
const PROTOCOL_VERSION: u8 = 1;

/// Fake Postgres CDC source that runs against an in-process SQLite
/// database. See the [module docs](super) for the pipeline overview.
///
/// # Examples
///
/// Build a source, drive one INSERT through the wrapped connection,
/// and inspect the typed [`ChangeEvent`] the source materialises
/// from SQLite's session changeset. Full engine-dispatch pipeline
/// lives in the [module docs](super#quickstart).
///
/// ```
/// use subql::backend::{RowKind, Value};
/// use subql::{EventKind, PgSqliteEmuSource};
/// use subql::backend::CdcEvent;
///
/// let mut source = PgSqliteEmuSource::open_in_memory(
///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
/// )?;
/// source.execute_sql("INSERT INTO orders (id, amount) VALUES (7, 250)")?;
///
/// let event = source.poll_next_event()?.expect("insert reaches the queue");
/// assert_eq!(event.kind(), EventKind::Insert);
/// assert_eq!(event.value_at(source.pg_catalog(), RowKind::New, 0).unwrap(), Value::Int(7));
/// assert_eq!(event.value_at(source.pg_catalog(), RowKind::New, 1).unwrap(), Value::Int(250));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct PgSqliteEmuSource {
    connection: SqliteConnection,
    session: Session,
    pg_catalog: ParserDB,
    decoder: PgOutputDecoder,
    pending: VecDeque<ChangeEvent>,
    announced: HashSet<Oid>,
    tables: HashMap<String, TableMeta>,
    /// Monotonic WAL position stamped on each data frame, since
    /// `pgoutput` data messages carry no body LSN.
    next_lsn: u64,
}

#[derive(Clone, Debug)]
struct TableMeta {
    oid: Oid,
    /// SQLite table name; matches the PG catalog's name after pg2sqlite
    /// preserves identifiers verbatim for basic `CREATE TABLE`.
    sqlite_table: String,
    /// Column metadata in column order, index-aligned with the PG
    /// catalog.
    columns: Vec<ColumnMeta>,
    /// Ordinals of the PK columns. Used to construct `WHERE` clauses
    /// when the source has to fetch the current row image for an
    /// UPDATE whose changeset carries `(None, None)` on some non-PK
    /// column.
    pk_column_indices: Vec<usize>,
}

#[derive(Clone, Debug)]
struct ColumnMeta {
    name: String,
    pg_type_oid: Oid,
    is_pk: bool,
}

impl PgSqliteEmuSource {
    /// Build a source from an owned diesel [`SqliteConnection`] plus a
    /// Postgres-flavored DDL string.
    ///
    /// The DDL is parsed twice: once as Postgres SQL to build the
    /// engine-facing [`ParserDB`] catalog, and once by `pg2sqlite` to
    /// translate every statement into SQLite DDL applied to
    /// `connection`. After translation the connection is attached to
    /// a fresh session tracking every declared table.
    ///
    /// # Errors
    ///
    /// See [`PgSqliteEmuError`].
    ///
    /// # Examples
    ///
    /// Wrap an already-open diesel connection. Callers pick this over
    /// [`Self::open_in_memory`] when the connection needs to point at
    /// a file, a shared cache URL, or an existing pool handle.
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use subql::{catalog_helpers, PgSqliteEmuSource};
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let source = PgSqliteEmuSource::new(
    ///     conn,
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// assert!(catalog_helpers::table_id(source.pg_catalog(), "orders").is_some());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(mut connection: SqliteConnection, pg_ddl: &str) -> Result<Self, PgSqliteEmuError> {
        let pg_catalog = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(pg_ddl)
            .map_err(|e| PgSqliteEmuError::Catalog(format!("{e:?}")))?;

        let translator = Pg2Sqlite::default()
            .sql(pg_ddl)
            .map_err(|e| PgSqliteEmuError::Translate(format!("{e:?}")))?;
        let sqlite_ddl = translator
            .translate_to_sql(&Pg2SqliteOptions::default())
            .map_err(|e| PgSqliteEmuError::Translate(format!("{e:?}")))?;
        for stmt in &sqlite_ddl {
            sql_query(stmt).execute(&mut connection)?;
        }

        let tables = build_table_meta(&pg_catalog)?;

        let mut session = connection.create_session()?;
        session.attach_all()?;

        Ok(Self {
            connection,
            session,
            pg_catalog,
            decoder: PgOutputDecoder::with_protocol_version(u32::from(PROTOCOL_VERSION)),
            pending: VecDeque::new(),
            announced: HashSet::new(),
            tables,
            next_lsn: 0,
        })
    }

    /// Convenience: open a fresh in-memory diesel [`SqliteConnection`]
    /// and build a source over it in one step.
    ///
    /// Equivalent to
    /// `PgSqliteEmuSource::new(SqliteConnection::establish(":memory:")?, pg_ddl)`
    /// but saves the diesel establish incantation on every caller.
    /// Intended for doctests and short-lived integration tests. Use
    /// [`Self::new`] when the connection needs to point at a file, at
    /// a shared cache URL, or at anything other than a private
    /// in-memory database.
    ///
    /// # Errors
    ///
    /// See [`PgSqliteEmuError`]. Adds a
    /// [`PgSqliteEmuError::ApplyDdl`] wrapping any diesel
    /// `ConnectionError` when the in-memory connection cannot open,
    /// which in practice happens only on OOM.
    ///
    /// # Examples
    ///
    /// Shortest usable pipeline: open, run one DML, drain one event.
    /// The typed row image round-trips through the pgoutput wire and
    /// back into a `ChangeEvent` before the assertion sees it.
    ///
    /// ```
    /// use subql::backend::{CdcEvent, RowKind, Value};
    /// use subql::{EventKind, PgSqliteEmuSource};
    ///
    /// let mut source = PgSqliteEmuSource::open_in_memory(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    /// )?;
    /// source.execute_sql("INSERT INTO orders VALUES (1, 'paid')")?;
    ///
    /// let event = source.poll_next_event()?.expect("one event pending");
    /// assert_eq!(event.kind(), EventKind::Insert);
    /// assert_eq!(
    ///     event.value_at(source.pg_catalog(), RowKind::New, 1).unwrap(),
    ///     Value::String("paid".to_string()),
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open_in_memory(pg_ddl: &str) -> Result<Self, PgSqliteEmuError> {
        use diesel::Connection;
        let conn = SqliteConnection::establish(":memory:")
            .map_err(|e| PgSqliteEmuError::Catalog(format!("open in-memory sqlite: {e}")))?;
        Self::new(conn, pg_ddl)
    }

    /// Convenience: run `sql` as a single diesel `sql_query` against
    /// the wrapped connection and return the number of rows affected.
    /// Named `execute_sql` rather than `execute` so it does not clash
    /// with diesel's `RunQueryDsl::execute`, whose by-value receiver
    /// would otherwise shadow this method whenever the query DSL is in
    /// scope.
    ///
    /// Saves doctests and short integration tests from importing
    /// `diesel::{sql_query, RunQueryDsl}` just to drive DML. Use
    /// [`Self::connection`] when the caller needs a bind-parameter
    /// query, a typed diesel expression, or a batch statement.
    ///
    /// # Errors
    ///
    /// [`PgSqliteEmuError::ApplyDdl`] on any diesel execution error.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::PgSqliteEmuSource;
    ///
    /// let mut source = PgSqliteEmuSource::open_in_memory(
    ///     "CREATE TABLE items (id INT PRIMARY KEY);",
    /// )?;
    /// let rows = source.execute_sql("INSERT INTO items VALUES (42)")?;
    /// assert_eq!(rows, 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn execute_sql(&mut self, sql: &str) -> Result<usize, PgSqliteEmuError> {
        Ok(sql_query(sql).execute(&mut self.connection)?)
    }

    /// Drain every event currently pending.
    ///
    /// Loops [`Self::poll_next_event`] until the session and pending
    /// queue both report empty, collecting the yielded events in
    /// order. Useful in tests that do not care about per-event
    /// pacing; production consumers should call `poll_next_event`
    /// directly.
    ///
    /// # Errors
    ///
    /// See [`PgSqliteEmuError`]. The first failure short-circuits and
    /// returns without yielding any of the events already collected.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::PgSqliteEmuSource;
    ///
    /// let mut source = PgSqliteEmuSource::open_in_memory(
    ///     "CREATE TABLE items (id INT PRIMARY KEY);",
    /// )?;
    /// for id in 1..=3 {
    ///     source.execute_sql(&format!("INSERT INTO items VALUES ({id})"))?;
    /// }
    /// let events = source.drain()?;
    /// assert_eq!(events.len(), 3);
    /// assert!(source.drain()?.is_empty(), "queue is now flushed");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn drain(&mut self) -> Result<Vec<ChangeEvent>, PgSqliteEmuError> {
        let mut out = Vec::new();
        while let Some(ev) = self.poll_next_event()? {
            out.push(ev);
        }
        Ok(out)
    }

    /// Drain the next event from the source.
    ///
    /// Returns `Ok(None)` when the session has accumulated no new
    /// changes since the last drain and the pending buffer is empty.
    ///
    /// # Errors
    ///
    /// See [`PgSqliteEmuError`].
    ///
    /// # Examples
    ///
    /// Polling an idle source returns `None` without touching the
    /// session extension. Callers can loop on this without a busy
    /// wait guard.
    ///
    /// ```
    /// use subql::PgSqliteEmuSource;
    ///
    /// let mut source = PgSqliteEmuSource::open_in_memory(
    ///     "CREATE TABLE items (id INT PRIMARY KEY);",
    /// )?;
    /// assert!(source.poll_next_event()?.is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn poll_next_event(&mut self) -> Result<Option<ChangeEvent>, PgSqliteEmuError> {
        if self.pending.is_empty() && !self.session.is_empty() {
            self.drain_session()?;
            // SQLite sessions accumulate: `.changeset()` snapshots
            // everything since the session was created. Recreate so
            // the next poll only sees changes AFTER this drain.
            let mut fresh = self.connection.create_session()?;
            fresh.attach_all()?;
            self.session = fresh;
        }
        Ok(self.pending.pop_front())
    }

    /// Encode + parse a single synthesised `Truncate` event.
    ///
    /// SQLite's session extension has no `TRUNCATE` primitive, but the
    /// engine's Truncate dispatch path still needs to be exercised.
    /// Callers can inject a truncate on any known PG table id through
    /// this helper.
    ///
    /// # Errors
    ///
    /// [`PgSqliteEmuError::UnknownTable`] when `table_id` was not part
    /// of the PG DDL used to build the source.
    ///
    /// # Examples
    ///
    /// The session extension has no TRUNCATE primitive, so callers
    /// synthesise one when they want the engine's TRUNCATE dispatch
    /// path exercised end-to-end.
    ///
    /// ```
    /// use subql::backend::CdcEvent;
    /// use subql::{catalog_helpers, EventKind, PgSqliteEmuSource};
    ///
    /// let mut source = PgSqliteEmuSource::open_in_memory(
    ///     "CREATE TABLE items (id INT PRIMARY KEY);",
    /// )?;
    /// let table_id = catalog_helpers::table_id(source.pg_catalog(), "items")
    ///     .expect("items table resolves");
    /// source.inject_truncate(table_id)?;
    /// let event = source.poll_next_event()?.expect("truncate emitted");
    /// assert_eq!(event.kind(), EventKind::Truncate);
    /// assert_eq!(event.table_id(source.pg_catalog()), table_id);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn inject_truncate(&mut self, table_id: TableId) -> Result<(), PgSqliteEmuError> {
        let table_name = self
            .name_by_table_id(table_id)
            .ok_or_else(|| PgSqliteEmuError::UnknownTable(format!("table id {table_id}")))?;
        let meta = self
            .tables
            .get(&table_name)
            .cloned()
            .ok_or(PgSqliteEmuError::UnknownTable(table_name.clone()))?;
        self.announce_if_needed(&meta);
        let msg = LogicalReplicationMessage::Truncate {
            relation_ids: alloc::vec![meta.oid],
            flags: 0,
        };
        self.push_frame(&msg)?;
        Ok(())
    }

    /// Mutable access to the underlying [`SqliteConnection`].
    ///
    /// Drive DML through this connection. Writes performed through a
    /// sibling connection to the same database bypass the session and
    /// will not surface as events.
    pub const fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.connection
    }

    /// Immutable access to the PG catalog. Downstream consumers pass
    /// this into `SubscriptionEngine::new` so the engine resolves the
    /// emitted events against the same catalog.
    #[must_use]
    pub const fn pg_catalog(&self) -> &ParserDB {
        &self.pg_catalog
    }

    fn drain_session(&mut self) -> Result<(), PgSqliteEmuError> {
        let bytes = self.session.changeset()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let parsed = ParsedDiffSet::parse(&bytes)?;
        let ParsedDiffSet::Changeset(diffset) = parsed else {
            return Err(PgSqliteEmuError::UnknownTable(
                "session emitted patchset instead of changeset".into(),
            ));
        };
        for op in diffset.iter() {
            let name = op.table().name().clone();
            let meta = self
                .tables
                .get(&name)
                .cloned()
                .ok_or(PgSqliteEmuError::UnknownTable(name))?;
            self.announce_if_needed(&meta);
            let fallback = self.fallback_row_for(&op, &meta)?;
            let cols = meta.column_schemas();
            let schema = RelationSchema {
                relation_oid: meta.oid,
                namespace: "",
                relation_name: meta.sqlite_table.as_str(),
                columns: cols.as_slice(),
            };
            let msg = op_to_message(&op, &schema, fallback.as_deref())
                .map_err(|e| PgSqliteEmuError::UnknownTable(format!("op_to_message: {e}")))?;
            self.push_frame(&msg)?;
        }
        Ok(())
    }

    /// Emit the `Relation` frame for a table the first time it appears
    /// in a drain. The parser needs it before any data frame for the
    /// same OID decodes.
    fn announce_if_needed(&mut self, meta: &TableMeta) {
        if !self.announced.insert(meta.oid) {
            return;
        }
        let cols = meta.column_schemas();
        let schema = RelationSchema {
            relation_oid: meta.oid,
            namespace: "",
            relation_name: meta.sqlite_table.as_str(),
            columns: cols.as_slice(),
        };
        let msg = relation_message(&schema);
        let mut buf = BytesMut::new();
        encode_message(&msg, PROTOCOL_VERSION, &mut buf);
        // Relation frames prime the decoder's relation cache and never
        // emit a data event, so we discard the decode result.
        let _ = self.decoder.decode_message(buf, Lsn::new(0));
    }

    fn push_frame(&mut self, msg: &LogicalReplicationMessage) -> Result<(), PgSqliteEmuError> {
        let mut buf = BytesMut::new();
        encode_message(msg, PROTOCOL_VERSION, &mut buf);
        // Fresh position per frame, else all events collapse to `Lsn(0)`.
        self.next_lsn += 1;
        if let Some(decoded) = self.decoder.decode_message(buf, Lsn::new(self.next_lsn))? {
            self.pending.extend(into_engine_events(decoded));
        }
        Ok(())
    }

    /// For an UPDATE with any `(None, None)` column pair (unchanged
    /// non-PK column), return the current row read from SQLite so we
    /// can fill in the missing positions. Non-UPDATE ops, and UPDATE
    /// ops that already carry values for every column, return `None`.
    ///
    /// Only the PK columns come from the changeset; the row we fetch
    /// is the DB state right after the DML that produced this
    /// changeset, which is by construction the correct new-image (and
    /// also the correct old-image for the unchanged columns, since
    /// they did not change).
    fn fallback_row_for(
        &mut self,
        op: &ChangesetOp<'_, TableSchema<String>, String, Vec<u8>>,
        meta: &TableMeta,
    ) -> Result<Option<FallbackRow>, PgSqliteEmuError> {
        let ChangesetOp::Update { values, .. } = op else {
            return Ok(None);
        };
        // Any None slot needs a fallback so the upstream encoder can
        // fill in the missing pgoutput cell. New-side None is a hard
        // error without fallback; old-side None would lower to
        // `ColumnData::unchanged`, which subql's parser accepts on
        // the old side but is a lossy shape we would rather avoid.
        // Filling both sides from the fallback keeps the emitted
        // event's old image and new image structurally identical to
        // what a real Postgres source under `REPLICA IDENTITY FULL`
        // would produce.
        if !values.iter().any(|p| p.0.is_none() || p.1.is_none()) {
            return Ok(None);
        }
        // The changeset always carries every PK column on both sides
        // for an UPDATE, so we prefer the NEW-side value (matches the
        // post-image row we are about to read out of SQLite).
        let mut pk_values: FallbackRow = Vec::with_capacity(meta.pk_column_indices.len());
        for &pk_idx in &meta.pk_column_indices {
            let pair = values
                .get(pk_idx)
                .ok_or_else(|| PgSqliteEmuError::UnknownTable(meta.sqlite_table.clone()))?;
            let value = pair.1.clone().or_else(|| pair.0.clone()).ok_or_else(|| {
                PgSqliteEmuError::UnknownTable(format!(
                    "{} update missing PK column {pk_idx}",
                    meta.sqlite_table
                ))
            })?;
            pk_values.push(value);
        }
        let row = fetch_current_row(&mut self.connection, meta, &pk_values)?;
        Ok(Some(row))
    }

    fn name_by_table_id(&self, table_id: TableId) -> Option<String> {
        self.pg_catalog
            .table_by_id(table_id as usize)
            .map(|t| t.table_name().to_string())
    }
}

impl TableMeta {
    /// Build a per-column [`ColumnSchema`] slice pointing at this
    /// `TableMeta`'s owned column names. The upstream API takes the
    /// resulting `Vec` by reference through
    /// [`RelationSchema::columns`]; the caller pins the vec to a
    /// local so the `RelationSchema` borrow lives long enough.
    fn column_schemas(&self) -> Vec<ColumnSchema<'_>> {
        self.columns
            .iter()
            .map(|c| ColumnSchema {
                name: c.name.as_str(),
                pg_type_oid: c.pg_type_oid,
                is_pk: c.is_pk,
            })
            .collect()
    }
}

impl crate::CdcSource for PgSqliteEmuSource {
    type Event = ChangeEvent;
    type Error = PgSqliteEmuError;

    fn next_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<Self::Event>, Self::Error>> + Send {
        core::future::ready(self.poll_next_event())
    }

    fn ack(
        &mut self,
        _upto: <Self::Event as crate::backend::CdcEvent>::Checkpoint,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        // The upstream is an in-process session extension. There is
        // no one to acknowledge.
        core::future::ready(Ok(()))
    }
}

// ---------------------------------------------------------------------
// Table metadata construction
// ---------------------------------------------------------------------

fn build_table_meta(pg_catalog: &ParserDB) -> Result<HashMap<String, TableMeta>, PgSqliteEmuError> {
    let mut out = HashMap::new();
    let n_tables = pg_catalog.number_of_tables();
    for idx in 0..n_tables {
        let table = pg_catalog
            .table_by_id(idx)
            .ok_or_else(|| PgSqliteEmuError::UnknownTable(format!("id {idx}")))?;
        let table_id: TableId = u32::try_from(idx).map_err(|_| {
            PgSqliteEmuError::UnknownTable(format!("id {idx} exceeds TableId space"))
        })?;
        let name = table.table_name().to_string();
        let arity = catalog_helpers::table_arity(pg_catalog, table_id)
            .ok_or_else(|| PgSqliteEmuError::UnknownTable(name.clone()))?;
        let pk_cols: HashSet<ColumnId> = catalog_helpers::primary_key_columns(pg_catalog, table_id)
            .unwrap_or_default()
            .into_iter()
            .collect();

        let mut columns: Vec<ColumnMeta> = Vec::with_capacity(arity);
        let mut pk_column_indices: Vec<usize> = Vec::new();
        let mut column_iter = table.columns(pg_catalog);
        for col_idx in 0..arity {
            let column_id = u16::try_from(col_idx).map_err(|_| {
                PgSqliteEmuError::UnknownTable(format!(
                    "column id {col_idx} exceeds ColumnId space"
                ))
            })?;
            let column = column_iter.next().ok_or_else(|| {
                PgSqliteEmuError::UnknownTable(format!(
                    "table {name} column index {col_idx} missing from catalog"
                ))
            })?;
            let column_name = column.column_name().to_string();
            let scalar_kind = catalog_helpers::column_scalar_kind(pg_catalog, table_id, column_id);
            let pg_type_oid = pg_type_oid_for_kind(scalar_kind);
            let is_pk = pk_cols.contains(&column_id);
            if is_pk {
                pk_column_indices.push(col_idx);
            }
            columns.push(ColumnMeta {
                name: column_name,
                pg_type_oid,
                is_pk,
            });
        }

        out.insert(
            name.clone(),
            TableMeta {
                oid: synth_oid(table_id),
                sqlite_table: name,
                columns,
                pk_column_indices,
            },
        );
    }
    Ok(out)
}

/// Synthetic `pgoutput` relation id derived from subql's compact
/// [`TableId`]. Add `1_000` so the OID stays clear of the tiny values
/// PostgreSQL reserves for its own catalog rows.
const fn synth_oid(table_id: TableId) -> Oid {
    1_000 + table_id
}

/// Map subql's [`ScalarKind`] to a PostgreSQL type OID for the encoded
/// `pgoutput` relation message. The OID labels the column on the wire,
/// while the engine decodes each cell against the catalog scalar kind.
/// Unknown or composite columns fall back to `TEXT` (25).
const fn pg_type_oid_for_kind(kind: Option<ScalarKind>) -> Oid {
    match kind {
        Some(ScalarKind::Bool) => 16,
        Some(ScalarKind::Int) => 20,
        Some(ScalarKind::Float) => 701,
        Some(ScalarKind::Bytes) => 17,
        Some(ScalarKind::Uuid) => 2950,
        Some(ScalarKind::Timestamp) => 1114,
        Some(ScalarKind::TimestampTz) => 1184,
        Some(ScalarKind::Date) => 1082,
        Some(ScalarKind::Time) => 1083,
        Some(ScalarKind::Decimal) => 1700,
        Some(ScalarKind::Json) => 114,
        Some(ScalarKind::Jsonb) => 3802,
        Some(ScalarKind::String) | None => 25,
    }
}

// ---------------------------------------------------------------------
// Row lookup for the unchanged-column fallback
// ---------------------------------------------------------------------

/// Diesel row destination for the `json_array(...)` projection below.
#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type = Text)]
    row_json: String,
}

/// Fetch the current post-image of one row through SQLite. Used only
/// when the changeset carried `(None, None)` on some non-PK columns of
/// an UPDATE.
///
/// Projects a `json_array(col0, col1, ...)` so we get every column back
/// in a single scalar string. The returned array elements decode into
/// `WireValue` per each column's SQLite storage class.
fn fetch_current_row(
    connection: &mut SqliteConnection,
    meta: &TableMeta,
    pk_values: &[WireValue<String, Vec<u8>>],
) -> Result<FallbackRow, PgSqliteEmuError> {
    if pk_values.len() != meta.pk_column_indices.len() {
        return Err(PgSqliteEmuError::UnknownTable(format!(
            "{} pk length mismatch: expected {}, got {}",
            meta.sqlite_table,
            meta.pk_column_indices.len(),
            pk_values.len()
        )));
    }

    let cols_csv = meta
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = meta
        .pk_column_indices
        .iter()
        .zip(pk_values)
        .map(|(&idx, val)| {
            let col_name = &meta.columns[idx].name;
            let literal = wire_value_to_sql_literal(val);
            format!("\"{col_name}\" = {literal}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT json_array({cols_csv}) AS row_json FROM \"{}\" WHERE {}",
        meta.sqlite_table, where_clause
    );

    let row: JsonRow = sql_query(&sql).get_result(connection)?;
    let value: serde_json::Value = serde_json::from_str(&row.row_json).map_err(|e| {
        PgSqliteEmuError::UnknownTable(format!(
            "row lookup json decode: {e} (raw={raw})",
            raw = row.row_json
        ))
    })?;
    let arr = value.as_array().ok_or_else(|| {
        PgSqliteEmuError::UnknownTable(format!(
            "row lookup expected JSON array, got {}",
            row.row_json
        ))
    })?;
    if arr.len() != meta.columns.len() {
        return Err(PgSqliteEmuError::UnknownTable(format!(
            "{} row lookup returned {} columns, expected {}",
            meta.sqlite_table,
            arr.len(),
            meta.columns.len()
        )));
    }
    Ok(arr.iter().map(json_value_to_wire).collect())
}

fn wire_value_to_sql_literal(v: &WireValue<String, Vec<u8>>) -> String {
    match v {
        WireValue::Null => "NULL".to_string(),
        WireValue::Integer(i) => i.to_string(),
        WireValue::Real(f) => format!("{f}"),
        WireValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        WireValue::Blob(b) => {
            let mut hex = String::with_capacity(3 + b.len() * 2);
            hex.push_str("X'");
            for byte in b {
                use core::fmt::Write;
                let _ = write!(&mut hex, "{byte:02x}");
            }
            hex.push('\'');
            hex
        }
    }
}

/// Owned wire row image indexed by column ordinal. Result payload of
/// [`fetch_current_row`] and the `Some` shape of
/// [`PgSqliteEmuSource::fallback_row_for`].
type FallbackRow = Vec<WireValue<String, Vec<u8>>>;

fn json_value_to_wire(v: &serde_json::Value) -> WireValue<String, Vec<u8>> {
    match v {
        serde_json::Value::Bool(b) => WireValue::Integer(i64::from(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(WireValue::Integer)
            .or_else(|| n.as_f64().map(WireValue::Real))
            .unwrap_or(WireValue::Null),
        serde_json::Value::String(s) => WireValue::Text(s.clone()),
        // Null, Array, Object, and anything else the enum grows: no
        // meaningful WireValue mapping. Fuzz-schema doesn't hit Blob
        // columns (which older SQLite json_array returns as an
        // integer array, newer as raw string); extend when a real
        // consumer needs BYTEA round trip.
        _ => WireValue::Null,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::{CdcEvent, RowKind, Value};
    use diesel::Connection;

    const ORDERS_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

    fn build_source() -> PgSqliteEmuSource {
        let conn = SqliteConnection::establish(":memory:").expect("open in-memory sqlite");
        PgSqliteEmuSource::new(conn, ORDERS_DDL).expect("source construction")
    }

    #[test]
    fn empty_source_yields_none() {
        let mut src = build_source();
        assert!(src.poll_next_event().unwrap().is_none());
    }

    #[test]
    fn insert_emits_pgoutput_event() {
        let mut src = build_source();
        sql_query("INSERT INTO orders (id, amount, status) VALUES (1, 250, 'paid')")
            .execute(src.connection())
            .unwrap();
        let ev = src.poll_next_event().unwrap().expect("one event pending");
        assert_eq!(ev.kind(), crate::EventKind::Insert);
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::New, 0).unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::New, 1).unwrap(),
            Value::Int(250)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );
        assert!(src.poll_next_event().unwrap().is_none());
    }

    #[test]
    fn update_carries_old_and_new_full_image() {
        let mut src = build_source();
        sql_query("INSERT INTO orders (id, amount, status) VALUES (5, 100, 'pending')")
            .execute(src.connection())
            .unwrap();
        let _ = src.poll_next_event().unwrap();
        sql_query("UPDATE orders SET status = 'shipped' WHERE id = 5")
            .execute(src.connection())
            .unwrap();
        let ev = src.poll_next_event().unwrap().expect("update event");
        assert_eq!(ev.kind(), crate::EventKind::Update);
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::Old, 1).unwrap(),
            Value::Int(100)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::Old, 2).unwrap(),
            Value::String("pending".into())
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::New, 1).unwrap(),
            Value::Int(100)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::New, 2).unwrap(),
            Value::String("shipped".into())
        );
    }

    #[test]
    fn pk_changing_update_surfaces_as_delete_plus_insert() {
        // SQLite session extension treats a PK-changing UPDATE as a
        // DELETE of the old row plus an INSERT of the new row. This
        // is a SQLite semantic, not an emulator choice: the changeset
        // op stream contains one Delete and one Insert with the old
        // and new PKs respectively. Any downstream that wants a
        // single Update event needs to reconcile at a higher layer.
        let mut src = build_source();
        sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 500, 'paid')")
            .execute(src.connection())
            .unwrap();
        let _ = src.poll_next_event().unwrap();
        sql_query("UPDATE orders SET id = 8 WHERE id = 7")
            .execute(src.connection())
            .unwrap();

        let first = src.poll_next_event().unwrap().expect("first event");
        let second = src.poll_next_event().unwrap().expect("second event");
        assert!(src.poll_next_event().unwrap().is_none());

        let mut events = [first, second];
        events.sort_by_key(|e| match e.kind() {
            crate::EventKind::Delete => 0,
            crate::EventKind::Insert => 1,
            _ => 2,
        });
        let [del, ins] = events;

        assert_eq!(del.kind(), crate::EventKind::Delete);
        assert_eq!(
            del.value_at(src.pg_catalog(), RowKind::Old, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            del.value_at(src.pg_catalog(), RowKind::Old, 1).unwrap(),
            Value::Int(500)
        );
        assert_eq!(
            del.value_at(src.pg_catalog(), RowKind::Old, 2).unwrap(),
            Value::String("paid".into())
        );

        assert_eq!(ins.kind(), crate::EventKind::Insert);
        assert_eq!(
            ins.value_at(src.pg_catalog(), RowKind::New, 0).unwrap(),
            Value::Int(8)
        );
        assert_eq!(
            ins.value_at(src.pg_catalog(), RowKind::New, 1).unwrap(),
            Value::Int(500)
        );
        assert_eq!(
            ins.value_at(src.pg_catalog(), RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );
    }

    #[test]
    fn delete_carries_full_old_image() {
        let mut src = build_source();
        sql_query("INSERT INTO orders (id, amount, status) VALUES (9, 500, 'paid')")
            .execute(src.connection())
            .unwrap();
        let _ = src.poll_next_event().unwrap();
        sql_query("DELETE FROM orders WHERE id = 9")
            .execute(src.connection())
            .unwrap();
        let ev = src.poll_next_event().unwrap().expect("delete event");
        assert_eq!(ev.kind(), crate::EventKind::Delete);
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::Old, 0).unwrap(),
            Value::Int(9)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::Old, 1).unwrap(),
            Value::Int(500)
        );
        assert_eq!(
            ev.value_at(src.pg_catalog(), RowKind::Old, 2).unwrap(),
            Value::String("paid".into())
        );
    }

    #[test]
    fn truncate_injection_reaches_engine_dispatch() {
        let mut src = build_source();
        let table_id = catalog_helpers::table_id(src.pg_catalog(), "orders").expect("orders id");
        src.inject_truncate(table_id).expect("truncate");
        let ev = src.poll_next_event().unwrap().expect("truncate event");
        assert_eq!(ev.kind(), crate::EventKind::Truncate);
        assert_eq!(ev.table_id(src.pg_catalog()), table_id);
    }

    #[test]
    fn events_carry_strictly_increasing_nonzero_lsns() {
        let mut src = build_source();
        for id in 1..=3 {
            sql_query(format!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {id}0, 'paid')"
            ))
            .execute(src.connection())
            .unwrap();
        }
        let events = src.drain().unwrap();
        assert_eq!(events.len(), 3);

        let checkpoints: Vec<crate::PgLsn> = events
            .iter()
            .map(|e| e.checkpoint().expect("emulator event carries an LSN"))
            .collect();
        assert!(
            checkpoints[0] > crate::PgLsn(0),
            "first checkpoint must start above zero, got {:?}",
            checkpoints[0]
        );
        assert!(
            checkpoints.windows(2).all(|w| w[1] > w[0]),
            "checkpoints must be strictly increasing, got {checkpoints:?}"
        );
    }
}
