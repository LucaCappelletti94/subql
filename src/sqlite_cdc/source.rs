//! In-process SQLite CDC source.
//!
//! [`SqliteCdcSource`] wraps a Diesel [`SqliteConnection`] plus a
//! [`ParserDB`] catalog, installs `AFTER INSERT` / `AFTER UPDATE` /
//! `BEFORE DELETE` triggers on every catalog-declared table, and
//! surfaces the resulting row mutations as subql [`WalEvent`]s through
//! the same [`crate::CdcSource`] trait the PG transports implement.
//!
//! # Why trigger-driven?
//!
//! The original Step 1 design relied on `sqlite3_update_hook` plus a
//! live-row refetch by `rowid` to build INSERT events. That design is
//! fine when the consumer drains events between every DML statement,
//! but it loses row content when several mutations affect the same row
//! before the consumer catches up (the refetch finds the latest live
//! state, not the state at hook time). Step 2 replaces the refetch with
//! a shadow log table populated by SQL triggers: each event records its
//! own `OLD`/`NEW` row image at the moment of mutation, so batched DML
//! preserves the per-event state until [`crate::CdcSource::next_event`]
//! drains it.
//!
//! Cells in the shadow log are encoded via SQLite's `json_array` and
//! decoded back to [`Cell`] values with catalog-aware type coercion
//! (because JSON conflates integer and floating-point numbers, while
//! subql distinguishes `Cell::Int` from `Cell::Float`).

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::future::Future;

use diesel::connection::{LoadConnection, SimpleConnection};
use diesel::row::{Field, Row};
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel::sqlite::SqliteConnection;
use diesel::RunQueryDsl;
use hashbrown::HashMap;
use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::sqlite_cdc::catalog;
use crate::sqlite_cdc::error::SqliteCdcError;
use crate::wal::CdcSource;
use crate::{
    catalog_helpers, Cell, ColumnId, ColumnType, NoCheckpoint, PrimaryKey, RowImage, TableId,
    WalEvent,
};

/// Name of the unified shadow log table used to record every observed
/// row mutation in deterministic order across all tracked tables.
const EVENT_LOG_TABLE: &str = "_subql_cdc_event_log";

/// Configuration for a [`SqliteCdcSource`].
///
/// `#[non_exhaustive]` so additional knobs can land without breaking
/// downstream call sites. Construct via [`SqliteCdcConfig::default`].
///
/// # Examples
///
/// ```
/// use subql::SqliteCdcConfig;
///
/// let config = SqliteCdcConfig::default();
/// // Pass `config` into `SqliteCdcSource::new` or `SqliteCdcSource::with_pg_ddl`.
/// let _ = config;
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct SqliteCdcConfig {}

#[derive(Debug, Clone)]
struct TableMeta {
    table_id: TableId,
    column_names: Vec<String>,
    column_types: Vec<ColumnType>,
    pk_column_ids: Vec<ColumnId>,
}

/// In-process SQLite-backed CDC source.
///
/// Constructed from an owned [`SqliteConnection`] plus an
/// [`Arc<ParserDB>`]. At construction time the source installs a unified
/// shadow-log table (`_subql_cdc_event_log`) and per-table triggers that
/// record each mutation's row image. From that point on, every
/// `INSERT` / `UPDATE` / `DELETE` executed against the wrapped
/// connection appends a row to the shadow log, which the consumer
/// drains via [`CdcSource::next_event`].
///
/// # Examples
///
/// The end-to-end test pattern: build a source from a single PG-dialect
/// DDL string, run DML against it, drain a typed event.
///
/// ```
/// use diesel::{Connection, SqliteConnection};
/// use subql::{Cell, EventKind, SqliteCdcConfig, SqliteCdcSource};
///
/// let conn = SqliteConnection::establish(":memory:")?;
/// let mut source = SqliteCdcSource::with_pg_ddl(
///     conn,
///     "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);",
///     SqliteCdcConfig::default(),
/// )?;
///
/// source.execute("INSERT INTO orders (id, price, status) VALUES (1, 9.5, 'paid')")?;
///
/// let event = source
///     .poll_next_event()?
///     .expect("INSERT must produce one event");
/// assert_eq!(event.kind(), EventKind::Insert);
/// assert_eq!(event.pk().values(), &[Cell::Int(1)]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct SqliteCdcSource {
    connection: SqliteConnection,
    catalog: Arc<ParserDB>,
    config: SqliteCdcConfig,
    tables: HashMap<String, TableMeta>,
}

impl SqliteCdcSource {
    /// Build a new source from an owned connection and catalog.
    ///
    /// The catalog is consulted to discover the SQLite tables for which
    /// triggers should be installed and the column type metadata used to
    /// decode shadow-log payloads back into [`Cell`] values. Tables
    /// present in SQLite but absent from the catalog stay untracked
    /// (Step 2's contract is "catalog drives coverage").
    ///
    /// # Errors
    ///
    /// Returns [`SqliteCdcError::Diesel`] when shadow-log creation or
    /// trigger installation fails on the underlying connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{SqliteCdcConfig, SqliteCdcSource};
    ///
    /// let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    /// )?);
    ///
    /// let mut conn = SqliteConnection::establish(":memory:")?;
    /// sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
    ///     .execute(&mut conn)?;
    ///
    /// let source = SqliteCdcSource::new(conn, catalog, SqliteCdcConfig::default())?;
    /// assert!(subql::catalog_helpers::table_id(source.catalog(), "orders").is_some());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(
        mut connection: SqliteConnection,
        catalog: Arc<ParserDB>,
        config: SqliteCdcConfig,
    ) -> Result<Self, SqliteCdcError> {
        let tables = build_table_index(&catalog);

        install_event_log(&mut connection)?;
        for (table_name, meta) in &tables {
            install_triggers_for_table(
                &mut connection,
                table_name,
                &meta.column_names,
                &meta.column_types,
            )?;
        }

        Ok(Self {
            connection,
            catalog,
            config,
            tables,
        })
    }

    /// Convenience constructor: translate a PG-dialect DDL stream onto
    /// SQLite via `pg2sqlite`, build a [`ParserDB`] from the same DDL,
    /// then forward to [`Self::new`] so a single call sets up both
    /// sides of the catalog in lockstep.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteCdcError::Pg2Sqlite`] on translation failure,
    /// [`SqliteCdcError::ParseDdl`] when `ParserDB` rejects the DDL,
    /// and [`SqliteCdcError::Diesel`] on Diesel statement failures.
    ///
    /// # Examples
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use subql::{SqliteCdcConfig, SqliteCdcSource};
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let source = SqliteCdcSource::with_pg_ddl(
    ///     conn,
    ///     "CREATE TABLE items (\
    ///         region_id INT, item_id INT, name TEXT, \
    ///         PRIMARY KEY (region_id, item_id));",
    ///     SqliteCdcConfig::default(),
    /// )?;
    /// let id = subql::catalog_helpers::table_id(source.catalog(), "items")
    ///     .expect("items table registered");
    /// assert_eq!(
    ///     subql::catalog_helpers::primary_key_columns(source.catalog(), id),
    ///     Some(vec![0u16, 1u16])
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_pg_ddl(
        mut connection: SqliteConnection,
        pg_ddl: &str,
        config: SqliteCdcConfig,
    ) -> Result<Self, SqliteCdcError> {
        catalog::apply_pg_ddl(&mut connection, pg_ddl)?;
        let parser_db = ParserDB::parse::<PostgreSqlDialect>(pg_ddl)
            .map_err(|e| SqliteCdcError::ParseDdl(format!("{e}")))?;
        Self::new(connection, Arc::new(parser_db), config)
    }

    /// Execute a raw SQL statement against the wrapped SQLite connection.
    ///
    /// Each row mutation is captured by the installed triggers, so by
    /// the time this call returns the shadow log holds one entry per
    /// affected row. The consumer then drains them via
    /// [`CdcSource::next_event`].
    ///
    /// # Errors
    ///
    /// Forwards any [`diesel::result::Error`] produced by the statement.
    ///
    /// # Examples
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use subql::{EventKind, SqliteCdcConfig, SqliteCdcSource};
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let mut source = SqliteCdcSource::with_pg_ddl(
    ///     conn,
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    ///     SqliteCdcConfig::default(),
    /// )?;
    ///
    /// let affected = source.execute(
    ///     "INSERT INTO orders (id, status) VALUES (1, 'paid'), (2, 'open')",
    /// )?;
    /// assert_eq!(affected, 2);
    ///
    /// // Two events appear on the shadow log, one per row.
    /// assert_eq!(
    ///     source.poll_next_event()?.map(|e| e.kind()),
    ///     Some(EventKind::Insert)
    /// );
    /// assert_eq!(
    ///     source.poll_next_event()?.map(|e| e.kind()),
    ///     Some(EventKind::Insert)
    /// );
    /// assert!(source.poll_next_event()?.is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn execute(&mut self, sql: &str) -> Result<usize, SqliteCdcError> {
        let rows = sql_query(sql).execute(&mut self.connection)?;
        Ok(rows)
    }

    /// Borrow the catalog the source resolves SQLite table names against.
    ///
    /// # Examples
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use subql::{catalog_helpers, SqliteCdcConfig, SqliteCdcSource};
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let source = SqliteCdcSource::with_pg_ddl(
    ///     conn,
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    ///     SqliteCdcConfig::default(),
    /// )?;
    /// let id = catalog_helpers::table_id(source.catalog(), "orders").unwrap();
    /// assert_eq!(catalog_helpers::table_arity(source.catalog(), id), Some(2));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn catalog(&self) -> &ParserDB {
        &self.catalog
    }

    /// Borrow the configuration the source was built with.
    ///
    /// # Examples
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use subql::{SqliteCdcConfig, SqliteCdcSource};
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let source = SqliteCdcSource::with_pg_ddl(
    ///     conn,
    ///     "CREATE TABLE t (id INT PRIMARY KEY);",
    ///     SqliteCdcConfig::default(),
    /// )?;
    /// // Reflexive equality is enough to prove the accessor returns the
    /// // stored config; nothing semantic to assert on an empty struct.
    /// let _: &SqliteCdcConfig = source.config();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub const fn config(&self) -> &SqliteCdcConfig {
        &self.config
    }

    /// Drain a single pending event, returning `Ok(None)` when the
    /// shadow log is empty. Mirrors the shape of the async
    /// [`CdcSource::next_event`] without forcing tests into an
    /// executor.
    ///
    /// # Errors
    ///
    /// Surfaces Diesel transport failures and shadow-decode errors as
    /// the corresponding [`SqliteCdcError`] variants.
    ///
    /// # Examples
    ///
    /// Each DML statement enqueues exactly one event per affected row;
    /// `poll_next_event` walks the shadow log in deterministic FIFO
    /// order.
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use subql::{Cell, EventKind, SqliteCdcConfig, SqliteCdcSource};
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let mut source = SqliteCdcSource::with_pg_ddl(
    ///     conn,
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    ///     SqliteCdcConfig::default(),
    /// )?;
    ///
    /// source.execute("INSERT INTO orders (id, status) VALUES (1, 'open')")?;
    /// source.execute("UPDATE orders SET status = 'paid' WHERE id = 1")?;
    /// source.execute("DELETE FROM orders WHERE id = 1")?;
    ///
    /// let insert = source.poll_next_event()?.unwrap();
    /// assert_eq!(insert.kind(), EventKind::Insert);
    /// let update = source.poll_next_event()?.unwrap();
    /// assert_eq!(update.kind(), EventKind::Update);
    /// assert_eq!(
    ///     update.old_row().unwrap().cells[1],
    ///     Cell::String("open".into())
    /// );
    /// let delete = source.poll_next_event()?.unwrap();
    /// assert_eq!(delete.kind(), EventKind::Delete);
    /// assert!(source.poll_next_event()?.is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn poll_next_event(&mut self) -> Result<Option<WalEvent>, SqliteCdcError> {
        let Some(record) = drain_next_shadow_row(&mut self.connection)? else {
            return Ok(None);
        };
        self.translate_record(&record).map(Some)
    }

    fn translate_record(&self, record: &ShadowRow) -> Result<WalEvent, SqliteCdcError> {
        let meta = self
            .tables
            .get(record.table_name.as_str())
            .ok_or_else(|| SqliteCdcError::UnknownTable(record.table_name.clone()))?;

        match record.op.as_str() {
            "I" => {
                let new_cells = decode_image(
                    record.new_image.as_ref(),
                    &meta.column_types,
                    &record.table_name,
                    "new",
                )?;
                let pk = build_pk(meta, &new_cells)?;
                WalEvent::builder(meta.table_id)
                    .insert()
                    .pk(pk)
                    .new_row(RowImage {
                        cells: Arc::from(new_cells.as_slice()),
                    })
                    .build()
                    .map_err(|e| SqliteCdcError::BuildEvent(format!("{e}")))
            }
            "U" => {
                let old_cells = decode_image(
                    record.old_image.as_ref(),
                    &meta.column_types,
                    &record.table_name,
                    "old",
                )?;
                let new_cells = decode_image(
                    record.new_image.as_ref(),
                    &meta.column_types,
                    &record.table_name,
                    "new",
                )?;
                let pk = build_pk(meta, &new_cells)?;
                let changed = changed_column_ids(&old_cells, &new_cells);
                WalEvent::builder(meta.table_id)
                    .update()
                    .pk(pk)
                    .old_row(RowImage {
                        cells: Arc::from(old_cells.as_slice()),
                    })
                    .new_row(RowImage {
                        cells: Arc::from(new_cells.as_slice()),
                    })
                    .changed_columns(Arc::from(changed.as_slice()))
                    .build()
                    .map_err(|e| SqliteCdcError::BuildEvent(format!("{e}")))
            }
            "D" => {
                let old_cells = decode_image(
                    record.old_image.as_ref(),
                    &meta.column_types,
                    &record.table_name,
                    "old",
                )?;
                let pk = build_pk(meta, &old_cells)?;
                WalEvent::builder(meta.table_id)
                    .delete()
                    .pk(pk)
                    .old_row(RowImage {
                        cells: Arc::from(old_cells.as_slice()),
                    })
                    .build()
                    .map_err(|e| SqliteCdcError::BuildEvent(format!("{e}")))
            }
            other => Err(SqliteCdcError::ShadowDecode(format!(
                "unknown op `{other}` in shadow log"
            ))),
        }
    }
}

fn build_table_index(catalog: &ParserDB) -> HashMap<String, TableMeta> {
    let mut tables = HashMap::new();
    for index in 0..catalog.number_of_tables() {
        let Some(table) = catalog.table_by_id(index) else {
            continue;
        };
        let Some(table_id) = catalog
            .table_id(table)
            .and_then(|id| u32::try_from(id).ok())
        else {
            continue;
        };
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();
        for column in table.columns(catalog) {
            let Some(ordinal) = column
                .column_id(catalog)
                .and_then(|id| u16::try_from(id).ok())
            else {
                continue;
            };
            column_names.push(column.column_name().to_string());
            column_types.push(
                catalog_helpers::column_type(catalog, table_id, ordinal)
                    .unwrap_or(ColumnType::Unknown),
            );
        }
        let pk_column_ids =
            catalog_helpers::primary_key_columns(catalog, table_id).unwrap_or_default();

        let meta = TableMeta {
            table_id,
            column_names,
            column_types,
            pk_column_ids,
        };
        tables.insert(table.table_name().to_string(), meta);
    }
    tables
}

fn install_event_log(connection: &mut SqliteConnection) -> Result<(), SqliteCdcError> {
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
            seq INTEGER PRIMARY KEY AUTOINCREMENT, \
            table_name TEXT NOT NULL, \
            op TEXT NOT NULL, \
            old_image TEXT, \
            new_image TEXT)",
        table = quote_ident(EVENT_LOG_TABLE),
    );
    connection.batch_execute(&ddl)?;
    Ok(())
}

fn install_triggers_for_table(
    connection: &mut SqliteConnection,
    table_name: &str,
    column_names: &[String],
    column_types: &[ColumnType],
) -> Result<(), SqliteCdcError> {
    if column_names.is_empty() {
        return Ok(());
    }
    let qt = quote_ident(table_name);
    let qlog = quote_ident(EVENT_LOG_TABLE);
    let new_img = json_array_expr(column_names, column_types, "NEW");
    let old_img = json_array_expr(column_names, column_types, "OLD");
    let table_lit = sql_string_literal(table_name);
    let trig_suffix = sanitize_for_identifier(table_name);

    let ins_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS {trig} \
         AFTER INSERT ON {qt} FOR EACH ROW BEGIN \
            INSERT INTO {qlog} (table_name, op, new_image) \
            VALUES ({table_lit}, 'I', {new_img}); \
         END",
        trig = quote_ident(&format!("_subql_cdc_ins__{trig_suffix}")),
    );
    connection.batch_execute(&ins_sql)?;

    let upd_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS {trig} \
         AFTER UPDATE ON {qt} FOR EACH ROW BEGIN \
            INSERT INTO {qlog} (table_name, op, old_image, new_image) \
            VALUES ({table_lit}, 'U', {old_img}, {new_img}); \
         END",
        trig = quote_ident(&format!("_subql_cdc_upd__{trig_suffix}")),
    );
    connection.batch_execute(&upd_sql)?;

    let del_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS {trig} \
         BEFORE DELETE ON {qt} FOR EACH ROW BEGIN \
            INSERT INTO {qlog} (table_name, op, old_image) \
            VALUES ({table_lit}, 'D', {old_img}); \
         END",
        trig = quote_ident(&format!("_subql_cdc_del__{trig_suffix}")),
    );
    connection.batch_execute(&del_sql)?;

    Ok(())
}

#[derive(Debug)]
struct ShadowRow {
    seq: i64,
    table_name: String,
    op: String,
    old_image: Option<String>,
    new_image: Option<String>,
}

fn drain_next_shadow_row(
    connection: &mut SqliteConnection,
) -> Result<Option<ShadowRow>, SqliteCdcError> {
    let select_sql = format!(
        "SELECT seq, table_name, op, old_image, new_image \
         FROM {log} ORDER BY seq LIMIT 1",
        log = quote_ident(EVENT_LOG_TABLE),
    );
    let row_opt = {
        let query = sql_query(select_sql);
        let mut rows = connection.load(query)?;
        let Some(first) = rows.next() else {
            return Ok(None);
        };
        let row = first?;

        let seq_field = row.get(0).ok_or_else(|| {
            SqliteCdcError::ShadowDecode("shadow log row missing seq column".into())
        })?;
        let mut seq_value = seq_field
            .value()
            .ok_or_else(|| SqliteCdcError::ShadowDecode("shadow log seq is NULL".into()))?;
        let seq = seq_value.read_long();

        let table_field = row.get(1).ok_or_else(|| {
            SqliteCdcError::ShadowDecode("shadow log row missing table_name column".into())
        })?;
        let mut table_value = table_field
            .value()
            .ok_or_else(|| SqliteCdcError::ShadowDecode("shadow log table_name is NULL".into()))?;
        let table_name = table_value.read_text().to_string();

        let op_field = row.get(2).ok_or_else(|| {
            SqliteCdcError::ShadowDecode("shadow log row missing op column".into())
        })?;
        let mut op_value = op_field
            .value()
            .ok_or_else(|| SqliteCdcError::ShadowDecode("shadow log op is NULL".into()))?;
        let op = op_value.read_text().to_string();

        let read_image = |idx: usize| {
            let field = row.get(idx)?;
            let mut value = field.value()?;
            Some(value.read_text().to_string())
        };
        let old_image = read_image(3);
        let new_image = read_image(4);

        ShadowRow {
            seq,
            table_name,
            op,
            old_image,
            new_image,
        }
    };

    let delete_sql = format!(
        "DELETE FROM {log} WHERE seq = ?",
        log = quote_ident(EVENT_LOG_TABLE)
    );
    sql_query(delete_sql)
        .bind::<BigInt, _>(row_opt.seq)
        .execute(connection)?;

    Ok(Some(row_opt))
}

fn decode_image(
    image_str: Option<&String>,
    column_types: &[ColumnType],
    table_name: &str,
    side: &'static str,
) -> Result<Vec<Cell>, SqliteCdcError> {
    let Some(image_str) = image_str else {
        return Err(SqliteCdcError::ShadowMissingSide {
            table: table_name.to_string(),
            side,
        });
    };
    let values: Vec<serde_json::Value> = serde_json::from_str(image_str).map_err(|e| {
        SqliteCdcError::ShadowDecode(format!(
            "json_array payload for table `{table_name}` ({side}): {e}"
        ))
    })?;
    let mut cells = Vec::with_capacity(values.len());
    for (i, value) in values.into_iter().enumerate() {
        let ty = column_types.get(i).copied().unwrap_or(ColumnType::Unknown);
        cells.push(json_value_to_cell(value, ty));
    }
    Ok(cells)
}

fn json_value_to_cell(value: serde_json::Value, ty: ColumnType) -> Cell {
    // Float columns are encoded by the trigger as the shortest decimal
    // string that uniquely identifies the f64 (see [`json_array_expr`]),
    // so a JSON string here is the parser's responsibility, not the
    // raw value's textual identity.
    if matches!(ty, ColumnType::Float) {
        if let serde_json::Value::String(ref s) = value {
            return s.parse::<f64>().map_or(Cell::Null, Cell::Float);
        }
    }
    match value {
        serde_json::Value::Bool(b) => Cell::Bool(b),
        serde_json::Value::String(s) => Cell::String(Arc::from(s.as_str())),
        serde_json::Value::Number(n) => json_number_to_cell(&n, ty),
        // Null, Array, and Object all collapse to Cell::Null because the
        // catalog does not declare JSON-typed columns yet.
        _ => Cell::Null,
    }
}

fn json_number_to_cell(n: &serde_json::Number, ty: ColumnType) -> Cell {
    match ty {
        ColumnType::Int => n.as_i64().map_or_else(
            || {
                #[allow(clippy::cast_possible_truncation)]
                n.as_f64().map_or(Cell::Null, |f| Cell::Int(f as i64))
            },
            Cell::Int,
        ),
        ColumnType::Float => n.as_f64().map_or(Cell::Null, Cell::Float),
        ColumnType::Bool => Cell::Bool(n.as_i64().is_some_and(|i| i != 0)),
        ColumnType::String | ColumnType::Unknown => n
            .as_i64()
            .map_or_else(|| n.as_f64().map_or(Cell::Null, Cell::Float), Cell::Int),
    }
}

fn build_pk(meta: &TableMeta, cells: &[Cell]) -> Result<PrimaryKey, SqliteCdcError> {
    if meta.pk_column_ids.is_empty() {
        return Ok(PrimaryKey::empty());
    }
    let mut columns = Vec::with_capacity(meta.pk_column_ids.len());
    let mut values = Vec::with_capacity(meta.pk_column_ids.len());
    for &col in &meta.pk_column_ids {
        let cell =
            cells
                .get(col as usize)
                .cloned()
                .ok_or_else(|| SqliteCdcError::MissingColumn {
                    table_id: meta.table_id,
                    column: meta
                        .column_names
                        .get(col as usize)
                        .cloned()
                        .unwrap_or_default(),
                })?;
        columns.push(col);
        values.push(cell);
    }
    PrimaryKey::new(Arc::from(columns), Arc::from(values))
        .map_err(|e| SqliteCdcError::BuildEvent(format!("{e}")))
}

fn changed_column_ids(old: &[Cell], new: &[Cell]) -> Vec<ColumnId> {
    let len = old.len().min(new.len());
    let mut changed = Vec::new();
    for i in 0..len {
        if old[i] != new[i] {
            if let Ok(id) = u16::try_from(i) {
                changed.push(id);
            }
        }
    }
    changed
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn json_array_expr(column_names: &[String], column_types: &[ColumnType], side: &str) -> String {
    let args = column_names
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let raw = format!("{side}.{}", quote_ident(c));
            // SQLite's json_array emits REAL values with limited
            // precision, which loses the last 1-2 digits of an f64.
            // Format the value with 17 significant digits (the minimum
            // count guaranteed by IEEE-754 to round-trip every finite
            // f64 through strtod), wrapped in a JSON string so the
            // decoder can call f64::from_str on it for an exact
            // recovery. Integer-typed columns skip the indirection.
            let ty = column_types.get(i).copied().unwrap_or(ColumnType::Unknown);
            if matches!(ty, ColumnType::Float) {
                // The `!` flag enables SQLite's altform-2 mode that
                // caps the digit budget at 26 (vs 16 default), and
                // `.17` ensures we ask for the 17 significant digits
                // IEEE-754 guarantees for round-tripping an f64.
                format!("printf('%!.17g', {raw})")
            } else {
                raw
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("json_array({args})")
}

/// Replace characters that would have to be quoted inside a SQLite
/// identifier with `_`, so trigger names stay deterministic and easy to
/// read in tooling that introspects `sqlite_schema`.
fn sanitize_for_identifier(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl CdcSource for SqliteCdcSource {
    type Checkpoint = NoCheckpoint;
    type Error = SqliteCdcError;

    #[allow(clippy::manual_async_fn)]
    fn next_event(
        &mut self,
    ) -> impl Future<Output = Result<Option<WalEvent<Self::Checkpoint>>, Self::Error>> + Send {
        async move { self.poll_next_event() }
    }

    #[allow(clippy::manual_async_fn)]
    fn ack(
        &mut self,
        _upto: Self::Checkpoint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }
}
