//! In-process `pgoutput` round-trip glue for [`SqliteCdcSource`].
//!
//! [`SqliteCdcSource`] already produces typed [`WalEvent`]s without going
//! through the wire format. The pgoutput round-trip doctests and proptests want a
//! stronger statement: that the engine produces the same notifications
//! when its input is `WalEvent`s decoded from real `pgoutput` bytes by
//! the production [`crate::PgOutputParser`], so the parser surface is
//! exercised inside `cargo test` (no Docker).
//!
//! [`encode_event`] takes one subql [`WalEvent`] plus the catalog and
//! returns one or two `pgoutput` message frames: a [`Relation`] message
//! the first time we see a table (so the parser caches the schema), then
//! the [`Insert`] / [`Update`] / [`Delete`] / [`Truncate`] message
//! itself. The caller feeds each frame to
//! [`crate::PgOutputParser::parse_wal_message`] in order.
//!
//! [`Relation`]: pg_walstream::LogicalReplicationMessage::Relation
//! [`Insert`]: pg_walstream::LogicalReplicationMessage::Insert
//! [`Update`]: pg_walstream::LogicalReplicationMessage::Update
//! [`Delete`]: pg_walstream::LogicalReplicationMessage::Delete
//! [`Truncate`]: pg_walstream::LogicalReplicationMessage::Truncate
//! [`SqliteCdcSource`]: crate::SqliteCdcSource

use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;

use bytes::BytesMut;
use hashbrown::HashSet;
use pg_walstream::{
    encode_message, ColumnData, ColumnInfo, LogicalReplicationMessage, Oid, TupleData,
};
use sql_traits::prelude::DatabaseLike;

use crate::sqlite_cdc::error::SqliteCdcError;
use crate::{catalog_helpers, Cell, ColumnType, RowImage, TableId, WalEvent};

/// `pgoutput` protocol version 1: base messages only. Matches what the
/// production parser handles by default.
const PROTOCOL_VERSION: u8 = 1;

/// `f` in pgoutput: every column ships in the old-image tuple. SQLite's
/// shadow log always records full rows, so this is honest.
const REPLICA_IDENTITY_FULL: u8 = b'f';

/// Tracks which relations have already been announced to a downstream
/// [`crate::PgOutputParser`].
///
/// A `pgoutput` parser caches relation schemas by OID. Data messages
/// fail until a [`Relation`] for their OID arrives. This struct
/// remembers which `(TableId, OID)` pairs have been emitted so
/// repeat events for the same table only ship the data frame.
///
/// [`Relation`]: pg_walstream::LogicalReplicationMessage::Relation
#[derive(Clone, Debug, Default)]
pub struct PgOutputBridge {
    announced: HashSet<TableId>,
}

impl PgOutputBridge {
    /// Build an empty bridge with no relations announced yet.
    ///
    /// # Examples
    ///
    /// pgoutput round trip: drive DML through [`crate::SqliteCdcSource`],
    /// encode the resulting event as `pgoutput` bytes, decode it back
    /// through [`crate::PgOutputParser`], dispatch with the engine.
    ///
    /// ```
    /// use diesel::{Connection, SqliteConnection};
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::wal::WalParser;
    /// use subql::{
    ///     DefaultIds, EventKind, PgOutputBridge, PgOutputParser, SqliteCdcConfig,
    ///     SqliteCdcSource, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// const PG_DDL: &str =
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";
    ///
    /// let mut engine = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(PG_DDL)?,
    ///     PostgreSqlDialect {},
    /// );
    /// engine.register(SubscriptionRequest::new(
    ///     42u64,
    ///     "SELECT * FROM orders WHERE amount > 100",
    /// ))?;
    ///
    /// let conn = SqliteConnection::establish(":memory:")?;
    /// let mut source = SqliteCdcSource::with_pg_ddl(conn, PG_DDL, SqliteCdcConfig::default())?;
    /// source.execute("INSERT INTO orders (id, amount, status) VALUES (1, 250, 'paid')")?;
    ///
    /// let source_event = source.poll_next_event()?.expect("INSERT must produce an event");
    /// assert_eq!(source_event.kind(), EventKind::Insert);
    ///
    /// // Encode the typed event as pgoutput wire bytes, decode them
    /// // back through the production parser, drive the engine with
    /// // the parser's output. This is the pgoutput round trip.
    /// let mut bridge = PgOutputBridge::new();
    /// let parser = PgOutputParser::new();
    /// let frames = bridge.encode_event(&source_event, source.catalog())?;
    /// let mut decoded = Vec::new();
    /// for frame in frames {
    ///     decoded.extend(parser.parse_wal_message(&frame, source.catalog())?);
    /// }
    /// let parsed = decoded.into_iter().next().expect("Insert decodes to one event");
    ///
    /// let notifs = engine.consumers(&parsed)?;
    /// assert_eq!(notifs.inserted(), &[42]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Encode a [`WalEvent`] as one or two `pgoutput` wire frames ready
    /// to feed into [`crate::PgOutputParser`]'s `parse_wal_message`.
    ///
    /// The first call for any given table emits two frames: a
    /// [`Relation`] message followed by the data message. Later calls
    /// for the same table emit only the data frame.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteCdcError::Bridge`] when the catalog is missing
    /// the table or any of its columns.
    ///
    /// [`Relation`]: pg_walstream::LogicalReplicationMessage::Relation
    pub fn encode_event<DB: DatabaseLike>(
        &mut self,
        event: &WalEvent,
        database: &DB,
    ) -> Result<Vec<BytesMut>, SqliteCdcError> {
        let table_id = event.table_id();
        let mut frames = Vec::with_capacity(2);
        if self.announced.insert(table_id) {
            frames.push(encode_one(&relation_message(table_id, database)?));
        }
        if let Some(msg) = data_message(event, database)? {
            frames.push(encode_one(&msg));
        }
        Ok(frames)
    }
}

fn encode_one(msg: &LogicalReplicationMessage) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_message(msg, PROTOCOL_VERSION, &mut buf);
    buf
}

fn relation_message<DB: DatabaseLike>(
    table_id: TableId,
    database: &DB,
) -> Result<LogicalReplicationMessage, SqliteCdcError> {
    let arity = catalog_helpers::table_arity(database, table_id).ok_or(SqliteCdcError::Bridge(
        format!("table id {table_id} missing from catalog when emitting Relation"),
    ))?;
    let pk_cols = catalog_helpers::primary_key_columns(database, table_id).unwrap_or_default();
    let pk_set: HashSet<_> = pk_cols.iter().copied().collect();

    let (namespace, relation_name) = table_name(database, table_id)?;

    let mut columns = Vec::with_capacity(arity);
    for col_id in 0..arity {
        let col_id_u16 = u16::try_from(col_id).map_err(|_| {
            SqliteCdcError::Bridge(format!(
                "table id {table_id} arity {arity} exceeds u16 column space"
            ))
        })?;
        let name = column_name(database, table_id, col_id_u16)?;
        let ctype = catalog_helpers::column_type(database, table_id, col_id_u16).ok_or(
            SqliteCdcError::Bridge(format!(
                "table id {table_id} column id {col_id_u16} missing type token"
            )),
        )?;
        let flags = u8::from(pk_set.contains(&col_id_u16));
        columns.push(ColumnInfo {
            flags,
            name: Arc::from(name.as_str()),
            type_id: pg_type_oid(ctype),
            type_modifier: -1,
        });
    }

    Ok(LogicalReplicationMessage::Relation {
        relation_id: relation_oid(table_id),
        namespace: Arc::from(namespace.as_str()),
        relation_name: Arc::from(relation_name.as_str()),
        replica_identity: REPLICA_IDENTITY_FULL,
        columns,
    })
}

fn data_message<DB: DatabaseLike>(
    event: &WalEvent,
    database: &DB,
) -> Result<Option<LogicalReplicationMessage>, SqliteCdcError> {
    let table_id = event.table_id();
    let column_types = column_types(database, table_id)?;
    let oid = relation_oid(table_id);

    Ok(Some(match event {
        WalEvent::Insert { new_row, .. } => LogicalReplicationMessage::Insert {
            relation_id: oid,
            tuple: row_to_tuple(new_row, &column_types),
        },
        WalEvent::Update {
            old_row, new_row, ..
        } => {
            let new_tuple = row_to_tuple(new_row, &column_types);
            let (old_tuple, key_type) = old_row.as_ref().map_or((None, None), |row| {
                (Some(row_to_tuple(row, &column_types)), Some('O'))
            });
            LogicalReplicationMessage::Update {
                relation_id: oid,
                old_tuple,
                new_tuple,
                key_type,
            }
        }
        WalEvent::Delete { old_row, .. } => LogicalReplicationMessage::Delete {
            relation_id: oid,
            old_tuple: row_to_tuple(old_row, &column_types),
            key_type: 'O',
        },
        WalEvent::Truncate { .. } => LogicalReplicationMessage::Truncate {
            relation_ids: vec![oid],
            flags: 0,
        },
    }))
}

fn row_to_tuple(row: &RowImage, column_types: &[ColumnType]) -> TupleData {
    let columns: Vec<ColumnData> = row
        .cells
        .iter()
        .enumerate()
        .map(|(idx, cell)| {
            let ctype = column_types
                .get(idx)
                .copied()
                .unwrap_or(ColumnType::Unknown);
            cell_to_column_data(cell, ctype)
        })
        .collect();
    TupleData::new(columns)
}

fn cell_to_column_data(cell: &Cell, _ctype: ColumnType) -> ColumnData {
    match cell {
        Cell::Missing => ColumnData::unchanged(),
        Cell::Null => ColumnData::null(),
        Cell::Bool(true) => ColumnData::text(b"t".to_vec()),
        Cell::Bool(false) => ColumnData::text(b"f".to_vec()),
        Cell::Int(i) => ColumnData::text(i.to_string().into_bytes()),
        Cell::Float(f) => ColumnData::text(format_float(*f).into_bytes()),
        Cell::String(s) => ColumnData::text(s.as_bytes().to_vec()),
    }
}

fn format_float(f: f64) -> alloc::string::String {
    // f64 Display drops trailing zeros and skips a decimal point for
    // integral values ("1" not "1.0"). Subql's parser routes float OIDs
    // through `f64::from_str`, which accepts both forms, so the round
    // trip stays lossless within Rust's f64 precision.
    format!("{f}")
}

const fn pg_type_oid(ctype: ColumnType) -> Oid {
    match ctype {
        ColumnType::Bool => 16,
        ColumnType::Int => 20,
        ColumnType::Float => 701,
        // `text` (25) is the universal pgoutput text container, and
        // subql's `text_to_cell_strict` returns `Cell::String` for it,
        // which matches what we expect for `Cell::String` round-tripping.
        // `Unknown` shares the same fallback because pgoutput has no
        // "unknown" OID and any value already encoded as text stays
        // textual through the parser.
        ColumnType::String | ColumnType::Unknown => 25,
    }
}

const fn relation_oid(table_id: TableId) -> Oid {
    // Synthesise an OID from subql's compact `TableId`. The encoder and
    // decoder both live in this test path, so any reversible mapping
    // works. The parser only uses OIDs as cache keys.
    table_id
}

fn column_types<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<Vec<ColumnType>, SqliteCdcError> {
    let arity = catalog_helpers::table_arity(database, table_id).ok_or(SqliteCdcError::Bridge(
        format!("table id {table_id} missing from catalog when reading column types"),
    ))?;
    let mut out = Vec::with_capacity(arity);
    for col_id in 0..arity {
        let col_id_u16 = u16::try_from(col_id).map_err(|_| {
            SqliteCdcError::Bridge(format!(
                "table id {table_id} arity {arity} exceeds u16 column space"
            ))
        })?;
        let ctype = catalog_helpers::column_type(database, table_id, col_id_u16).ok_or(
            SqliteCdcError::Bridge(format!(
                "table id {table_id} column id {col_id_u16} missing type token"
            )),
        )?;
        out.push(ctype);
    }
    Ok(out)
}

fn table_name<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<(alloc::string::String, alloc::string::String), SqliteCdcError> {
    use sql_traits::prelude::TableLike;
    let table = database
        .table_by_id(table_id as usize)
        .ok_or(SqliteCdcError::Bridge(format!(
            "table id {table_id} missing from catalog when reading name"
        )))?;
    // sql-traits' `ParserDB` does not surface a per-table schema name on
    // `TableLike`; subql's resolver treats an empty namespace string as
    // "no schema qualifier", which is what we want for the round-trip.
    Ok((alloc::string::String::new(), table.table_name().to_string()))
}

fn column_name<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: crate::ColumnId,
) -> Result<alloc::string::String, SqliteCdcError> {
    use sql_traits::prelude::{ColumnLike, TableLike};
    let table = database
        .table_by_id(table_id as usize)
        .ok_or(SqliteCdcError::Bridge(format!(
            "table id {table_id} missing from catalog when reading column name"
        )))?;
    let column = table
        .columns(database)
        .nth(column_id as usize)
        .ok_or(SqliteCdcError::Bridge(format!(
            "table id {table_id} column id {column_id} missing from catalog"
        )))?;
    Ok(column.column_name().to_string())
}
