//! WAL stream parsing: convert raw CDC bytes into typed
//! [`CdcEvent`](crate::backend::CdcEvent)s.
//!
//! The pgoutput, wal2json, and Maxwell paths implement `CdcEvent` directly on
//! the ecosystem message types (`pg_walstream::ChangeEvent`,
//! `wal2json_events::{MessageV2, ChangeV1}`, and `maxwell_cdc::Message`). The
//! [`WalParser`] trait remains for the SQLite changeset path.

mod change_event;
mod maxwell;
#[cfg(feature = "pg-streaming")]
mod pg_streaming;
pub(crate) mod pg_type;
pub(crate) mod shared_helpers;
#[cfg(feature = "std")]
mod streaming;
mod wal2json;
pub(crate) mod wire_event;

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
pub(crate) use change_event::into_engine_events;
pub use maxwell::parse_messages as parse_maxwell;
pub use maxwell_cdc::Message as MaxwellMessage;
#[cfg(feature = "pg-streaming")]
pub use pg_streaming::{PgStreamingCdcSource, PgStreamingConfig, PgStreamingError};
pub use pg_walstream::ChangeEvent;
pub(crate) use shared_helpers::{changed_columns_by_name, resolve_table};
#[cfg(feature = "std")]
pub use streaming::CdcSource;
pub use wal2json::{parse_wal2json_v1, parse_wal2json_v2};
pub use wal2json_events::{ChangeV1, MessageV2};

use crate::{Checkpoint, TableId};
use alloc::string::String;
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use thiserror::Error;

/// Trait for converting raw WAL bytes into typed [`CdcEvent`] instances.
///
/// Parameterized by the concrete [`DatabaseLike`] implementation supplying
/// schema metadata at parse time. Each parser type nominates the concrete
/// [`CdcEvent`] it emits via [`Self::Event`]; the engine dispatches on the
/// typed event directly with no intermediate `WalEvent` shim. The pgoutput,
/// wal2json, and Maxwell paths implement [`CdcEvent`] directly on their
/// ecosystem message types, leaving `SqliteChangesetParser` as the sole
/// implementor.
///
/// [`CdcEvent`]: crate::backend::CdcEvent
pub trait WalParser<DB: DatabaseLike>: Send + Sync {
    /// The [`Checkpoint`] type events from this parser anchor at.
    ///
    /// Must equal [`crate::backend::CdcEvent::Checkpoint`] on
    /// [`Self::Event`]. PostgreSQL-flavored parsers choose [`crate::PgLsn`];
    /// MySQL parsers choose [`crate::MysqlBinlogPos`]; position-free
    /// parsers use [`crate::NoCheckpoint`].
    type Checkpoint: Checkpoint;

    /// The typed CDC event this parser emits. Implements
    /// [`CdcEvent`](crate::backend::CdcEvent) so the engine consumes parser
    /// output through the same trait every other event surface uses.
    type Event: crate::backend::CdcEvent<Checkpoint = Self::Checkpoint> + Send + Sync;

    /// Parse a raw WAL message into zero or more typed events.
    ///
    /// A single SQLite changeset frame expands to one event per changed
    /// row. Control frames carrying no row changes return an empty vector.
    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError>;
}

/// Errors that can occur during WAL message parsing.
#[derive(Error, Clone, Debug)]
pub enum WalParseError {
    /// Raw bytes were not valid UTF-8 (required by JSON formats).
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(String),

    /// JSON deserialization failed.
    #[error("JSON error: {0}")]
    JsonError(String),

    /// Unrecognized event kind / action value.
    #[error("Unknown event kind: {0}")]
    UnknownEventKind(String),

    /// Table not found in schema database.
    #[error("Unknown table: {schema}.{table}")]
    UnknownTable { schema: String, table: String },

    /// Table reference resolves to conflicting qualified/unqualified IDs.
    #[error(
        "Ambiguous table resolution for {schema}.{table}: qualified '{qualified}' -> {qualified_id}, unqualified '{table}' -> {unqualified_id}"
    )]
    AmbiguousTable {
        schema: String,
        table: String,
        qualified: String,
        qualified_id: TableId,
        unqualified_id: TableId,
    },

    /// Column name not found in schema database.
    #[error("Unknown column '{column}' in table {table_id}")]
    UnknownColumn { table_id: TableId, column: String },

    /// A required JSON field was absent.
    #[error("Missing field: {0}")]
    MissingField(String),

    /// Payload structure is malformed (mismatched lengths, invalid counts, etc.).
    #[error("Malformed payload: {0}")]
    MalformedPayload(String),

    /// Numeric value cannot be represented in target runtime type.
    #[error("Numeric overflow in '{field}': value {value} does not fit into {target}")]
    NumericOverflow {
        field: String,
        value: String,
        target: &'static str,
    },

    /// WAL column count does not match database arity.
    #[error("Arity mismatch for table {table_id}: WAL has {wal_count} columns, database has {catalog_arity}")]
    ArityMismatch {
        table_id: TableId,
        wal_count: usize,
        catalog_arity: usize,
    },

    /// Binary message too short.
    #[error("Truncated binary message: expected {expected} bytes, got {actual}")]
    TruncatedMessage { expected: usize, actual: usize },

    /// DML references unknown relation OID (no preceding Relation message).
    #[error("Unknown relation OID: {0}")]
    UnknownRelationOid(u32),

    /// Unrecognized tuple data tag byte (expected 'n', 'u', or 't').
    #[error("Unknown tuple data tag: 0x{0:02X}")]
    UnknownTupleTag(u8),
}
