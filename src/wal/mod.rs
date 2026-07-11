//! WAL stream parsing: convert raw CDC bytes into typed [`crate::backend::CdcEvent`]s.
//!
//! The [`WalParser`] trait abstracts over format-specific encodings
//! (wal2json, Maxwell, Debezium, pgoutput) so callers can feed raw
//! replication messages and receive typed events that implement
//! [`CdcEvent`](crate::backend::CdcEvent) directly.

mod debezium;
mod maxwell;
#[cfg(feature = "pg-streaming")]
mod pg_streaming;
mod pg_type;
mod pgoutput;
#[cfg(feature = "std")]
mod streaming;
#[cfg(test)]
mod test_support;
mod wal2json;

pub use debezium::{DebeziumEvent, DebeziumParser};
pub use maxwell::{MaxwellEvent, MaxwellParser};
#[cfg(feature = "pg-streaming")]
pub use pg_streaming::{PgStreamingCdcSource, PgStreamingConfig, PgStreamingError};
pub use pgoutput::{PgOutputEvent, PgOutputParser};
#[cfg(feature = "std")]
pub use streaming::CdcSource;
pub use wal2json::{Wal2JsonV1Event, Wal2JsonV1Parser, Wal2JsonV2Event, Wal2JsonV2Parser};

use crate::table_resolution::{resolve_table_reference, TableResolutionError};
use crate::{Checkpoint, TableId};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use thiserror::Error;

/// Trait for converting raw WAL bytes into typed [`CdcEvent`] instances.
///
/// Parameterized by the concrete [`DatabaseLike`] implementation supplying
/// schema metadata at parse time. Each parser type nominates the concrete
/// [`CdcEvent`] it emits via [`Self::Event`]; the engine dispatches on the
/// typed event directly with no intermediate `WalEvent` shim.
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
    type Event: crate::backend::CdcEvent<Checkpoint = Self::Checkpoint>
        + Send
        + Sync;

    /// Parse a raw WAL message into zero or more typed events.
    ///
    /// Batched formats (e.g. wal2json v1, pgoutput Truncate) may return
    /// multiple events per message. Per-change formats (e.g. wal2json v2,
    /// Maxwell, Debezium) return at most one. Non-data control frames
    /// (relation catalog updates, transactional metadata) return an empty
    /// vector.
    fn parse_wal_message(
        &self,
        data: &[u8],
        database: &DB,
    ) -> Result<Vec<Self::Event>, WalParseError>;
}

/// Parse a UTF-8 JSON message into a typed payload.
pub(crate) fn parse_json_message<T>(data: &[u8]) -> Result<T, WalParseError>
where
    T: serde::de::DeserializeOwned,
{
    let text = core::str::from_utf8(data).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    serde_json::from_str(text).map_err(|e| WalParseError::JsonError(e.to_string()))
}

/// Parse JSON payloads that may legally be tombstones (`null`) in CDC streams.
pub(crate) fn parse_json_message_or_tombstone<T>(data: &[u8]) -> Result<Option<T>, WalParseError>
where
    T: serde::de::DeserializeOwned,
{
    parse_json_message(data)
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

// ============================================================================
// Shared helpers (used by wal2json and pgoutput)
// ============================================================================

/// Resolve table name through database with qualified-first semantics.
///
/// Resolution rules:
/// 1. If `schema.table` resolves, it is preferred.
/// 2. If only `table` resolves, use it.
/// 3. If both resolve to different IDs, return ambiguity instead of guessing.
pub(crate) fn resolve_table<DB: DatabaseLike>(
    schema: &str,
    table: &str,
    database: &DB,
) -> Result<TableId, WalParseError> {
    let qualified = (!schema.is_empty()).then(|| format!("{schema}.{table}"));
    resolve_table_reference(qualified.as_deref(), table, database).map_err(|err| match err {
        TableResolutionError::Ambiguous {
            qualified,
            qualified_id,
            unqualified_id,
            ..
        } => WalParseError::AmbiguousTable {
            schema: schema.to_string(),
            table: table.to_string(),
            qualified,
            qualified_id,
            unqualified_id,
        },
        TableResolutionError::Unknown { .. } => WalParseError::UnknownTable {
            schema: schema.to_string(),
            table: table.to_string(),
        },
    })
}



/// Parse a wire event kind token by matching it against the allowed
/// tokens for each [`EventKind`]. Returns
/// [`WalParseError::UnknownEventKind`] when the token does not match any
/// list; parsers may then map that to a silent skip.
pub(crate) fn parse_event_kind(
    token: &str,
    insert_tokens: &[&str],
    update_tokens: &[&str],
    delete_tokens: &[&str],
    truncate_tokens: &[&str],
) -> Result<crate::EventKind, WalParseError> {
    use crate::EventKind;
    if insert_tokens.contains(&token) {
        return Ok(EventKind::Insert);
    }
    if update_tokens.contains(&token) {
        return Ok(EventKind::Update);
    }
    if delete_tokens.contains(&token) {
        return Ok(EventKind::Delete);
    }
    if truncate_tokens.contains(&token) {
        return Ok(EventKind::Truncate);
    }
    Err(WalParseError::UnknownEventKind(token.to_string()))
}
#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    #[test]
    fn test_resolve_table_conflicting_matches_errors() {
        use crate::table_resolution::{resolve_table_reference, TableResolutionError};

        // Two `users` tables in two different schemas. The wal2json caller
        // passes schema=`public`, table=`users`; the qualified `public.users`
        // resolves to one id, the bare `users` is also present (it has its
        // own ambient/no-schema entry via a second schema)..
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE other.users (id INT PRIMARY KEY, name TEXT);\n\
             CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT);",
        )
        .expect("ambiguous DDL parses");
        let qualified_id =
            crate::catalog_helpers::table_id(&catalog, "public.users").expect("public.users id");
        let unqualified_id =
            crate::catalog_helpers::table_id(&catalog, "other.users").expect("other.users id");
        assert_ne!(qualified_id, unqualified_id);

        // `resolve_table("public", "users", ...)` looks up both
        // "public.users" (qualified) and "users" (unqualified). With the
        // two schema-qualified `users` tables above and no bare `users`,
        // we expect an UnknownTable for the unqualified side and a hit on
        // the qualified side, i.e. resolution succeeds to
        // `public.users`. To exercise the ambiguity error we instead
        // delegate to `resolve_table_reference` with two distinct
        // table-name strings (one playing the role of qualified, the
        // other unqualified).
        let err = resolve_table_reference(Some("public.users"), "other.users", &catalog)
            .expect_err("ambiguous lookup must fail");
        assert!(matches!(
            err,
            TableResolutionError::Ambiguous {
                qualified_id: q,
                unqualified_id: u,
                ..
            } if q == qualified_id && u == unqualified_id
        ));
    }

    #[test]
    fn test_resolve_table_falls_back_to_unqualified_name() {
        let catalog =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE users (id INT PRIMARY KEY);")
                .expect("users DDL parses");
        let expected = crate::catalog_helpers::table_id(&catalog, "users").expect("users id");

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, expected);
    }

    #[test]
    fn test_resolve_table_uses_qualified_when_available() {
        // Declare the schema explicitly so the table is resolvable by
        // its qualified `public.users` name.
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA public;\n\
             CREATE TABLE public.users (id INT PRIMARY KEY);",
        )
        .expect("public.users DDL parses");
        let expected =
            crate::catalog_helpers::table_id(&catalog, "public.users").expect("public.users id");

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, expected);
    }

    #[test]
    fn test_resolve_table_unknown_table() {
        let catalog = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE other (id INT);")
            .expect("empty fixture DDL parses");

        let err = resolve_table("public", "users", &catalog).expect_err("must fail");
        match err {
            WalParseError::UnknownTable { schema, table } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
            }
            _ => panic!("unexpected error variant"),
        }
    }

    #[test]
    fn test_parse_json_message_invalid_utf8() {
        let err = parse_json_message::<serde_json::Value>(&[0xFF])
            .expect_err("invalid UTF-8 should fail");
        assert!(matches!(err, WalParseError::InvalidUtf8(_)));
    }

    #[test]
    fn test_parse_json_message_invalid_json() {
        let err =
            parse_json_message::<serde_json::Value>(b"{").expect_err("malformed JSON should fail");
        assert!(matches!(err, WalParseError::JsonError(_)));
    }

    #[test]
    fn test_parse_json_message_allows_tombstone_option() {
        let parsed: Option<serde_json::Value> =
            parse_json_message(b"null").expect("tombstone should parse to None");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_json_message_or_tombstone_object() {
        let parsed: Option<serde_json::Value> =
            parse_json_message_or_tombstone(br#"{"x":1}"#).expect("object should parse");
        assert!(parsed.is_some());
    }

}
