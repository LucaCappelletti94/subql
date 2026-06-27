//! Error type surfaced by [`SqliteCdcSource`](super::SqliteCdcSource).
//!
//! The variants are split by where the failure originated (Diesel transport,
//! schema lookup, Sqlite row decoding, unsupported event kind) so callers
//! can match on the failure mode without parsing strings.

use alloc::string::String;
use thiserror::Error;

use crate::TableId;

/// Errors returned by [`SqliteCdcSource`](super::SqliteCdcSource).
///
/// # Examples
///
/// Pattern-matching the error returned by an invalid PG DDL stream:
///
/// ```
/// use diesel::{Connection, SqliteConnection};
/// use subql::{SqliteCdcConfig, SqliteCdcError, SqliteCdcSource};
///
/// let conn = SqliteConnection::establish(":memory:")?;
/// let result = SqliteCdcSource::with_pg_ddl(conn, "not a DDL stream;", SqliteCdcConfig::default());
/// let err = match result {
///     Ok(_) => panic!("pg2sqlite must reject garbage"),
///     Err(e) => e,
/// };
/// assert!(matches!(
///     err,
///     SqliteCdcError::Pg2Sqlite(_) | SqliteCdcError::ParseDdl(_) | SqliteCdcError::Diesel(_)
/// ));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteCdcError {
    /// Underlying Diesel connection error (statement execution, transport).
    #[error("diesel: {0}")]
    Diesel(#[from] diesel::result::Error),

    /// The shadow log recorded a mutation for a Sqlite table that the
    /// source has no catalog entry for. Either the table was created
    /// after the source was constructed (the source only tracks tables
    /// declared in the [`crate::ParserDB`] at construction time) or
    /// catalog and Sqlite drifted out of sync.
    #[error("unknown table `{0}` in shadow log entry")]
    UnknownTable(String),

    /// The shadow log payload carries fewer cells than the catalog has
    /// PK columns, so PK derivation cannot complete. Indicates catalog
    /// drift between subql's `ParserDB` and the live Sqlite schema.
    #[error("column `{column}` missing from Sqlite row for table `{table_id}`")]
    MissingColumn {
        /// subql `TableId` that triggered the mismatch.
        table_id: TableId,
        /// Column name as declared in the catalog.
        column: String,
    },

    /// Failed to build a `WalEvent` from a decoded shadow row. Implies
    /// the primary-key shape violates the catalog's invariants, which
    /// only happens under catalog drift.
    #[error("could not build WalEvent: {0}")]
    BuildEvent(String),

    /// The pg2sqlite translator could not handle a PG-dialect DDL stream.
    /// Carries the translator's own error message verbatim.
    #[error("pg2sqlite translation failed: {0}")]
    Pg2Sqlite(String),

    /// subql's `ParserDB` could not parse the PG-dialect DDL stream the
    /// caller supplied to a convenience constructor.
    #[error("ParserDB rejected PG DDL: {0}")]
    ParseDdl(String),

    /// The shadow log produced by the BEFORE/AFTER triggers held a row
    /// shape that could not be decoded back into [`crate::Cell`] values.
    /// Indicates either a catalog drift or a malformed JSON payload (the
    /// shadow uses SQLite `json_array` to encode row images).
    #[error("shadow log decode failed: {0}")]
    ShadowDecode(String),

    /// The shadow log expected a paired entry (e.g., an UPDATE event has
    /// both old and new images) but the trigger produced an incomplete
    /// row. Indicates a programming error in the trigger installer.
    #[error("shadow log row for table `{table}` is missing the `{side}` image")]
    ShadowMissingSide {
        /// Sqlite table name the broken row references.
        table: String,
        /// Side label that was absent (`"old"` or `"new"`).
        side: &'static str,
    },

    /// The `pgoutput` round-trip bridge could not translate an event
    /// because the catalog was missing a table, column, or type token
    /// it needed. Tier-3 doctests and proptests surface here when the
    /// `ParserDB` falls out of sync with the events the SQLite source
    /// produced.
    #[error("pgoutput bridge: {0}")]
    Bridge(String),
}
