//! Error surface for [`PgSqliteEmuSource`](super::PgSqliteEmuSource).

use alloc::string::String;

use diesel_sqlite_session::SessionError;

use pg_walstream::error::ReplicationError;

/// Everything a `PgSqliteEmuSource` can fail with.
///
/// `#[non_exhaustive]` so we can add variants without breaking downstream
/// callers.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgSqliteEmuError {
    /// `pg2sqlite` refused to translate the PG DDL.
    #[error("pg2sqlite translate: {0}")]
    Translate(String),
    /// The translated SQLite DDL failed to apply to the local database.
    #[error("apply sqlite DDL: {0}")]
    ApplyDdl(#[from] diesel::result::Error),
    /// The PG DDL string could not be parsed into a `ParserDB`.
    #[error("parse PG catalog: {0}")]
    Catalog(String),
    /// Underlying `diesel-sqlite-session` failure (session creation,
    /// attach, or changeset export).
    #[error("diesel-sqlite-session: {0}")]
    Session(#[from] SessionError),
    /// The changeset bytes emitted by SQLite did not parse.
    #[error("changeset parse: {0}")]
    Changeset(#[from] sqlite_diff_rs::ParseError),
    /// The re-encoded pgoutput frames failed to decode back into a
    /// `ChangeEvent` through the `PgOutputDecoder`.
    #[error("pgoutput decode: {0}")]
    PgOutput(#[from] ReplicationError),
    /// The changeset referenced a table the emulator's PG catalog does
    /// not know about. Fires when the caller ran DDL directly against
    /// the underlying `SqliteConnection` instead of routing it through
    /// the emulator's PG-DDL bootstrap.
    #[error("changeset referenced unknown table: {0}")]
    UnknownTable(String),
    /// A PG column carries a type OID that the encoder does not know
    /// how to shape from a SQLite value.
    #[error("unsupported column type: {0}")]
    UnsupportedColumnType(String),
}
