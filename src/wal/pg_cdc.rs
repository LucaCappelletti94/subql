//! Polling-based PostgreSQL CDC reader.
//!
//! Wraps a `PgConnection` and a `wal2json` replication slot, drains
//! pending changes via `pg_logical_slot_get_changes`, parses them with
//! [`Wal2JsonV2Parser`], and surfaces them as `Vec<WalEvent<PgLsn>>`.
//! Slot lifecycle (create, advance, drop) is exposed too so a consumer
//! can replay from a known LSN.
//!
//! Sync API by design: the underlying diesel call is synchronous, so an
//! async wrapper would only hide a blocking call. Wrap [`next_batch`] in
//! `spawn_blocking` (tokio) or a worker thread if you need it inside an
//! async loop. The native `START_REPLICATION` streaming path is a
//! planned follow-on.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use diesel::{sql_query, PgConnection, RunQueryDsl};
use sql_traits::prelude::DatabaseLike;

use super::{Wal2JsonV2Parser, WalParseError, WalParser};
use crate::{PgLsn, WalEvent};

/// Errors returned by [`PgCdcReader`] operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReaderError {
    /// Database error from diesel (query failed, connection lost, etc.).
    #[error("diesel error: {0}")]
    Diesel(#[from] diesel::result::Error),
    /// A WAL message returned by the slot could not be parsed.
    #[error("wal parse error: {0}")]
    Parse(#[from] WalParseError),
}

/// Polling-based PostgreSQL CDC reader.
///
/// Holds an owned [`PgConnection`] and the slot name to poll. Created
/// once per slot; the connection is consumed for the reader's lifetime.
///
/// Not `Send` (diesel's `PgConnection` is `!Send`). If you need to move
/// the reader between threads, wrap it in `thread_local!` or recreate it
/// on the destination thread.
pub struct PgCdcReader<DB: DatabaseLike> {
    conn: PgConnection,
    slot_name: String,
    parser: Wal2JsonV2Parser,
    catalog: Arc<DB>,
}

impl<DB: DatabaseLike> PgCdcReader<DB> {
    /// Wrap an owned [`PgConnection`] for polling `slot_name`. The
    /// `catalog` is used by the parser to resolve table and column names
    /// in the wal2json messages into subql `TableId` / `ColumnId`.
    pub const fn new(conn: PgConnection, slot_name: String, catalog: Arc<DB>) -> Self {
        Self {
            conn,
            slot_name,
            parser: Wal2JsonV2Parser,
            catalog,
        }
    }

    /// Create the wal2json replication slot if it does not already exist.
    /// Returns `Ok(true)` if the slot was created here, `Ok(false)` if
    /// it already existed.
    pub fn ensure_slot(&mut self) -> Result<bool, ReaderError> {
        let exists: Vec<ExistsRow> = sql_query(format!(
            "SELECT 1 AS one FROM pg_replication_slots WHERE slot_name = '{}'",
            self.slot_name
        ))
        .load(&mut self.conn)?;
        if !exists.is_empty() {
            return Ok(false);
        }
        sql_query(format!(
            "SELECT pg_create_logical_replication_slot('{}', 'wal2json')",
            self.slot_name
        ))
        .execute(&mut self.conn)?;
        Ok(true)
    }

    /// Drop the slot. Subsequent [`next_batch`](Self::next_batch) calls
    /// will fail until [`ensure_slot`](Self::ensure_slot) is called
    /// again.
    pub fn drop_slot(&mut self) -> Result<(), ReaderError> {
        sql_query(format!(
            "SELECT pg_drop_replication_slot('{}')",
            self.slot_name
        ))
        .execute(&mut self.conn)?;
        Ok(())
    }

    /// Peek pending changes from the slot and parse them into
    /// [`WalEvent<PgLsn>`] entries. Returns an empty `Vec` when no
    /// changes are pending; the caller decides how long to wait before
    /// calling again.
    ///
    /// Uses wal2json options `format-version=2`, `include-pk=true`,
    /// `include-lsn=true` so every event carries its commit-time LSN.
    ///
    /// **Idempotent.** Uses `pg_logical_slot_peek_changes` rather than
    /// `..._get_changes`, so the slot's `confirmed_flush_lsn` does not
    /// advance as a side effect. Call [`advance`](Self::advance) after
    /// successfully applying the batch to release WAL the server can
    /// recycle.
    pub fn next_batch(&mut self) -> Result<Vec<WalEvent<PgLsn>>, ReaderError> {
        let rows: Vec<DataRow> = sql_query(format!(
            "SELECT data FROM pg_logical_slot_peek_changes(\
                '{}', NULL, NULL, \
                'format-version', '2', \
                'include-pk', 'true', \
                'include-lsn', 'true'\
            )",
            self.slot_name
        ))
        .load(&mut self.conn)?;
        let mut events = Vec::new();
        for row in rows {
            events.extend(
                self.parser
                    .parse_wal_message(row.data.as_bytes(), &*self.catalog)?,
            );
        }
        Ok(events)
    }

    /// Advance the slot's `confirmed_flush_lsn` to `upto`, allowing the
    /// server to recycle WAL segments before that point. Call after
    /// successfully applying events up to (and including) `upto`.
    pub fn advance(&mut self, upto: PgLsn) -> Result<(), ReaderError> {
        // `pg_replication_slot_advance` expects the LSN as a `pg_lsn`
        // literal in "hi/lo" hex notation. The u64 splits into two
        // u32 halves; truncation is intentional because LSNs are
        // 64-bit and exposed in PG as `(hi, lo)` u32 pairs.
        #[allow(clippy::cast_possible_truncation)]
        let hi = (upto.0 >> 32) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let lo = upto.0 as u32;
        let lsn_text = alloc::format!("{hi:X}/{lo:X}");
        sql_query(format!(
            "SELECT pg_replication_slot_advance('{}', '{}')",
            self.slot_name, lsn_text
        ))
        .execute(&mut self.conn)?;
        Ok(())
    }
}

#[derive(diesel::QueryableByName)]
struct DataRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    data: String,
}

#[derive(diesel::QueryableByName)]
struct ExistsRow {
    #[allow(dead_code)]
    #[diesel(sql_type = diesel::sql_types::Integer)]
    one: i32,
}
