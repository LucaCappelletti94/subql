#![allow(clippy::type_complexity)]
//! Pool-backed LSN-aware sync [`Connector`] for PostgreSQL via r2d2.

use super::diesel_backend::boxed_postgres_read_query;
use super::diesel_connector::{load_page_postgres, load_scalar, load_scalar_row};
use super::pg_diesel_connector::read_current_lsn;
use super::{
    drain_cursor_buffer, run_setup_statements, Connector, CursorError, CursorId, ReadQuery,
    RowPage, ScalarRowError, SessionSetup, Snapshot,
};
use crate::backend::{ScalarFamily, Value};
use alloc::string::String;
use diesel::{sql_query, Connection, QueryResult, RunQueryDsl};
use thiserror::Error;

/// Pool-backed [`Connector`] for PostgreSQL.
///
/// Wraps an `r2d2::Pool` over `ConnectionManager<PgConnection>`, is
/// `Send + Sync`, and reads `pg_current_wal_lsn()` before the user query's
/// transaction for LSN-anchored snapshots.
///
/// Use this connector when the engine dispatches re-executions
/// concurrently (the async engine resolving a burst of reads) or when
/// snapshots may interleave with CDC re-executions. Each call to
/// [`execute_scalar`](Connector::execute_scalar) borrows a connection
/// from the pool for the duration of the transaction and releases it on
/// completion.
///
/// The pool's `get` failures (timeout, pool exhausted) surface as
/// [`PgR2D2Error::Pool`]. The transaction's diesel errors surface as
/// [`PgR2D2Error::Diesel`].
///
/// `Send + Sync` so it can be shared across async tasks running on a
/// multi-threaded runtime.
#[cfg(feature = "executor-diesel-postgres-r2d2")]
pub struct PgR2D2DieselConnector<S = ()> {
    pool: r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
    /// Cursors this connector holds open, each pinning one pooled connection
    /// inside its own transaction until closed. A cursor is the only way pages
    /// of a keyless result describe one instant, and holding a connection is
    /// its price, which is why only the pooled connector offers it.
    /// Locked per cursor rather than map-wide: a cursor is a serial resource,
    /// so two reads of the same one must not interleave their `FETCH`es, but
    /// reads of different cursors have no reason to wait on each other. A
    /// single map-wide lock held across a round trip would make them.
    cursors: parking_lot::Mutex<
        hashbrown::HashMap<CursorId, alloc::sync::Arc<parking_lot::Mutex<PgCursor>>>,
    >,
    next_cursor: core::sync::atomic::AtomicU64,
    _setup: core::marker::PhantomData<fn() -> S>,
}

/// One open cursor: the connection it pins, the position its snapshot sits at,
/// and rows already fetched but not yet delivered.
///
/// The leftover buffer is what keeps the byte budget exact. `FETCH` cannot be
/// undone, so a batch that overshoots the budget would otherwise have to be
/// returned whole or thrown away; carrying the remainder into the next page
/// does neither.
#[cfg(feature = "executor-diesel-postgres-r2d2")]
struct PgCursor {
    conn: r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
    name: String,
    /// `CLOSE <name>`, rendered once at open time.
    ///
    /// Held ready so [`Drop`] runs no allocation: `batch_execute` takes a
    /// `&str`, where `sql_query` would want an owned `String`.
    close_sql: String,
    checkpoint: Option<crate::PgLsn>,
    columns: alloc::vec::Vec<String>,
    leftover: alloc::collections::VecDeque<alloc::vec::Vec<Value<crate::backend::Postgres>>>,
}

/// Diesel's transaction manager for `PgConnection`.
///
/// Transaction control for a held cursor goes through this rather than raw
/// `BEGIN` and `COMMIT` text, so diesel's own depth counter tracks it. That
/// counter is what `r2d2` consults to decide a released connection is dirty,
/// and a raw `BEGIN` leaves it at zero, which makes an abandoned transaction
/// invisible to the pool.
#[cfg(feature = "executor-diesel-postgres-r2d2")]
use diesel::connection::TransactionManager as _;

#[cfg(feature = "executor-diesel-postgres-r2d2")]
type PgTxn = <diesel::PgConnection as diesel::Connection>::TransactionManager;

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl Drop for PgCursor {
    /// Ends the cursor's transaction whenever this value goes away.
    ///
    /// It runs when the registry entry is removed, by
    /// [`Connector::close_cursor`] or by a failed read, and when the connector
    /// itself is dropped with cursors still registered. It does NOT run merely
    /// because a read panicked: the registry holds the handle, so the entry
    /// outlives the unwind, which is what the caller-side guard in
    /// `AutoResolvingEngine::read_whole` is for. Blocking here is fine, this is
    /// sync code, and it is exactly what an async cursor cannot do.
    fn drop(&mut self) {
        use diesel::connection::SimpleConnection;
        // Best-effort and deliberately silent. The transaction is read only,
        // so failing to end it politely costs nothing that dropping the
        // connection would not already cost, and there is no caller left to
        // report to. Nothing here can panic, which `Drop` requires.
        let _ = self.conn.batch_execute(&self.close_sql);
        let _ = PgTxn::rollback_transaction(&mut *self.conn);
    }
}

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl PgR2D2DieselConnector {
    /// Wrap an `r2d2::Pool` already configured by the caller (max size,
    /// connection timeout, etc.).
    #[must_use]
    pub fn new(pool: r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>) -> Self {
        Self {
            pool,
            cursors: parking_lot::Mutex::new(hashbrown::HashMap::new()),
            next_cursor: core::sync::atomic::AtomicU64::new(1),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl<S: SessionSetup> PgR2D2DieselConnector<S> {
    /// Wrap an `r2d2::Pool` whose reads run the setup statements carried by the
    /// per-read [`SessionSetup`] value `S`.
    #[must_use]
    pub fn with_session_setup(
        pool: r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
    ) -> Self {
        Self {
            pool,
            cursors: parking_lot::Mutex::new(hashbrown::HashMap::new()),
            next_cursor: core::sync::atomic::AtomicU64::new(1),
            _setup: core::marker::PhantomData,
        }
    }
}

/// Errors returned by [`PgR2D2DieselConnector`]. Distinguishes "could not
/// get a connection from the pool" from "the database rejected the query"
/// so callers can decide whether to back off vs. propagate.
#[cfg(feature = "executor-diesel-postgres-r2d2")]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PgR2D2Error {
    /// The pool refused to hand out a connection (timeout, exhausted,
    /// shutting down).
    #[error("r2d2 pool error: {0}")]
    Pool(r2d2::Error),
    /// Diesel returned a database error while executing the query.
    #[error("diesel error: {0}")]
    Diesel(diesel::result::Error),
}

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl From<r2d2::Error> for PgR2D2Error {
    fn from(e: r2d2::Error) -> Self {
        Self::Pool(e)
    }
}

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl From<diesel::result::Error> for PgR2D2Error {
    fn from(e: diesel::result::Error) -> Self {
        Self::Diesel(e)
    }
}

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl<S: SessionSetup> Connector for PgR2D2DieselConnector<S> {
    type AuthContext = S;
    type Error = PgR2D2Error;
    type Checkpoint = crate::PgLsn;
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kind: ScalarFamily,
        auth: &S,
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.pool.get()?;
        // The position is read before the snapshot exists. A caller replays the
        // change stream from it, so it must sit at or behind the snapshot:
        // behind re-delivers a few changes the snapshot already holds, which
        // keyed application absorbs, while ahead silently loses a transaction
        // that committed after the position and is invisible to the snapshot.
        // `pg_current_wal_lsn()` is not snapshot-bound, measured rather than
        // assumed: inside one repeatable-read transaction it advances when
        // another connection commits.
        let lsn = read_current_lsn(&mut conn)?;
        let result: Result<(Value<Self::Backend>, Option<crate::PgLsn>), diesel::result::Error> =
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
                sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                    .execute(conn)?;
                run_setup_statements(conn, auth.setup_statements())?;
                let value = load_scalar::<_, Self::Backend>(conn, query, kind)?;
                Ok((value, lsn))
            });
        Ok(result?)
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<crate::backend::Postgres>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.pool.get().map_err(PgR2D2Error::Pool)?;
        // The position is read before the snapshot exists. A caller replays the
        // change stream from it, so it must sit at or behind the snapshot:
        // behind re-delivers a few changes the snapshot already holds, which
        // keyed application absorbs, while ahead silently loses a transaction
        // that committed after the position and is invisible to the snapshot.
        // `pg_current_wal_lsn()` is not snapshot-bound, measured rather than
        // assumed: inside one repeatable-read transaction it advances when
        // another connection commits.
        let lsn = read_current_lsn(&mut conn)?;
        let result = conn.transaction::<_, diesel::result::Error, _>(|conn| {
            // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
            diesel::sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                .execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_page_postgres(conn, query, max_bytes)?;
            Ok(Snapshot {
                value,
                checkpoint: lsn,
            })
        });
        Ok(result?)
    }

    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kinds: &[ScalarFamily],
        auth: &S,
    ) -> Result<
        (
            alloc::vec::Vec<Value<Self::Backend>>,
            Option<Self::Checkpoint>,
        ),
        ScalarRowError<Self::Error>,
    > {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| ScalarRowError::Connector(e.into()))?;
        // The position is read before the snapshot exists. A caller replays the
        // change stream from it, so it must sit at or behind the snapshot:
        // behind re-delivers a few changes the snapshot already holds, which
        // keyed application absorbs, while ahead silently loses a transaction
        // that committed after the position and is invisible to the snapshot.
        // `pg_current_wal_lsn()` is not snapshot-bound, measured rather than
        // assumed: inside one repeatable-read transaction it advances when
        // another connection commits.
        let lsn = read_current_lsn(&mut conn)
            .map_err(|e| ScalarRowError::Connector(PgR2D2Error::Diesel(e)))?;
        let result: Result<
            (alloc::vec::Vec<Value<Self::Backend>>, Option<crate::PgLsn>),
            diesel::result::Error,
        > = diesel::connection::Connection::transaction(&mut *conn, |conn| {
            // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let values = load_scalar_row::<_, Self::Backend>(conn, query, kinds)?;
            Ok((values, lsn))
        });
        result.map_err(|e| ScalarRowError::Connector(e.into()))
    }

    fn open_cursor(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        auth: &S,
    ) -> Result<CursorId, CursorError<Self::Error>> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| CursorError::Connector(PgR2D2Error::Pool(e)))?;
        let id = CursorId(
            self.next_cursor
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed),
        );
        let name = alloc::format!("subql_cursor_{}", id.0);

        // `DECLARE CURSOR` has no query-DSL spelling, and the transaction has
        // to outlive this call, so diesel's scoped `transaction` closure cannot
        // express it. Transaction control still goes through diesel's own
        // `TransactionManager` rather than a raw `BEGIN`, which is what keeps
        // the pool able to see that this connection is inside a transaction:
        // `r2d2`'s broken check consults diesel's tracked depth, and a raw
        // `BEGIN` never touches it, so a connection released mid-transaction
        // would be handed to the next caller still inside this one. Measured
        // on the async side, where it silently ate an unrelated caller's write.
        let opened = (|| -> QueryResult<Option<crate::PgLsn>> {
            // The position is read BEFORE the snapshot exists, on purpose. It
            // is what a caller replays the change stream from, so it must sit
            // at or behind the snapshot: behind means a few changes already in
            // the snapshot arrive again, which keyed application absorbs, while
            // ahead means a transaction that committed after the position but
            // is invisible to the snapshot is never delivered at all. Reading
            // it after `DECLARE` would give exactly that, because `DECLARE` is
            // what establishes the snapshot and `pg_current_wal_lsn()` is not
            // snapshot-bound. Debezium orders its initial snapshot the same
            // way, position first and scan second.
            let lsn = read_current_lsn(&mut conn)?;
            PgTxn::begin_transaction(&mut *conn)?;
            // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
            diesel::sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                .execute(&mut *conn)?;
            run_setup_statements(&mut *conn, auth.setup_statements())?;
            let declaration = ReadQuery::owned(
                alloc::format!("DECLARE {name} NO SCROLL CURSOR FOR {}", query.sql()),
                query.binds().to_vec(),
            );
            boxed_postgres_read_query(&declaration)?.execute(&mut *conn)?;
            Ok(lsn)
        })();

        let checkpoint = match opened {
            Ok(lsn) => lsn,
            Err(e) => {
                // Leave no transaction behind on a failed open. Through the
                // manager, so its depth counter matches the server's state.
                let _ = PgTxn::rollback_transaction(&mut *conn);
                return Err(CursorError::Connector(PgR2D2Error::Diesel(e)));
            }
        };

        self.cursors.lock().insert(
            id,
            alloc::sync::Arc::new(parking_lot::Mutex::new(PgCursor {
                conn,
                close_sql: alloc::format!("CLOSE {name}"),
                name,
                checkpoint,
                columns: alloc::vec::Vec::new(),
                leftover: alloc::collections::VecDeque::new(),
            })),
        );
        Ok(id)
    }

    fn fetch_cursor(
        &self,
        cursor: CursorId,
        max_bytes: usize,
    ) -> Result<Snapshot<RowPage<Self::Backend>, Self::Checkpoint>, CursorError<Self::Error>> {
        /// Rows per `FETCH`. Overshoot is carried into the next page rather
        /// than discarded, so this trades round trips against buffered rows
        /// and never against correctness.
        const BATCH: usize = 64;

        let entry = self
            .cursors
            .lock()
            .get(&cursor)
            .map(alloc::sync::Arc::clone)
            .ok_or(CursorError::Unknown(cursor))?;
        // Refused rather than queued, matching the async connector. A cursor is
        // serial, so a second reader could only wait, and blocking this thread
        // on a resource it cannot queue for hides the caller's own bug.
        let Some(mut guard) = entry.try_lock() else {
            return Err(CursorError::Busy(cursor));
        };

        let outcome = fetch_page_from(&mut guard, max_bytes, BATCH);
        drop(guard);
        match outcome {
            Ok(page) => Ok(page),
            // A failed fetch leaves the cursor's server-side state unknown, so
            // it is dropped rather than left registered, again matching async.
            // Dropping the last handle runs `PgCursor::drop`, which ends the
            // transaction, and `entry` here is the last handle.
            Err(e) => {
                self.cursors.lock().remove(&cursor);
                Err(CursorError::Connector(PgR2D2Error::Diesel(e)))
            }
        }
    }

    fn close_cursor(&self, cursor: CursorId) -> Result<(), CursorError<Self::Error>> {
        // Idempotent: an already-closed cursor is not an error, so an
        // abandoned read cannot leak a transaction through a double close.
        let Some(entry) = self.cursors.lock().remove(&cursor) else {
            return Ok(());
        };
        let held = &mut *entry.lock();
        let closed = diesel::sql_query(held.close_sql.clone())
            .execute(&mut *held.conn)
            .and_then(|_| PgTxn::commit_transaction(&mut *held.conn));
        match closed {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = PgTxn::rollback_transaction(&mut *held.conn);
                Err(CursorError::Connector(PgR2D2Error::Diesel(e)))
            }
        }
    }
}

/// Fill one page from an open cursor, buffering whatever a `FETCH` overshot.
///
/// The sync twin of `PgAsyncDieselConnector::fetch_from`, split out for the
/// same reason: the caller decides what a failure means for the cursor's
/// registration, and that decision does not belong inside the read loop.
#[cfg(feature = "executor-diesel-postgres-r2d2")]
fn fetch_page_from(
    held: &mut PgCursor,
    max_bytes: usize,
    batch: usize,
) -> QueryResult<Snapshot<RowPage<crate::backend::Postgres>, crate::PgLsn>> {
    let mut rows: alloc::vec::Vec<alloc::vec::Vec<Value<crate::backend::Postgres>>> =
        alloc::vec::Vec::new();
    let mut spent = 0_usize;
    loop {
        if drain_cursor_buffer(&mut held.leftover, &mut rows, &mut spent, max_bytes) {
            return Ok(Snapshot {
                value: RowPage {
                    columns: held.columns.clone(),
                    rows,
                    more: true,
                },
                checkpoint: held.checkpoint,
            });
        }
        // `FETCH FORWARD` is a cursor command with no typed DSL equivalent.
        let page = load_page_postgres(
            &mut held.conn,
            &ReadQuery::without_binds(&alloc::format!("FETCH FORWARD {batch} FROM {}", held.name)),
            usize::MAX,
        )?;
        if held.columns.is_empty() {
            held.columns = page.columns;
        }
        // An empty batch is the cursor's own end-of-result signal, so the loop
        // exits on what the database said rather than on a short-batch guess. A
        // guess costs a round trip when right and a hang when wrong, which is a
        // bad trade for a loop.
        let fetched = page.rows.len();
        held.leftover.extend(page.rows);
        if fetched == 0 {
            return Ok(Snapshot {
                value: RowPage {
                    columns: held.columns.clone(),
                    rows,
                    more: false,
                },
                checkpoint: held.checkpoint,
            });
        }
    }
}
