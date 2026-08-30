#![allow(clippy::manual_async_fn)]
#![allow(clippy::type_complexity)]
//! LSN-aware async [`AsyncConnector`] for PostgreSQL.

use super::super::async_connector::AsyncConnector;
use super::super::connector::{
    boxed_postgres_read_query_owned, drain_cursor_buffer, CursorError, CursorId, PgLsnRow,
    ReadQuery, RowPage, ScalarRowError, SessionSetup, Snapshot,
};
use super::{
    load_page_postgres_async, load_scalar_postgres_async, load_scalar_row_postgres_async,
    run_setup_statements_async, DieselAsyncError,
};
use crate::backend::{BuiltinKind, Value};
use alloc::vec::Vec;
use core::future::Future;
use diesel::sql_query;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl as _};

/// Async LSN-aware [`AsyncConnector`] for PostgreSQL, the async peer of
/// [`PgDieselConnector`](crate::reexec::connector::PgDieselConnector).
///
/// Wraps a `bb8` pool over
/// [`AsyncPgConnection`](diesel_async::AsyncPgConnection). Each
/// `execute_scalar` reads `pg_current_wal_lsn()`, opens a `READ ONLY
/// REPEATABLE READ` transaction for the user's SQL, and returns the
/// [`Value<Postgres>`](crate::backend::Postgres) with the parsed
/// [`PgLsn`](crate::PgLsn). Pure Rust: `diesel-async` speaks the PG wire
/// protocol through `tokio-postgres`, no libpq.
///
/// # Errors
///
/// Returns [`DieselAsyncError`] for pool or database failures.
#[cfg(feature = "executor-diesel-async-postgres")]
pub struct PgAsyncDieselConnector<S = ()> {
    pool: Pool<diesel_async::AsyncPgConnection>,
    /// Cursors held open, each pinning one pooled connection inside its own
    /// transaction until closed.
    ///
    /// A fetch takes its cursor out of its slot and leaves the slot marked, so
    /// no lock is held across an await and no async mutex is needed. One map
    /// rather than a map plus a separate busy set, because two structures
    /// cannot be updated together under one lock, and the gap between them
    /// reported a cursor that was busy as one that never existed.
    cursors: parking_lot::Mutex<hashbrown::HashMap<CursorId, CursorSlot>>,
    next_cursor: core::sync::atomic::AtomicU64,
    _setup: core::marker::PhantomData<fn() -> S>,
}

/// Diesel's transaction manager for `AsyncPgConnection`.
///
/// A held cursor's transaction goes through this rather than raw `BEGIN` and
/// `COMMIT` text, so diesel tracks its depth. That depth is what makes the pool
/// discard a connection released mid-transaction, which is the only cleanup
/// available when a cancelled future gets no chance to run any.
#[cfg(feature = "executor-diesel-async-postgres")]
use diesel_async::TransactionManager as _;

#[cfg(feature = "executor-diesel-async-postgres")]
type PgAsyncTxn =
    <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::TransactionManager;

/// One open async cursor: the pooled connection it pins, the position its
/// snapshot sits at, and rows fetched but not yet delivered.
///
/// The leftover buffer keeps the byte budget exact, since `FETCH` cannot be
/// undone: a batch that overshoots carries into the next page rather than
/// being returned whole or discarded.
/// What a registered cursor id currently is.
///
/// The `Reading` state is why this is a state machine rather than a plain map:
/// a read holds the cursor outside the map for the duration, and both a
/// concurrent read and a concurrent close need to tell "in use" from "gone".
#[cfg(feature = "executor-diesel-async-postgres")]
enum CursorSlot {
    /// Nobody is reading it. Boxed so an idle slot does not make every
    /// `Reading` slot carry a cursor's worth of space.
    Idle(alloc::boxed::Box<PgAsyncCursor>),
    /// A read holds it. `close_asked` records a close that arrived meanwhile,
    /// which the read honours when it finishes, since the caller was already
    /// told the close succeeded.
    Reading { close_asked: bool },
}

/// Clears a cursor's slot if the read holding it never comes back.
///
/// Cancellation runs only destructors, so without this a cancelled read leaves
/// the slot marked `Reading` forever and every later caller is told the cursor
/// is busy, for a cursor whose connection has already been discarded. Removing
/// the slot is right rather than restoring it: the cursor went with the
/// cancelled future.
#[cfg(feature = "executor-diesel-async-postgres")]
struct SlotGuard<'a> {
    cursors: &'a parking_lot::Mutex<hashbrown::HashMap<CursorId, CursorSlot>>,
    cursor: CursorId,
    armed: bool,
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl Drop for SlotGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            // Nothing here may panic and there is no caller left to tell.
            self.cursors.lock().remove(&self.cursor);
        }
    }
}

#[cfg(feature = "executor-diesel-async-postgres")]
struct PgAsyncCursor {
    conn: diesel_async::pooled_connection::bb8::PooledConnection<
        'static,
        diesel_async::AsyncPgConnection,
    >,
    name: alloc::string::String,
    checkpoint: Option<crate::PgLsn>,
    columns: alloc::vec::Vec<alloc::string::String>,
    leftover: alloc::collections::VecDeque<alloc::vec::Vec<Value<crate::backend::Postgres>>>,
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl PgAsyncDieselConnector {
    /// Wrap a `bb8` pool over `AsyncPgConnection` already configured by the
    /// caller.
    ///
    /// # Cancellation
    ///
    /// A row-returning read that is cancelled part way costs one pooled
    /// connection: the cursor's transaction is opened through diesel's own
    /// transaction manager, so the pool sees the released connection is still
    /// inside a transaction and discards it rather than handing it to the next
    /// caller. Cleanup cannot be done in a destructor, because ending a
    /// transaction needs I/O and a destructor cannot await, so this is
    /// delegated to connection teardown by design. Size the pool with that in
    /// mind if reads are cancelled often.
    #[must_use]
    pub fn new(pool: Pool<diesel_async::AsyncPgConnection>) -> Self {
        Self {
            pool,
            cursors: parking_lot::Mutex::new(hashbrown::HashMap::new()),
            next_cursor: core::sync::atomic::AtomicU64::new(1),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl<S: SessionSetup + Send + Sync> PgAsyncDieselConnector<S> {
    /// Wrap a `bb8` pool over `AsyncPgConnection` whose reads run the setup
    /// statements carried by the per-read [`SessionSetup`] value `S`.
    #[must_use]
    pub fn with_session_setup(pool: Pool<diesel_async::AsyncPgConnection>) -> Self {
        Self {
            pool,
            cursors: parking_lot::Mutex::new(hashbrown::HashMap::new()),
            next_cursor: core::sync::atomic::AtomicU64::new(1),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl<S> PgAsyncDieselConnector<S> {
    /// End a cursor: close it, then commit the transaction it was reading in.
    ///
    /// Rolls back if either step fails, so a failure cannot leave the
    /// connection inside a transaction that the pool would then have to discard.
    async fn end_cursor(held: &mut PgAsyncCursor) -> diesel::QueryResult<()> {
        let closed = async {
            sql_query(alloc::format!("CLOSE {}", held.name))
                .execute(&mut *held.conn)
                .await?;
            PgAsyncTxn::commit_transaction(&mut *held.conn).await
        }
        .await;
        if closed.is_err() {
            let _ = PgAsyncTxn::rollback_transaction(&mut *held.conn).await;
        }
        closed
    }

    /// Fill one page from an open cursor, buffering whatever a `FETCH`
    /// overshot.
    ///
    /// Split out so the caller can hold the cursor outside the map for the
    /// duration without holding a lock across an await.
    async fn fetch_from(
        held: &mut PgAsyncCursor,
        max_bytes: usize,
        batch: usize,
    ) -> diesel::QueryResult<Snapshot<RowPage<crate::backend::Postgres>, crate::PgLsn>> {
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
            let page = load_page_postgres_async(
                &mut held.conn,
                &ReadQuery::without_binds(&alloc::format!(
                    "FETCH FORWARD {batch} FROM {}",
                    held.name
                )),
                usize::MAX,
            )
            .await?;
            if held.columns.is_empty() {
                held.columns = page.columns;
            }
            // An empty batch is the cursor's own end-of-result signal, so the
            // loop exits on what the database said rather than on a short-batch
            // guess. A guess costs a round trip when right and a hang when
            // wrong, a bad trade for a loop.
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
}

/// Read the current WAL LSN before opening the snapshot transaction.
///
/// `pg_current_wal_lsn()` is a Postgres-specific function with no typed DSL
/// equivalent, so `sql_query` is required here.
#[cfg(feature = "executor-diesel-async-postgres")]
async fn read_current_lsn_async(
    conn: &mut diesel_async::AsyncPgConnection,
) -> diesel::QueryResult<Option<crate::PgLsn>> {
    let row: PgLsnRow = sql_query("SELECT pg_current_wal_lsn()::text AS lsn")
        .get_result(conn)
        .await?;
    Ok(crate::PgLsn::parse(&row.lsn))
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl<S: SessionSetup + Send + Sync> AsyncConnector for PgAsyncDieselConnector<S> {
    type AuthContext = S;
    type Error = DieselAsyncError;
    type Checkpoint = crate::PgLsn;
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kind: BuiltinKind,
        auth: &S,
    ) -> impl Future<Output = Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        let query = query.clone().into_owned();
        async move {
            let mut pooled = self.pool.get().await.map_err(DieselAsyncError::Pool)?;
            let conn: &mut diesel_async::AsyncPgConnection = &mut pooled;
            // The position is read before the snapshot exists, because
            // `pg_current_wal_lsn()` is not snapshot-bound: a position taken
            // after the query can sit ahead of the snapshot, and a replay
            // starting there skips a transaction that committed before it yet
            // was invisible to the snapshot. Behind is safe, ahead loses data.
            let lsn = read_current_lsn_async(conn)
                .await
                .map_err(DieselAsyncError::Diesel)?;
            conn.transaction::<(Value<Self::Backend>, Option<crate::PgLsn>), diesel::result::Error, _>(
                |c| {
                    async move {
                        // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
                        sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                            .execute(c)
                            .await?;
                        run_setup_statements_async(c, auth.setup_statements()).await?;
                        let value =
                            load_scalar_postgres_async(c, &query, kind).await?;
                        Ok((value, lsn))
                    }
                    .scope_boxed()
                },
            )
            .await
            .map_err(DieselAsyncError::Diesel)
        }
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        max_bytes: usize,
        auth: &S,
    ) -> impl Future<Output = Result<Snapshot<RowPage<Self::Backend>, Self::Checkpoint>, Self::Error>>
           + Send {
        let query = query.clone().into_owned();
        async move {
            let mut pooled = self.pool.get().await.map_err(DieselAsyncError::Pool)?;
            let conn: &mut diesel_async::AsyncPgConnection = &mut pooled;
            // The position is read before the snapshot exists, because
            // `pg_current_wal_lsn()` is not snapshot-bound: a position taken
            // after the query can sit ahead of the snapshot, and a replay
            // starting there skips a transaction that committed before it yet
            // was invisible to the snapshot. Behind is safe, ahead loses data.
            let lsn = read_current_lsn_async(conn)
                .await
                .map_err(DieselAsyncError::Diesel)?;
            conn.transaction::<Snapshot<RowPage<Self::Backend>, crate::PgLsn>, diesel::result::Error, _>(
                |c| {
                    async move {
                        // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
                        sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                            .execute(c)
                            .await?;
                        run_setup_statements_async(c, auth.setup_statements()).await?;
                        let value =
                            load_page_postgres_async(c, &query, max_bytes).await?;
                        Ok(Snapshot {
                            value,
                            checkpoint: lsn,
                        })
                    }
                    .scope_boxed()
                },
            )
            .await
            .map_err(DieselAsyncError::Diesel)
        }
    }

    fn open_cursor(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        auth: &S,
    ) -> impl Future<Output = Result<CursorId, CursorError<Self::Error>>> + Send {
        let query = query.clone().into_owned();
        async move {
            // Owned, so the connection can be held in the cursor map across
            // calls rather than borrowed from the pool for one await.
            let mut conn = self
                .pool
                .get_owned()
                .await
                .map_err(|e| CursorError::Connector(DieselAsyncError::Pool(e)))?;
            let id = CursorId(
                self.next_cursor
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed),
            );
            let name = alloc::format!("subql_cursor_{}", id.0);

            let opened = async {
                // The position is read BEFORE the snapshot exists, on purpose.
                // A caller replays the change stream from it, so it must sit at
                // or behind the snapshot: behind re-delivers a few changes the
                // snapshot already holds, which keyed application absorbs,
                // while ahead silently drops a transaction that committed after
                // the position and was invisible to the snapshot.
                let lsn = read_current_lsn_async(&mut conn).await?;
                // Through diesel's transaction manager, never a raw `BEGIN`.
                // The manager's depth counter is what tells the pool this
                // connection is inside a transaction, and a cancelled read
                // releases the connection with no chance to clean up. With a
                // raw `BEGIN` the counter stays zero, the pool believes the
                // connection is clean, and the next caller inherits an open
                // transaction: measured to swallow that caller's write whole.
                PgAsyncTxn::begin_transaction(&mut *conn).await?;
                // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
                sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                    .execute(&mut *conn)
                    .await?;
                run_setup_statements_async(&mut *conn, auth.setup_statements()).await?;
                let declaration = ReadQuery::owned(
                    alloc::format!("DECLARE {name} NO SCROLL CURSOR FOR {}", query.sql()),
                    query.binds().to_vec(),
                );
                boxed_postgres_read_query_owned(&declaration)?
                    .execute(&mut *conn)
                    .await?;
                Ok::<_, diesel::result::Error>(lsn)
            }
            .await;

            let checkpoint = match opened {
                Ok(lsn) => lsn,
                Err(e) => {
                    // Leave no transaction behind on a failed open. Dropping
                    // `conn` here would also do it, by discarding the
                    // connection, but rolling back keeps it reusable.
                    let _ = PgAsyncTxn::rollback_transaction(&mut *conn).await;
                    return Err(CursorError::Connector(DieselAsyncError::Diesel(e)));
                }
            };

            self.cursors.lock().insert(
                id,
                CursorSlot::Idle(alloc::boxed::Box::new(PgAsyncCursor {
                    conn,
                    name,
                    checkpoint,
                    columns: alloc::vec::Vec::new(),
                    leftover: alloc::collections::VecDeque::new(),
                })),
            );
            Ok(id)
        }
    }

    fn fetch_cursor(
        &self,
        cursor: CursorId,
        max_bytes: usize,
    ) -> impl Future<
        Output = Result<
            Snapshot<RowPage<Self::Backend>, Self::Checkpoint>,
            CursorError<Self::Error>,
        >,
    > + Send {
        /// Rows per `FETCH`. Overshoot carries into the next page rather than
        /// being discarded, so this trades round trips against buffered rows
        /// and never against correctness.
        const BATCH: usize = 64;

        async move {
            // Taken out of its slot for the duration, so no lock is held across
            // an await. The slot stays behind marked `Reading`, which is what
            // tells a concurrent caller the difference between "in use" and
            // "gone", and lets a close arriving meanwhile be honoured.
            let mut held = match self.cursors.lock().get_mut(&cursor) {
                Some(slot @ CursorSlot::Idle(_)) => {
                    match core::mem::replace(slot, CursorSlot::Reading { close_asked: false }) {
                        CursorSlot::Idle(held) => held,
                        // Total: just matched `Idle`.
                        CursorSlot::Reading { .. } => return Err(CursorError::Busy(cursor)),
                    }
                }
                // A cursor is serial: two interleaved `FETCH`es would scramble
                // the rows between them, so this is reported, not served.
                Some(CursorSlot::Reading { .. }) => return Err(CursorError::Busy(cursor)),
                None => return Err(CursorError::Unknown(cursor)),
            };

            // Cancellation runs no cleanup, so the slot has to be cleared by a
            // destructor or it says `Busy` forever for a cursor that is gone.
            let mut slot_guard = SlotGuard {
                cursors: &self.cursors,
                cursor,
                armed: true,
            };

            let outcome = Self::fetch_from(&mut held, max_bytes, BATCH).await;
            slot_guard.armed = false;

            // Whether a close arrived while this read was in flight decides
            // where the cursor goes now.
            let close_asked = matches!(
                self.cursors.lock().remove(&cursor),
                Some(CursorSlot::Reading { close_asked: true })
            );
            match outcome {
                Ok(page) if close_asked => {
                    // The caller was told the close succeeded, so honour it
                    // here rather than putting the cursor back. Putting it back
                    // would hold a connection and an open transaction for the
                    // connector's whole life, for a cursor nobody can reach.
                    Self::end_cursor(&mut held)
                        .await
                        .map_err(|e| CursorError::Connector(DieselAsyncError::Diesel(e)))?;
                    Ok(page)
                }
                Ok(page) => {
                    self.cursors.lock().insert(cursor, CursorSlot::Idle(held));
                    Ok(page)
                }
                // Not put back: the cursor's server-side state is unknown after
                // a failed fetch, and dropping it discards the connection,
                // which the pool replaces. Leaving it registered strands it.
                Err(e) => Err(CursorError::Connector(DieselAsyncError::Diesel(e))),
            }
        }
    }

    fn close_cursor(
        &self,
        cursor: CursorId,
    ) -> impl Future<Output = Result<(), CursorError<Self::Error>>> + Send {
        async move {
            // Idempotent: an already-closed cursor is not an error, so an
            // abandoned read cannot fail through a double close.
            // One lock scope, taken once. `parking_lot::Mutex` is not
            // reentrant, so locking again inside a match that still holds the
            // guard deadlocks, and closing an idle cursor is the ordinary path.
            let mut held = {
                let mut map = self.cursors.lock();
                match map.get_mut(&cursor) {
                    // A read holds it. Recorded rather than refused, so the
                    // caller gets one answer: the read closes it when it ends.
                    Some(CursorSlot::Reading { close_asked }) => {
                        *close_asked = true;
                        return Ok(());
                    }
                    Some(CursorSlot::Idle(_)) => {}
                    None => return Ok(()),
                }
                match map.remove(&cursor) {
                    Some(CursorSlot::Idle(held)) => held,
                    // Total: just matched `Idle` under the same lock.
                    _ => return Ok(()),
                }
            };
            Self::end_cursor(&mut held)
                .await
                .map_err(|e| CursorError::Connector(DieselAsyncError::Diesel(e)))
        }
    }

    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kinds: &[BuiltinKind],
        auth: &S,
    ) -> impl Future<
        Output = Result<
            (Vec<Value<Self::Backend>>, Option<Self::Checkpoint>),
            ScalarRowError<Self::Error>,
        >,
    > + Send {
        let query = query.clone().into_owned();
        let kinds = kinds.to_vec();
        async move {
            let mut pooled = self
                .pool
                .get()
                .await
                .map_err(|e| ScalarRowError::Connector(DieselAsyncError::Pool(e)))?;
            let conn: &mut diesel_async::AsyncPgConnection = &mut pooled;
            // Position before snapshot, for the reason spelled out on
            // `execute_scalar`: a position taken after can sit ahead of the
            // snapshot and a replay from there loses data.
            let lsn = read_current_lsn_async(conn)
                .await
                .map_err(|e| ScalarRowError::Connector(DieselAsyncError::Diesel(e)))?;
            conn.transaction::<(Vec<Value<Self::Backend>>, Option<crate::PgLsn>), diesel::result::Error, _>(
                |c| {
                    async move {
                        // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
                        sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                            .execute(c)
                            .await?;
                        run_setup_statements_async(c, auth.setup_statements()).await?;
                        let values =
                            load_scalar_row_postgres_async(c, &query, &kinds).await?;
                        Ok((values, lsn))
                    }
                    .scope_boxed()
                },
            )
            .await
            .map_err(|e| ScalarRowError::Connector(DieselAsyncError::Diesel(e)))
        }
    }
}
