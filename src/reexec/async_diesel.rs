#![allow(clippy::manual_async_fn)]
//! Async [`AsyncConnector`] impls backed by `diesel-async` connection pools.
//!
//! Async peers of the sync connectors in [`connector`](super::connector).
//! Hand any of these to an
//! [`AutoResolvingEngine`](super::AutoResolvingEngine) with [`AsyncMode`](super::AsyncMode) to drive
//! re-execution end to end on an async runtime (tokio multi-thread, etc.).
//!
//! # Why a pool
//!
//! The [`AsyncConnector`] trait takes `&self` and returns `Send` futures, so
//! the connector cannot hand out a `&mut` to a single owned connection the
//! way the sync [`DieselConnector`](super::connector::DieselConnector) does
//! through a [`RefCell`](core::cell::RefCell). A `bb8` pool is `Clone + Send
//! + Sync` and hands each call its own connection, which keeps the returned
//! futures `Send`. A single-connection variant would instead need to wrap a
//! lone `AsyncPgConnection` in a `tokio::sync::Mutex`.
//!
//! # Scope
//!
//! `execute_scalar` resolves the MIN/MAX flavor of re-execution, mirroring
//! the sync connectors. `execute_rows` returns
//! [`DieselAsyncError::RowsUnsupported`] until total row re-execution lands
//! (tracked in `MILESTONES.md`), matching the sync connectors' deferral.
//!
//! Backends are Postgres and MySQL only: `diesel-async` 0.7 has no real
//! async SQLite backend.

use super::async_connector::AsyncConnector;
#[cfg(feature = "executor-diesel-async-mysql")]
use super::connector::boxed_mysql_read_query_owned;
#[cfg(feature = "executor-diesel-async-postgres")]
use super::connector::boxed_postgres_read_query_owned;
#[cfg(feature = "executor-diesel-async-mysql")]
use super::connector::LogStatusRow;
#[cfg(feature = "executor-diesel-async-postgres")]
use super::connector::PgLsnRow;
use super::connector::{
    FloatRow, IntRow, ReadQuery, ScalarRowError, SessionSetup, Snapshot, TextRow,
};
use crate::backend::{Backend, BuiltinKind, ScalarKind, Value};
use alloc::vec::Vec;
use core::future::Future;
use diesel::query_builder::SqlQuery;
use diesel::sql_query;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl as _};
use thiserror::Error;

/// Run each setup statement in order on `conn`. The async peer of
/// [`run_setup_statements`](super::connector) shared helper: called inside the
/// transaction that serves the read and before the caller's SQL.
async fn run_setup_statements_async<C>(
    conn: &mut C,
    statements: &[alloc::string::String],
) -> diesel::QueryResult<()>
where
    C: AsyncConnection,
    SqlQuery: diesel_async::methods::ExecuteDsl<C>,
{
    for statement in statements {
        sql_query(statement.as_str()).execute(conn).await?;
    }
    Ok(())
}

/// Errors returned by the async diesel connectors.
///
/// Separates "could not get a connection from the pool" from "the database
/// rejected the query" so callers can decide whether to back off or
/// propagate, mirroring [`PgR2D2Error`](super::connector::PgR2D2Error).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DieselAsyncError {
    /// The `bb8` pool refused to hand out a connection (timeout, exhausted,
    /// connection setup failed).
    #[error("bb8 pool error: {0}")]
    Pool(diesel_async::pooled_connection::bb8::RunError),
    /// Diesel returned a database error while executing the query.
    #[error("diesel error: {0}")]
    Diesel(diesel::result::Error),
    /// `execute_rows` is reserved for total row re-execution and is not
    /// implemented for these connectors yet. Use the scalar path or supply a
    /// custom [`AsyncConnector`] impl.
    #[error("execute_rows is reserved for total row reexec; use the scalar path or a custom AsyncConnector impl")]
    RowsUnsupported,
}

/// Async counterpart of
/// [`load_scalar`](super::connector) (module-private there): route the
/// projected column through the `Nullable<BigInt|Double|Text>` row shape
/// that matches `kind`, then lift into [`Value<B>`]. Decimals travel as text
/// so precision is not lost through `f64`.
#[cfg(feature = "executor-diesel-async-postgres")]
async fn load_scalar_postgres_async(
    conn: &mut diesel_async::AsyncPgConnection,
    query: &ReadQuery<'_, crate::backend::Postgres>,
    kind: BuiltinKind,
) -> diesel::QueryResult<Value<crate::backend::Postgres>> {
    let value = match kind {
        BuiltinKind::Int => {
            let sql = alloc::format!("SELECT CAST(({}) AS BIGINT) AS v", query.sql());
            let query = ReadQuery::borrowed(&sql, query.binds());
            boxed_postgres_read_query_owned(&query)?
                .get_result::<IntRow>(conn)
                .await?
                .v
                .map_or(Value::Null, Value::Int)
        }
        BuiltinKind::Float => boxed_postgres_read_query_owned(query)?
            .get_result::<FloatRow>(conn)
            .await?
            .v
            .map_or(Value::Null, Value::Float),
        _ => boxed_postgres_read_query_owned(query)?
            .get_result::<TextRow>(conn)
            .await?
            .v
            .map_or(Value::Null, Value::String),
    };
    Ok(value)
}

#[cfg(feature = "executor-diesel-async-mysql")]
async fn load_scalar_mysql_async(
    conn: &mut diesel_async::AsyncMysqlConnection,
    query: &ReadQuery<'_, crate::backend::MySql>,
    kind: BuiltinKind,
) -> diesel::QueryResult<Value<crate::backend::MySql>> {
    let value = match kind {
        BuiltinKind::Int => {
            let sql = alloc::format!("SELECT CAST(({}) AS SIGNED) AS v", query.sql());
            let query = ReadQuery::borrowed(&sql, query.binds());
            boxed_mysql_read_query_owned(&query)?
                .get_result::<IntRow>(conn)
                .await?
                .v
                .map_or(Value::Null, Value::Int)
        }
        BuiltinKind::Float => boxed_mysql_read_query_owned(query)?
            .get_result::<FloatRow>(conn)
            .await?
            .v
            .map_or(Value::Null, Value::Float),
        _ => boxed_mysql_read_query_owned(query)?
            .get_result::<TextRow>(conn)
            .await?
            .v
            .map_or(Value::Null, Value::String),
    };
    Ok(value)
}

#[cfg(feature = "executor-diesel-async-postgres")]
async fn load_scalar_row_postgres_async(
    conn: &mut diesel_async::AsyncPgConnection,
    query: &ReadQuery<'_, crate::backend::Postgres>,
    kinds: &[BuiltinKind],
) -> diesel::QueryResult<Vec<Value<crate::backend::Postgres>>> {
    let row = boxed_postgres_read_query_owned(query)?
        .get_result::<crate::diesel_decode::DynamicRow<crate::backend::Postgres>>(conn)
        .await?;
    if row.values.len() != kinds.len() {
        return Err(diesel::result::Error::DeserializationError(
            "aggregate seed row has the wrong arity".into(),
        ));
    }
    Ok(row
        .values
        .into_iter()
        .zip(kinds)
        .map(|(value, kind)| {
            crate::backend::Postgres::decode_group_value(ScalarKind::from(*kind), value)
                .unwrap_or(Value::Missing)
        })
        .collect())
}

#[cfg(feature = "executor-diesel-async-mysql")]
async fn load_scalar_row_mysql_async(
    conn: &mut diesel_async::AsyncMysqlConnection,
    query: &ReadQuery<'_, crate::backend::MySql>,
    kinds: &[BuiltinKind],
) -> diesel::QueryResult<Vec<Value<crate::backend::MySql>>> {
    let row = boxed_mysql_read_query_owned(query)?
        .get_result::<crate::diesel_decode::DynamicRow<crate::backend::MySql>>(conn)
        .await?;
    if row.values.len() != kinds.len() {
        return Err(diesel::result::Error::DeserializationError(
            "aggregate seed row has the wrong arity".into(),
        ));
    }
    Ok(row
        .values
        .into_iter()
        .zip(kinds)
        .map(|(value, kind)| {
            crate::backend::MySql::decode_group_value(ScalarKind::from(*kind), value)
                .unwrap_or(Value::Missing)
        })
        .collect())
}

// ---------------------------------------------------------------------------
// PgAsyncDieselConnector: LSN-aware PG impl behind executor-diesel-async-postgres.
// ---------------------------------------------------------------------------

/// Async LSN-aware [`AsyncConnector`] for PostgreSQL, the async peer of
/// [`PgDieselConnector`](super::connector::PgDieselConnector).
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
    cursors: parking_lot::Mutex<hashbrown::HashMap<super::CursorId, CursorSlot>>,
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
    cursors: &'a parking_lot::Mutex<hashbrown::HashMap<super::CursorId, CursorSlot>>,
    cursor: super::CursorId,
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
    name: String,
    checkpoint: Option<crate::PgLsn>,
    columns: alloc::vec::Vec<String>,
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
    ) -> diesel::QueryResult<Snapshot<crate::reexec::RowPage<crate::backend::Postgres>, crate::PgLsn>>
    {
        let mut rows: alloc::vec::Vec<alloc::vec::Vec<Value<crate::backend::Postgres>>> =
            alloc::vec::Vec::new();
        let mut spent = 0_usize;
        loop {
            if crate::reexec::connector::drain_cursor_buffer(
                &mut held.leftover,
                &mut rows,
                &mut spent,
                max_bytes,
            ) {
                return Ok(Snapshot {
                    value: crate::reexec::RowPage {
                        columns: held.columns.clone(),
                        rows,
                        more: true,
                    },
                    checkpoint: held.checkpoint,
                });
            }
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
                    value: crate::reexec::RowPage {
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
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Self::Backend>, Self::Checkpoint>,
            Self::Error,
        >,
    > + Send {
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
            conn.transaction::<Snapshot<crate::reexec::RowPage<Self::Backend>, crate::PgLsn>, diesel::result::Error, _>(
                |c| {
                    async move {
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
    ) -> impl Future<Output = Result<super::CursorId, super::CursorError<Self::Error>>> + Send {
        let query = query.clone().into_owned();
        async move {
            // Owned, so the connection can be held in the cursor map across
            // calls rather than borrowed from the pool for one await.
            let mut conn = self
                .pool
                .get_owned()
                .await
                .map_err(|e| super::CursorError::Connector(DieselAsyncError::Pool(e)))?;
            let id = super::CursorId(
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
                    return Err(super::CursorError::Connector(DieselAsyncError::Diesel(e)));
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
        cursor: super::CursorId,
        max_bytes: usize,
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Self::Backend>, Self::Checkpoint>,
            super::CursorError<Self::Error>,
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
                        CursorSlot::Reading { .. } => return Err(super::CursorError::Busy(cursor)),
                    }
                }
                // A cursor is serial: two interleaved `FETCH`es would scramble
                // the rows between them, so this is reported, not served.
                Some(CursorSlot::Reading { .. }) => return Err(super::CursorError::Busy(cursor)),
                None => return Err(super::CursorError::Unknown(cursor)),
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
                        .map_err(|e| super::CursorError::Connector(DieselAsyncError::Diesel(e)))?;
                    Ok(page)
                }
                Ok(page) => {
                    self.cursors.lock().insert(cursor, CursorSlot::Idle(held));
                    Ok(page)
                }
                // Not put back: the cursor's server-side state is unknown after
                // a failed fetch, and dropping it discards the connection,
                // which the pool replaces. Leaving it registered strands it.
                Err(e) => Err(super::CursorError::Connector(DieselAsyncError::Diesel(e))),
            }
        }
    }

    fn close_cursor(
        &self,
        cursor: super::CursorId,
    ) -> impl Future<Output = Result<(), super::CursorError<Self::Error>>> + Send {
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
                .map_err(|e| super::CursorError::Connector(DieselAsyncError::Diesel(e)))
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

// ---------------------------------------------------------------------------
// MysqlAsyncDieselConnector: binlog-position-aware impl behind
// executor-diesel-async-mysql.
// ---------------------------------------------------------------------------

/// Async binlog-position-aware [`AsyncConnector`] for MySQL, the async peer
/// of [`MysqlDieselConnector`](super::connector::MysqlDieselConnector).
///
/// Wraps a `bb8` pool over
/// [`AsyncMysqlConnection`](diesel_async::AsyncMysqlConnection). Each
/// `execute_scalar` runs the user's SQL and reads
/// `performance_schema.log_status` in one transaction, returning the
/// [`Value<MySql>`](crate::backend::MySql) with the parsed
/// [`MysqlBinlogPos`](crate::MysqlBinlogPos). `log_status` is used rather
/// than `SHOW MASTER STATUS` because diesel's prepared-statement protocol
/// cannot read `SHOW` result metadata. The coordinate is the server's
/// current binlog position (best-effort "at or after the read"), so it
/// degrades to `None` when binary logging is off or unreadable.
///
/// # Errors
///
/// Returns [`DieselAsyncError`] for pool or database failures.
#[cfg(feature = "executor-diesel-async-mysql")]
pub struct MysqlAsyncDieselConnector<S = ()> {
    pool: Pool<diesel_async::AsyncMysqlConnection>,
    _setup: core::marker::PhantomData<fn() -> S>,
}

#[cfg(feature = "executor-diesel-async-mysql")]
impl MysqlAsyncDieselConnector {
    /// Wrap a `bb8` pool over `AsyncMysqlConnection` with no session setup.
    #[must_use]
    pub const fn new(pool: Pool<diesel_async::AsyncMysqlConnection>) -> Self {
        Self {
            pool,
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel-async-mysql")]
impl<S: SessionSetup + Send + Sync> MysqlAsyncDieselConnector<S> {
    /// Wrap a `bb8` pool over `AsyncMysqlConnection` whose reads run the setup
    /// statements carried by the per-read [`SessionSetup`] value `S`.
    #[must_use]
    pub const fn with_session_setup(pool: Pool<diesel_async::AsyncMysqlConnection>) -> Self {
        Self {
            pool,
            _setup: core::marker::PhantomData,
        }
    }
}

/// Read the current binlog coordinate from `performance_schema.log_status`.
/// Best-effort: any read failure (missing privilege, binary logging off,
/// unsupported server) degrades to `None` rather than failing the
/// re-execution. Mirrors the sync `read_binlog_pos`.
#[cfg(feature = "executor-diesel-async-mysql")]
async fn read_binlog_pos_async(
    conn: &mut diesel_async::AsyncMysqlConnection,
) -> Option<crate::MysqlBinlogPos> {
    use diesel::result::OptionalExtension;
    const SQL: &str = "SELECT \
        JSON_UNQUOTE(JSON_EXTRACT(LOCAL, '$.binary_log_file')) AS file, \
        CAST(JSON_EXTRACT(LOCAL, '$.binary_log_position') AS UNSIGNED) AS position \
        FROM performance_schema.log_status";
    let LogStatusRow {
        file: Some(file),
        position: Some(position),
    } = sql_query(SQL)
        .get_result::<LogStatusRow>(conn)
        .await
        .optional()
        .unwrap_or(None)?
    else {
        return None;
    };
    // Binlog file like "mysql-bin.000003" -> numeric suffix 3.
    let file = file.rsplit('.').next().and_then(|s| s.parse::<u32>().ok());
    let pos = u32::try_from(position).ok();
    match (file, pos) {
        (Some(file), Some(pos)) => Some(crate::MysqlBinlogPos { file, pos }),
        _ => None,
    }
}

#[cfg(feature = "executor-diesel-async-mysql")]
impl<S: SessionSetup + Send + Sync> AsyncConnector for MysqlAsyncDieselConnector<S> {
    type AuthContext = S;
    type Error = DieselAsyncError;
    type Checkpoint = crate::MysqlBinlogPos;
    type Backend = crate::backend::MySql;

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
            let conn: &mut diesel_async::AsyncMysqlConnection = &mut pooled;
            // Position before snapshot, per `Connector::Checkpoint`: the
            // coordinate is the server's current one, so taken after the read
            // it can sit ahead of the snapshot and a replay from there loses a
            // commit.
            let pos = read_binlog_pos_async(conn).await;
            conn.transaction::<(Value<Self::Backend>, Option<crate::MysqlBinlogPos>), diesel::result::Error, _>(
                |c| {
                    async move {
                        run_setup_statements_async(c, auth.setup_statements()).await?;
                        let value = load_scalar_mysql_async(c, &query, kind).await?;
                        Ok((value, pos))
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
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Self::Backend>, Self::Checkpoint>,
            Self::Error,
        >,
    > + Send {
        let query = query.clone().into_owned();
        async move {
            let mut pooled = self.pool.get().await.map_err(DieselAsyncError::Pool)?;
            let conn: &mut diesel_async::AsyncMysqlConnection = &mut pooled;
            let setup = auth.setup_statements();
            // Decision 5: this path opens no transaction today, so an empty
            // setup keeps that byte for byte, and a non-empty setup gets a real
            // transaction so the statements take hold for this read.
            let value = if setup.is_empty() {
                load_page_mysql_async(conn, &query, max_bytes)
                    .await
                    .map_err(DieselAsyncError::Diesel)?
            } else {
                conn.transaction::<crate::reexec::RowPage<Self::Backend>, diesel::result::Error, _>(
                    |c| {
                        async move {
                            run_setup_statements_async(c, setup).await?;
                            load_page_mysql_async(c, &query, max_bytes).await
                        }
                        .scope_boxed()
                    },
                )
                .await
                .map_err(DieselAsyncError::Diesel)?
            };
            Ok(Snapshot {
                value,
                checkpoint: None,
            })
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
            let conn: &mut diesel_async::AsyncMysqlConnection = &mut pooled;
            // Position before snapshot, per `Connector::Checkpoint`.
            let pos = read_binlog_pos_async(conn).await;
            conn.transaction::<(Vec<Value<Self::Backend>>, Option<crate::MysqlBinlogPos>), diesel::result::Error, _>(
                |c| {
                    async move {
                        run_setup_statements_async(c, auth.setup_statements()).await?;
                        let values =
                            load_scalar_row_mysql_async(c, &query, &kinds).await?;
                        Ok((values, pos))
                    }
                    .scope_boxed()
                },
            )
            .await
            .map_err(|e| ScalarRowError::Connector(DieselAsyncError::Diesel(e)))
        }
    }
}

/// Read one page off an async diesel connection, decoding each row without a
/// compile-time schema and stopping at `max_bytes`.
///
/// The async peer of the sync `load_page`. `diesel_async`'s `load` yields a
/// stream, so the budget stops the decode rather than trimming a materialized
/// vector, and the row after the budget answers `more` without guessing.
#[cfg(feature = "executor-diesel-async-postgres")]
async fn load_page_postgres_async(
    conn: &mut diesel_async::AsyncPgConnection,
    query: &ReadQuery<'_, crate::backend::Postgres>,
    max_bytes: usize,
) -> diesel::QueryResult<crate::reexec::RowPage<crate::backend::Postgres>> {
    use diesel_async::RunQueryDsl;

    let decoded: Vec<crate::diesel_decode::DynamicRow<crate::backend::Postgres>> =
        boxed_postgres_read_query_owned(query)?.load(conn).await?;
    Ok(finish_page(decoded, max_bytes))
}

#[cfg(feature = "executor-diesel-async-mysql")]
async fn load_page_mysql_async(
    conn: &mut diesel_async::AsyncMysqlConnection,
    query: &ReadQuery<'_, crate::backend::MySql>,
    max_bytes: usize,
) -> diesel::QueryResult<crate::reexec::RowPage<crate::backend::MySql>> {
    use diesel_async::RunQueryDsl;

    let decoded: Vec<crate::diesel_decode::DynamicRow<crate::backend::MySql>> =
        boxed_mysql_read_query_owned(query)?.load(conn).await?;
    Ok(finish_page(decoded, max_bytes))
}

fn finish_page<B: crate::backend::Backend>(
    decoded: Vec<crate::diesel_decode::DynamicRow<B>>,
    max_bytes: usize,
) -> crate::reexec::RowPage<B> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut spent = 0_usize;
    let mut more = false;
    for row in decoded {
        if columns.is_empty() {
            columns = row.columns;
        }
        let cost = crate::reexec::RowPage::<B>::row_bytes_of(&row.values);
        if !rows.is_empty() && spent + cost > max_bytes {
            more = true;
            break;
        }
        spent += cost;
        rows.push(row.values);
    }
    crate::reexec::RowPage {
        columns,
        rows,
        more,
    }
}
