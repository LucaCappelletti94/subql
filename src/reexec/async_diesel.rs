#![allow(clippy::manual_async_fn)]
//! Async [`AsyncConnector`] impls backed by `diesel-async` connection pools.
//!
//! Async peers of the sync connectors in [`connector`](super::connector).
//! Hand any of these to an
//! [`AsyncAutoResolvingEngine`](super::AsyncAutoResolvingEngine) to drive
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
use super::connector::LogStatusRow;
#[cfg(feature = "executor-diesel-async-postgres")]
use super::connector::PgLsnRow;
use super::connector::{DieselBackend, FloatRow, IntRow, ScalarRowError, Snapshot, TextRow};
use crate::backend::{BuiltinKind, ScalarKind, Value};
use alloc::string::ToString;
use alloc::vec::Vec;
use core::future::Future;
use diesel::query_builder::SqlQuery;
use diesel::sql_query;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl as _};
use thiserror::Error;

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
async fn load_scalar_async<C, B>(
    conn: &mut C,
    sql: &str,
    kind: BuiltinKind,
) -> diesel::QueryResult<Value<B>>
where
    B: DieselBackend,
    C: AsyncConnection,
    for<'q> SqlQuery: diesel_async::methods::LoadQuery<'q, C, IntRow>
        + diesel_async::methods::LoadQuery<'q, C, FloatRow>
        + diesel_async::methods::LoadQuery<'q, C, TextRow>,
{
    let value = match kind {
        ScalarKind::Int => sql_query(sql)
            .get_result::<IntRow>(conn)
            .await?
            .v
            .map_or(Value::Null, B::value_from_i64),
        ScalarKind::Float => sql_query(sql)
            .get_result::<FloatRow>(conn)
            .await?
            .v
            .map_or(Value::Null, B::value_from_f64),
        ScalarKind::Bool
        | ScalarKind::String
        | ScalarKind::Bytes
        | ScalarKind::Uuid
        | ScalarKind::Timestamp
        | ScalarKind::TimestampTz
        | ScalarKind::Date
        | ScalarKind::Time
        | ScalarKind::Decimal
        | ScalarKind::Json
        | ScalarKind::Jsonb => sql_query(sql)
            .get_result::<TextRow>(conn)
            .await?
            .v
            .map_or(Value::Null, B::value_from_string),
    };
    Ok(value)
}

/// Async peer of `load_scalar_row`: run one aliased subquery per component
/// through [`load_scalar_async`], casting `Float` (`SUM`) components to the
/// backend's double type. Callers wrap this in a transaction so the
/// components share one snapshot. Column `i` is projected as `ci`.
async fn load_scalar_row_async<C, B>(
    conn: &mut C,
    sql: &str,
    kinds: &[BuiltinKind],
) -> diesel::QueryResult<Vec<Value<B>>>
where
    B: DieselBackend,
    C: AsyncConnection,
    for<'q> SqlQuery: diesel_async::methods::LoadQuery<'q, C, IntRow>
        + diesel_async::methods::LoadQuery<'q, C, FloatRow>
        + diesel_async::methods::LoadQuery<'q, C, TextRow>,
{
    let mut out = Vec::with_capacity(kinds.len());
    for (i, kind) in kinds.iter().enumerate() {
        let wrapped = if matches!(kind, ScalarKind::Float) {
            alloc::format!(
                "SELECT CAST(c{i} AS {cast}) AS v FROM ({sql}) AS agg_seed",
                cast = B::double_cast_type()
            )
        } else {
            alloc::format!("SELECT c{i} AS v FROM ({sql}) AS agg_seed")
        };
        out.push(load_scalar_async::<C, B>(conn, &wrapped, *kind).await?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// PgAsyncDieselConnector: LSN-aware PG impl behind executor-diesel-async-postgres.
// ---------------------------------------------------------------------------

/// Async LSN-aware [`AsyncConnector`] for PostgreSQL, the async peer of
/// [`PgDieselConnector`](super::connector::PgDieselConnector).
///
/// Wraps a `bb8` pool over
/// [`AsyncPgConnection`](diesel_async::AsyncPgConnection). Each
/// `execute_scalar` opens a `READ ONLY REPEATABLE READ` transaction, runs the
/// user's SQL and `pg_current_wal_lsn()` under the same MVCC snapshot, and
/// returns the [`Value<Postgres>`](crate::backend::Postgres) with the parsed
/// [`PgLsn`](crate::PgLsn). Pure Rust: `diesel-async` speaks the PG wire
/// protocol through `tokio-postgres`, no libpq.
///
/// # Errors
///
/// Returns [`DieselAsyncError`] for pool or database failures.
#[cfg(feature = "executor-diesel-async-postgres")]
pub struct PgAsyncDieselConnector {
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
        }
    }
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl PgAsyncDieselConnector {
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
            while let Some(row) = held.leftover.front() {
                let cost = crate::reexec::RowPage::<crate::backend::Postgres>::row_bytes_of(row);
                if !rows.is_empty() && spent + cost > max_bytes {
                    return Ok(Snapshot {
                        value: crate::reexec::RowPage {
                            columns: held.columns.clone(),
                            rows,
                            more: true,
                        },
                        checkpoint: held.checkpoint,
                    });
                }
                spent += cost;
                // Total: `front` just answered `Some`.
                if let Some(row) = held.leftover.pop_front() {
                    rows.push(row);
                }
            }
            let page = load_page_async::<_, diesel::pg::Pg, crate::backend::Postgres>(
                &mut held.conn,
                &alloc::format!("FETCH FORWARD {batch} FROM {}", held.name),
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
impl AsyncConnector for PgAsyncDieselConnector {
    type AuthContext = ();
    type Error = DieselAsyncError;
    type Checkpoint = crate::PgLsn;
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: BuiltinKind,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        let sql = sql.to_string();
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
                        let value = load_scalar_async::<_, Self::Backend>(c, &sql, kind).await?;
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
        sql: &str,
        max_bytes: usize,
        _auth: &(),
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Self::Backend>, Self::Checkpoint>,
            Self::Error,
        >,
    > + Send {
        let sql = sql.to_string();
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
                        let value = load_page_async::<_, diesel::pg::Pg, crate::backend::Postgres>(c, &sql, max_bytes).await?;
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
        sql: &str,
        _auth: &(),
    ) -> impl Future<Output = Result<super::CursorId, super::CursorError<Self::Error>>> + Send {
        let sql = sql.to_string();
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
                sql_query(alloc::format!("DECLARE {name} NO SCROLL CURSOR FOR {sql}"))
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
        sql: &str,
        kinds: &[BuiltinKind],
        _auth: &(),
    ) -> impl Future<
        Output = Result<
            (Vec<Value<Self::Backend>>, Option<Self::Checkpoint>),
            ScalarRowError<Self::Error>,
        >,
    > + Send {
        let sql = sql.to_string();
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
                        let values =
                            load_scalar_row_async::<_, Self::Backend>(c, &sql, &kinds).await?;
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
pub struct MysqlAsyncDieselConnector {
    pool: Pool<diesel_async::AsyncMysqlConnection>,
}

#[cfg(feature = "executor-diesel-async-mysql")]
impl MysqlAsyncDieselConnector {
    /// Wrap a `bb8` pool over `AsyncMysqlConnection` already configured by the
    /// caller.
    #[must_use]
    pub const fn new(pool: Pool<diesel_async::AsyncMysqlConnection>) -> Self {
        Self { pool }
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
impl AsyncConnector for MysqlAsyncDieselConnector {
    type AuthContext = ();
    type Error = DieselAsyncError;
    type Checkpoint = crate::MysqlBinlogPos;
    type Backend = crate::backend::MySql;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: BuiltinKind,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        let sql = sql.to_string();
        async move {
            let mut pooled = self.pool.get().await.map_err(DieselAsyncError::Pool)?;
            let conn: &mut diesel_async::AsyncMysqlConnection = &mut pooled;
            conn.transaction::<(Value<Self::Backend>, Option<crate::MysqlBinlogPos>), diesel::result::Error, _>(
                |c| {
                    async move {
                        let value = load_scalar_async::<_, Self::Backend>(c, &sql, kind).await?;
                        let pos = read_binlog_pos_async(c).await;
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
        sql: &str,
        max_bytes: usize,
        _auth: &(),
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Self::Backend>, Self::Checkpoint>,
            Self::Error,
        >,
    > + Send {
        let sql = sql.to_string();
        async move {
            let mut pooled = self.pool.get().await.map_err(DieselAsyncError::Pool)?;
            let conn: &mut diesel_async::AsyncMysqlConnection = &mut pooled;
            let value = load_page_async::<_, diesel::mysql::Mysql, crate::backend::MySql>(
                conn, &sql, max_bytes,
            )
            .await
            .map_err(DieselAsyncError::Diesel)?;
            Ok(Snapshot {
                value,
                checkpoint: None,
            })
        }
    }

    fn execute_scalar_row(
        &self,
        sql: &str,
        kinds: &[BuiltinKind],
        _auth: &(),
    ) -> impl Future<
        Output = Result<
            (Vec<Value<Self::Backend>>, Option<Self::Checkpoint>),
            ScalarRowError<Self::Error>,
        >,
    > + Send {
        let sql = sql.to_string();
        let kinds = kinds.to_vec();
        async move {
            let mut pooled = self
                .pool
                .get()
                .await
                .map_err(|e| ScalarRowError::Connector(DieselAsyncError::Pool(e)))?;
            let conn: &mut diesel_async::AsyncMysqlConnection = &mut pooled;
            conn.transaction::<(Vec<Value<Self::Backend>>, Option<crate::MysqlBinlogPos>), diesel::result::Error, _>(
                |c| {
                    async move {
                        let values =
                            load_scalar_row_async::<_, Self::Backend>(c, &sql, &kinds).await?;
                        let pos = read_binlog_pos_async(c).await;
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
async fn load_page_async<C, DB, B>(
    conn: &mut C,
    sql: &str,
    max_bytes: usize,
) -> diesel::QueryResult<crate::reexec::RowPage<B>>
where
    C: diesel_async::AsyncConnection<Backend = DB>,
    DB: crate::diesel_decode::RowFieldDecode
        + diesel::backend::DieselReserveSpecialization
        + 'static,
    B: crate::diesel_decode::SpellCanonical,
    crate::diesel_decode::DynamicRow<B>:
        diesel::deserialize::FromSqlRow<diesel::sql_types::Untyped, DB> + Send + 'static,
{
    use diesel_async::RunQueryDsl;

    let decoded: Vec<crate::diesel_decode::DynamicRow<B>> = sql_query(sql).load(conn).await?;

    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut spent = 0_usize;
    let mut more = false;
    for row in decoded {
        if columns.is_empty() {
            columns = row.columns;
        }
        let cost = crate::reexec::RowPage::<B>::row_bytes_of(&row.values);
        // A page always makes progress: the budget stops the row after the
        // first, never the first itself.
        if !rows.is_empty() && spent + cost > max_bytes {
            more = true;
            break;
        }
        spent += cost;
        rows.push(row.values);
    }
    Ok(crate::reexec::RowPage {
        columns,
        rows,
        more,
    })
}
