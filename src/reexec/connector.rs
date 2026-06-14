//! Opt-in connector abstraction for auto-resolving re-execution.
//!
//! The bare [`ReExecEngine`](super::ReExecEngine) emits
//! [`ReExecutionTrigger`](super::ReExecutionTrigger)s and leaves the
//! re-execution to its caller. Some consumers prefer subql to run the
//! re-execution itself: they implement [`Connector`] over their database
//! handle, hand it to an [`AutoResolvingEngine`](super::AutoResolvingEngine),
//! and receive [`ScalarUpdate`](super::ScalarUpdate)s directly. Consumers
//! that want explicit control (e.g. cross-batch coalescing,
//! retry-with-backoff, per-viewer authorization that does not fit the trait)
//! keep using the trigger path.
//!
//! # Scope
//!
//! v1 is **sync only** and exposes a single method, [`Connector::execute_scalar`],
//! sufficient for the [`MinMaxQuery`](super::maintain) flavor of re-execution.
//! An async variant (`AsyncConnector`, async-fn-in-trait) and a row-set
//! method (`execute_rows`, needed for single-table total re-execution) are
//! planned follow-ons that ride the same auth-context machinery.
//!
//! # Authorization
//!
//! [`Connector::AuthContext`] is an associated type carried per
//! subscription. Consumers that re-execute under per-viewer auth (e.g.
//! PostgreSQL RLS via `set_config('request.jwt', ...)`) store the JWT or
//! identity in the context; consumers without per-viewer state use `()`.
//!
//! # Error handling
//!
//! [`Connector::Error`] propagates as [`ReExecError::Connector`]. A single
//! Connector failure aborts the entire `consumers()` batch; the caller is
//! expected to retry the batch. Retry policy lives in the Connector impl
//! (or above the engine), never inside subql.

use crate::{Cell, Checkpoint, ColumnType, DispatchError, RowImage};
use thiserror::Error;

/// A captured-state snapshot of a query's value, together with the
/// [`Checkpoint`] at which it was read.
///
/// Returned by [`Connector::execute_rows`] (and the future
/// [`AutoResolvingEngine::snapshot`](super::AutoResolvingEngine::snapshot))
/// so downstream replay layers (oplogs, client cursors) can anchor a
/// snapshot to a position in the source stream. The `checkpoint` is
/// `None` when the backend has no native notion of position (e.g.
/// in-memory SQLite).
// PartialEq only: callers that need to compare Snapshots get it for Cell
// scalars (Cell is not Eq because of f64). Deriving Eq would forbid the
// Cell parameterization.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct Snapshot<T, C: Checkpoint> {
    /// The snapshot value (a scalar `Cell` or a `Vec<RowImage>`).
    pub value: T,
    /// The position at which the snapshot was read, when known.
    pub checkpoint: Option<C>,
}

#[cfg(feature = "executor-diesel")]
use alloc::string::String;
#[cfg(feature = "executor-diesel")]
use alloc::sync::Arc;
#[cfg(feature = "executor-diesel")]
use core::cell::RefCell;
#[cfg(feature = "executor-diesel")]
use diesel::{
    query_builder::SqlQuery,
    query_dsl::LoadQuery,
    sql_query,
    sql_types::{BigInt, Double, Nullable, Text},
    Connection, QueryResult, RunQueryDsl,
};

/// Executes a captured query's SQL when maintenance cannot resolve it.
///
/// Implementors own the database connection and any retry, pooling, or
/// per-viewer auth policy that the execution requires. Subql calls
/// [`execute_scalar`](Self::execute_scalar) whenever the in-process state
/// machine emits `NeedsReexecution`, then installs the returned [`Cell`].
///
/// # Auth context
///
/// One `AuthContext` value is stored per registered subscription and passed
/// to each `execute_scalar` invocation. Use it to carry a session id, a JWT,
/// or a tenant tag that the impl converts into a database-level identity
/// (e.g. `SET LOCAL ROLE`, `set_config('request.jwt', ...)`) before
/// executing the SQL. Subql itself never inspects the context.
///
/// # Errors
///
/// Any failure (network, authentication, malformed SQL) bubbles up as
/// `Self::Error`. The auto-resolving engine wraps it in
/// [`ReExecError::Connector`] and aborts the rest of the dispatch batch.
pub trait Connector {
    /// Per-subscription auth state carried verbatim to each execution.
    /// Subql stores it opaquely; the connector's impl interprets it.
    type AuthContext;
    /// Connector-specific error returned by [`execute_scalar`].
    ///
    /// [`execute_scalar`]: Self::execute_scalar
    type Error;
    /// Position token the connector tags reads with.
    ///
    /// PG-aware connectors choose [`crate::PgLsn`] and call
    /// `pg_current_wal_lsn()` inside the same transaction as the read.
    /// Backends with no native position (in-memory SQLite, MySQL absent of
    /// binlog tracking) choose [`crate::NoCheckpoint`] and return `None`.
    type Checkpoint: Checkpoint;

    /// Run the re-execution SQL and decode a single scalar value with the
    /// expected [`ColumnType`], optionally reporting the position at which
    /// the read was taken.
    ///
    /// `sql` is exactly the string the plan rendered for re-execution and
    /// returned via [`Registered::ReExec`](super::Registered::ReExec) at
    /// registration, with its projection already aliased. `column_type`
    /// is the plan's decode hint; impls may use it to pick the right diesel
    /// `QueryableByName` row, sqlx `Row::try_get` slot, etc., or ignore it
    /// and inspect the runtime row shape.
    ///
    /// The returned tuple is `(value, Option<checkpoint>)`. The checkpoint
    /// is informational for downstream replay layers; subql does not gate
    /// on it. An empty result set must return [`Cell::Null`] as the value
    /// (matches the "set went empty" semantics of MIN/MAX).
    fn execute_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
        auth: &Self::AuthContext,
    ) -> Result<(Cell, Option<Self::Checkpoint>), Self::Error>;

    /// Run `sql` as a row-returning query and decode every row.
    ///
    /// Used by the auto-resolving engine's `snapshot` method to bootstrap
    /// a subscription, and (in a future phase) by total single-table row
    /// re-execution. Impls should open a read-only repeatable-read
    /// transaction so the rows and the returned [`Snapshot::checkpoint`]
    /// agree on a single point in the source stream.
    fn execute_rows(
        &self,
        sql: &str,
        auth: &Self::AuthContext,
    ) -> Result<Snapshot<alloc::vec::Vec<RowImage>, Self::Checkpoint>, Self::Error>;
}

/// Error returned by [`AutoResolvingEngine::consumers`].
///
/// [`AutoResolvingEngine::consumers`]: super::AutoResolvingEngine::consumers
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ReExecError<E> {
    /// The core engine could not dispatch the event (e.g. unknown table id,
    /// missing required row image).
    #[error("dispatch failed: {0}")]
    Dispatch(#[from] DispatchError),
    /// The [`Connector`] failed to execute the re-execution SQL. The whole
    /// batch is aborted; the caller is expected to retry it.
    #[error("connector failed: {0}")]
    Connector(E),
}

// ---------------------------------------------------------------------------
// DieselConnector: opt-in sync impl shipped behind `executor-diesel`.
// ---------------------------------------------------------------------------

/// Sync [`Connector`] backed by a single diesel [`Connection`].
///
/// Holds the connection in a [`RefCell`] for the interior-mutability the
/// trait's `&self` requires. Not `Send`/`Sync` (diesel connections are not
/// `Send`); for multi-threaded use, either keep the connector thread-local
/// or implement [`Connector`] yourself over a connection pool
/// (`r2d2::Pool<ConnectionManager<C>>`, `deadpool`, `bb8`).
///
/// Bounds:
/// * `C: Connection` - any diesel connection.
/// * For each backend the HRTB on the [`Connector`] impl ensures
///   [`sql_query`] can decode the nullable scalar rows
///   (`Nullable<BigInt>`, `Nullable<Double>`, `Nullable<Text>`).
///   PostgreSQL, MySQL, and SQLite all satisfy these.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure
/// (network drop, statement error, decoding mismatch).
#[cfg(feature = "executor-diesel")]
pub struct DieselConnector<C: Connection> {
    conn: RefCell<C>,
}

#[cfg(feature = "executor-diesel")]
impl<C: Connection> DieselConnector<C> {
    /// Wrap an owned connection. The connector takes exclusive ownership
    /// and serializes access through interior mutability.
    pub const fn new(conn: C) -> Self {
        Self {
            conn: RefCell::new(conn),
        }
    }
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
struct IntRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    v: Option<i64>,
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
struct FloatRow {
    #[diesel(sql_type = Nullable<Double>)]
    v: Option<f64>,
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Nullable<Text>)]
    v: Option<String>,
}

#[cfg(feature = "executor-diesel")]
fn load_cell<C>(conn: &mut C, sql: &str, column_type: ColumnType) -> QueryResult<Cell>
where
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    let cell = match column_type {
        ColumnType::Int => sql_query(sql)
            .get_result::<IntRow>(conn)?
            .v
            .map_or(Cell::Null, Cell::Int),
        ColumnType::Float => sql_query(sql)
            .get_result::<FloatRow>(conn)?
            .v
            .map_or(Cell::Null, Cell::Float),
        ColumnType::Bool | ColumnType::String | ColumnType::Unknown => sql_query(sql)
            .get_result::<TextRow>(conn)?
            .v
            .map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
    };
    Ok(cell)
}

#[cfg(feature = "executor-diesel")]
impl<C> Connector for DieselConnector<C>
where
    C: Connection,
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    type AuthContext = ();
    type Error = diesel::result::Error;
    /// Backend-agnostic v1 default: this connector does not read the
    /// underlying source's position. PG-aware variants
    /// (`PgDieselConnector`, planned) override this to `PgLsn` and read
    /// `pg_current_wal_lsn()` inside the snapshot transaction.
    type Checkpoint = crate::NoCheckpoint;

    fn execute_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
        _auth: &(),
    ) -> Result<(Cell, Option<Self::Checkpoint>), Self::Error> {
        let cell = load_cell(&mut self.conn.borrow_mut(), sql, column_type)?;
        Ok((cell, None))
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<Snapshot<alloc::vec::Vec<RowImage>, Self::Checkpoint>, Self::Error> {
        // Row-set decoding through diesel's `sql_query` requires the
        // caller to know the schema at compile time (each column wants
        // its own typed accessor). Phase 2 ships the trait shape; the
        // generic row-decoding path lands with the total reexec feature
        // (see docs/connetto-alignment-plan.md). For now this method is
        // wired up as a panic so any caller that opts into it gets a
        // clear signal that it is not yet implemented for the generic
        // DieselConnector.
        #[allow(clippy::unimplemented)]
        {
            unimplemented!(
                "DieselConnector::execute_rows is reserved for total row reexec; \
                 use the scalar path or supply a custom Connector impl"
            )
        }
    }
}

// ---------------------------------------------------------------------------
// PgDieselConnector: LSN-aware sync impl behind `executor-diesel-postgres`.
// ---------------------------------------------------------------------------

/// Sync [`Connector`] backed by a diesel [`PgConnection`] that anchors
/// every read to a PostgreSQL WAL position.
///
/// On each `execute_scalar` (and future `execute_rows`) call the connector
/// opens a `READ ONLY REPEATABLE READ` transaction, queries
/// `pg_current_wal_lsn()` alongside the user's SQL, and returns the
/// resulting [`Cell`] together with the parsed [`crate::PgLsn`]. The user
/// query and the LSN observe the same MVCC snapshot, so downstream replay
/// layers (an oplog, a client cursor) can chain WAL events onto the
/// snapshot at exactly the position the snapshot was taken.
///
/// Holds the connection in a [`RefCell`] for the interior-mutability the
/// trait's `&self` requires. Not `Send`/`Sync`; for multi-threaded use,
/// either keep the connector thread-local or implement [`Connector`]
/// yourself over a connection pool.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure
/// (network drop, statement error, malformed LSN response).
#[cfg(feature = "executor-diesel-postgres")]
pub struct PgDieselConnector {
    conn: RefCell<diesel::PgConnection>,
}

#[cfg(feature = "executor-diesel-postgres")]
impl PgDieselConnector {
    /// Wrap an owned [`PgConnection`](diesel::PgConnection). The connector
    /// takes exclusive ownership and serializes access through interior
    /// mutability.
    #[must_use]
    pub const fn new(conn: diesel::PgConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
        }
    }
}

#[cfg(feature = "executor-diesel-postgres")]
#[derive(diesel::QueryableByName)]
struct PgLsnRow {
    #[diesel(sql_type = Text)]
    lsn: String,
}

#[cfg(feature = "executor-diesel-postgres")]
fn read_current_lsn(conn: &mut diesel::PgConnection) -> diesel::QueryResult<Option<crate::PgLsn>> {
    let row: PgLsnRow = sql_query("SELECT pg_current_wal_lsn()::text AS lsn").get_result(conn)?;
    Ok(crate::PgLsn::parse(&row.lsn))
}

#[cfg(feature = "executor-diesel-postgres")]
impl Connector for PgDieselConnector {
    type AuthContext = ();
    type Error = diesel::result::Error;
    type Checkpoint = crate::PgLsn;

    fn execute_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
        _auth: &(),
    ) -> Result<(Cell, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            // Pin the transaction's MVCC snapshot so the user query and the
            // LSN agree on a single point in the WAL.
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            let cell = load_cell(conn, sql, column_type)?;
            let lsn = read_current_lsn(conn)?;
            Ok((cell, lsn))
        })
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<Snapshot<alloc::vec::Vec<RowImage>, Self::Checkpoint>, Self::Error> {
        // Same restriction as DieselConnector: generic row decoding is
        // deferred to the total row reexec feature.
        #[allow(clippy::unimplemented)]
        {
            unimplemented!(
                "PgDieselConnector::execute_rows is reserved for total row reexec; \
                 use the scalar path or supply a custom Connector impl"
            )
        }
    }
}

// ---------------------------------------------------------------------------
// PgR2D2DieselConnector: pool-backed PG impl behind `executor-diesel-postgres-r2d2`.
// ---------------------------------------------------------------------------

/// Pool-backed [`Connector`] for PostgreSQL.
///
/// Wraps an `r2d2::Pool` over `ConnectionManager<PgConnection>`, is
/// `Send + Sync`, and reads `pg_current_wal_lsn()` inside the same
/// transaction as the user query for LSN-anchored snapshots.
///
/// Use this connector when the engine dispatches re-executions
/// concurrently (the async engine with `consumers_batch`) or when
/// snapshots may interleave with CDC re-executions. Each call to
/// [`execute_scalar`](Connector::execute_scalar) borrows a connection
/// from the pool for the duration of the transaction and releases it on
/// completion.
///
/// The pool's `get` failures (timeout, pool exhausted) surface as
/// [`PgR2D2Error::Pool`]; the transaction's diesel errors surface as
/// [`PgR2D2Error::Diesel`].
///
/// `Send + Sync` so it can be shared across async tasks running on a
/// multi-threaded runtime.
#[cfg(feature = "executor-diesel-postgres-r2d2")]
pub struct PgR2D2DieselConnector {
    pool: r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
}

#[cfg(feature = "executor-diesel-postgres-r2d2")]
impl PgR2D2DieselConnector {
    /// Wrap an `r2d2::Pool` already configured by the caller (max size,
    /// connection timeout, etc.).
    #[must_use]
    pub const fn new(
        pool: r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
    ) -> Self {
        Self { pool }
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
impl Connector for PgR2D2DieselConnector {
    type AuthContext = ();
    type Error = PgR2D2Error;
    type Checkpoint = crate::PgLsn;

    fn execute_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
        _auth: &(),
    ) -> Result<(Cell, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.pool.get()?;
        let result: Result<(Cell, Option<crate::PgLsn>), diesel::result::Error> =
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                    .execute(conn)?;
                let cell = load_cell(conn, sql, column_type)?;
                let lsn = read_current_lsn(conn)?;
                Ok((cell, lsn))
            });
        Ok(result?)
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<Snapshot<alloc::vec::Vec<RowImage>, Self::Checkpoint>, Self::Error> {
        #[allow(clippy::unimplemented)]
        {
            unimplemented!(
                "PgR2D2DieselConnector::execute_rows is reserved for total row reexec; \
                 use the scalar path or supply a custom Connector impl"
            )
        }
    }
}
