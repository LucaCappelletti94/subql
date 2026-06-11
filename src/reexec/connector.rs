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

use crate::{Cell, ColumnType, DispatchError};
use thiserror::Error;

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

    /// Run the re-execution SQL and decode a single scalar value with the
    /// expected [`ColumnType`].
    ///
    /// `sql` is exactly the string the plan rendered for re-execution and
    /// returned via [`Registered::ReExec`](super::Registered::ReExec) at
    /// registration, with its projection already aliased. `column_type`
    /// is the plan's decode hint; impls may use it to pick the right diesel
    /// `QueryableByName` row, sqlx `Row::try_get` slot, etc., or ignore it
    /// and inspect the runtime row shape.
    ///
    /// An empty result set must return [`Cell::Null`] (matches the
    /// "set went empty" semantics of MIN/MAX).
    fn execute_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
        auth: &Self::AuthContext,
    ) -> Result<Cell, Self::Error>;
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

    fn execute_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
        _auth: &(),
    ) -> Result<Cell, Self::Error> {
        load_cell(&mut self.conn.borrow_mut(), sql, column_type)
    }
}
