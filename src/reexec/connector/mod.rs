#![allow(clippy::type_complexity)]
//! Opt-in connector abstraction for auto-resolving re-execution.
//!
//! [`SubscriptionEngine`](crate::SubscriptionEngine) emits
//! [`ReExecutionTrigger`](super::ReExecutionTrigger) and leaves SQL execution
//! to downstream Rust code. Code that wants subql to call the database uses
//! [`AutoResolvingEngine`](super::AutoResolvingEngine) with [`SyncMode`](super::SyncMode) and implements [`Connector`] over its database
//! handle, hand it to an [`AutoResolvingEngine`](super::AutoResolvingEngine),
//! and receive [`ScalarUpdate`](super::ScalarUpdate)s directly. Consumers
//! that want explicit control (e.g. cross-batch coalescing,
//! retry-with-backoff, per-viewer authorization that does not fit the trait)
//! keep using the trigger path.
//!
//! # Scope
//!
//! [`Connector::execute_scalar`] resolves the [`MinMaxQuery`](super::maintain)
//! flavor of re-execution and is the only method backed by a shipped impl.
//! [`Connector::execute_rows`] exists for single-table total re-execution but
//! the diesel-backed impls still stub it (see `MILESTONES.md`). An async peer
//! lives in [`AsyncConnector`](super::AsyncConnector).
//!
//! # Authorization
//!
//! [`Connector::AuthContext`] is an associated type carried per
//! subscription. Consumers that re-execute under per-viewer auth (e.g.
//! PostgreSQL RLS via `set_config('request.jwt', ...)`) store the JWT or
//! identity in the context. Consumers without per-viewer state use `()`.
//!
//! # Error handling
//!
//! [`Connector::Error`] propagates as [`ReExecError::Connector`]. A single
//! Connector failure aborts the entire `consumers()` batch. The caller is
//! expected to retry the batch. Retry policy lives in the Connector impl
//! (or above the engine), never inside subql.

use crate::backend::{Backend, BuiltinKind, Value};
use crate::{Checkpoint, DispatchError};
use thiserror::Error;

#[cfg(feature = "executor-diesel")]
use diesel::query_builder::SqlQuery;
// run_setup_statements uses Connection, sql_query, and RunQueryDsl.
// These are session-control and user-supplied statements that the typed DSL
// cannot express, so sql_query is the correct tool here.
#[cfg(feature = "executor-diesel")]
use diesel::{sql_query, Connection, QueryResult, RunQueryDsl};

#[cfg(feature = "executor-diesel")]
mod diesel_backend;
#[cfg(feature = "executor-diesel")]
mod diesel_connector;
#[cfg(any(
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql"
))]
mod mysql_diesel_connector;
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
mod pg_diesel_connector;
#[cfg(feature = "executor-diesel-postgres-r2d2")]
mod pg_r2d2_diesel_connector;

// Public types needed by reexec/mod.rs re-exports and by async_diesel.
#[cfg(feature = "executor-diesel")]
pub use diesel_backend::DieselBackend;
#[cfg(feature = "executor-diesel")]
pub use diesel_connector::DieselConnector;
// The scalar row shapes travel to the async connectors only, so the
// re-export follows their backend features rather than `executor-diesel`.
#[cfg(any(
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-async-mysql"
))]
pub use diesel_connector::{FloatRow, IntRow, TextRow};
#[cfg(any(
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql"
))]
pub use mysql_diesel_connector::LogStatusRow;
#[cfg(feature = "executor-diesel-mysql")]
pub use mysql_diesel_connector::MysqlDieselConnector;
#[cfg(feature = "executor-diesel-postgres")]
pub use pg_diesel_connector::PgDieselConnector;
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
pub use pg_diesel_connector::PgLsnRow;
#[cfg(feature = "executor-diesel-postgres-r2d2")]
pub use pg_r2d2_diesel_connector::{PgR2D2DieselConnector, PgR2D2Error};

// Helpers that async_diesel and other reexec siblings import directly.
// pub(super) = visible to reexec and all its submodules.
#[cfg(feature = "executor-diesel-async-mysql")]
pub(super) use diesel_backend::boxed_mysql_read_query_owned;
#[cfg(feature = "executor-diesel-async-postgres")]
pub(super) use diesel_backend::boxed_postgres_read_query_owned;

/// Owned SQL and typed binds shared across runtime state.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "", transparent)]
pub struct BoundQuery<B: Backend> {
    inner: alloc::sync::Arc<BoundQueryInner<B>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
struct BoundQueryInner<B: Backend> {
    sql: alloc::string::String,
    binds: alloc::vec::Vec<Value<B>>,
}

impl<B: Backend> BoundQuery<B> {
    /// Creates an owned bound query.
    #[must_use]
    pub fn new(sql: alloc::string::String, binds: alloc::vec::Vec<Value<B>>) -> Self {
        Self {
            inner: alloc::sync::Arc::new(BoundQueryInner { sql, binds }),
        }
    }

    /// Returns the SQL text.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.inner.sql
    }

    /// Returns binds in placeholder order.
    #[must_use]
    pub fn binds(&self) -> &[Value<B>] {
        &self.inner.binds
    }

    /// Borrows this query for a connector call.
    #[must_use]
    pub fn as_read_query(&self) -> ReadQuery<'_, B> {
        ReadQuery::borrowed(self.sql(), self.binds())
    }
}

impl<B: Backend> Clone for BoundQuery<B> {
    fn clone(&self) -> Self {
        Self {
            inner: alloc::sync::Arc::clone(&self.inner),
        }
    }
}

impl<B: Backend> core::fmt::Debug for BoundQuery<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BoundQuery")
            .field("sql", &self.inner.sql)
            .field("binds", &self.inner.binds)
            .finish()
    }
}

impl<B: Backend> PartialEq for BoundQuery<B> {
    fn eq(&self, other: &Self) -> bool {
        alloc::sync::Arc::ptr_eq(&self.inner, &other.inner)
            || (self.inner.sql == other.inner.sql && self.inner.binds == other.inner.binds)
    }
}

/// SQL and typed binds passed to every connector read.
pub struct ReadQuery<'a, B: Backend> {
    sql: alloc::borrow::Cow<'a, str>,
    binds: alloc::borrow::Cow<'a, [Value<B>]>,
}

impl<'a, B: Backend> ReadQuery<'a, B> {
    /// Creates an owned query.
    #[must_use]
    pub const fn owned(sql: alloc::string::String, binds: alloc::vec::Vec<Value<B>>) -> Self {
        Self {
            sql: alloc::borrow::Cow::Owned(sql),
            binds: alloc::borrow::Cow::Owned(binds),
        }
    }

    /// Creates a borrowed query.
    #[must_use]
    pub const fn borrowed(sql: &'a str, binds: &'a [Value<B>]) -> Self {
        Self {
            sql: alloc::borrow::Cow::Borrowed(sql),
            binds: alloc::borrow::Cow::Borrowed(binds),
        }
    }

    /// Creates a borrowed query without binds.
    #[must_use]
    pub const fn without_binds(sql: &'a str) -> Self {
        Self::borrowed(sql, &[])
    }

    /// Returns the SQL text.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns binds in placeholder order.
    #[must_use]
    pub fn binds(&self) -> &[Value<B>] {
        &self.binds
    }

    /// Converts borrowed fields to owned storage.
    #[must_use]
    pub fn into_owned(self) -> ReadQuery<'static, B> {
        ReadQuery::owned(self.sql.into_owned(), self.binds.into_owned())
    }
}

impl<B: Backend> Clone for ReadQuery<'_, B> {
    fn clone(&self) -> Self {
        Self {
            sql: self.sql.clone(),
            binds: self.binds.clone(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for ReadQuery<'_, B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReadQuery")
            .field("sql", &self.sql)
            .field("binds", &self.binds)
            .finish()
    }
}

impl<B: Backend> PartialEq for ReadQuery<'_, B> {
    fn eq(&self, other: &Self) -> bool {
        self.sql == other.sql && self.binds == other.binds
    }
}

/// A captured-state snapshot of a query's value, together with the
/// [`Checkpoint`] at which it was read.
///
/// Returned by the row-returning reads and by
/// [`AutoResolvingEngine::snapshot`](super::AutoResolvingEngine::snapshot)
/// so downstream replay layers (oplogs, client cursors) can anchor a
/// snapshot to a position in the source stream. The `checkpoint` is
/// `None` when the backend has no native notion of position (e.g.
/// in-memory SQLite).
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct Snapshot<T, C: Checkpoint> {
    /// The snapshot value (a scalar [`Value`], or a [`RowPage`]).
    pub value: T,
    /// The position at which the snapshot was read, when known.
    pub checkpoint: Option<C>,
}

/// One bounded page of a row-returning read.
///
/// A captured query's answer can be far larger than memory, so a read hands
/// back as much as fits a byte budget and says whether the result went on.
/// The budget is bytes rather than rows because a row is not a bounded thing:
/// one row carrying a large text column is unbounded on its own.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct RowPage<B: Backend> {
    /// Column names as the database reported them, in projection order.
    ///
    /// A captured query's projection can be an expression or an alias that no
    /// catalog describes, so the names travel with the rows rather than being
    /// looked up.
    pub columns: alloc::vec::Vec<alloc::string::String>,
    /// Rows, each in [`Self::columns`] order.
    pub rows: alloc::vec::Vec<alloc::vec::Vec<Value<B>>>,
    /// Whether the budget stopped the read before the result ended.
    ///
    /// `false` means this page is the last one. It is the connector's answer
    /// and not a guess: a caller that inferred "more" from a full page would
    /// make one wasted read on every exactly-fitting result.
    pub more: bool,
}

// Every caller is a diesel-backed reader spending a page budget.
#[cfg(feature = "executor-diesel")]
impl<B: Backend> RowPage<B> {
    /// The encoded size of `row`, which is what a page's budget is spent in.
    ///
    /// postcard, because that is already the crate's encoding for a row's
    /// identity (`crate::row_set`), so one row costs the same number of bytes
    /// here as it does there.
    ///
    /// This is the canonical per-row cost an outside connector spends against
    /// the [`read_page`](Connector::read_page) budget, so call it rather than
    /// recompute a size that could drift from the one the budget is measured in.
    pub fn row_bytes_of(row: &[Value<B>]) -> usize {
        postcard::experimental::serialized_size(row).unwrap_or(usize::MAX)
    }
}

/// Handle to a cursor a connector holds open, with the transaction behind it.
///
/// A result with no key cannot be resumed by asking for "everything after the
/// last row", so those pages come from one cursor inside one read-only
/// repeatable-read transaction, which is the only way successive pages describe
/// a single instant. The id is opaque and connector-minted, matching how this
/// module already addresses a maintained answer by
/// [`SubscriptionId`](crate::SubscriptionId).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CursorId(pub u64);

/// Error from the cursor reads.
///
/// Separates "this connector holds no cursors" from a genuine failure, so a
/// caller can tell a connector that cannot serve keyless captures from a
/// database that refused.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CursorError<E> {
    /// This connector does not implement cursors, so it cannot serve a
    /// captured query whose result has no key to resume from.
    #[error("connector holds no cursors; a keyless captured query needs one")]
    Unsupported,
    /// No cursor is open under this id: it was closed, or never opened.
    #[error("cursor {0:?} is not open")]
    Unknown(CursorId),
    /// The cursor exists but another read holds it. A cursor is serial: two
    /// interleaved `FETCH`es would scramble the rows between them, so this is
    /// reported rather than served.
    #[error("cursor {0:?} is in use by another read")]
    Busy(CursorId),
    /// The connector failed.
    #[error(transparent)]
    Connector(E),
}

/// Executes a captured query's SQL when maintenance cannot resolve it.
///
/// Implementors own the database connection and any retry, pooling, or
/// per-viewer auth policy that the execution requires. Subql calls
/// [`execute_scalar`](Self::execute_scalar) whenever the in-process state
/// machine emits `NeedsReexecution`, then installs the returned
/// [`Value`].
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
    /// Subql stores it opaquely. The connector's impl interprets it.
    type AuthContext;
    /// Connector-specific error returned by [`execute_scalar`].
    ///
    /// [`execute_scalar`]: Self::execute_scalar
    type Error;
    /// Position token the connector tags reads with.
    ///
    /// An implementation MUST take the position before the read's snapshot
    /// opens, never after: behind the snapshot re-delivers changes the
    /// snapshot already holds, which keyed application absorbs, while ahead
    /// of it silently drops a transaction the snapshot never saw. PG-aware
    /// connectors choose [`crate::PgLsn`] and read `pg_current_wal_lsn()`,
    /// which is not snapshot-bound, before opening the read's transaction.
    /// Backends with no native position (in-memory SQLite, MySQL absent of
    /// binlog tracking) choose [`crate::NoCheckpoint`] and return `None`.
    type Checkpoint: Checkpoint;
    /// Subql backend whose [`Value`] shape this connector produces.
    type Backend: Backend;

    /// Run the SQL and decode one scalar using the expected [`BuiltinKind`].
    ///
    /// `sql` is exactly the string the plan rendered for re-execution and
    /// returned via [`Tier::Scalar`](crate::Tier::Scalar) at
    /// registration, with its projection already aliased. `kind` is the
    /// plan's decode hint. Impls may use it to pick the right diesel
    /// `QueryableByName` row, sqlx `Row::try_get` slot, etc., or ignore it
    /// and inspect the runtime row shape.
    ///
    /// The returned tuple is `(value, Option<checkpoint>)`. The checkpoint
    /// is informational for downstream replay layers. Subql does not gate
    /// on it. Which side of the snapshot the position is taken on is
    /// [`Checkpoint`](Self::Checkpoint)'s rule. An empty result set must
    /// return [`Value::Null`] as the value (matches the "set went empty"
    /// semantics of MIN/MAX).
    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kind: BuiltinKind,
        auth: &Self::AuthContext,
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>;

    /// Read one page of `sql`, stopping once the decoded rows reach
    /// `max_bytes`.
    ///
    /// Stateless on purpose. A result that can be resumed carries a key, and
    /// the caller renders the resume predicate into `sql` itself
    /// (`... AND (k) > (last seen) ORDER BY k`), so this needs no cursor, holds
    /// no transaction between pages, and each page is its own short read. That
    /// is what keeps a large result from pinning a connection: the price is
    /// that successive pages see successive states, which the caller
    /// reconciles against the change stream using
    /// [`Snapshot::checkpoint`].
    ///
    /// Which side of the snapshot the position is taken on is
    /// [`Checkpoint`](Self::Checkpoint)'s rule.
    ///
    /// Stop at the first row that would take the page past `max_bytes`, and
    /// report [`RowPage::more`] as whether the result had further rows. A page
    /// always carries at least one row when the result is non-empty, even if
    /// that row alone exceeds the budget, since returning nothing would make
    /// no progress and the caller would ask again forever.
    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        max_bytes: usize,
        auth: &Self::AuthContext,
    ) -> Result<Snapshot<RowPage<Self::Backend>, Self::Checkpoint>, Self::Error>;

    /// Open a cursor over `sql` inside one read-only repeatable-read
    /// transaction, for a result with no key to resume from.
    ///
    /// A `DISTINCT`, a set operation, or a join with a computed projection has
    /// nothing to seek on, so its pages can only describe one instant if they
    /// all come from one snapshot. That costs a transaction and a borrowed
    /// connection until [`close_cursor`](Self::close_cursor), which is why it
    /// is not the path a keyed result takes.
    ///
    /// The snapshot opens when the cursor is declared, so
    /// [`Checkpoint`](Self::Checkpoint)'s rule puts the position before that.
    ///
    /// The default refuses: a connector over a source with no cursors is
    /// honest to say so, and the refusal names what the caller loses.
    fn open_cursor(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        auth: &Self::AuthContext,
    ) -> Result<CursorId, CursorError<Self::Error>> {
        let _ = (query, auth);
        Err(CursorError::Unsupported)
    }

    /// Read the next page from an open cursor, under the same budget rule as
    /// [`read_page`](Self::read_page).
    ///
    /// Every page carries the same [`Snapshot::checkpoint`], because every
    /// page is the same snapshot.
    ///
    /// A cursor is serial, so an implementation MUST report a concurrent read
    /// as [`CursorError::Busy`] rather than queueing behind the first: waiting
    /// on a resource that cannot be shared hides the caller's own bug. On
    /// failure the cursor's server-side state is unknown, so an implementation
    /// MUST drop it, after which this and [`close_cursor`](Self::close_cursor)
    /// behave as they do for a cursor that was never opened. Both are pinned by
    /// tests against every connector that implements them.
    fn fetch_cursor(
        &self,
        cursor: CursorId,
        max_bytes: usize,
    ) -> Result<Snapshot<RowPage<Self::Backend>, Self::Checkpoint>, CursorError<Self::Error>> {
        let _ = (cursor, max_bytes);
        Err(CursorError::Unsupported)
    }

    /// Close a cursor and commit its transaction, releasing the connection.
    ///
    /// Idempotent: closing an already-closed cursor succeeds, so an abandoned
    /// read cannot leak a transaction through a double close.
    fn close_cursor(&self, cursor: CursorId) -> Result<(), CursorError<Self::Error>> {
        let _ = cursor;
        Err(CursorError::Unsupported)
    }

    /// Run one bound multi-column scalar seed query and decode each column by
    /// its [`BuiltinKind`].
    ///
    /// Bootstraps or re-seeds an in-process aggregate accumulator from
    /// [`Served::aggregate_bootstrap`](crate::Served::aggregate_bootstrap):
    /// run [`AggregateBootstrap::query`](crate::AggregateBootstrap::query), typing each
    /// column by [`AggregateBootstrap::kinds`](crate::AggregateBootstrap::kinds),
    /// then feed the returned row to
    /// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall).
    /// `query` returns exactly one row (aggregate queries always do, yielding
    /// the empty-aggregate row over an empty table). Run the components in
    /// the same read-only repeatable-read transaction
    /// [`execute_scalar`](Self::execute_scalar) uses so they share one
    /// snapshot. The single returned checkpoint is the transaction's, taken on
    /// the side [`Checkpoint`](Self::Checkpoint) requires.
    ///
    /// The default rejects the seed with [`ScalarRowError::Unsupported`] so
    /// existing external impls keep compiling. The shipped diesel connectors
    /// override it for the full aggregate family.
    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kinds: &[BuiltinKind],
        auth: &Self::AuthContext,
    ) -> Result<
        (
            alloc::vec::Vec<Value<Self::Backend>>,
            Option<Self::Checkpoint>,
        ),
        ScalarRowError<Self::Error>,
    > {
        let _ = (query, kinds, auth);
        Err(ScalarRowError::Unsupported)
    }
}

/// Transaction-scoped setup a connector runs before the caller's read SQL.
///
/// The shipped diesel connectors carry this as their
/// [`Connector::AuthContext`]. Before each read, the connector runs every
/// statement here in order, inside the transaction that serves the read, so a
/// per-tier statement timeout or a per-viewer identity takes hold for that read
/// and no other. The list is borrowed from the value the caller already builds
/// per read, so a statement that varies per call costs no allocation the caller
/// was not already making.
///
/// The bound belongs on a connector's own generic parameter, never on
/// [`Connector::AuthContext`] itself: a third-party connector may carry an auth
/// value that runs no SQL setup at all.
pub trait SessionSetup {
    /// Statements to run, in order, at the start of each read transaction.
    fn setup_statements(&self) -> &[alloc::string::String];
}

/// The empty setup: the default every shipped connector carries, byte for byte
/// the behaviour before the seam existed.
impl SessionSetup for () {
    fn setup_statements(&self) -> &[alloc::string::String] {
        &[]
    }
}

/// Run each setup statement in order on `conn`. Shared by every sync
/// diesel-backed connector, called inside the transaction that serves the read
/// and before the caller's SQL.
///
/// Each statement is arbitrary SQL the caller supplies (e.g. `SET LOCAL ROLE`,
/// `set_config('request.jwt', ...)`). The typed DSL cannot express these, so
/// `sql_query` is correct here.
#[cfg(feature = "executor-diesel")]
fn run_setup_statements<C>(conn: &mut C, statements: &[alloc::string::String]) -> QueryResult<()>
where
    C: Connection,
    for<'q> SqlQuery: diesel::query_dsl::methods::ExecuteDsl<C, C::Backend>,
{
    for statement in statements {
        sql_query(statement.as_str()).execute(conn)?;
    }
    Ok(())
}

/// Error returned by [`AutoResolvingEngine::resolve`].
///
/// [`AutoResolvingEngine::resolve`]: super::AutoResolvingEngine::resolve
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ReExecError<E> {
    /// The core engine could not dispatch the event (e.g. unknown table id,
    /// missing required row image).
    #[error("dispatch failed: {0}")]
    Dispatch(#[from] DispatchError),
    /// The [`Connector`] failed to execute the re-execution SQL for
    /// `subscription`. The whole batch is aborted. The id names the read that
    /// failed, so a caller whose read fails deterministically (a statement
    /// timeout on a query that always exceeds its budget) can end that one
    /// subscription rather than retrying the same failure forever.
    #[error("connector failed for subscription {subscription}: {error}")]
    Connector {
        /// The subscription whose triggered read failed.
        subscription: crate::SubscriptionId,
        /// The connector's error.
        error: E,
    },
    /// The database result does not match the subscription it was read for.
    #[error("install failed: {0}")]
    Install(#[from] crate::InstallError),
    /// A grouped aggregate result does not match its maintained state.
    #[error("aggregate install failed: {0}")]
    AggregateInstall(#[from] crate::AggregateInstallError),
    /// A cursor read failed, or the connector holds no cursors and so cannot
    /// serve a captured query that has to be re-read whole. Kept apart from
    /// [`Self::Connector`] because "this connector cannot do that" is a
    /// configuration answer and a failed read is a retry answer.
    #[error("cursor read failed for subscription {subscription}: {error}")]
    Cursor {
        /// The subscription whose cursor read failed.
        subscription: crate::SubscriptionId,
        /// The cursor error.
        error: CursorError<E>,
    },
}

impl<E> ReExecError<E> {
    /// Whether retrying the same read can change the outcome.
    ///
    /// An install failure means the database answer does not match the
    /// subscription, so the same read returns the same mismatch and a
    /// retry can only repeat it. Everything else reports a condition that
    /// can clear, a failed connection or statement above all.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !matches!(self, Self::Install(_) | Self::AggregateInstall(_))
    }
}

/// Error from [`Connector::execute_scalar_row`] and its async peer.
///
/// Distinguishes "this connector has no multi-column seed support" (the
/// default) from a genuine execution failure, so a caller can tell an
/// unsupported connector apart from a database error.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ScalarRowError<E> {
    /// The connector did not override `execute_scalar_row`; the default
    /// rejects the multi-column aggregate seed.
    #[error("this connector does not support multi-column aggregate seeds")]
    Unsupported,
    /// The connector failed while running the seed query.
    #[error("connector failed during multi-column seed: {0}")]
    Connector(E),
}

/// Drain buffered cursor rows into `rows` until the byte budget stops it.
///
/// `true` when the budget was hit with buffered rows left, which is the
/// page's `more`. The budget rule lives here once, shared by the sync and
/// async cursor loops, whose only real difference is the await on the
/// `FETCH` round trip.
#[cfg(any(
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-async-postgres"
))]
pub(super) fn drain_cursor_buffer<B: crate::backend::Backend>(
    leftover: &mut alloc::collections::VecDeque<alloc::vec::Vec<Value<B>>>,
    rows: &mut alloc::vec::Vec<alloc::vec::Vec<Value<B>>>,
    spent: &mut usize,
    max_bytes: usize,
) -> bool {
    while let Some(row) = leftover.front() {
        let cost = RowPage::<B>::row_bytes_of(row);
        if !rows.is_empty() && *spent + cost > max_bytes {
            return true;
        }
        *spent += cost;
        // Total: `front` just answered `Some`.
        if let Some(row) = leftover.pop_front() {
            rows.push(row);
        }
    }
    false
}

#[cfg(test)]
mod bound_query_tests {
    use super::BoundQuery;
    use crate::backend::{Postgres, Value};
    use alloc::sync::Arc;

    #[test]
    fn bound_query_exposes_owned_and_borrowed_views() {
        let query = BoundQuery::<Postgres>::new(
            "SELECT amount FROM orders WHERE id = $1".to_string(),
            vec![Value::Int(7)],
        );

        assert_eq!(query.sql(), "SELECT amount FROM orders WHERE id = $1");
        assert_eq!(query.binds(), &[Value::Int(7)]);
        let read = query.as_read_query();
        assert_eq!(read.sql(), query.sql());
        assert_eq!(read.binds(), query.binds());
    }

    #[test]
    fn bound_query_clone_shares_storage() {
        let query = BoundQuery::<Postgres>::new(
            "SELECT amount FROM orders WHERE id = $1".to_string(),
            vec![Value::Int(7)],
        );
        let cloned = query.clone();

        assert!(Arc::ptr_eq(&query.inner, &cloned.inner));
    }
}
