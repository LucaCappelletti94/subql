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
// Only the diesel-backed readers route a column by its kind.
#[cfg(feature = "executor-diesel")]
use crate::backend::ScalarKind;
use crate::{Checkpoint, DispatchError};
use thiserror::Error;

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

#[cfg(feature = "executor-diesel")]
use alloc::string::String;
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

    /// Run the re-execution SQL and decode a single scalar value with the
    /// expected [`ScalarKind`], optionally reporting the position at which
    /// the read was taken.
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
        sql: &str,
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
        sql: &str,
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
        sql: &str,
        auth: &Self::AuthContext,
    ) -> Result<CursorId, CursorError<Self::Error>> {
        let _ = (sql, auth);
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

    /// Run a single-row, multi-column scalar seed query and decode each
    /// column by the matching [`ScalarKind`].
    ///
    /// Bootstraps or re-seeds an in-process aggregate accumulator from
    /// [`Served::aggregate_bootstrap`](crate::Served::aggregate_bootstrap):
    /// run [`AggregateBootstrap::sql`](crate::AggregateBootstrap::sql), typing
    /// each column by [`AggregateBootstrap::kinds`](crate::AggregateBootstrap::kinds),
    /// then feed the returned row to
    /// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall).
    /// `sql` returns exactly one row (aggregate queries always do, yielding
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
        sql: &str,
        kinds: &[BuiltinKind],
        auth: &Self::AuthContext,
    ) -> Result<
        (
            alloc::vec::Vec<Value<Self::Backend>>,
            Option<Self::Checkpoint>,
        ),
        ScalarRowError<Self::Error>,
    > {
        let _ = (sql, kinds, auth);
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
    /// batch is aborted. The caller is expected to retry it.
    #[error("connector failed: {0}")]
    Connector(E),
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
    #[error("cursor read failed: {0}")]
    Cursor(CursorError<E>),
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

// ---------------------------------------------------------------------------
// DieselBackend: subql-backend selector for the diesel connectors.
// ---------------------------------------------------------------------------

/// Bridge trait: names the subql [`Backend`] a diesel-backed connector
/// produces [`Value`]s for, and constructs those [`Value`]s from the
/// scalar wire shapes diesel's `sql_query` hands back.
///
/// Implemented for the three shipped backends ([`crate::backend::Postgres`],
/// [`crate::backend::MySql`], [`crate::backend::SQLite`]), each of which
/// spells [`Backend::Int`] / [`Backend::Float`] / [`Backend::String`] as
/// `i64` / `f64` / `String` respectively; the constructors are trivial and
/// let the generic [`DieselConnector<C, B>`] stay backend-agnostic at the
/// type level while producing correctly typed [`Value<B>`]s.
#[cfg(feature = "executor-diesel")]
pub trait DieselBackend: crate::backend::Backend + Sized {
    /// Wrap an `i64` decoded via `Nullable<BigInt>` as [`Value::Int`].
    fn value_from_i64(x: i64) -> Value<Self>;
    /// Wrap an `f64` decoded via `Nullable<Double>` as [`Value::Float`].
    fn value_from_f64(x: f64) -> Value<Self>;
    /// Wrap a `String` decoded via `Nullable<Text>` as [`Value::String`].
    fn value_from_string(s: String) -> Value<Self>;
    /// SQL type name to cast a `SUM` component to double precision in this
    /// backend's dialect, so `SUM`'s promoted integer type decodes as `f64`
    /// for the accumulator. Defaults to `DOUBLE PRECISION` (PostgreSQL, and
    /// SQLite via `REAL` affinity); MySQL overrides to `DOUBLE`.
    #[must_use]
    fn double_cast_type() -> &'static str {
        "DOUBLE PRECISION"
    }
    /// SQL type name to cast an integer scalar read to eight bytes, so an
    /// aggregate over a column narrower than `bigint` decodes through
    /// `Nullable<BigInt>`. Defaults to `BIGINT` (PostgreSQL, and SQLite via
    /// `INTEGER` affinity); MySQL overrides to `SIGNED`, its `CAST` spelling.
    #[must_use]
    fn int_cast_type() -> &'static str {
        "BIGINT"
    }
}

#[cfg(feature = "executor-diesel")]
impl DieselBackend for crate::backend::Postgres {
    fn value_from_i64(x: i64) -> Value<Self> {
        Value::Int(x)
    }
    fn value_from_f64(x: f64) -> Value<Self> {
        Value::Float(x)
    }
    fn value_from_string(s: String) -> Value<Self> {
        Value::String(s)
    }
}

#[cfg(feature = "executor-diesel")]
impl DieselBackend for crate::backend::MySql {
    fn value_from_i64(x: i64) -> Value<Self> {
        Value::Int(x)
    }
    fn value_from_f64(x: f64) -> Value<Self> {
        Value::Float(x)
    }
    fn value_from_string(s: String) -> Value<Self> {
        Value::String(s)
    }
    fn double_cast_type() -> &'static str {
        "DOUBLE"
    }
    fn int_cast_type() -> &'static str {
        "SIGNED"
    }
}

#[cfg(feature = "executor-diesel")]
impl DieselBackend for crate::backend::SQLite {
    fn value_from_i64(x: i64) -> Value<Self> {
        Value::Int(x)
    }
    fn value_from_f64(x: f64) -> Value<Self> {
        Value::Float(x)
    }
    fn value_from_string(s: String) -> Value<Self> {
        Value::String(s)
    }
}

// ---------------------------------------------------------------------------
// DieselConnector: opt-in sync impl shipped behind `executor-diesel`.
// ---------------------------------------------------------------------------

/// Sync [`Connector`] backed by a single diesel [`Connection`].
///
/// Holds the connection in a [`RefCell`] for the interior-mutability the
/// trait's `&self` requires. Not `Send`/`Sync` (diesel connections are
/// not `Send`). For multi-threaded use, either keep the connector
/// thread-local or implement [`Connector`] yourself over a connection
/// pool (`r2d2::Pool<ConnectionManager<C>>`, `deadpool`, `bb8`).
///
/// Type parameters:
/// * `C: Connection` - any diesel connection.
/// * `B: DieselBackend` - the subql [`Backend`] whose [`Value`] shape the
///   connector produces. Pick to match the diesel connection's backend
///   ([`crate::backend::Postgres`] for `PgConnection`,
///   [`crate::backend::SQLite`] for `SqliteConnection`,
///   [`crate::backend::MySql`] for `MysqlConnection`).
///
/// Bounds on the [`Connector`] impl: an HRTB on [`sql_query`] so the
/// nullable scalar rows (`Nullable<BigInt>`, `Nullable<Double>`,
/// `Nullable<Text>`) decode. PostgreSQL, MySQL, and SQLite all satisfy
/// these.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure
/// (network drop, statement error, decoding mismatch).
#[cfg(feature = "executor-diesel")]
pub struct DieselConnector<C: Connection, B: DieselBackend, S = ()> {
    conn: RefCell<C>,
    _backend: core::marker::PhantomData<fn() -> B>,
    _setup: core::marker::PhantomData<fn() -> S>,
}

#[cfg(feature = "executor-diesel")]
impl<C: Connection, B: DieselBackend> DieselConnector<C, B> {
    /// Wrap an owned connection with no session setup. The connector takes
    /// exclusive ownership and serializes access through interior mutability.
    pub const fn new(conn: C) -> Self {
        Self {
            conn: RefCell::new(conn),
            _backend: core::marker::PhantomData,
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel")]
impl<C: Connection, B: DieselBackend, S: SessionSetup> DieselConnector<C, B, S> {
    /// Wrap an owned connection whose reads run the setup statements carried by
    /// the per-read [`SessionSetup`] value `S`. `S` is named by the return
    /// type, since the value itself arrives per read rather than being stored.
    pub const fn with_session_setup(conn: C) -> Self {
        Self {
            conn: RefCell::new(conn),
            _backend: core::marker::PhantomData,
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
pub struct IntRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    pub v: Option<i64>,
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
pub struct FloatRow {
    #[diesel(sql_type = Nullable<Double>)]
    pub v: Option<f64>,
}

#[cfg(feature = "executor-diesel")]
#[derive(diesel::QueryableByName)]
pub struct TextRow {
    #[diesel(sql_type = Nullable<Text>)]
    pub v: Option<String>,
}

/// Route the projected column through the `Nullable<BigInt|Double|Text>`
/// row shape that matches `kind`, then lift into [`Value<B>`].
///
/// Aggregate-only column kinds ([`ScalarKind::Int`] / [`ScalarKind::Float`])
/// map to the numeric rows; every other kind reads through the `Text`
/// row. Decimals are carried as text through this path so precision is
/// not lost through `f64`.
#[cfg(feature = "executor-diesel")]
fn load_scalar<C, B>(conn: &mut C, sql: &str, kind: BuiltinKind) -> QueryResult<Value<B>>
where
    B: DieselBackend,
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    let value = match kind {
        // Raw SQL of necessity: the statement is the subscription's own text,
        // so no `table!` exists to type it. Wrapped as a scalar subquery so
        // the read produces what the row decodes: eight bytes, under the
        // alias `v`, whatever width and name the inner projection has.
        ScalarKind::Int => {
            let widened = alloc::format!(
                "SELECT CAST(({sql}) AS {cast}) AS v",
                cast = B::int_cast_type()
            );
            sql_query(widened)
                .get_result::<IntRow>(conn)?
                .v
                .map_or(Value::Null, B::value_from_i64)
        }
        ScalarKind::Float => sql_query(sql)
            .get_result::<FloatRow>(conn)?
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
            .get_result::<TextRow>(conn)?
            .v
            .map_or(Value::Null, B::value_from_string),
    };
    Ok(value)
}

/// Decode a single-row, multi-column aggregate seed by running one aliased
/// subquery per component through [`load_scalar`], reusing its per-kind
/// decode. `Float` components (`SUM` / `SUM(x*x)`) are cast to the backend's
/// double type because `SUM` promotes the source integer type; `Int`
/// components (counts) are read as-is. Column `i` is projected as `ci` by the
/// bootstrap SQL. Callers run this inside their own transaction so the
/// components share one snapshot.
#[cfg(feature = "executor-diesel")]
fn load_scalar_row<C, B>(
    conn: &mut C,
    sql: &str,
    kinds: &[BuiltinKind],
) -> QueryResult<alloc::vec::Vec<Value<B>>>
where
    B: DieselBackend,
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    let mut out = alloc::vec::Vec::with_capacity(kinds.len());
    for (i, kind) in kinds.iter().enumerate() {
        let wrapped = if matches!(kind, ScalarKind::Float) {
            alloc::format!(
                "SELECT CAST(c{i} AS {cast}) AS v FROM ({sql}) AS agg_seed",
                cast = B::double_cast_type()
            )
        } else {
            alloc::format!("SELECT c{i} AS v FROM ({sql}) AS agg_seed")
        };
        out.push(load_scalar::<C, B>(conn, &wrapped, *kind)?);
    }
    Ok(out)
}

#[cfg(feature = "executor-diesel")]
/// Reading a row needs a decoder for the connection's backend, so this impl
/// asks for one. Every backend subql speaks has it behind that backend's own
/// feature (`diesel-typed` for Postgres, `diesel-typed-sqlite`,
/// `diesel-typed-mysql`, `executor-diesel-postgres`, `executor-diesel-mysql`),
/// and a connector that cannot decode a row cannot honestly claim to read one.
impl<C, B, S> Connector for DieselConnector<C, B, S>
where
    C: Connection + diesel::connection::LoadConnection<diesel::connection::DefaultLoadingMode>,
    C::Backend: crate::diesel_decode::RowFieldDecode + diesel::backend::DieselReserveSpecialization,
    B: DieselBackend + crate::diesel_decode::SpellCanonical,
    S: SessionSetup,
    for<'q> SqlQuery: LoadQuery<'q, C, IntRow>
        + LoadQuery<'q, C, FloatRow>
        + LoadQuery<'q, C, TextRow>
        + LoadQuery<'q, C, crate::diesel_decode::DynamicRow<B>>
        + diesel::query_dsl::methods::ExecuteDsl<C, C::Backend>,
{
    type AuthContext = S;
    type Error = diesel::result::Error;
    /// Backend-agnostic v1 default: this connector does not read the
    /// underlying source's position. PG-aware variants
    /// (`PgDieselConnector`) override this to `PgLsn` and read
    /// `pg_current_wal_lsn()` before the snapshot transaction.
    type Checkpoint = crate::NoCheckpoint;
    type Backend = B;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: BuiltinKind,
        auth: &S,
    ) -> Result<(Value<B>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        let setup = auth.setup_statements();
        // Decision 5: this path opens no transaction today, so a caller
        // supplying nothing gets that behaviour byte for byte, and one
        // supplying statements gets a real transaction so the setup takes hold.
        let value = if setup.is_empty() {
            load_scalar::<_, B>(&mut *conn, sql, kind)?
        } else {
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                run_setup_statements(conn, setup)?;
                load_scalar::<_, B>(conn, sql, kind)
            })?
        };
        Ok((value, None))
    }

    fn read_page(
        &self,
        sql: &str,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<B>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.conn.borrow_mut();
        let setup = auth.setup_statements();
        // Decision 5, as in `execute_scalar`: a transaction only when the
        // caller supplied statements.
        let value = if setup.is_empty() {
            load_page::<_, B>(&mut *conn, sql, max_bytes)?
        } else {
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                run_setup_statements(conn, setup)?;
                load_page::<_, B>(conn, sql, max_bytes)
            })?
        };
        Ok(Snapshot {
            value,
            checkpoint: None,
        })
    }

    fn execute_scalar_row(
        &self,
        sql: &str,
        kinds: &[BuiltinKind],
        auth: &S,
    ) -> Result<(alloc::vec::Vec<Value<B>>, Option<Self::Checkpoint>), ScalarRowError<Self::Error>>
    {
        let mut conn = self.conn.borrow_mut();
        // Read every component in one transaction so a variance seed's sum,
        // sum-of-squares, and count come from a single snapshot. This basic
        // connector uses the connection's default isolation; the LSN-aware
        // `PgDieselConnector` additionally pins REPEATABLE READ.
        let values = diesel::connection::Connection::transaction(&mut *conn, |conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            load_scalar_row::<_, B>(conn, sql, kinds)
        })
        .map_err(ScalarRowError::Connector)?;
        Ok((values, None))
    }
}

// ---------------------------------------------------------------------------
// PgDieselConnector: LSN-aware sync impl behind `executor-diesel-postgres`.
// ---------------------------------------------------------------------------

/// Sync [`Connector`] backed by a diesel `PgConnection` that anchors
/// every read to a PostgreSQL WAL position.
///
/// Each read takes the WAL position first, then opens a `READ ONLY REPEATABLE
/// READ` transaction for the user's SQL, and returns the resulting
/// [`Value<crate::backend::Postgres>`] with the parsed [`crate::PgLsn`].
///
/// The order is the whole point. `pg_current_wal_lsn()` is not bound to the
/// transaction snapshot: inside one repeatable-read transaction it advances
/// when another connection commits, which is measured rather than assumed. So a
/// position read after the query can sit ahead of the snapshot, and a replay
/// layer starting there would skip a transaction that committed before the
/// position yet was invisible to the snapshot. Reading first puts the position
/// at or behind the snapshot instead, so a replay re-delivers a few changes the
/// snapshot already holds, which keyed application absorbs.
///
/// Holds the connection in a [`RefCell`] for the interior-mutability the
/// trait's `&self` requires. Not `Send`/`Sync`. For multi-threaded use,
/// either keep the connector thread-local or implement [`Connector`]
/// yourself over a connection pool.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure
/// (network drop, statement error, malformed LSN response).
#[cfg(feature = "executor-diesel-postgres")]
pub struct PgDieselConnector<S = ()> {
    conn: RefCell<diesel::PgConnection>,
    _setup: core::marker::PhantomData<fn() -> S>,
}

#[cfg(feature = "executor-diesel-postgres")]
impl PgDieselConnector {
    /// Wrap an owned [`PgConnection`](diesel::PgConnection) with no session
    /// setup. The connector takes exclusive ownership and serializes access
    /// through interior mutability.
    #[must_use]
    pub const fn new(conn: diesel::PgConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel-postgres")]
impl<S: SessionSetup> PgDieselConnector<S> {
    /// Wrap an owned [`PgConnection`](diesel::PgConnection) whose reads run the
    /// setup statements carried by the per-read [`SessionSetup`] value `S`.
    #[must_use]
    pub const fn with_session_setup(conn: diesel::PgConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
#[derive(diesel::QueryableByName)]
pub struct PgLsnRow {
    #[diesel(sql_type = Text)]
    pub lsn: String,
}

#[cfg(feature = "executor-diesel-postgres")]
fn read_current_lsn(conn: &mut diesel::PgConnection) -> diesel::QueryResult<Option<crate::PgLsn>> {
    let row: PgLsnRow = sql_query("SELECT pg_current_wal_lsn()::text AS lsn").get_result(conn)?;
    Ok(crate::PgLsn::parse(&row.lsn))
}

#[cfg(feature = "executor-diesel-postgres")]
impl<S: SessionSetup> Connector for PgDieselConnector<S> {
    type AuthContext = S;
    type Error = diesel::result::Error;
    type Checkpoint = crate::PgLsn;
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: BuiltinKind,
        auth: &S,
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        // The position is read before the snapshot exists. A caller replays the
        // change stream from it, so it must sit at or behind the snapshot:
        // behind re-delivers a few changes the snapshot already holds, which
        // keyed application absorbs, while ahead silently loses a transaction
        // that committed after the position and is invisible to the snapshot.
        // `pg_current_wal_lsn()` is not snapshot-bound, measured rather than
        // assumed: inside one repeatable-read transaction it advances when
        // another connection commits.
        let lsn = read_current_lsn(&mut conn)?;
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_scalar::<_, Self::Backend>(conn, sql, kind)?;
            Ok((value, lsn))
        })
    }

    fn read_page(
        &self,
        sql: &str,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<crate::backend::Postgres>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.conn.borrow_mut();
        // The page and the LSN share one snapshot, so a caller reconciling
        // pages against the change stream knows exactly where this one sits.
        // The position is read before the snapshot exists. A caller replays the
        // change stream from it, so it must sit at or behind the snapshot:
        // behind re-delivers a few changes the snapshot already holds, which
        // keyed application absorbs, while ahead silently loses a transaction
        // that committed after the position and is invisible to the snapshot.
        // `pg_current_wal_lsn()` is not snapshot-bound, measured rather than
        // assumed: inside one repeatable-read transaction it advances when
        // another connection commits.
        let lsn = read_current_lsn(&mut conn)?;
        conn.transaction(|conn| {
            diesel::sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                .execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_page::<_, crate::backend::Postgres>(conn, sql, max_bytes)?;
            Ok(Snapshot {
                value,
                checkpoint: lsn,
            })
        })
    }

    fn execute_scalar_row(
        &self,
        sql: &str,
        kinds: &[BuiltinKind],
        auth: &S,
    ) -> Result<
        (
            alloc::vec::Vec<Value<Self::Backend>>,
            Option<Self::Checkpoint>,
        ),
        ScalarRowError<Self::Error>,
    > {
        let mut conn = self.conn.borrow_mut();
        // The position is read before the snapshot exists. A caller replays the
        // change stream from it, so it must sit at or behind the snapshot:
        // behind re-delivers a few changes the snapshot already holds, which
        // keyed application absorbs, while ahead silently loses a transaction
        // that committed after the position and is invisible to the snapshot.
        // `pg_current_wal_lsn()` is not snapshot-bound, measured rather than
        // assumed: inside one repeatable-read transaction it advances when
        // another connection commits.
        let lsn = read_current_lsn(&mut conn).map_err(ScalarRowError::Connector)?;
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let values = load_scalar_row::<_, Self::Backend>(conn, sql, kinds)?;
            Ok((values, lsn))
        })
        .map_err(ScalarRowError::Connector)
    }
}

// ---------------------------------------------------------------------------
// MysqlDieselConnector: binlog-position-aware sync impl behind
// `executor-diesel-mysql`.
// ---------------------------------------------------------------------------

/// Sync [`Connector`] backed by a diesel `MysqlConnection` that anchors every
/// read to a MySQL binary-log position.
///
/// On each `execute_scalar` call the connector reads
/// `performance_schema.log_status` and then runs the user's SQL, returning the
/// resulting [`Value<crate::backend::MySql>`] together with the parsed
/// [`crate::MysqlBinlogPos`] (the
/// binlog file's numeric suffix + byte offset).
///
/// `log_status` is used rather than `SHOW MASTER STATUS` because diesel's
/// prepared-statement protocol cannot read result-set metadata for `SHOW`
/// commands ("No metadata exists"). `log_status` is a regular table, so the
/// metadata is present.
///
/// Unlike PostgreSQL's `pg_current_wal_lsn()`, this reports the server's
/// *current* binlog coordinate rather than one tied to the transaction's
/// snapshot, so reading it first makes it an "at or before the read" marker
/// rather than a strict MVCC-consistent position. Returns `None` for the
/// checkpoint when binary
/// logging is disabled (no `log_status` row).
///
/// Holds the connection in a [`RefCell`]; not `Send`/`Sync`. Requires MySQL
/// 8.0.22+ binary logging (`--log-bin`) and `BACKUP_ADMIN` to read
/// `performance_schema.log_status`.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure.
#[cfg(feature = "executor-diesel-mysql")]
pub struct MysqlDieselConnector<S = ()> {
    conn: RefCell<diesel::MysqlConnection>,
    _setup: core::marker::PhantomData<fn() -> S>,
}

#[cfg(feature = "executor-diesel-mysql")]
impl MysqlDieselConnector {
    /// Wrap an owned [`MysqlConnection`](diesel::MysqlConnection) with no
    /// session setup. The connector takes exclusive ownership and serializes
    /// access through interior mutability.
    #[must_use]
    pub const fn new(conn: diesel::MysqlConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(feature = "executor-diesel-mysql")]
impl<S: SessionSetup> MysqlDieselConnector<S> {
    /// Wrap an owned [`MysqlConnection`](diesel::MysqlConnection) whose reads
    /// run the setup statements carried by the per-read [`SessionSetup`] `S`.
    #[must_use]
    pub const fn with_session_setup(conn: diesel::MysqlConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
            _setup: core::marker::PhantomData,
        }
    }
}

#[cfg(any(
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql"
))]
#[derive(diesel::QueryableByName)]
pub struct LogStatusRow {
    #[diesel(sql_type = Nullable<Text>, column_name = "file")]
    pub file: Option<String>,
    #[diesel(
        sql_type = Nullable<diesel::sql_types::Unsigned<BigInt>>,
        column_name = "position"
    )]
    pub position: Option<u64>,
}

/// Read the current binlog coordinate from `performance_schema.log_status`.
///
/// `SHOW MASTER STATUS` returns no result-set metadata over diesel's
/// prepared-statement protocol ("No metadata exists"), so the same `(file,
/// position)` is read from `performance_schema.log_status` (MySQL 8.0.22+,
/// requires the `BACKUP_ADMIN` privilege). Returns `None` when binary logging is
/// off, the table/privilege is unavailable, or the coordinate doesn't fit the
/// compact [`crate::MysqlBinlogPos`]. Best-effort: the checkpoint is
/// informational (subql does not gate on it), so any failure degrades to `None`
/// rather than failing the re-execution.
#[cfg(feature = "executor-diesel-mysql")]
fn read_binlog_pos(conn: &mut diesel::MysqlConnection) -> Option<crate::MysqlBinlogPos> {
    use diesel::result::OptionalExtension;
    const SQL: &str = "SELECT \
        JSON_UNQUOTE(JSON_EXTRACT(LOCAL, '$.binary_log_file')) AS file, \
        CAST(JSON_EXTRACT(LOCAL, '$.binary_log_position') AS UNSIGNED) AS position \
        FROM performance_schema.log_status";
    // Best-effort: degrade to no checkpoint on any read failure (missing
    // privilege, binary logging off, unsupported server version).
    let LogStatusRow {
        file: Some(file),
        position: Some(position),
    } = sql_query(SQL)
        .get_result::<LogStatusRow>(conn)
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

#[cfg(feature = "executor-diesel-mysql")]
impl<S: SessionSetup> Connector for MysqlDieselConnector<S> {
    type AuthContext = S;
    type Error = diesel::result::Error;
    type Checkpoint = crate::MysqlBinlogPos;
    type Backend = crate::backend::MySql;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: BuiltinKind,
        auth: &S,
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        // Position before snapshot, per `Connector::Checkpoint`: the coordinate
        // is the server's current one, so taken after the read it can sit ahead
        // of the snapshot and a replay from there loses a commit.
        let pos = read_binlog_pos(&mut conn);
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_scalar::<_, Self::Backend>(conn, sql, kind)?;
            Ok((value, pos))
        })
    }

    fn read_page(
        &self,
        sql: &str,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<crate::backend::MySql>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.conn.borrow_mut();
        // Position before snapshot, per `Connector::Checkpoint`.
        let pos = read_binlog_pos(&mut conn);
        conn.transaction(|conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_page::<_, crate::backend::MySql>(conn, sql, max_bytes)?;
            Ok(Snapshot {
                value,
                checkpoint: pos,
            })
        })
    }

    fn execute_scalar_row(
        &self,
        sql: &str,
        kinds: &[BuiltinKind],
        auth: &S,
    ) -> Result<
        (
            alloc::vec::Vec<Value<Self::Backend>>,
            Option<Self::Checkpoint>,
        ),
        ScalarRowError<Self::Error>,
    > {
        let mut conn = self.conn.borrow_mut();
        // Position before snapshot, per `Connector::Checkpoint`.
        let pos = read_binlog_pos(&mut conn);
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            let values = load_scalar_row::<_, Self::Backend>(conn, sql, kinds)?;
            Ok((values, pos))
        })
        .map_err(ScalarRowError::Connector)
    }
}

// ---------------------------------------------------------------------------
// PgR2D2DieselConnector: pool-backed PG impl behind `executor-diesel-postgres-r2d2`.
// ---------------------------------------------------------------------------

/// Pool-backed [`Connector`] for PostgreSQL.
///
/// Wraps an `r2d2::Pool` over `ConnectionManager<PgConnection>`, is
/// `Send + Sync`, and reads `pg_current_wal_lsn()` before the user query's
/// transaction for LSN-anchored snapshots.
///
/// Use this connector when the engine dispatches re-executions
/// concurrently (the async engine with `consumers_batch`) or when
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
        sql: &str,
        kind: BuiltinKind,
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
                sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                    .execute(conn)?;
                run_setup_statements(conn, auth.setup_statements())?;
                let value = load_scalar::<_, Self::Backend>(conn, sql, kind)?;
                Ok((value, lsn))
            });
        Ok(result?)
    }

    fn read_page(
        &self,
        sql: &str,
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
            diesel::sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                .execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_page::<_, crate::backend::Postgres>(conn, sql, max_bytes)?;
            Ok(Snapshot {
                value,
                checkpoint: lsn,
            })
        });
        Ok(result?)
    }

    fn execute_scalar_row(
        &self,
        sql: &str,
        kinds: &[BuiltinKind],
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
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let values = load_scalar_row::<_, Self::Backend>(conn, sql, kinds)?;
            Ok((values, lsn))
        });
        result.map_err(|e| ScalarRowError::Connector(e.into()))
    }

    fn open_cursor(&self, sql: &str, auth: &S) -> Result<CursorId, CursorError<Self::Error>> {
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
            diesel::sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                .execute(&mut *conn)?;
            run_setup_statements(&mut *conn, auth.setup_statements())?;
            diesel::sql_query(alloc::format!("DECLARE {name} NO SCROLL CURSOR FOR {sql}"))
                .execute(&mut *conn)?;
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

/// Drain buffered cursor rows into `rows` until the byte budget stops it.
///
/// `true` when the budget was hit with buffered rows left, which is the
/// page's `more`. The budget rule lives here once, shared by the sync and
/// async cursor loops, whose only real difference is the await on the
/// `FETCH` round trip.
#[cfg(any(
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-async"
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
        let page = load_page::<_, crate::backend::Postgres>(
            &mut held.conn,
            &alloc::format!("FETCH FORWARD {batch} FROM {}", held.name),
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

/// Read one page of `sql` off a diesel connection, decoding each row without a
/// compile-time schema and stopping at `max_bytes`.
///
/// The shared body behind every diesel-backed connector's
/// [`Connector::read_page`]. `conn.load` hands back an iterator, so the budget
/// stops the decode rather than trimming an already-materialized vector, and
/// one extra row is pulled to answer [`RowPage::more`] without guessing.
#[cfg(feature = "executor-diesel")]
fn load_page<C, B>(conn: &mut C, sql: &str, max_bytes: usize) -> QueryResult<RowPage<B>>
where
    C: diesel::connection::LoadConnection<diesel::connection::DefaultLoadingMode>,
    C::Backend: crate::diesel_decode::RowFieldDecode + diesel::backend::DieselReserveSpecialization,
    B: crate::diesel_decode::SpellCanonical,
    for<'q> SqlQuery: LoadQuery<'q, C, crate::diesel_decode::DynamicRow<B>>,
{
    let mut columns = alloc::vec::Vec::new();
    let mut rows: alloc::vec::Vec<alloc::vec::Vec<Value<B>>> = alloc::vec::Vec::new();
    let mut spent = 0_usize;
    let mut more = false;

    // Lazy on purpose: the iterator lets the budget stop the decode rather
    // than trim a vector that was already built in full.
    let iter =
        diesel::query_dsl::LoadQuery::<'_, C, crate::diesel_decode::DynamicRow<B>>::internal_load(
            diesel::sql_query(sql),
            conn,
        )?;
    for row in iter {
        let row = row?;
        if columns.is_empty() {
            columns = row.columns;
        }
        // A page always makes progress: the budget stops the row after the
        // first, never the first itself, or an oversized row would stall the
        // read forever.
        let cost = RowPage::<B>::row_bytes_of(&row.values);
        if !rows.is_empty() && spent + cost > max_bytes {
            more = true;
            break;
        }
        spent += cost;
        rows.push(row.values);
    }

    Ok(RowPage {
        columns,
        rows,
        more,
    })
}
