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

use crate::backend::{Backend, ScalarKind, Value};
use crate::{Checkpoint, DispatchError};
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
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct Snapshot<T, C: Checkpoint> {
    /// The snapshot value (a scalar [`Value`] or a `Vec<Vec<Value<_>>>`,
    /// one inner `Vec` per row).
    pub value: T,
    /// The position at which the snapshot was read, when known.
    pub checkpoint: Option<C>,
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
    /// PG-aware connectors choose [`crate::PgLsn`] and call
    /// `pg_current_wal_lsn()` inside the same transaction as the read.
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
    /// returned via [`Registered::ReExec`](super::Registered::ReExec) at
    /// registration, with its projection already aliased. `kind` is the
    /// plan's decode hint. Impls may use it to pick the right diesel
    /// `QueryableByName` row, sqlx `Row::try_get` slot, etc., or ignore it
    /// and inspect the runtime row shape.
    ///
    /// The returned tuple is `(value, Option<checkpoint>)`. The checkpoint
    /// is informational for downstream replay layers. Subql does not gate
    /// on it. An empty result set must return [`Value::Null`] as the
    /// value (matches the "set went empty" semantics of MIN/MAX).
    fn execute_scalar(
        &self,
        sql: &str,
        kind: ScalarKind,
        auth: &Self::AuthContext,
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>;

    /// Run `sql` as a row-returning query and decode every row into a
    /// column-ordered `Vec<Value<Self::Backend>>`.
    ///
    /// Used by the auto-resolving engine's `snapshot` method to bootstrap
    /// a subscription, and by total single-table row re-execution once
    /// that lands (see `MILESTONES.md`). Impls should open a read-only
    /// repeatable-read transaction so the rows and the returned
    /// [`Snapshot::checkpoint`] agree on a single point in the source
    /// stream.
    fn execute_rows(
        &self,
        sql: &str,
        auth: &Self::AuthContext,
    ) -> Result<
        Snapshot<alloc::vec::Vec<alloc::vec::Vec<Value<Self::Backend>>>, Self::Checkpoint>,
        Self::Error,
    >;
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
pub struct DieselConnector<C: Connection, B: DieselBackend> {
    conn: RefCell<C>,
    _backend: core::marker::PhantomData<fn() -> B>,
}

#[cfg(feature = "executor-diesel")]
impl<C: Connection, B: DieselBackend> DieselConnector<C, B> {
    /// Wrap an owned connection. The connector takes exclusive ownership
    /// and serializes access through interior mutability.
    pub const fn new(conn: C) -> Self {
        Self {
            conn: RefCell::new(conn),
            _backend: core::marker::PhantomData,
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

/// Route the projected column through the `Nullable<BigInt|Double|Text>`
/// row shape that matches `kind`, then lift into [`Value<B>`].
///
/// Aggregate-only column kinds ([`ScalarKind::Int`] / [`ScalarKind::Float`])
/// map to the numeric rows; every other kind reads through the `Text`
/// row. Decimals are carried as text through this path so precision is
/// not lost through `f64`.
#[cfg(feature = "executor-diesel")]
fn load_scalar<C, B>(conn: &mut C, sql: &str, kind: ScalarKind) -> QueryResult<Value<B>>
where
    B: DieselBackend,
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    let value = match kind {
        ScalarKind::Int => sql_query(sql)
            .get_result::<IntRow>(conn)?
            .v
            .map_or(Value::Null, B::value_from_i64),
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

#[cfg(feature = "executor-diesel")]
impl<C, B> Connector for DieselConnector<C, B>
where
    C: Connection,
    B: DieselBackend,
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    type AuthContext = ();
    type Error = diesel::result::Error;
    /// Backend-agnostic v1 default: this connector does not read the
    /// underlying source's position. PG-aware variants
    /// (`PgDieselConnector`) override this to `PgLsn` and read
    /// `pg_current_wal_lsn()` inside the snapshot transaction.
    type Checkpoint = crate::NoCheckpoint;
    type Backend = B;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: ScalarKind,
        _auth: &(),
    ) -> Result<(Value<B>, Option<Self::Checkpoint>), Self::Error> {
        let value = load_scalar::<_, B>(&mut self.conn.borrow_mut(), sql, kind)?;
        Ok((value, None))
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<Snapshot<alloc::vec::Vec<alloc::vec::Vec<Value<B>>>, Self::Checkpoint>, Self::Error>
    {
        // Row-set decoding through diesel's `sql_query` requires the
        // caller to know the schema at compile time (each column wants
        // its own typed accessor). The generic row-decoding path lands
        // with the total reexec feature (tracked in `MILESTONES.md`).
        // For now this method is wired up as a panic so any caller that
        // opts into it gets a clear signal that it is not yet
        // implemented for the generic `DieselConnector`.
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

/// Sync [`Connector`] backed by a diesel `PgConnection` that anchors
/// every read to a PostgreSQL WAL position.
///
/// On each `execute_scalar` (and future `execute_rows`) call the connector
/// opens a `READ ONLY REPEATABLE READ` transaction, queries
/// `pg_current_wal_lsn()` alongside the user's SQL, and returns the
/// resulting [`Value<crate::backend::Postgres>`] together with the
/// parsed [`crate::PgLsn`]. The user
/// query and the LSN observe the same MVCC snapshot, so downstream replay
/// layers (an oplog, a client cursor) can chain WAL events onto the
/// snapshot at exactly the position the snapshot was taken.
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
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: ScalarKind,
        _auth: &(),
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            // Pin the transaction's MVCC snapshot so the user query and
            // the LSN agree on a single point in the WAL.
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            let value = load_scalar::<_, Self::Backend>(conn, sql, kind)?;
            let lsn = read_current_lsn(conn)?;
            Ok((value, lsn))
        })
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<
        Snapshot<alloc::vec::Vec<alloc::vec::Vec<Value<Self::Backend>>>, Self::Checkpoint>,
        Self::Error,
    > {
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
// MysqlDieselConnector: binlog-position-aware sync impl behind
// `executor-diesel-mysql`.
// ---------------------------------------------------------------------------

/// Sync [`Connector`] backed by a diesel `MysqlConnection` that anchors every
/// read to a MySQL binary-log position.
///
/// On each `execute_scalar` call the connector runs the user's SQL and then
/// reads `performance_schema.log_status` inside one transaction, returning the
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
/// snapshot, so the anchor is a best-effort "at or after the read" marker -
/// adequate for chaining binlog events onto the snapshot, but not a strict
/// MVCC-consistent position. Returns `None` for the checkpoint when binary
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
pub struct MysqlDieselConnector {
    conn: RefCell<diesel::MysqlConnection>,
}

#[cfg(feature = "executor-diesel-mysql")]
impl MysqlDieselConnector {
    /// Wrap an owned [`MysqlConnection`](diesel::MysqlConnection). The connector
    /// takes exclusive ownership and serializes access through interior
    /// mutability.
    #[must_use]
    pub const fn new(conn: diesel::MysqlConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
        }
    }
}

#[cfg(feature = "executor-diesel-mysql")]
#[derive(diesel::QueryableByName)]
struct LogStatusRow {
    #[diesel(sql_type = Nullable<Text>, column_name = "file")]
    file: Option<String>,
    #[diesel(
        sql_type = Nullable<diesel::sql_types::Unsigned<BigInt>>,
        column_name = "position"
    )]
    position: Option<u64>,
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
impl Connector for MysqlDieselConnector {
    type AuthContext = ();
    type Error = diesel::result::Error;
    type Checkpoint = crate::MysqlBinlogPos;
    type Backend = crate::backend::MySql;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: ScalarKind,
        _auth: &(),
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            let value = load_scalar::<_, Self::Backend>(conn, sql, kind)?;
            let pos = read_binlog_pos(conn);
            Ok((value, pos))
        })
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<
        Snapshot<alloc::vec::Vec<alloc::vec::Vec<Value<Self::Backend>>>, Self::Checkpoint>,
        Self::Error,
    > {
        // Same restriction as the other diesel connectors: generic row
        // decoding is deferred to the total row reexec feature.
        #[allow(clippy::unimplemented)]
        {
            unimplemented!(
                "MysqlDieselConnector::execute_rows is reserved for total row reexec; \
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
/// [`PgR2D2Error::Pool`]. The transaction's diesel errors surface as
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
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        sql: &str,
        kind: ScalarKind,
        _auth: &(),
    ) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error> {
        let mut conn = self.pool.get()?;
        let result: Result<(Value<Self::Backend>, Option<crate::PgLsn>), diesel::result::Error> =
            diesel::connection::Connection::transaction(&mut *conn, |conn| {
                sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                    .execute(conn)?;
                let value = load_scalar::<_, Self::Backend>(conn, sql, kind)?;
                let lsn = read_current_lsn(conn)?;
                Ok((value, lsn))
            });
        Ok(result?)
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<
        Snapshot<alloc::vec::Vec<alloc::vec::Vec<Value<Self::Backend>>>, Self::Checkpoint>,
        Self::Error,
    > {
        #[allow(clippy::unimplemented)]
        {
            unimplemented!(
                "PgR2D2DieselConnector::execute_rows is reserved for total row reexec; \
                 use the scalar path or supply a custom Connector impl"
            )
        }
    }
}
