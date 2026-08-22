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
}

#[cfg(feature = "executor-diesel-async-postgres")]
impl PgAsyncDieselConnector {
    /// Wrap a `bb8` pool over `AsyncPgConnection` already configured by the
    /// caller.
    #[must_use]
    pub const fn new(pool: Pool<diesel_async::AsyncPgConnection>) -> Self {
        Self { pool }
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
            conn.transaction::<(Value<Self::Backend>, Option<crate::PgLsn>), diesel::result::Error, _>(
                |c| {
                    async move {
                        // Pin the transaction's MVCC snapshot so the user query
                        // and the LSN agree on a single point in the WAL.
                        sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                            .execute(c)
                            .await?;
                        let value = load_scalar_async::<_, Self::Backend>(c, &sql, kind).await?;
                        let lsn = read_current_lsn_async(c).await?;
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
            conn.transaction::<Snapshot<crate::reexec::RowPage<Self::Backend>, crate::PgLsn>, diesel::result::Error, _>(
                |c| {
                    async move {
                        sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                            .execute(c)
                            .await?;
                        let value = load_page_async::<_, diesel::pg::Pg, crate::backend::Postgres>(c, &sql, max_bytes).await?;
                        let lsn = read_current_lsn_async(c).await?;
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
            conn.transaction::<(Vec<Value<Self::Backend>>, Option<crate::PgLsn>), diesel::result::Error, _>(
                |c| {
                    async move {
                        sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                            .execute(c)
                            .await?;
                        let values =
                            load_scalar_row_async::<_, Self::Backend>(c, &sql, &kinds).await?;
                        let lsn = read_current_lsn_async(c).await?;
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
