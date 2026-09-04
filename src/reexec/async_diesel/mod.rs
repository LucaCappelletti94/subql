#![allow(clippy::manual_async_fn)]
#![allow(clippy::type_complexity)]
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

// The MySQL connector lives in this module; the Postgres one lives in its own
// file and imports what it needs there.
#[cfg(feature = "executor-diesel-async-mysql")]
use super::async_connector::AsyncConnector;
#[cfg(feature = "executor-diesel-async-mysql")]
use super::connector::boxed_mysql_read_query_owned;
#[cfg(feature = "executor-diesel-async-postgres")]
use super::connector::boxed_postgres_read_query_owned;
#[cfg(feature = "executor-diesel-async-mysql")]
use super::connector::LogStatusRow;
use super::connector::{FloatRow, IntRow, ReadQuery, TextRow};
#[cfg(feature = "executor-diesel-async-mysql")]
use super::connector::{ScalarRowError, SessionSetup, Snapshot};
use crate::backend::{Backend, BuiltinKind, Value};
use alloc::vec::Vec;
#[cfg(feature = "executor-diesel-async-mysql")]
use core::future::Future;
use diesel::query_builder::SqlQuery;
use diesel::sql_query;
#[cfg(feature = "executor-diesel-async-mysql")]
use diesel_async::pooled_connection::bb8::Pool;
#[cfg(feature = "executor-diesel-async-mysql")]
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl as _};
use thiserror::Error;

#[cfg(feature = "executor-diesel-async-postgres")]
mod pg_async_diesel_connector;

#[cfg(feature = "executor-diesel-async-postgres")]
pub use pg_async_diesel_connector::PgAsyncDieselConnector;

/// Run each setup statement in order on `conn`. The async peer of the
/// [`run_setup_statements`](super::connector) shared helper: called inside the
/// transaction that serves the read and before the caller's SQL.
///
/// Each statement is arbitrary SQL the caller supplies. The typed DSL cannot
/// express these, so `sql_query` is correct here.
pub(super) async fn run_setup_statements_async<C>(
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

/// Async counterpart of `load_scalar` (module-private there): route the
/// projected column through the `Nullable<BigInt|Double|Text>` row shape
/// that matches `kind`, then lift into [`Value<B>`]. Decimals travel as text
/// so precision is not lost through `f64`.
#[cfg(feature = "executor-diesel-async-postgres")]
pub(super) async fn load_scalar_postgres_async(
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
pub(super) async fn load_scalar_mysql_async(
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
pub(super) async fn load_scalar_row_postgres_async(
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
            crate::backend::Postgres::decode_group_value(
                crate::backend::ValueKind::from(*kind),
                value,
            )
            .unwrap_or(Value::Missing)
        })
        .collect())
}

#[cfg(feature = "executor-diesel-async-mysql")]
pub(super) async fn load_scalar_row_mysql_async(
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
            crate::backend::MySql::decode_group_value(crate::backend::ValueKind::from(*kind), value)
                .unwrap_or(Value::Missing)
        })
        .collect())
}

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
    // `performance_schema.log_status` is a regular table that returns JSON
    // columns; `SHOW MASTER STATUS` lacks result-set metadata in the prepared
    // protocol, so we cannot use the typed DSL here.
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
pub(super) async fn load_page_postgres_async(
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
