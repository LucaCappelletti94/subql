#![allow(clippy::type_complexity)]
//! Binlog-position-aware sync [`Connector`] for MySQL.

// The async connector reads `LogStatusRow` from this module, so the module is
// compiled without the sync feature: everything the sync connector alone needs
// follows it.
#[cfg(feature = "executor-diesel-mysql")]
use super::diesel_connector::{load_page, load_scalar, load_scalar_row};
#[cfg(feature = "executor-diesel-mysql")]
use super::{
    run_setup_statements, Connector, ReadQuery, RowPage, ScalarRowError, SessionSetup, Snapshot,
};
#[cfg(feature = "executor-diesel-mysql")]
use crate::backend::{BuiltinKind, Value};
use alloc::string::String;
#[cfg(feature = "executor-diesel-mysql")]
use core::cell::RefCell;
use diesel::sql_types::{BigInt, Nullable, Text};
#[cfg(feature = "executor-diesel-mysql")]
use diesel::{sql_query, Connection, RunQueryDsl};

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

/// Row type for reading `performance_schema.log_status`.
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
    // `performance_schema.log_status` is a regular table that returns JSON
    // columns; `SHOW MASTER STATUS` lacks result-set metadata in the prepared
    // protocol, so we cannot use the typed DSL here.
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
        query: &ReadQuery<'_, Self::Backend>,
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
            let value = load_scalar::<_, Self::Backend>(conn, query, kind)?;
            Ok((value, pos))
        })
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<crate::backend::MySql>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.conn.borrow_mut();
        // Position before snapshot, per `Connector::Checkpoint`.
        let pos = read_binlog_pos(&mut conn);
        conn.transaction(|conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_page::<_, crate::backend::MySql>(conn, query, max_bytes)?;
            Ok(Snapshot {
                value,
                checkpoint: pos,
            })
        })
    }

    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
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
        let pos = read_binlog_pos(&mut conn);
        diesel::connection::Connection::transaction(&mut *conn, |conn| {
            run_setup_statements(conn, auth.setup_statements())?;
            let values = load_scalar_row::<_, Self::Backend>(conn, query, kinds)?;
            Ok((values, pos))
        })
        .map_err(ScalarRowError::Connector)
    }
}
