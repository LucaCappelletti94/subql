#![allow(clippy::type_complexity)]
//! LSN-aware sync [`Connector`] for PostgreSQL.

#[cfg(feature = "executor-diesel-postgres")]
use super::diesel_connector::{load_page_postgres, load_scalar, load_scalar_row};
use super::{
    run_setup_statements, Connector, ReadQuery, RowPage, ScalarRowError, SessionSetup, Snapshot,
};
use crate::backend::{BuiltinKind, Value};
use core::cell::RefCell;
use diesel::sql_types::Text;
use diesel::{sql_query, Connection, RunQueryDsl};

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

/// Row type for reading `pg_current_wal_lsn()`.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
#[derive(diesel::QueryableByName)]
pub struct PgLsnRow {
    #[diesel(sql_type = Text)]
    pub lsn: String,
}

/// Read the current WAL LSN before opening the snapshot transaction.
///
/// `pg_current_wal_lsn()` is a Postgres-specific function with no typed DSL
/// equivalent, so `sql_query` is required here.
#[cfg(feature = "executor-diesel-postgres")]
pub(super) fn read_current_lsn(
    conn: &mut diesel::PgConnection,
) -> diesel::QueryResult<Option<crate::PgLsn>> {
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
        query: &ReadQuery<'_, Self::Backend>,
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
            // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_scalar::<_, Self::Backend>(conn, query, kind)?;
            Ok((value, lsn))
        })
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
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
            // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
            diesel::sql_query("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ")
                .execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let value = load_page_postgres(conn, query, max_bytes)?;
            Ok(Snapshot {
                value,
                checkpoint: lsn,
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
            // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
            sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ").execute(conn)?;
            run_setup_statements(conn, auth.setup_statements())?;
            let values = load_scalar_row::<_, Self::Backend>(conn, query, kinds)?;
            Ok((values, lsn))
        })
        .map_err(ScalarRowError::Connector)
    }
}
