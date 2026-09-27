#![allow(clippy::type_complexity)]
//! Sync [`Connector`](super::Connector) for PostgreSQL that reports the read's
//! own snapshot fence.

#[cfg(feature = "executor-diesel-postgres")]
use super::diesel_connector::{load_page_postgres, load_scalar, load_scalar_row};
// The async connector reads the fence row type from this module, so the
// module is compiled without the sync feature: everything the sync connector
// alone needs follows it.
#[cfg(feature = "executor-diesel-postgres")]
use super::{
    run_setup_statements, Connector, ReadQuery, RowPage, ScalarRowError, SessionSetup, Snapshot,
    PG_READ_SNAPSHOT,
};
#[cfg(feature = "executor-diesel-postgres")]
use crate::backend::{ScalarFamily, Value};
use alloc::string::String;
#[cfg(feature = "executor-diesel-postgres")]
use core::cell::RefCell;
use diesel::sql_types::Text;
#[cfg(feature = "executor-diesel-postgres")]
use diesel::{sql_query, Connection, RunQueryDsl};

/// Sync [`Connector`] backed by a diesel `PgConnection`.
///
/// Each read runs in a `READ ONLY REPEATABLE READ` transaction and reports
/// that transaction's [`crate::PgSnapshotFence`].
///
/// Holds the connection in a [`RefCell`] for the interior-mutability the
/// trait's `&self` requires. Not `Send`/`Sync`. For multi-threaded use,
/// either keep the connector thread-local or implement [`Connector`]
/// yourself over a connection pool.
///
/// # Errors
///
/// Returns [`diesel::result::Error`] for any underlying database failure
/// (network drop, statement error, an unparseable snapshot or LSN response).
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

/// Row type for reading the current transaction's snapshot and the WAL insert
/// position after it.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
#[derive(diesel::QueryableByName)]
pub struct PgSnapshotFenceRow {
    #[diesel(sql_type = Text)]
    pub snapshot: String,
    #[diesel(sql_type = Text)]
    pub lsn: String,
}

#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres"
))]
impl PgSnapshotFenceRow {
    /// Raw SQL because both are functions with no table to type them against.
    ///
    /// A transaction's snapshot is fixed by its first statement, so the insert
    /// position this reads bounds every commit that snapshot sees.
    pub const SQL: &'static str = "SELECT pg_current_snapshot()::text AS snapshot, \
        pg_current_wal_insert_lsn()::text AS lsn";

    /// The fence this row spells. An unparseable answer is an error rather
    /// than a read without a fence.
    ///
    /// # Errors
    ///
    /// [`diesel::result::Error::QueryBuilderError`] when either column is not
    /// the text Postgres prints for its type.
    pub fn into_fence(self) -> diesel::QueryResult<crate::PgSnapshotFence> {
        let unparseable = |what: &str, text: &str| {
            diesel::result::Error::QueryBuilderError(
                alloc::format!("unparseable {what} from the read's own transaction: {text}").into(),
            )
        };
        let insert_lsn = crate::PgLsn::parse(&self.lsn)
            .ok_or_else(|| unparseable("WAL insert LSN", &self.lsn))?;
        crate::PgSnapshotFence::parse(&self.snapshot, insert_lsn)
            .ok_or_else(|| unparseable("snapshot", &self.snapshot))
    }
}

/// The fence of the snapshot the current repeatable-read transaction took.
#[cfg(feature = "executor-diesel-postgres")]
pub(super) fn read_fence(
    conn: &mut diesel::PgConnection,
) -> diesel::QueryResult<crate::PgSnapshotFence> {
    sql_query(PgSnapshotFenceRow::SQL)
        .get_result::<PgSnapshotFenceRow>(conn)?
        .into_fence()
}

/// Run `body` under `setup` in a read snapshot and report the snapshot's fence.
#[cfg(feature = "executor-diesel-postgres")]
pub(super) fn read_in_snapshot<T>(
    conn: &mut diesel::PgConnection,
    setup: &[String],
    body: impl FnOnce(&mut diesel::PgConnection) -> diesel::QueryResult<T>,
) -> diesel::QueryResult<(T, Option<crate::PgSnapshotFence>)> {
    conn.transaction(|conn| {
        sql_query(PG_READ_SNAPSHOT).execute(conn)?;
        let fence = read_fence(conn)?;
        run_setup_statements(conn, setup)?;
        body(conn).map(|value| (value, Some(fence)))
    })
}

#[cfg(feature = "executor-diesel-postgres")]
impl<S: SessionSetup> Connector for PgDieselConnector<S> {
    type AuthContext = S;
    type Error = diesel::result::Error;
    type Checkpoint = crate::PgCommitPosition;
    type Backend = crate::backend::Postgres;

    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kind: ScalarFamily,
        auth: &S,
    ) -> Result<(Value<Self::Backend>, Option<crate::PgSnapshotFence>), Self::Error> {
        let mut conn = self.conn.borrow_mut();
        read_in_snapshot(&mut conn, auth.setup_statements(), |conn| {
            load_scalar::<_, Self::Backend>(conn, query, kind)
        })
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        max_bytes: usize,
        auth: &S,
    ) -> Result<Snapshot<RowPage<crate::backend::Postgres>, Self::Checkpoint>, Self::Error> {
        let mut conn = self.conn.borrow_mut();
        // The page and the fence share one snapshot, so a caller reconciling
        // pages against the change stream knows exactly which changes this one
        // holds.
        let (value, fence) = read_in_snapshot(&mut conn, auth.setup_statements(), |conn| {
            load_page_postgres(conn, query, max_bytes)
        })?;
        Ok(Snapshot { value, fence })
    }

    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kinds: &[ScalarFamily],
        auth: &S,
    ) -> Result<
        (
            alloc::vec::Vec<Value<Self::Backend>>,
            Option<crate::PgSnapshotFence>,
        ),
        ScalarRowError<Self::Error>,
    > {
        let mut conn = self.conn.borrow_mut();
        read_in_snapshot(&mut conn, auth.setup_statements(), |conn| {
            load_scalar_row::<_, Self::Backend>(conn, query, kinds)
        })
        .map_err(ScalarRowError::Connector)
    }
}
