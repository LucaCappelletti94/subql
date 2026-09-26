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
    /// Cursors `DECLARE`d on the one connection, inside one read-only
    /// repeatable-read transaction that the first opens and the last closes.
    cursors: RefCell<alloc::collections::BTreeMap<super::CursorId, PgCursorState>>,
    next_cursor: core::cell::Cell<u64>,
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
            cursors: RefCell::new(alloc::collections::BTreeMap::new()),
            next_cursor: core::cell::Cell::new(0),
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
            cursors: RefCell::new(alloc::collections::BTreeMap::new()),
            next_cursor: core::cell::Cell::new(0),
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

    /// A cursor here lives on the connector's one connection, so while any is
    /// open every read on this connector runs inside the cursors'
    /// transaction. The first cursor's session setup is the transaction's.
    fn open_cursor(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        auth: &S,
    ) -> Result<super::CursorId, super::CursorError<Self::Error>> {
        use diesel::connection::TransactionManager as _;
        type PgTxn = <diesel::PgConnection as Connection>::TransactionManager;

        let mut conn = self.conn.borrow_mut();
        let mut cursors = self.cursors.borrow_mut();
        let id = super::CursorId(self.next_cursor.get());
        self.next_cursor.set(id.0 + 1);
        let name = alloc::format!("subql_cursor_{}", id.0);
        let first = cursors.is_empty();
        let opened = (|| -> diesel::QueryResult<Option<crate::PgSnapshotFence>> {
            // The fence is read inside the transaction, before `DECLARE` fixes
            // the snapshot, as `read_in_snapshot` reads it. A later cursor
            // shares the transaction and so the first one's fence.
            let fence = if first {
                PgTxn::begin_transaction(&mut *conn)?;
                // SET TRANSACTION is DDL-like; no typed DSL equivalent exists.
                sql_query(PG_READ_SNAPSHOT).execute(&mut *conn)?;
                let fence = read_fence(&mut conn)?;
                run_setup_statements(&mut *conn, auth.setup_statements())?;
                Some(fence)
            } else {
                cursors.values().next().and_then(|held| held.fence.clone())
            };
            // `DECLARE CURSOR` has no query-DSL spelling.
            let declaration = ReadQuery::owned(
                alloc::format!("DECLARE {name} NO SCROLL CURSOR FOR {}", query.sql()),
                query.binds().to_vec(),
            );
            super::diesel_backend::boxed_postgres_read_query(&declaration)?.execute(&mut *conn)?;
            Ok(fence)
        })();
        match opened {
            Ok(fence) => {
                cursors.insert(id, PgCursorState::new(name, fence));
                Ok(id)
            }
            Err(error) => {
                if first {
                    let _ = PgTxn::rollback_transaction(&mut *conn);
                }
                Err(super::CursorError::Connector(error))
            }
        }
    }

    fn fetch_cursor(
        &self,
        cursor: super::CursorId,
        max_bytes: usize,
    ) -> Result<Snapshot<RowPage<Self::Backend>, Self::Checkpoint>, super::CursorError<Self::Error>>
    {
        let mut conn = self.conn.borrow_mut();
        let mut cursors = self.cursors.borrow_mut();
        let held = cursors
            .get_mut(&cursor)
            .ok_or(super::CursorError::Unknown(cursor))?;
        match fetch_page_from(&mut conn, held, max_bytes, CURSOR_BATCH) {
            Ok(page) => Ok(page),
            // A failed fetch aborts the transaction every cursor here shares,
            // so all of them are gone, and the transaction is ended.
            Err(error) => {
                cursors.clear();
                end_cursor_transaction(&mut conn, false);
                Err(super::CursorError::Connector(error))
            }
        }
    }

    fn close_cursor(&self, cursor: super::CursorId) -> Result<(), super::CursorError<Self::Error>> {
        let mut conn = self.conn.borrow_mut();
        let mut cursors = self.cursors.borrow_mut();
        // Idempotent: an already-closed cursor is not an error.
        let Some(held) = cursors.remove(&cursor) else {
            return Ok(());
        };
        // `CLOSE` is a cursor command with no typed DSL spelling. The name is
        // the connector's own, never a caller's.
        let closed = sql_query(alloc::format!("CLOSE {}", held.name)).execute(&mut *conn);
        if closed.is_err() {
            cursors.clear();
        }
        if cursors.is_empty() {
            end_cursor_transaction(&mut conn, closed.is_ok());
        }
        closed.map(|_| ()).map_err(super::CursorError::Connector)
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

/// End the transaction the cursors held, committing when they closed cleanly.
/// Best-effort on the way out of a failure: the transaction is read only, so
/// failing to end it politely loses nothing.
#[cfg(feature = "executor-diesel-postgres")]
fn end_cursor_transaction(conn: &mut diesel::PgConnection, commit: bool) {
    use diesel::connection::TransactionManager as _;
    type PgTxn = <diesel::PgConnection as Connection>::TransactionManager;
    if !commit || PgTxn::commit_transaction(conn).is_err() {
        let _ = PgTxn::rollback_transaction(conn);
    }
}

/// Rows per `FETCH`. Overshoot is carried into the next page rather than
/// discarded, so this trades round trips against buffered rows and never
/// against correctness.
#[cfg(feature = "executor-diesel-postgres")]
pub(super) const CURSOR_BATCH: usize = 64;

/// One `DECLARE`d cursor apart from the connection it lives on: its name, the
/// fence of the snapshot its pages report, and rows already fetched but not
/// yet delivered.
///
/// The leftover buffer is what keeps the byte budget exact. `FETCH` cannot be
/// undone, so a batch that overshoots the budget would otherwise have to be
/// returned whole or thrown away, and carrying the remainder into the next
/// page does neither.
#[cfg(feature = "executor-diesel-postgres")]
pub(super) struct PgCursorState {
    /// The cursor's `DECLARE`d name, which the per-page `FETCH` and the
    /// closing `CLOSE` are built from.
    pub(super) name: String,
    fence: Option<crate::PgSnapshotFence>,
    columns: alloc::vec::Vec<String>,
    leftover: alloc::collections::VecDeque<alloc::vec::Vec<Value<crate::backend::Postgres>>>,
}

#[cfg(feature = "executor-diesel-postgres")]
impl PgCursorState {
    pub(super) const fn new(name: String, fence: Option<crate::PgSnapshotFence>) -> Self {
        Self {
            name,
            fence,
            columns: alloc::vec::Vec::new(),
            leftover: alloc::collections::VecDeque::new(),
        }
    }
}

/// Fill one page from an open cursor, buffering whatever a `FETCH` overshot.
///
/// The sync twin of `PgAsyncDieselConnector::fetch_from`, shared by the r2d2
/// and the single-connection connectors, and split out for the same reason: the caller decides what a failure means for the cursor's
/// registration, and that decision does not belong inside the read loop.
#[cfg(feature = "executor-diesel-postgres")]
pub(super) fn fetch_page_from(
    conn: &mut diesel::PgConnection,
    held: &mut PgCursorState,
    max_bytes: usize,
    batch: usize,
) -> diesel::QueryResult<Snapshot<RowPage<crate::backend::Postgres>, crate::PgCommitPosition>> {
    let mut rows: alloc::vec::Vec<alloc::vec::Vec<Value<crate::backend::Postgres>>> =
        alloc::vec::Vec::new();
    let mut spent = 0_usize;
    loop {
        if super::drain_cursor_buffer(&mut held.leftover, &mut rows, &mut spent, max_bytes) {
            return Ok(Snapshot {
                value: RowPage {
                    columns: held.columns.clone(),
                    rows,
                    more: true,
                },
                fence: held.fence.clone(),
            });
        }
        // `FETCH FORWARD` is a cursor command with no typed DSL equivalent.
        let page = load_page_postgres(
            conn,
            &ReadQuery::without_binds(&alloc::format!("FETCH FORWARD {batch} FROM {}", held.name)),
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
                fence: held.fence.clone(),
            });
        }
    }
}

#[cfg(all(test, feature = "executor-diesel-postgres"))]
mod tests {
    use super::PgSnapshotFenceRow;

    fn row(snapshot: &str, lsn: &str) -> PgSnapshotFenceRow {
        PgSnapshotFenceRow {
            snapshot: snapshot.into(),
            lsn: lsn.into(),
        }
    }

    /// A server answer that is not the text Postgres prints fails the read
    /// rather than passing it off as one without a fence.
    #[test]
    fn an_unparseable_fence_answer_fails_the_read() {
        let fence = row("740:745:742", "0/7D0").into_fence();
        assert_eq!(
            fence.map(|fence| fence.insert_lsn()).ok(),
            Some(crate::PgLsn(0x7D0))
        );
        assert!(row("740:745:742", "not an lsn").into_fence().is_err());
        assert!(row("745:740:", "0/7D0").into_fence().is_err());
    }
}
