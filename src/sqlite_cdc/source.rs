//! Live-connection SQLite CDC source (gated behind `sqlite-cdc`).
//!
//! Wraps a diesel [`SqliteConnection`] with the SQLite session extension
//! attached via [`diesel_sqlite_session`]. Every DML the caller runs
//! through [`SqliteCdcSource::connection`] accumulates into the session.
//! [`SqliteCdcSource::poll_next_event`] drains the accumulated bytes as
//! a changeset, parses them via [`super::SqliteChangesetParser`], and
//! yields the resulting [`SqliteChangesetEvent`] instances one at a
//! time.
//!
//! Analogue of [`crate::PgStreamingCdcSource`]. The parser stays
//! unconditional. The live source layers a real connection lifecycle
//! on top and implements [`crate::CdcSource`] so it drops into the
//! same consume-and-ack loop as the Postgres sources. `impl CdcSource`
//! rides on the upstream `unsafe impl Send for Session` added in
//! `diesel-sqlite-session`.

use alloc::collections::VecDeque;

use diesel::SqliteConnection;
use diesel_sqlite_session::{Session, SessionError, SqliteSessionExt};
use sql_traits::prelude::DatabaseLike;

use super::event::SqliteChangesetEvent;
use super::parser::SqliteChangesetParser;
use crate::wal::{WalParseError, WalParser};

/// Errors surfaced by [`SqliteCdcSource`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteCdcError {
    /// Underlying `diesel-sqlite-session` failure (session creation,
    /// attach, or changeset export).
    #[error("diesel-sqlite-session: {0}")]
    Session(#[from] SessionError),
    /// The changeset parser could not decode the bytes surfaced by the
    /// session extension.
    #[error("changeset parse: {0}")]
    Parse(#[from] WalParseError),
    /// Underlying diesel connection error (statement execution).
    #[error("diesel: {0}")]
    Diesel(#[from] diesel::result::Error),
}

/// Live SQLite CDC source. See the [module docs](super) for the lifecycle
/// contract.
pub struct SqliteCdcSource<DB: DatabaseLike> {
    connection: SqliteConnection,
    session: Session,
    catalog: DB,
    /// Events buffered from the last changeset drain. Consumed FIFO by
    /// [`Self::poll_next_event`]. When empty and the session has
    /// accumulated new changes, the next poll drains a fresh changeset.
    pending: VecDeque<SqliteChangesetEvent>,
}

impl<DB: DatabaseLike> SqliteCdcSource<DB> {
    /// Construct a source over `connection`, attaching every table
    /// SQLite knows about to a fresh session.
    ///
    /// # Errors
    ///
    /// [`SqliteCdcError::Session`] when session creation or table
    /// attachment fails.
    pub fn new(mut connection: SqliteConnection, catalog: DB) -> Result<Self, SqliteCdcError> {
        let mut session = connection.create_session()?;
        // `attach_all` covers every table SQLite currently knows about.
        // Iterating the catalog and calling `attach_by_name` per table
        // would work too but `TableLike::table_name()` may return a
        // schema-qualified or quoted identifier and
        // `sqlite3session_attach` wants the bare SQLite name.
        // `attach_all` sidesteps that at the cost of also tracking
        // tables outside the catalog. Non-catalog tables that slip
        // through fail the parser's `resolve_table` step and surface
        // as an error, which is loud enough for now.
        session.attach_all()?;
        Ok(Self {
            connection,
            session,
            catalog,
            pending: VecDeque::new(),
        })
    }

    /// Drain the next event from the source.
    ///
    /// Returns `Ok(None)` when the session has accumulated no new
    /// changes since the last drain and the pending buffer is empty.
    ///
    /// # Errors
    ///
    /// [`SqliteCdcError::Session`] on any SQLite failure during
    /// `.changeset()`. [`SqliteCdcError::Parse`] when the emitted
    /// changeset bytes cannot be decoded.
    pub fn poll_next_event(&mut self) -> Result<Option<SqliteChangesetEvent>, SqliteCdcError> {
        if self.pending.is_empty() && !self.session.is_empty() {
            let bytes = self.session.changeset()?;
            let events = SqliteChangesetParser.parse_wal_message(&bytes, &self.catalog)?;
            self.pending.extend(events);
            // SQLite sessions accumulate: `.changeset()` snapshots
            // everything tracked since the session was created rather
            // than draining. Recreate the session so the next poll only
            // sees changes that happen AFTER this drain.
            let mut fresh = self.connection.create_session()?;
            fresh.attach_all()?;
            self.session = fresh;
        }
        Ok(self.pending.pop_front())
    }

    /// Mutable access to the underlying diesel [`SqliteConnection`].
    ///
    /// The caller uses this to drive DML that the session then observes.
    /// Writes performed through any *other* connection (a sibling
    /// handle to the same database file, for example) bypass the
    /// session and will not surface as events.
    pub const fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.connection
    }

    /// Immutable access to the catalog the source was built with.
    pub const fn catalog(&self) -> &DB {
        &self.catalog
    }
}

impl<DB: DatabaseLike> crate::CdcSource for SqliteCdcSource<DB> {
    type Event = SqliteChangesetEvent;
    type Error = SqliteCdcError;

    fn next_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<Self::Event>, Self::Error>> + Send {
        // `poll_next_event` never blocks. It touches the in-process
        // session state and, on drain, the in-memory changeset buffer.
        // Wrapping the sync body in `core::future::ready` avoids the
        // extra state machine an `async` block would spin up.
        core::future::ready(self.poll_next_event())
    }

    fn ack(
        &mut self,
        _upto: <Self::Event as crate::backend::CdcEvent>::Checkpoint,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        // Changeset events carry `NoCheckpoint`. The session extension
        // has no upstream to acknowledge against, so `ack` is a no-op.
        core::future::ready(Ok(()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::{CdcEvent, RowKind, Value};
    use diesel::{sql_query, Connection, RunQueryDsl};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::SQLiteDialect;

    fn open_orders_db() -> (SqliteConnection, ParserDB) {
        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE _pad (id INTEGER);\n\
             CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT);",
        )
        .expect("orders DDL parses");
        let mut conn = SqliteConnection::establish(":memory:").expect("open in-memory sqlite");
        sql_query("CREATE TABLE _pad (id INTEGER)")
            .execute(&mut conn)
            .expect("pad DDL");
        sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)")
            .execute(&mut conn)
            .expect("orders DDL");
        (conn, catalog)
    }

    #[test]
    fn typed_sqlite_source_yields_insert_event() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        assert!(
            source.poll_next_event().expect("empty poll").is_none(),
            "no changes yet -> None"
        );
        sql_query("INSERT INTO orders (id, amount, status) VALUES (7, 250, 'paid')")
            .execute(source.connection())
            .expect("insert");
        let event = source
            .poll_next_event()
            .expect("poll succeeds")
            .expect("one event pending");
        assert_eq!(event.kind(), crate::EventKind::Insert);
        assert_eq!(event.pk_columns(source.catalog()), &[0u16]);
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 1).unwrap(),
            Value::Int(250)
        );
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 2).unwrap(),
            Value::String("paid".into())
        );
        assert!(source.poll_next_event().expect("post-drain poll").is_none());
    }

    #[test]
    fn typed_sqlite_source_yields_multiple_events() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        for (id, amt) in [(1, 10), (2, 20), (3, 30)] {
            sql_query(alloc::format!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {amt}, 's')"
            ))
            .execute(source.connection())
            .expect("insert");
        }
        let mut drained = alloc::vec::Vec::new();
        while let Some(ev) = source.poll_next_event().expect("poll") {
            drained.push(ev);
        }
        assert_eq!(drained.len(), 3);
        for ev in &drained {
            assert_eq!(ev.kind(), crate::EventKind::Insert);
        }
        assert!(source.poll_next_event().expect("post-drain poll").is_none());
    }

    #[test]
    fn typed_sqlite_source_delete_and_update() {
        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (5, 100, 'pending')")
            .execute(source.connection())
            .expect("insert");
        let _ = source.poll_next_event().expect("insert poll");
        assert!(source
            .poll_next_event()
            .expect("post-insert poll")
            .is_none());

        sql_query("UPDATE orders SET status = 'shipped' WHERE id = 5")
            .execute(source.connection())
            .expect("update");
        let update = source
            .poll_next_event()
            .expect("update poll")
            .expect("one event");
        assert_eq!(update.kind(), crate::EventKind::Update);
        let mut changed = update.changed_columns(source.catalog());
        changed.sort_unstable();
        assert!(changed.contains(&2u16));
        assert_eq!(
            update.value_at(source.catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(5)
        );

        sql_query("DELETE FROM orders WHERE id = 5")
            .execute(source.connection())
            .expect("delete");
        let delete = source
            .poll_next_event()
            .expect("delete poll")
            .expect("one event");
        assert_eq!(delete.kind(), crate::EventKind::Delete);
        assert_eq!(
            delete.value_at(source.catalog(), RowKind::Pk, 0).unwrap(),
            Value::Int(5)
        );
    }

    #[test]
    fn typed_sqlite_source_impls_cdc_source() {
        use core::future::Future;
        use core::pin::pin;
        use core::task::{Context, Poll};
        use std::sync::Arc;
        use std::task::Wake;

        struct NoopWake;
        #[allow(unknown_lints, clippy::manual_noop_waker)]
        impl Wake for NoopWake {
            fn wake(self: Arc<Self>) {}
        }

        fn block_on<F: Future>(fut: F) -> F::Output {
            let waker = Arc::new(NoopWake).into();
            let mut ctx = Context::from_waker(&waker);
            let mut pinned = pin!(fut);
            loop {
                if let Poll::Ready(v) = pinned.as_mut().poll(&mut ctx) {
                    return v;
                }
            }
        }

        fn assert_cdc_source_send<S: crate::CdcSource + Send>() {}
        assert_cdc_source_send::<SqliteCdcSource<ParserDB>>();

        let (conn, catalog) = open_orders_db();
        let mut source = SqliteCdcSource::new(conn, catalog).expect("source construction");
        sql_query("INSERT INTO orders (id, amount, status) VALUES (1, 42, 'ok')")
            .execute(source.connection())
            .expect("insert");

        let ev = <SqliteCdcSource<ParserDB> as crate::CdcSource>::next_event(&mut source);
        let event = block_on(ev)
            .expect("next_event succeeds")
            .expect("one event pending");
        assert_eq!(event.kind(), crate::EventKind::Insert);
        assert_eq!(
            event.value_at(source.catalog(), RowKind::New, 0).unwrap(),
            Value::Int(1)
        );

        let ack =
            <SqliteCdcSource<ParserDB> as crate::CdcSource>::ack(&mut source, crate::NoCheckpoint);
        block_on(ack).expect("ack succeeds");
    }
}
