//! Async parallel of [`AutoResolvingEngine`](super::AutoResolvingEngine).
//!
//! Same surface (`register`, `install`, `snapshot`, `consumers`,
//! `unregister_*`), with the methods that touch the connector returning
//! `Send` futures. Pick this engine when the database driver is async
//! (sqlx, tokio-postgres, diesel-async). Pick the sync engine when the
//! driver is sync (diesel, rusqlite) or when you want the simpler
//! testing surface.

use super::async_connector::AsyncConnector;
use super::auto::{ResolveContext, SnapshotResult};
use super::connector::ReExecError;
use super::engine::{
    ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecUnregisterReport, Registered,
    ScalarUpdate,
};
use crate::{
    Cell, IdTypes, RegisterError, SubscriptionRequest, SubscriptionScope, UnregisterReport,
    WalEvent,
};
use alloc::vec::Vec;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;
use sqlparser::dialect::Dialect;

/// Auto-resolving engine driven by an [`AsyncConnector`].
///
/// Mirrors [`AutoResolvingEngine`](super::AutoResolvingEngine) one-for-one;
/// see there for the behavior contract. The only difference is that
/// methods which call the connector ([`snapshot`](Self::snapshot),
/// [`consumers`](Self::consumers)) return futures.
///
/// The struct itself is `Send + Sync` whenever `X: AsyncConnector` and
/// `X::AuthContext: Send + Sync` (the trait bound already requires this),
/// so the engine can be moved between async tasks freely.
pub struct AsyncAutoResolvingEngine<D, I, DB, X>
where
    D: Dialect,
    I: IdTypes,
    DB: DatabaseLike,
    X: AsyncConnector,
{
    inner: ReExecEngine<D, I, DB>,
    connector: X,
    contexts: HashMap<ReExecQueryId, ResolveContext<I, X::AuthContext>>,
}

impl<D, I, DB, X> AsyncAutoResolvingEngine<D, I, DB, X>
where
    D: Dialect,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: AsyncConnector,
{
    /// Wrap an existing [`ReExecEngine`] with the given [`AsyncConnector`].
    pub fn new(inner: ReExecEngine<D, I, DB>, connector: X) -> Self {
        Self {
            inner,
            connector,
            contexts: HashMap::new(),
        }
    }

    /// The wrapped trigger-emitting engine.
    pub const fn inner(&self) -> &ReExecEngine<D, I, DB> {
        &self.inner
    }

    /// The connector this engine drives.
    pub const fn connector(&self) -> &X {
        &self.connector
    }

    /// Number of captured re-execution queries (matches the inner engine).
    pub fn reexec_query_count(&self) -> usize {
        self.inner.reexec_query_count()
    }

    /// Register a subscription. Sync because registration only touches
    /// in-memory engine state; the connector is not called.
    pub fn register(
        &mut self,
        spec: SubscriptionRequest<I>,
        auth: X::AuthContext,
    ) -> Result<Registered, RegisterError> {
        let session = match &spec.scope {
            SubscriptionScope::Session(s) => Some(*s),
            SubscriptionScope::Durable => None,
        };
        let result = self.inner.register(spec)?;
        if let Registered::ReExec {
            query_id,
            sql,
            column_type,
        } = &result
        {
            self.contexts.insert(
                *query_id,
                ResolveContext {
                    sql: sql.clone(),
                    column_type: *column_type,
                    session,
                    auth,
                },
            );
        }
        Ok(result)
    }

    /// Install a value directly, bypassing the connector.
    pub fn install(&mut self, query_id: ReExecQueryId, value: Cell) -> bool {
        self.inner.install(query_id, value)
    }

    /// Bootstrap a captured query by reading its current value through the
    /// async connector and installing the result. Async analogue of
    /// [`AutoResolvingEngine::snapshot`](super::AutoResolvingEngine::snapshot).
    ///
    /// # Errors
    ///
    /// Returns [`ReExecError::Connector`] if the connector fails. Returns
    /// `Ok(None)` if `query_id` does not exist.
    pub async fn snapshot(
        &mut self,
        query_id: ReExecQueryId,
    ) -> Result<Option<SnapshotResult<X::Checkpoint>>, ReExecError<X::Error>> {
        let Some(ctx) = self.contexts.get(&query_id) else {
            return Ok(None);
        };
        let (value, checkpoint) = self
            .connector
            .execute_scalar(&ctx.sql, ctx.column_type, &ctx.auth)
            .await
            .map_err(ReExecError::Connector)?;
        self.inner.install(query_id, value.clone());
        Ok(Some(SnapshotResult::Scalar(value, checkpoint)))
    }

    /// Dispatch a CDC event.
    ///
    /// Method-generic on `C: Checkpoint` so the same engine can handle
    /// events from sources with different position types. For every
    /// [`ReExecutionTrigger`] the inner engine emits, this method looks
    /// up the auth context, awaits the connector's `execute_scalar`,
    /// installs the result, and replaces the trigger with a
    /// [`ScalarUpdate`]. The first connector failure aborts the rest of
    /// the batch and is surfaced as [`ReExecError::Connector`].
    ///
    /// [`ReExecutionTrigger`]: super::ReExecutionTrigger
    pub async fn consumers<C: crate::Checkpoint>(
        &mut self,
        event: &WalEvent<C>,
    ) -> Result<ReExecNotifications<I, C>, ReExecError<X::Error>> {
        let ReExecNotifications {
            engine,
            mut scalar_updates,
            triggers,
        } = self.inner.consumers(event)?;

        for trigger in triggers {
            let ctx = self.contexts.get(&trigger.query_id).expect(
                "every captured query stores its resolve context at register time; \
                 trigger.query_id must exist in `contexts`",
            );
            let (value, _db_checkpoint) = self
                .connector
                .execute_scalar(&ctx.sql, ctx.column_type, &ctx.auth)
                .await
                .map_err(ReExecError::Connector)?;
            self.inner.install(trigger.query_id, value.clone());
            scalar_updates.push(ScalarUpdate {
                query_id: trigger.query_id,
                consumer_id: trigger.consumer_id,
                value,
                checkpoint: trigger.checkpoint.clone(),
            });
        }

        Ok(ReExecNotifications {
            engine,
            scalar_updates,
            triggers: Vec::new(),
        })
    }

    /// Unregister a session and drop every stored auth context that
    /// belonged to it.
    pub fn unregister_session(&mut self, session_id: I::SessionId) -> ReExecUnregisterReport {
        let report = self.inner.unregister_session(session_id);
        self.contexts
            .retain(|_, ctx| ctx.session != Some(session_id));
        report
    }

    /// Unregister a captured query by id; drops its auth context too.
    pub fn unregister_reexec_query(&mut self, query_id: ReExecQueryId) -> bool {
        let removed = self.inner.unregister_reexec_query(query_id);
        if removed {
            self.contexts.remove(&query_id);
        }
        removed
    }

    /// Unregister an engine subscription by `(consumer_id, sql)`.
    pub fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        self.inner.unregister_query(consumer_id, sql)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::connector::Snapshot;
    use super::*;
    use crate::{
        ColumnId, ColumnType, DefaultIds, NoCheckpoint, RowImage, SubscriptionEngine,
        SubscriptionRequest,
    };
    use alloc::sync::Arc;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::sync::Mutex;
    use std::task::Wake;

    /// No-op `Wake` implementation: the `MockAsyncConnector` futures
    /// never park, so `wake` is never invoked. Built on the safe `Wake`
    /// trait so the workspace's `forbid(unsafe_code)` lint passes; the
    /// nightly-only `Waker::noop` would be the alternative, hence the
    /// `allow` below.
    struct NoopWake;
    #[allow(clippy::manual_noop_waker)]
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    /// Tiny block-on for the test futures (which never park). Drives the
    /// future to completion by polling; if a real-world future returned
    /// Pending it would loop forever, but the `MockAsyncConnector`
    /// futures complete in one poll.
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

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    /// `Mutex`-backed mock so the futures are `Send`.
    struct MockAsyncConnector {
        values: Mutex<Vec<Cell>>,
        call_count: Mutex<usize>,
    }

    impl MockAsyncConnector {
        fn new(values: Vec<Cell>) -> Self {
            Self {
                values: Mutex::new(values),
                call_count: Mutex::new(0),
            }
        }
        fn call_count(&self) -> usize {
            *self.call_count.lock().unwrap()
        }
    }

    #[derive(Debug)]
    struct MockError(&'static str);

    impl core::fmt::Display for MockError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    // The `+ Send` bound on the returned futures is the whole point of
    // the trait shape; `async fn in trait` cannot express it directly.
    #[allow(clippy::manual_async_fn)]
    impl AsyncConnector for MockAsyncConnector {
        type AuthContext = ();
        type Error = MockError;
        type Checkpoint = NoCheckpoint;

        fn execute_scalar(
            &self,
            _sql: &str,
            _column_type: ColumnType,
            _auth: &(),
        ) -> impl Future<Output = Result<(Cell, Option<Self::Checkpoint>), Self::Error>> + Send
        {
            async move {
                *self.call_count.lock().unwrap() += 1;
                let cell = self
                    .values
                    .lock()
                    .unwrap()
                    .pop()
                    .ok_or(MockError("queue empty"))?;
                Ok((cell, None))
            }
        }

        fn execute_rows(
            &self,
            _sql: &str,
            _auth: &(),
        ) -> impl Future<Output = Result<Snapshot<Vec<RowImage>, Self::Checkpoint>, Self::Error>> + Send
        {
            async move { Err(MockError("execute_rows not exercised in v1 tests")) }
        }
    }

    fn row(price: f64) -> RowImage {
        RowImage {
            cells: Arc::from([
                Cell::Int(0),
                Cell::Float(price),
                Cell::Int(1),
                Cell::String(Arc::from("paid")),
            ]),
        }
    }

    fn insert_event(id: i64, price: f64) -> WalEvent {
        WalEvent::builder(0)
            .insert()
            .pk_cell(0, Cell::Int(id))
            .new_row(row(price))
            .build()
            .unwrap()
    }

    fn delete_event(id: i64, price: f64) -> WalEvent {
        WalEvent::builder(0)
            .delete()
            .pk_cell(0, Cell::Int(id))
            .old_row(row(price))
            .build()
            .unwrap()
    }

    fn engine_with_values(
        values: Vec<Cell>,
    ) -> AsyncAutoResolvingEngine<PostgreSqlDialect, DefaultIds, ParserDB, MockAsyncConnector> {
        let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            Arc::new(catalog()),
            PostgreSqlDialect {},
        );
        AsyncAutoResolvingEngine::new(ReExecEngine::new(inner), MockAsyncConnector::new(values))
    }

    /// Full path through the async engine: register, snapshot (which
    /// installs), unrelated insert (no connector call), delete of the
    /// extreme (one connector call, ScalarUpdate emitted).
    #[test]
    fn async_engine_dispatch_round_trip() {
        // Two values for: snapshot bootstrap (5.0), delete re-execution (9.0).
        // Mock pops from the back so push in reverse order.
        let mut e = engine_with_values(vec![Cell::Float(9.0), Cell::Float(5.0)]);

        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };

        // Snapshot bootstraps. Future is Send-bound and ready immediately.
        let snap = block_on(e.snapshot(qid)).unwrap().expect("query_id exists");
        match snap {
            SnapshotResult::Scalar(Cell::Float(v), None) => assert!((v - 5.0).abs() < f64::EPSILON),
            other => panic!("expected Scalar(5.0, None), got {other:?}"),
        }
        assert_eq!(e.connector().call_count(), 1);

        // Insert above the extreme: in-process Unchanged, no connector call.
        let n = block_on(e.consumers(&insert_event(2, 9.0))).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(e.connector().call_count(), 1);

        // Delete the extreme: trigger -> connector -> ScalarUpdate.
        let n = block_on(e.consumers(&delete_event(1, 5.0))).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].query_id, qid);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(9.0));
        assert!(n.triggers.is_empty(), "async engine drains triggers");
        assert_eq!(e.connector().call_count(), 2);
    }

    /// `unrelated-column` UPDATE skip optimization still works under
    /// the async engine.
    #[test]
    fn async_engine_unrelated_column_update_skips_connector() {
        let mut e = engine_with_values(vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        assert!(e.install(qid, Cell::Float(10.0)));

        let event = WalEvent::builder(0)
            .update()
            .new_row(row(10.0))
            .pk_cell(0, Cell::Int(1))
            .maybe_old_row(Some(row(10.0)))
            .changed_columns(Arc::from([3 as ColumnId]))
            .build()
            .unwrap();

        let n = block_on(e.consumers(&event)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
        assert_eq!(e.connector().call_count(), 0);
    }

    /// `snapshot` on an unknown id returns `Ok(None)`.
    #[test]
    fn async_engine_snapshot_unknown_query_returns_none() {
        let mut e = engine_with_values(vec![]);
        assert!(block_on(e.snapshot(99999)).unwrap().is_none());
        assert_eq!(e.connector().call_count(), 0);
    }

    /// Connector failure aborts the batch with `ReExecError::Connector`.
    #[test]
    fn async_engine_connector_error_aborts_batch() {
        let mut e = engine_with_values(vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        assert!(e.install(qid, Cell::Float(5.0)));

        match block_on(e.consumers(&delete_event(1, 5.0))) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector(MockError(msg))) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// `unregister_reexec_query` drops the stored auth context.
    #[test]
    fn async_engine_unregister_drops_context() {
        let mut e = engine_with_values(vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        assert_eq!(e.contexts.len(), 1);
        assert!(e.unregister_reexec_query(qid));
        assert_eq!(e.contexts.len(), 0);
    }
}
