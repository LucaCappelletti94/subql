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
    BatchOutcome, ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecUnregisterReport,
    Registered, ScalarUpdate,
};
use crate::backend::{Backend, CdcEvent, Value};
use crate::clock::{duration_between, ClockHandle};
use crate::compiler::literals::SqlLiteralParse;
use crate::{IdTypes, RegisterError, SubscriptionRequest, SubscriptionScope, UnregisterReport};
use alloc::sync::Arc;
use alloc::vec::Vec;
use async_lock::{Semaphore, SemaphoreGuardArc};
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;

/// Internal state for the persistent re-execution concurrency cap.
///
/// The cap is enforced by an [`async_lock::Semaphore`]. The
/// [`AtomicUsize`] tracks how many permits are currently held so callers
/// can read [`AsyncAutoResolvingEngine::inflight`] without re-deriving
/// it from the semaphore itself (which `async-lock` does not expose).
struct ThrottleState {
    sem: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
    cap: usize,
}

/// RAII guard returned by the throttle's acquire path. Releases the
/// semaphore permit and decrements the inflight counter on drop, even
/// when dropped because the awaiting future was cancelled.
struct InflightGuard {
    inflight: Arc<AtomicUsize>,
    _permit: SemaphoreGuardArc,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Release);
    }
}

/// Acquire a throttle permit if a cap is configured. Returns `None`
/// when no cap is set (the unbounded case), avoiding any
/// synchronisation overhead in that path. The returned guard releases
/// the permit on drop, which makes the call cancellation-safe: a
/// future that is dropped while awaiting `acquire_permit` (because the
/// outer `try_collect` short-circuited on a connector error or because
/// the caller cancelled) leaves the semaphore in a clean state.
async fn acquire_permit(
    throttle: Option<&(Arc<Semaphore>, Arc<AtomicUsize>)>,
    query_id: ReExecQueryId,
) -> Option<InflightGuard> {
    let Some((sem, inflight)) = throttle else {
        let _ = query_id;
        return None;
    };
    let permit = Arc::clone(sem).acquire_arc().await;
    let now = inflight.fetch_add(1, Ordering::AcqRel) + 1;
    #[cfg(feature = "observability")]
    tracing::trace!(
        query_id,
        inflight = now,
        "subql reexec throttle: permit acquired",
    );
    #[cfg(not(feature = "observability"))]
    let _ = now;
    Some(InflightGuard {
        inflight: Arc::clone(inflight),
        _permit: permit,
    })
}

/// Auto-resolving engine driven by an [`AsyncConnector`].
///
/// Mirrors [`AutoResolvingEngine`](super::AutoResolvingEngine) one-for-one.
/// See there for the behavior contract. The only difference is that
/// methods which call the connector ([`snapshot`](Self::snapshot),
/// [`consumers`](Self::consumers)) return futures.
///
/// The struct itself is `Send + Sync` whenever `X: AsyncConnector` and
/// `X::AuthContext: Send + Sync` (the trait bound already requires this),
/// so the engine can be moved between async tasks freely.
pub struct AsyncAutoResolvingEngine<E, I, DB, X>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike,
    X: AsyncConnector<Backend = E::Backend>,
{
    inner: ReExecEngine<E, I, DB>,
    connector: X,
    contexts: HashMap<ReExecQueryId, ResolveContext<I, X::AuthContext>>,
    /// Optional [`Clock`](crate::Clock) used for per-query debounce.
    clock: Option<ClockHandle>,
    /// Minimum interval between two re-executions of the same captured
    /// query. Requires `clock` to be set.
    debounce: Option<Duration>,
    /// Last `now_micros` at which each captured query was re-executed.
    last_reexec_at: HashMap<ReExecQueryId, u64>,
    /// Persistent throttle state that caps how many
    /// `connector.execute_scalar` calls are in flight at any moment
    /// across every [`consumers`](Self::consumers) and
    /// [`consumers_batch`](Self::consumers_batch) call.
    ///
    /// `None` means unbounded (no throttle).
    permits: Option<ThrottleState>,
}

impl<E, I, DB, X> AsyncAutoResolvingEngine<E, I, DB, X>
where
    E: CdcEvent + Sync,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: AsyncConnector<Backend = E::Backend>,
{
    /// Wrap an existing [`ReExecEngine`] with the given [`AsyncConnector`].
    pub fn new(inner: ReExecEngine<E, I, DB>, connector: X) -> Self {
        Self {
            inner,
            connector,
            contexts: HashMap::new(),
            clock: None,
            debounce: None,
            last_reexec_at: HashMap::new(),
            permits: None,
        }
    }

    /// Cap the number of trigger re-executions that may be in flight
    /// simultaneously across all [`consumers`](Self::consumers) and
    /// [`consumers_batch`](Self::consumers_batch) calls.
    ///
    /// The cap is enforced by a persistent semaphore on the engine: each
    /// `connector.execute_scalar(...)` acquires a permit before running
    /// and releases it on completion (including cancellation). With cap
    /// `N`, no more than `N` SQL queries are in flight at any moment,
    /// regardless of whether triggers arrive one per event or in batched
    /// bursts.
    ///
    /// Default `None`: every deduplicated trigger is dispatched
    /// concurrently. The connector's own pool may still throttle.
    ///
    /// A `cap` of 0 is treated as 1 to avoid a deadlock.
    #[must_use]
    pub fn with_max_concurrent_reexecutions(mut self, cap: usize) -> Self {
        let cap = cap.max(1);
        self.permits = Some(ThrottleState {
            sem: Arc::new(Semaphore::new(cap)),
            inflight: Arc::new(AtomicUsize::new(0)),
            cap,
        });
        self
    }

    /// Number of re-execution permits currently held, i.e. concurrent
    /// connector calls in flight. Returns 0 when no cap is configured
    /// (unbounded mode does not track inflight). Useful for operator
    /// dashboards alerting on sustained `inflight ~= cap`.
    #[must_use]
    pub fn inflight(&self) -> usize {
        self.permits
            .as_ref()
            .map_or(0, |state| state.inflight.load(Ordering::Acquire))
    }

    /// Configured concurrency cap, if any. `None` means unbounded.
    #[must_use]
    pub fn concurrency_cap(&self) -> Option<usize> {
        self.permits.as_ref().map(|s| s.cap)
    }

    /// Attach a [`Clock`](crate::Clock) for per-query debounce. See
    /// [`AutoResolvingEngine::with_clock`](super::AutoResolvingEngine::with_clock).
    #[must_use]
    pub fn with_clock(mut self, clock: ClockHandle) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Configure a minimum interval between two re-executions of the same
    /// captured query.
    ///
    /// See [`AutoResolvingEngine::with_debounce_per_query`](super::AutoResolvingEngine::with_debounce_per_query)
    /// for the contract.
    #[must_use]
    pub const fn with_debounce_per_query(mut self, debounce: Duration) -> Self {
        self.debounce = Some(debounce);
        self
    }

    fn debounce_skip(&self, query_id: ReExecQueryId) -> bool {
        let (Some(clock), Some(window)) = (self.clock.as_ref(), self.debounce) else {
            return false;
        };
        let Some(last_micros) = self.last_reexec_at.get(&query_id).copied() else {
            return false;
        };
        duration_between(last_micros, clock.now_micros()) < window
    }

    fn stamp_reexec(&mut self, query_id: ReExecQueryId) {
        if let Some(clock) = self.clock.as_ref() {
            self.last_reexec_at.insert(query_id, clock.now_micros());
        }
    }

    /// The wrapped trigger-emitting engine.
    pub const fn inner(&self) -> &ReExecEngine<E, I, DB> {
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
    /// in-memory engine state. The connector is not called.
    pub fn register(
        &mut self,
        spec: SubscriptionRequest<I, E::Backend>,
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
            column_kind,
        } = &result
        {
            self.contexts.insert(
                *query_id,
                ResolveContext {
                    sql: sql.clone(),
                    column_kind: *column_kind,
                    session,
                    auth,
                },
            );
        }
        Ok(result)
    }

    /// Install a value directly, bypassing the connector.
    pub fn install(&mut self, query_id: ReExecQueryId, value: Value<E::Backend>) -> bool {
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
    ) -> Result<Option<SnapshotResult<E::Backend, X::Checkpoint>>, ReExecError<X::Error>> {
        let Some(ctx) = self.contexts.get(&query_id) else {
            return Ok(None);
        };
        let (value, checkpoint) = self
            .connector
            .execute_scalar(&ctx.sql, ctx.column_kind, &ctx.auth)
            .await
            .map_err(ReExecError::Connector)?;
        self.inner.install(query_id, value.clone());
        Ok(Some(SnapshotResult::Scalar(value, checkpoint)))
    }

    /// Dispatch a CDC event.
    ///
    /// For every [`ReExecutionTrigger`] the inner engine emits, this
    /// method looks up the auth context, awaits the connector's
    /// `execute_scalar`, installs the result, and replaces the trigger
    /// with a [`ScalarUpdate`]. The first connector failure aborts the
    /// rest of the batch and is surfaced as [`ReExecError::Connector`].
    ///
    /// [`ReExecutionTrigger`]: super::ReExecutionTrigger
    pub async fn consumers(
        &mut self,
        event: &E,
    ) -> Result<ReExecNotifications<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        use futures_util::stream::{StreamExt, TryStreamExt};

        let ReExecNotifications {
            engine,
            mut scalar_updates,
            triggers,
        } = self.inner.consumers(event).map_err(ReExecError::Dispatch)?;

        // Pre-filter debounced triggers.
        let actionable: Vec<_> = triggers
            .into_iter()
            .filter(|t| !self.debounce_skip(t.query_id))
            .collect();

        if actionable.is_empty() {
            return Ok(ReExecNotifications {
                engine,
                scalar_updates,
                triggers: Vec::new(),
            });
        }

        // Borrow the immutable fields the futures need. `inner` and
        // `last_reexec_at` stay free for the post-resolution mutation.
        let connector = &self.connector;
        let contexts = &self.contexts;
        let throttle = self
            .permits
            .as_ref()
            .map(|s| (Arc::clone(&s.sem), Arc::clone(&s.inflight)));
        let actionable_len = actionable.len();

        #[allow(clippy::type_complexity)]
        let resolved: Vec<(
            super::ReExecutionTrigger<I, E::Checkpoint>,
            (Value<E::Backend>, Option<X::Checkpoint>),
        )> = futures_util::stream::iter(actionable.into_iter().map(|trigger| {
            let ctx = contexts.get(&trigger.query_id).expect(
                "every captured query stores its resolve context at register time, \
                 trigger.query_id must exist in `contexts`",
            );
            let sql = ctx.sql.clone();
            let column_kind = ctx.column_kind;
            let auth = &ctx.auth;
            let throttle = throttle.clone();
            async move {
                let _guard = acquire_permit(throttle.as_ref(), trigger.query_id).await;
                let result = connector.execute_scalar(&sql, column_kind, auth).await;
                result.map(|r| (trigger, r))
            }
        }))
        .buffer_unordered(actionable_len)
        .try_collect()
        .await
        .map_err(ReExecError::Connector)?;

        // All borrows released. Apply the resolutions.
        for (trigger, (value, _db_checkpoint)) in resolved {
            self.inner.install(trigger.query_id, value.clone());
            self.stamp_reexec(trigger.query_id);
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

    /// Async batch variant of [`consumers`](Self::consumers).
    ///
    /// Runs each event through the inner trigger-emitting engine in input
    /// order, then awaits the connector for each **deduplicated** trigger.
    /// Dispatches the deduplicated triggers concurrently, keeping at most
    /// [`with_max_concurrent_reexecutions`](Self::with_max_concurrent_reexecutions)
    /// in flight at any time (unbounded when not configured). Per-event
    /// engine notifications stay in input order. The returned
    /// [`BatchOutcome::triggers`] is always empty after resolution.
    ///
    /// The first connector failure aborts the whole batch (remaining
    /// in-flight futures are dropped). Partial notifications are
    /// discarded. The caller retries.
    pub async fn consumers_batch(
        &mut self,
        events: &[E],
    ) -> Result<BatchOutcome<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        use futures_util::stream::{StreamExt, TryStreamExt};

        let BatchOutcome {
            per_event,
            mut scalar_updates,
            triggers,
        } = self
            .inner
            .consumers_batch(events)
            .map_err(ReExecError::Dispatch)?;

        // Pre-filter debounced triggers.
        let actionable: alloc::vec::Vec<_> = triggers
            .into_iter()
            .filter(|t| !self.debounce_skip(t.query_id))
            .collect();

        if actionable.is_empty() {
            return Ok(BatchOutcome {
                per_event,
                scalar_updates,
                triggers: Vec::new(),
            });
        }

        // Borrow the immutable fields the futures need. `inner` and
        // `last_reexec_at` stay free for the mutation pass below.
        let connector = &self.connector;
        let contexts = &self.contexts;
        let throttle = self
            .permits
            .as_ref()
            .map(|s| (Arc::clone(&s.sem), Arc::clone(&s.inflight)));
        let actionable_len = actionable.len();

        #[allow(clippy::type_complexity)]
        let resolved: alloc::vec::Vec<(
            super::ReExecutionTrigger<I, E::Checkpoint>,
            (Value<E::Backend>, Option<X::Checkpoint>),
        )> = futures_util::stream::iter(actionable.into_iter().map(|trigger| {
            let ctx = contexts.get(&trigger.query_id).expect(
                "every captured query stores its resolve context at register time, \
                 trigger.query_id must exist in `contexts`",
            );
            let sql = ctx.sql.clone();
            let column_kind = ctx.column_kind;
            let auth = &ctx.auth;
            let throttle = throttle.clone();
            async move {
                let _guard = acquire_permit(throttle.as_ref(), trigger.query_id).await;
                let result = connector.execute_scalar(&sql, column_kind, auth).await;
                result.map(|r| (trigger, r))
            }
        }))
        .buffer_unordered(actionable_len)
        .try_collect()
        .await
        .map_err(ReExecError::Connector)?;

        // All borrows released. Apply the resolutions.
        for (trigger, (value, _db_checkpoint)) in resolved {
            self.inner.install(trigger.query_id, value.clone());
            self.stamp_reexec(trigger.query_id);
            scalar_updates.push(ScalarUpdate {
                query_id: trigger.query_id,
                consumer_id: trigger.consumer_id,
                value,
                checkpoint: trigger.checkpoint.clone(),
            });
        }

        Ok(BatchOutcome {
            per_event,
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

    /// Unregister a captured query by id. Drops its auth context too.
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

impl<E, I, DB, X> crate::AsyncSubscriptionDispatch<I, E> for AsyncAutoResolvingEngine<E, I, DB, X>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    E::Checkpoint: Send + Sync,
    I: IdTypes,
    I::ConsumerId: Send,
    I::SessionId: Send,
    DB: DatabaseLike + Send + Sync + 'static,
    X: AsyncConnector<Backend = E::Backend>,
    X::AuthContext: Send + Sync,
{
    type Notifications = ReExecNotifications<I, E::Backend, E::Checkpoint>;
    type Error = ReExecError<X::Error>;

    #[allow(clippy::manual_async_fn)]
    fn consumers(
        &mut self,
        event: &E,
    ) -> impl core::future::Future<Output = Result<Self::Notifications, Self::Error>> + Send {
        async move { Self::consumers(self, event).await }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::connector::Snapshot;
    use super::*;
    use crate::backend::{BuiltinKind, Postgres};
    use crate::testing::TestEvent;
    use crate::{DefaultIds, NoCheckpoint, SubscriptionEngine, SubscriptionRequest, TableId};
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll};
    use parking_lot::Mutex;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::task::Wake;

    /// No-op `Wake` implementation: the `MockAsyncConnector` futures
    /// never park, so `wake` is never invoked. Built on the safe `Wake`
    /// trait so the workspace's `forbid(unsafe_code)` lint passes. The
    /// nightly-only `Waker::noop` would be the alternative, hence the
    /// `allow` below.
    struct NoopWake;
    #[allow(unknown_lints, clippy::manual_noop_waker)]
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    /// Tiny block-on for the test futures (which never park). Drives the
    /// future to completion by polling. If a real-world future returned
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

    /// `parking_lot::Mutex`-backed mock so the futures are `Send`.
    struct MockAsyncConnector {
        values: Mutex<Vec<Value<Postgres>>>,
        call_count: Mutex<usize>,
    }

    impl MockAsyncConnector {
        fn new(values: Vec<Value<Postgres>>) -> Self {
            Self {
                values: Mutex::new(values),
                call_count: Mutex::new(0),
            }
        }
        fn call_count(&self) -> usize {
            *self.call_count.lock()
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
    // the trait shape. `async fn in trait` cannot express it directly.
    #[allow(clippy::manual_async_fn)]
    impl AsyncConnector for MockAsyncConnector {
        type AuthContext = ();
        type Error = MockError;
        type Checkpoint = NoCheckpoint;
        type Backend = Postgres;

        fn execute_scalar(
            &self,
            _sql: &str,
            _kind: BuiltinKind,
            _auth: &(),
        ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
        {
            async move {
                *self.call_count.lock() += 1;
                let value = self.values.lock().pop().ok_or(MockError("queue empty"))?;
                Ok((value, None))
            }
        }

        fn read_page(
            &self,
            _sql: &str,
            _max_bytes: usize,
            _auth: &(),
        ) -> impl Future<
            Output = Result<
                Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>,
                Self::Error,
            >,
        > + Send {
            async move { Err(MockError("read_page is not exercised by the scalar tests")) }
        }
    }

    fn row(id: i64, price: f64) -> Vec<Value<Postgres>> {
        alloc::vec![
            Value::Int(id),
            Value::Float(price),
            Value::Int(1),
            Value::String("paid".into()),
        ]
    }

    fn insert_event(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::insert(table_id, row(id, price)).with_pk_columns([0u16])
    }

    fn delete_event(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::delete(table_id, row(id, price)).with_pk_columns([0u16])
    }

    fn update_status_only(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::update(table_id, row(id, price), row(id, price))
            .with_pk_columns([0u16])
            .with_changed_columns([3u16])
    }

    fn engine_with_values(
        values: Vec<Value<Postgres>>,
    ) -> (
        AsyncAutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, MockAsyncConnector>,
        TableId,
    ) {
        let database = catalog();
        let orders_id =
            crate::catalog_helpers::table_id(&database, "orders").expect("orders table exists");
        let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            database,
            PostgreSqlDialect {},
        );
        (
            AsyncAutoResolvingEngine::new(
                ReExecEngine::new(inner),
                MockAsyncConnector::new(values),
            ),
            orders_id,
        )
    }

    /// Full path through the async engine: register, snapshot (which
    /// installs), unrelated insert (no connector call), delete of the
    /// extreme (one connector call, ScalarUpdate emitted).
    #[test]
    fn async_engine_dispatch_round_trip() {
        // Two values for: snapshot bootstrap (5.0), delete re-execution (9.0).
        // Mock pops from the back so push in reverse order.
        let (mut e, tid) = engine_with_values(vec![Value::Float(9.0), Value::Float(5.0)]);

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
            SnapshotResult::Scalar(Value::Float(v), None) => {
                assert!((v - 5.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Scalar(5.0, None), got {other:?}"),
        }
        assert_eq!(e.connector().call_count(), 1);

        // Insert above the extreme: in-process Unchanged, no connector call.
        let n = block_on(e.consumers(&insert_event(tid, 2, 9.0))).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(e.connector().call_count(), 1);

        // Delete the extreme: trigger -> connector -> ScalarUpdate.
        let n = block_on(e.consumers(&delete_event(tid, 1, 5.0))).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].query_id, qid);
        assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
        assert!(n.triggers.is_empty(), "async engine drains triggers");
        assert_eq!(e.connector().call_count(), 2);
    }

    /// `unrelated-column` UPDATE skip optimization still works under
    /// the async engine.
    #[test]
    fn async_engine_unrelated_column_update_skips_connector() {
        let (mut e, tid) = engine_with_values(vec![]);
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
        assert!(e.install(qid, Value::Float(10.0)));

        let event = update_status_only(tid, 1, 10.0);

        let n = block_on(e.consumers(&event)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
        assert_eq!(e.connector().call_count(), 0);
    }

    /// `snapshot` on an unknown id returns `Ok(None)`.
    #[test]
    fn async_engine_snapshot_unknown_query_returns_none() {
        let (mut e, _tid) = engine_with_values(vec![]);
        assert!(block_on(e.snapshot(99999)).unwrap().is_none());
        assert_eq!(e.connector().call_count(), 0);
    }

    /// Connector failure aborts the batch with `ReExecError::Connector`.
    #[test]
    fn async_engine_connector_error_aborts_batch() {
        let (mut e, tid) = engine_with_values(vec![]);
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
        assert!(e.install(qid, Value::Float(5.0)));

        match block_on(e.consumers(&delete_event(tid, 1, 5.0))) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector(MockError(msg))) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// Async batch coalesces repeated triggers for the same query into a
    /// single connector call. Mirrors the sync engine's T4.1 assertion.
    #[test]
    fn async_engine_consumers_batch_coalesces_repeated_triggers() {
        let (mut e, tid) = engine_with_values(vec![Value::Float(99.0)]);
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
        assert!(e.install(qid, Value::Float(5.0)));

        let events = vec![
            delete_event(tid, 1, 5.0),
            delete_event(tid, 2, 5.0),
            delete_event(tid, 3, 5.0),
        ];

        let outcome = block_on(e.consumers_batch(&events)).unwrap();
        assert_eq!(outcome.per_event.len(), 3, "per_event positional alignment");
        assert_eq!(
            e.connector().call_count(),
            1,
            "three displacing events collapse to one connector call"
        );
        assert_eq!(outcome.scalar_updates.len(), 1);
        assert_eq!(outcome.scalar_updates[0].value, Value::Float(99.0));
        assert!(outcome.triggers.is_empty());
    }

    /// `with_max_concurrent_reexecutions` does not change the result of
    /// `consumers_batch`. Correctness is preserved. The cap is a
    /// throughput / fairness knob, not a semantic one.
    #[test]
    #[allow(clippy::similar_names)]
    fn async_engine_consumers_batch_respects_max_concurrent_cap() {
        // Two distinct captured queries, each displaced once in the
        // batch. Both must resolve regardless of the cap.
        let (e0, tid) = engine_with_values(vec![Value::Float(22.0), Value::Float(11.0)]);
        let mut e = e0.with_max_concurrent_reexecutions(1);
        let qid1 = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec for MIN"),
        };
        let qid2 = match e
            .register(
                SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec for MAX"),
        };
        assert!(e.install(qid1, Value::Float(7.0)));
        assert!(e.install(qid2, Value::Float(7.0)));

        let events = vec![delete_event(tid, 1, 7.0)];
        let outcome = block_on(e.consumers_batch(&events)).unwrap();
        assert_eq!(e.connector().call_count(), 2);
        assert_eq!(outcome.scalar_updates.len(), 2);
        let qids: std::collections::BTreeSet<_> =
            outcome.scalar_updates.iter().map(|u| u.query_id).collect();
        assert!(qids.contains(&qid1));
        assert!(qids.contains(&qid2));
    }

    /// `unregister_reexec_query` drops the stored auth context.
    #[test]
    fn async_engine_unregister_drops_context() {
        let (mut e, _tid) = engine_with_values(vec![]);
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

    // ----------------------------------------------------------------
    // Re-execution concurrency throttle
    // ----------------------------------------------------------------
    //
    // The `MockAsyncConnector` futures complete in one poll, so the
    // tests here cannot observe the *peak* inflight count during a
    // batch (that needs a real multi-tasking runtime, integration
    // tests exercise it). Unit tests validate the invariants that DO
    // hold under `block_on`: the accessors, the post-batch invariant
    // (`inflight == 0`), the zero-cap normalisation, and the
    // result-preservation contract.

    /// No cap by default: `inflight()` is 0, `concurrency_cap()` is `None`.
    #[test]
    fn throttle_disabled_by_default() {
        let (e, _tid) = engine_with_values(vec![]);
        assert_eq!(e.inflight(), 0);
        assert_eq!(e.concurrency_cap(), None);
    }

    /// `with_max_concurrent_reexecutions(n)` records `n` as the cap and
    /// starts with `inflight() == 0`.
    #[test]
    fn throttle_set_cap_observable_via_accessors() {
        let (e0, _tid) = engine_with_values(vec![]);
        let e = e0.with_max_concurrent_reexecutions(4);
        assert_eq!(e.concurrency_cap(), Some(4));
        assert_eq!(e.inflight(), 0);
    }

    /// `cap = 0` is normalised to 1 to prevent a deadlock on first
    /// `acquire`.
    #[test]
    fn throttle_zero_cap_normalised_to_one() {
        let (e0, _tid) = engine_with_values(vec![]);
        let e = e0.with_max_concurrent_reexecutions(0);
        assert_eq!(e.concurrency_cap(), Some(1));
    }

    /// Cleanup invariant: after a successful `consumers_batch` the
    /// inflight counter is back to 0. Tests that the `InflightGuard`
    /// drop path actually fires when futures complete.
    #[test]
    fn throttle_inflight_returns_to_zero_after_batch() {
        let (e0, tid) = engine_with_values(vec![Value::Float(22.0), Value::Float(11.0)]);
        let mut e = e0.with_max_concurrent_reexecutions(1);
        let qid1 = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        let qid2 = match e
            .register(
                SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        assert!(e.install(qid1, Value::Float(7.0)));
        assert!(e.install(qid2, Value::Float(7.0)));

        let _ = block_on(e.consumers_batch(&[delete_event(tid, 1, 7.0)])).unwrap();
        assert_eq!(
            e.inflight(),
            0,
            "every InflightGuard must drop after batch completes"
        );
    }

    /// Cleanup invariant on the error path: when the connector fails
    /// mid-batch, every permit must still be released.
    #[test]
    fn throttle_inflight_returns_to_zero_after_connector_error() {
        // Two captured queries, only one value in the queue: the second
        // connector call hits "queue empty" and the batch aborts.
        let (e0, tid) = engine_with_values(vec![Value::Float(22.0)]);
        let mut e = e0.with_max_concurrent_reexecutions(2);
        let qid1 = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        let qid2 = match e
            .register(
                SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        assert!(e.install(qid1, Value::Float(7.0)));
        assert!(e.install(qid2, Value::Float(7.0)));

        assert!(block_on(e.consumers_batch(&[delete_event(tid, 1, 7.0)])).is_err());
        assert_eq!(
            e.inflight(),
            0,
            "InflightGuards must drop even when the batch aborts on connector error"
        );
    }

    /// The throttle preserves correctness: total connector call count
    /// equals the number of deduplicated triggers regardless of cap.
    #[test]
    fn throttle_total_call_count_unchanged_with_cap() {
        // Three queries, one trigger each, cap = 1.
        let values = vec![Value::Float(30.0), Value::Float(20.0), Value::Float(10.0)];
        let (e0, tid) = engine_with_values(values);
        let mut e = e0.with_max_concurrent_reexecutions(1);
        let qids: Vec<_> = (1u64..=3)
            .map(|c| {
                match e
                    .register(
                        SubscriptionRequest::new(
                            c,
                            "SELECT MIN(price) FROM orders WHERE quantity = 1",
                        ),
                        (),
                    )
                    .unwrap()
                {
                    Registered::ReExec { query_id, .. } => query_id,
                    Registered::Engine(_) => panic!("expected ReExec"),
                }
            })
            .collect();
        for q in &qids {
            assert!(e.install(*q, Value::Float(7.0)));
        }
        let outcome = block_on(e.consumers_batch(&[delete_event(tid, 1, 7.0)])).unwrap();
        assert_eq!(
            e.connector().call_count(),
            3,
            "three distinct queries each get one connector call regardless of cap"
        );
        assert_eq!(outcome.scalar_updates.len(), 3);
    }
}
