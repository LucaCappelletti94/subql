//! Auto-resolving re-execution engine: wraps [`ReExecEngine`] with a
//! [`Connector`] and converts in-flight [`ReExecutionTrigger`]s into
//! [`ScalarUpdate`]s inline.
//!
//! Pick this engine when you want subql to drive the re-execution loop end
//! to end: pass a `Connector` impl over your database handle and receive
//! `ScalarUpdate`s directly. Keep the bare [`ReExecEngine`] when you want
//! explicit control over re-execution (e.g. cross-batch coalescing,
//! retry-with-backoff, or auth that does not fit the [`Connector::AuthContext`]
//! shape).
//!
//! See [`super::connector`] for the trait contract and error semantics.
//!
//! [`ReExecutionTrigger`]: super::ReExecutionTrigger

use super::connector::{Connector, ReExecError};
use super::engine::{
    BatchOutcome, ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecUnregisterReport,
    Registered, ScalarUpdate,
};
use crate::clock::{duration_between, ClockHandle};
use crate::RowImage;
use crate::{
    Cell, ColumnType, IdTypes, RegisterError, SubscriptionRequest, SubscriptionScope,
    UnregisterReport, WalEvent,
};
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;
use sqlparser::dialect::Dialect;

/// Result of [`AutoResolvingEngine::snapshot`]: the captured query's
/// current value at a known position in the source stream.
///
/// Tagged so future captured-query flavors (single-table row re-execution,
/// multi-table aggregate re-execution) can be added without changing the
/// engine method's signature.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SnapshotResult<C: crate::Checkpoint> {
    /// Single-table scalar (today's only flavor): a `MIN`/`MAX` value.
    Scalar(Cell, Option<C>),
    /// Reserved: a single-table row set. Will surface when total row
    /// re-execution lands.
    Rows(Vec<RowImage>, Option<C>),
}

/// Per-query state needed to drive an automatic re-execution.
///
/// Shared by both the sync and async engines; private to the `reexec`
/// module so the engine internals can read its fields directly.
pub(super) struct ResolveContext<I: IdTypes, A> {
    /// Re-execution SQL produced by the plan.
    pub(super) sql: String,
    /// Decode type for the scalar result.
    pub(super) column_type: ColumnType,
    /// Session owning the query, used to drop contexts on
    /// [`unregister_session`](AutoResolvingEngine::unregister_session).
    pub(super) session: Option<I::SessionId>,
    /// Per-subscription auth state, passed verbatim to the connector.
    pub(super) auth: A,
}

/// Opt-in wrapper that auto-resolves [`ReExecutionTrigger`](super::ReExecutionTrigger)s
/// by calling a [`Connector`].
///
/// Behavior contract:
/// * `register` delegates to the inner [`ReExecEngine`] and stores the
///   per-subscription [`Connector::AuthContext`] alongside the SQL and
///   decode type returned via [`Registered::ReExec`].
/// * `consumers` delegates to the inner engine for filtering, then drains
///   each emitted trigger: for each, the connector is called with the
///   stored auth context, the returned value is installed via the inner
///   engine, and a [`ScalarUpdate`] is appended to the result. The
///   returned [`ReExecNotifications::triggers`] is always empty.
/// * A single connector failure aborts the rest of the batch and returns
///   [`ReExecError::Connector`].
pub struct AutoResolvingEngine<D, I, DB, X>
where
    D: Dialect,
    I: IdTypes,
    DB: DatabaseLike,
    X: Connector,
{
    inner: ReExecEngine<D, I, DB>,
    connector: X,
    contexts: HashMap<ReExecQueryId, ResolveContext<I, X::AuthContext>>,
    /// Optional [`Clock`](crate::Clock) used for per-query debounce.
    clock: Option<ClockHandle>,
    /// Minimum interval between two re-executions of the same captured
    /// query. `None` means no debounce. Requires `clock` to be set.
    debounce: Option<Duration>,
    /// Last `now_micros` at which each captured query was re-executed.
    /// Populated only when `clock` + `debounce` are set.
    last_reexec_at: HashMap<ReExecQueryId, u64>,
}

impl<D, I, DB, X> AutoResolvingEngine<D, I, DB, X>
where
    D: Dialect,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: Connector,
{
    /// Wrap an existing [`ReExecEngine`] with the given [`Connector`].
    pub fn new(inner: ReExecEngine<D, I, DB>, connector: X) -> Self {
        Self {
            inner,
            connector,
            contexts: HashMap::new(),
            clock: None,
            debounce: None,
            last_reexec_at: HashMap::new(),
        }
    }

    /// Attach a [`Clock`](crate::Clock) for time-based decisions
    /// (per-query debounce). Defaults to no clock; without one, debounce
    /// is silently disabled even if [`with_debounce_per_query`](Self::with_debounce_per_query)
    /// is set.
    #[must_use]
    pub fn with_clock(mut self, clock: ClockHandle) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Configure a minimum interval between two re-executions of the
    /// same captured query.
    ///
    /// Triggers within the window are silently dropped: the connector is
    /// not called and no [`ScalarUpdate`] is emitted (the engine's stored
    /// value, set by the prior re-execution, remains current). Requires
    /// [`with_clock`](Self::with_clock); without a clock the debounce
    /// config is ignored.
    #[must_use]
    pub const fn with_debounce_per_query(mut self, debounce: Duration) -> Self {
        self.debounce = Some(debounce);
        self
    }

    /// The wrapped trigger-emitting engine.
    pub const fn inner(&self) -> &ReExecEngine<D, I, DB> {
        &self.inner
    }

    /// The connector this engine drives.
    pub const fn connector(&self) -> &X {
        &self.connector
    }

    /// Whether the trigger for `query_id` should be skipped under the
    /// debounce policy. Returns `false` (proceed) when no clock + debounce
    /// is configured, when no prior re-exec is known, or when enough time
    /// has elapsed.
    fn debounce_skip(&self, query_id: ReExecQueryId) -> bool {
        let (Some(clock), Some(window)) = (self.clock.as_ref(), self.debounce) else {
            return false;
        };
        let Some(last_micros) = self.last_reexec_at.get(&query_id).copied() else {
            return false;
        };
        duration_between(last_micros, clock.now_micros()) < window
    }

    /// Stamp `query_id` with the current clock instant. No-op if clock is
    /// not configured; debounce stamping is unnecessary without a clock.
    fn stamp_reexec(&mut self, query_id: ReExecQueryId) {
        if let Some(clock) = self.clock.as_ref() {
            self.last_reexec_at.insert(query_id, clock.now_micros());
        }
    }

    /// Number of captured re-execution queries (matches the inner engine).
    pub fn reexec_query_count(&self) -> usize {
        self.inner.reexec_query_count()
    }

    /// Register a subscription. `auth` is stored alongside the captured
    /// query and re-presented to the [`Connector`] on each re-execution.
    /// Engine-supported queries pass through unchanged (no auth stored).
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

    /// Install a value directly, bypassing the connector. Mirrors
    /// [`ReExecEngine::install`] for callers that occasionally need to seed
    /// or override a captured query's state (e.g. a snapshot bootstrap).
    pub fn install(&mut self, query_id: ReExecQueryId, value: Cell) -> bool {
        self.inner.install(query_id, value)
    }

    /// Bootstrap a captured query by reading its current value through the
    /// connector and installing the result.
    ///
    /// Returns a [`SnapshotResult`] tagged with the connector's
    /// [`Checkpoint`](Connector::Checkpoint). Today every captured query
    /// is a scalar (single-table `MIN`/`MAX`), so the variant is always
    /// `Scalar`; the enum leaves room for the future single-table row
    /// re-execution flavor without a breaking-bump.
    ///
    /// The value is also installed via [`ReExecEngine::install`], so the
    /// engine is fully primed once this returns. Subsequent
    /// [`consumers`](Self::consumers) calls see the snapshot as the
    /// starting state.
    ///
    /// # Errors
    ///
    /// Returns [`ReExecError::Connector`] if the connector fails. Returns
    /// `Ok(None)` if `query_id` does not exist; the absence is signaled
    /// (rather than panicking) so callers can race a snapshot against an
    /// `unregister_*` without crashing.
    pub fn snapshot(
        &mut self,
        query_id: ReExecQueryId,
    ) -> Result<Option<SnapshotResult<X::Checkpoint>>, ReExecError<X::Error>> {
        let Some(ctx) = self.contexts.get(&query_id) else {
            return Ok(None);
        };
        let (value, checkpoint) = self
            .connector
            .execute_scalar(&ctx.sql, ctx.column_type, &ctx.auth)
            .map_err(ReExecError::Connector)?;
        self.inner.install(query_id, value.clone());
        Ok(Some(SnapshotResult::Scalar(value, checkpoint)))
    }

    /// Dispatch a CDC event.
    ///
    /// For every [`ReExecutionTrigger`] the inner engine emits, this method
    /// looks up the captured query's auth context, calls
    /// [`Connector::execute_scalar`] with the plan's SQL and decode type,
    /// installs the result via [`ReExecEngine::install`], and pushes a
    /// [`ScalarUpdate`] in the trigger's place. The returned
    /// [`ReExecNotifications::triggers`] is always empty under this engine.
    ///
    /// The first connector failure aborts the rest of the batch and is
    /// surfaced as [`ReExecError::Connector`].
    ///
    /// [`ReExecutionTrigger`]: super::ReExecutionTrigger
    pub fn consumers<C: crate::Checkpoint>(
        &mut self,
        event: &WalEvent<C>,
    ) -> Result<ReExecNotifications<I, C>, ReExecError<X::Error>> {
        let ReExecNotifications {
            engine,
            mut scalar_updates,
            triggers,
        } = self.inner.consumers(event)?;

        for trigger in triggers {
            // Debounce: drop the trigger if we re-executed this query
            // within the configured window. The engine's installed value
            // (set by the prior re-exec) stays the current view; no
            // ScalarUpdate emitted.
            if self.debounce_skip(trigger.query_id) {
                continue;
            }
            let ctx = self.contexts.get(&trigger.query_id).expect(
                "every captured query stores its resolve context at register time; \
                 trigger.query_id must exist in `contexts`",
            );
            // The connector also reports the position at which it took
            // the read (in `_db_checkpoint`); we ignore it here because
            // the ScalarUpdate is correlated with the *event* that
            // triggered the re-execution, not the DB-side read position.
            // Snapshots use the DB-side checkpoint instead (see `snapshot`).
            let (value, _db_checkpoint) = self
                .connector
                .execute_scalar(&ctx.sql, ctx.column_type, &ctx.auth)
                .map_err(ReExecError::Connector)?;
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

    /// Batch variant of [`consumers`](Self::consumers).
    ///
    /// Runs each event through the inner trigger-emitting engine in input
    /// order, then resolves the **deduplicated** triggers serially via the
    /// connector. With N events that displace the same captured query K
    /// times, the connector is called once instead of K times. Per-event
    /// engine notifications stay in input order; the returned
    /// [`BatchOutcome::triggers`] is always empty after resolution.
    ///
    /// The first connector failure aborts the whole batch; partial
    /// notifications are dropped. The caller is expected to retry the
    /// batch.
    pub fn consumers_batch<C: crate::Checkpoint>(
        &mut self,
        events: &[WalEvent<C>],
    ) -> Result<BatchOutcome<I, C>, ReExecError<X::Error>> {
        let BatchOutcome {
            per_event,
            mut scalar_updates,
            triggers,
        } = self.inner.consumers_batch(events)?;

        for trigger in triggers {
            if self.debounce_skip(trigger.query_id) {
                continue;
            }
            let ctx = self.contexts.get(&trigger.query_id).expect(
                "every captured query stores its resolve context at register time; \
                 trigger.query_id must exist in `contexts`",
            );
            let (value, _db_checkpoint) = self
                .connector
                .execute_scalar(&ctx.sql, ctx.column_type, &ctx.auth)
                .map_err(ReExecError::Connector)?;
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

    /// Unregister a captured query by id; drops its auth context too.
    /// Returns false if no such query existed.
    pub fn unregister_reexec_query(&mut self, query_id: ReExecQueryId) -> bool {
        let removed = self.inner.unregister_reexec_query(query_id);
        if removed {
            self.contexts.remove(&query_id);
        }
        removed
    }

    /// Unregister an engine subscription by `(consumer_id, sql)`. No
    /// captured query is affected. Mirrors [`ReExecEngine::unregister_query`].
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
    use super::*;
    use crate::{ColumnId, DefaultIds, RowImage, SubscriptionEngine};
    use alloc::sync::Arc;
    use core::cell::RefCell;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    /// Records every call and serves a programmed value queue. Errors are
    /// modeled by leaving the queue empty when `panic_on_empty` is false.
    struct MockConnector {
        values: RefCell<alloc::vec::Vec<Cell>>,
        calls: RefCell<alloc::vec::Vec<(String, ColumnType)>>,
    }

    impl MockConnector {
        fn new(values: alloc::vec::Vec<Cell>) -> Self {
            Self {
                values: RefCell::new(values),
                calls: RefCell::new(alloc::vec::Vec::new()),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.borrow().len()
        }
    }

    #[derive(Debug, PartialEq)]
    struct MockError(&'static str);

    impl core::fmt::Display for MockError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Connector for MockConnector {
        type AuthContext = ();
        type Error = MockError;
        type Checkpoint = crate::NoCheckpoint;

        fn execute_scalar(
            &self,
            sql: &str,
            column_type: ColumnType,
            _auth: &(),
        ) -> Result<(Cell, Option<Self::Checkpoint>), Self::Error> {
            self.calls
                .borrow_mut()
                .push((String::from(sql), column_type));
            let cell = self
                .values
                .borrow_mut()
                .pop()
                .ok_or(MockError("queue empty"))?;
            Ok((cell, None))
        }

        fn execute_rows(
            &self,
            _sql: &str,
            _auth: &(),
        ) -> Result<super::super::connector::Snapshot<Vec<RowImage>, Self::Checkpoint>, Self::Error>
        {
            #[allow(clippy::unimplemented)]
            {
                unimplemented!("MockConnector::execute_rows is not exercised in v1 tests")
            }
        }
    }

    /// orders columns: id=0, price=1, quantity=2, status=3.
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

    fn update_status_only(id: i64, price: f64) -> WalEvent {
        WalEvent::builder(0)
            .update()
            .new_row(row(price))
            .pk_cell(0, Cell::Int(id))
            .maybe_old_row(Some(row(price)))
            .changed_columns(Arc::from([3 as ColumnId]))
            .build()
            .unwrap()
    }

    fn engine_with_values(
        values: alloc::vec::Vec<Cell>,
    ) -> AutoResolvingEngine<PostgreSqlDialect, DefaultIds, ParserDB, MockConnector> {
        let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            Arc::new(catalog()),
            PostgreSqlDialect {},
        );
        AutoResolvingEngine::new(ReExecEngine::new(inner), MockConnector::new(values))
    }

    /// Full path: register, bootstrap install, insert that does not displace
    /// the extreme (in-process scalar update, no connector call), delete of
    /// the current extreme (trigger -> connector -> ScalarUpdate). The
    /// returned notifications carry no triggers under AutoResolvingEngine.
    #[test]
    fn delete_of_extreme_resolves_via_connector() {
        // Connector returns 7.0 when re-run after the extreme is removed.
        let mut e = engine_with_values(alloc::vec![Cell::Float(7.0)]);

        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };
        // Bootstrap: model = {1=>5.0}. Current MIN = 5.0.
        assert!(e.install(qid, Cell::Float(5.0)));

        // Insert price=9.0 (>5.0): in-process Unchanged, no scalar update, no trigger.
        let n = e.consumers(&insert_event(2, 9.0)).unwrap();
        assert!(n.scalar_updates.is_empty(), "insert above extreme");
        assert!(n.triggers.is_empty(), "auto-resolving never emits triggers");
        assert_eq!(e.connector().call_count(), 0, "no re-execution yet");

        // Delete id=1, price=5.0 (the current extreme): trigger -> connector -> 7.0.
        let n = e.consumers(&delete_event(1, 5.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].query_id, qid);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(7.0));
        assert!(
            n.triggers.is_empty(),
            "AutoResolvingEngine consumes triggers internally"
        );
        assert_eq!(e.connector().call_count(), 1);
        let (sql, ct) = e.connector().calls.borrow()[0].clone();
        assert!(sql.contains("MIN"));
        assert_eq!(ct, ColumnType::Float);
    }

    #[test]
    fn unrelated_column_update_does_not_call_connector() {
        let mut e = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };
        assert!(e.install(qid, Cell::Float(10.0)));

        let n = e.consumers(&update_status_only(1, 10.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
        assert_eq!(e.connector().call_count(), 0);
    }

    #[test]
    fn connector_error_aborts_batch() {
        // Empty queue: the connector errors on first call.
        let mut e = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };
        assert!(e.install(qid, Cell::Float(5.0)));

        match e.consumers(&delete_event(1, 5.0)) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector(MockError(msg))) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// `snapshot(query_id)` reads through the connector and installs the
    /// value so subsequent dispatches see it as the current state.
    #[test]
    fn snapshot_installs_via_connector() {
        let mut e = engine_with_values(alloc::vec![Cell::Float(12.5)]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };

        // No bootstrap install: snapshot does it.
        let snap = e.snapshot(qid).unwrap().expect("query_id exists");
        match snap {
            SnapshotResult::Scalar(value, checkpoint) => {
                assert_eq!(value, Cell::Float(12.5));
                // MockConnector returns checkpoint = None.
                assert!(checkpoint.is_none());
            }
            SnapshotResult::Rows(_, _) => panic!("expected Scalar"),
        }
        assert_eq!(e.connector().call_count(), 1);

        // After snapshot, the engine treats 12.5 as the current MIN.
        // An insert below it (e.g. 9.0) becomes the new in-process MIN.
        let n = e.consumers(&insert_event(2, 9.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(9.0));
        // No additional connector call: the insert was resolved in-process.
        assert_eq!(e.connector().call_count(), 1);
    }

    /// `snapshot(query_id)` on an unknown id returns `Ok(None)` rather
    /// than panicking so callers can race snapshot against unregister.
    #[test]
    fn snapshot_unknown_query_returns_none() {
        let mut e = engine_with_values(alloc::vec![]);
        assert!(e.snapshot(99999).unwrap().is_none());
        // Connector was never called.
        assert_eq!(e.connector().call_count(), 0);
    }

    /// T4.1 + T4.2: a batch of 3 events that displace the same captured
    /// query's extreme produces ONE connector call (dedup), and engine
    /// notifications come back in input order.
    #[test]
    fn consumers_batch_coalesces_repeated_triggers() {
        // Connector serves a single value, which is what we expect since
        // the trigger should be deduplicated to one call.
        let mut e = engine_with_values(alloc::vec![Cell::Float(99.0)]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };
        // Bootstrap: extreme is 5.0.
        assert!(e.install(qid, Cell::Float(5.0)));

        // Three DELETEs of the current extreme. Each one in isolation
        // would emit a trigger; the batch should collapse them.
        let events = alloc::vec![
            delete_event(1, 5.0),
            delete_event(2, 5.0),
            delete_event(3, 5.0),
        ];

        let outcome = e.consumers_batch(&events).unwrap();
        assert_eq!(
            outcome.per_event.len(),
            3,
            "per_event must align positionally with input"
        );
        assert_eq!(
            e.connector().call_count(),
            1,
            "three displacing events must collapse to one connector call"
        );
        assert_eq!(outcome.scalar_updates.len(), 1);
        assert_eq!(outcome.scalar_updates[0].value, Cell::Float(99.0));
        assert!(
            outcome.triggers.is_empty(),
            "auto-resolving drains triggers"
        );
    }

    /// T4.3: a connector failure mid-batch aborts the whole batch with
    /// `ReExecError::Connector` and the caller is expected to retry.
    #[test]
    fn consumers_batch_connector_error_aborts() {
        // Empty value queue: connector errors on first call.
        let mut e = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };
        assert!(e.install(qid, Cell::Float(5.0)));

        let events = alloc::vec![delete_event(1, 5.0)];

        match e.consumers_batch(&events) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector(MockError(msg))) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// Coalescing only collapses the **same** `query_id`. Distinct captured
    /// queries each trigger their own connector call.
    #[test]
    #[allow(clippy::similar_names)]
    fn consumers_batch_does_not_coalesce_distinct_queries() {
        // Two captured queries on the same table. Connector returns 11.0
        // (popped first) for one and 22.0 (popped second) for the other.
        // MockConnector pops from the back, so push values in reverse:
        // first pop = 22.0, second pop = 11.0.
        let mut e = engine_with_values(alloc::vec![Cell::Float(22.0), Cell::Float(11.0)]);
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
        // Bootstrap both at 7.0 so deleting price=7.0 displaces both.
        assert!(e.install(qid1, Cell::Float(7.0)));
        assert!(e.install(qid2, Cell::Float(7.0)));

        let events = alloc::vec![delete_event(1, 7.0)];
        let outcome = e.consumers_batch(&events).unwrap();
        assert_eq!(e.connector().call_count(), 2, "one call per distinct query");
        assert_eq!(outcome.scalar_updates.len(), 2);
        let qids: alloc::collections::BTreeSet<_> =
            outcome.scalar_updates.iter().map(|u| u.query_id).collect();
        assert!(qids.contains(&qid1));
        assert!(qids.contains(&qid2));
    }

    /// T6.1: a second displacing event within the debounce window is
    /// dropped: connector is not called and no ScalarUpdate is emitted.
    /// T6.2 in the same test: after the clock ticks past the window the
    /// next trigger fires normally.
    #[test]
    // The `.clone()` below needs the Arc<ManualClock> -> Arc<dyn Clock>
    // unsize coercion at the assignment site; `Arc::clone(&clock)` would
    // need an already-coerced source. Allow the clippy lint here.
    #[allow(clippy::clone_on_ref_ptr)]
    fn debounce_skips_within_window_and_fires_after() {
        let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
        let engine_clock: crate::ClockHandle = clock.clone();
        // Two values in the connector queue: one for the first re-exec,
        // one for the post-window re-exec. The "within window" re-exec
        // is debounced and never reaches the connector.
        let mut e = engine_with_values(alloc::vec![Cell::Float(20.0), Cell::Float(7.0)])
            .with_clock(engine_clock)
            .with_debounce_per_query(core::time::Duration::from_millis(100));

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

        // First displacing event: re-exec proceeds (no prior stamp).
        let n = e.consumers(&delete_event(1, 5.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(7.0));
        assert_eq!(e.connector().call_count(), 1);

        // Second displacing event within 50ms (window is 100ms): skipped.
        clock.advance(core::time::Duration::from_millis(50));
        // The engine's MIN is currently 7.0 from the prior re-exec. To
        // force a second trigger we delete a row matching 7.0.
        let n = e.consumers(&delete_event(2, 7.0)).unwrap();
        assert!(
            n.scalar_updates.is_empty(),
            "debounced trigger must not emit a ScalarUpdate"
        );
        assert_eq!(
            e.connector().call_count(),
            1,
            "debounced trigger must not call the connector"
        );

        // Past the window now: 50ms + 100ms = 150ms total since first.
        clock.advance(core::time::Duration::from_millis(100));
        // Reinstall the value the engine thinks is current so the next
        // displacement is well-defined; the test exercises debounce,
        // not the state machine.
        assert!(e.install(qid, Cell::Float(7.0)));
        let n = e.consumers(&delete_event(3, 7.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1, "post-window trigger must fire");
        assert_eq!(n.scalar_updates[0].value, Cell::Float(20.0));
        assert_eq!(e.connector().call_count(), 2);
    }

    /// Without a configured clock, `with_debounce_per_query` is a no-op:
    /// triggers fire as normal.
    #[test]
    fn debounce_without_clock_is_a_noop() {
        let mut e = engine_with_values(alloc::vec![Cell::Float(9.0), Cell::Float(7.0)])
            .with_debounce_per_query(core::time::Duration::from_secs(3600));

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

        assert!(!e
            .consumers(&delete_event(1, 5.0))
            .unwrap()
            .scalar_updates
            .is_empty());
        // The engine now has 7.0 installed. Deleting it again fires another
        // trigger - the debounce-without-clock case must NOT skip it.
        let n = e.consumers(&delete_event(2, 7.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1, "no clock -> no debounce");
        assert_eq!(e.connector().call_count(), 2);
    }

    #[test]
    fn unregister_drops_auth_context() {
        let mut e = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec, got Engine"),
        };
        assert_eq!(e.contexts.len(), 1);
        assert!(e.unregister_reexec_query(qid));
        assert_eq!(e.contexts.len(), 0);
        assert!(!e.unregister_reexec_query(qid), "second drop is a no-op");
    }
}
