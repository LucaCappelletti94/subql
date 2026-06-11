//! Front-door wrapper that delegates to the core engine and captures the
//! queries it rejects (single-table scalar `MIN`/`MAX`) for re-execution.
//!
//! subql is **DB-free**: it classifies and maintains in-process. When the
//! maintenance state machine cannot resolve a result on its own (extreme
//! removal, or future Total/aggregate triggers), the engine emits a
//! [`ReExecutionTrigger`] for the caller. The caller (the Subscription
//! Materializer in connetto-rs) services the trigger - re-runs the query,
//! authorizes, builds a patchset - and feeds the recomputed value back via
//! [`ReExecEngine::install`].

use super::maintain::{Maintenance, MinMaxQuery, QueryRuntime};
use super::plan::{build_plan, MinMaxPlan, QueryPlan};
use crate::catalog_helpers::table_has_rls;
use crate::compiler::Vm;
use crate::{
    Cell, ColumnType, ConsumerNotifications, DispatchError, EventKind, IdTypes, RegisterError,
    RegisterResult, SubscriptionEngine, SubscriptionRequest, SubscriptionScope, TableId,
    UnregisterReport, WalEvent,
};
use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use sql_traits::prelude::DatabaseLike;
use sqlparser::dialect::Dialect;

/// Identifier for a captured re-execution query, assigned by the wrapper.
pub type ReExecQueryId = u64;

/// A captured re-execution query: the table it reads (for cleanup routing),
/// who owns it, and the maintenance state machine that tracks its value.
struct ReExecEntry<I: IdTypes> {
    /// Consumer that registered the query.
    consumer_id: I::ConsumerId,
    /// Session owning the query, if session-scoped (for cleanup).
    session: Option<I::SessionId>,
    /// Table the query reads from (routing + cleanup).
    table_id: TableId,
    /// In-process maintenance state machine.
    runtime: QueryRuntime,
}

/// Outcome of registering a query through the wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Registered {
    /// The core engine accepted the query (rows or a delta-composable
    /// aggregate).
    Engine(RegisterResult),
    /// The query was captured for re-execution. The materializer must
    /// bootstrap (run `sql`, decode the scalar as `column_type`) and call
    /// [`ReExecEngine::install`] with the initial value. Until that happens
    /// the wrapper holds `Cell::Null` for this query.
    ReExec {
        /// Wrapper-assigned id for the captured query.
        query_id: ReExecQueryId,
        /// SQL to run for the initial value and for any later trigger.
        sql: String,
        /// Decode type for the scalar result.
        column_type: ColumnType,
    },
}

/// A re-executed scalar whose value changed, to be delivered to its consumer.
///
/// Emitted when the in-process maintenance state machine produced a new value
/// from the event's row image (no DB round-trip needed).
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarUpdate<I: IdTypes> {
    /// The captured query whose value changed.
    pub query_id: ReExecQueryId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// The new scalar value (`Cell::Null` if the aggregate is now empty).
    pub value: Cell,
}

/// Signal that a captured query needs to be re-executed.
///
/// Emitted when the in-process maintenance state machine cannot resolve the
/// result for an event. The caller (materializer) must re-execute the query
/// and call [`ReExecEngine::install`] with the recomputed value.
///
/// Triggers are designed to be **idempotent and coalescible**: emitting the
/// same trigger twice is safe (a single re-execution serves any number of
/// pending triggers), and `install` unconditionally overwrites the stored
/// value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReExecutionTrigger<I: IdTypes> {
    /// The captured query needing re-execution.
    pub query_id: ReExecQueryId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
}

/// Combined dispatch result: the core engine's per-consumer notifications,
/// any in-process scalar updates, and any re-execution triggers for the
/// materializer to service.
pub struct ReExecNotifications<I: IdTypes> {
    /// View-relative notifications from the core engine.
    pub engine: ConsumerNotifications<I>,
    /// Scalar values that changed in-process (no DB round-trip).
    pub scalar_updates: Vec<ScalarUpdate<I>>,
    /// Queries whose maintenance could not resolve in-process; the
    /// materializer must re-execute and call [`ReExecEngine::install`].
    pub triggers: Vec<ReExecutionTrigger<I>>,
}

/// Counts from unregistering through the wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReExecUnregisterReport {
    /// Core-engine unregistration counts.
    pub engine: UnregisterReport,
    /// Number of captured re-execution queries removed.
    pub reexec_queries_removed: usize,
}

/// Front-door wrapper over [`SubscriptionEngine`].
///
/// Registration flows through this type: queries the core engine supports pass
/// straight through, and single-table scalar `MIN`/`MAX` queries it rejects are
/// captured here and maintained incrementally. The wrapper never opens a
/// database connection; the materializer services [`ReExecutionTrigger`]s and
/// calls [`install`](Self::install).
pub struct ReExecEngine<D: Dialect, I: IdTypes, DB: DatabaseLike> {
    inner: SubscriptionEngine<D, I, DB>,
    reexec: HashMap<ReExecQueryId, ReExecEntry<I>>,
    /// Table -> captured queries reading from it (CDC event routing).
    table_deps: HashMap<TableId, HashSet<ReExecQueryId>>,
    /// Session -> its captured queries (session-scoped cleanup).
    session_index: HashMap<I::SessionId, Vec<ReExecQueryId>>,
    /// VM reused for in-process WHERE-membership evaluation during dispatch.
    vm: Vm,
    next_id: ReExecQueryId,
}

impl<D: Dialect, I: IdTypes, DB: DatabaseLike + 'static> ReExecEngine<D, I, DB> {
    /// Wrap an existing engine. No database connection is taken or required.
    pub fn new(inner: SubscriptionEngine<D, I, DB>) -> Self {
        Self {
            inner,
            reexec: HashMap::new(),
            table_deps: HashMap::new(),
            session_index: HashMap::new(),
            vm: Vm::new(),
            next_id: 1,
        }
    }

    /// The wrapped core engine.
    pub const fn inner(&self) -> &SubscriptionEngine<D, I, DB> {
        &self.inner
    }

    /// Mutable access to the wrapped core engine.
    pub const fn inner_mut(&mut self) -> &mut SubscriptionEngine<D, I, DB> {
        &mut self.inner
    }

    /// Number of captured re-execution queries.
    pub fn reexec_query_count(&self) -> usize {
        self.reexec.len()
    }

    /// Register a subscription.
    ///
    /// Queries the core engine supports are delegated to it
    /// ([`Registered::Engine`]). A single-table scalar `MIN`/`MAX` it rejects
    /// is captured for re-execution and returned as
    /// [`Registered::ReExec`] - the materializer must then bootstrap the
    /// initial value (run `sql`) and call [`install`](Self::install). Any
    /// other engine rejection surfaces as a [`RegisterError`].
    pub fn register(&mut self, spec: SubscriptionRequest<I>) -> Result<Registered, RegisterError> {
        // Capture what tracking needs before `spec` is moved into the inner
        // engine (which consumes it).
        let sql = spec.sql.clone();
        let consumer_id = spec.consumer_id;
        let session = match &spec.scope {
            SubscriptionScope::Session(s) => Some(*s),
            SubscriptionScope::Durable => None,
        };

        match self.inner.register(spec) {
            Ok(result) => Ok(Registered::Engine(result)),
            Err(RegisterError::UnsupportedSql(msg)) => {
                match build_plan(&sql, self.inner.dialect(), self.inner.database()) {
                    Ok(QueryPlan::Partial(plan)) => {
                        // Per-viewer RLS makes a shared in-process IVM state
                        // unsafe: different consumers observe different rows.
                        // Hard-reject the registration; total re-execution
                        // with per-consumer state is a planned follow-on.
                        if table_has_rls(self.inner.database(), plan.table_id).unwrap_or(false) {
                            return Err(RegisterError::AggregatorOnRlsTable {
                                table_id: plan.table_id,
                            });
                        }
                        Ok(self.capture(plan, consumer_id, session))
                    }
                    // Not a re-executable query (build_plan rejected it):
                    // surface the engine's original rejection.
                    Err(_) => Err(RegisterError::UnsupportedSql(msg)),
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Build the maintenance runtime, populate the routing/session indexes,
    /// and return the `ReExec` registration. The initial value is `Cell::Null`
    /// until the materializer bootstraps via [`install`](Self::install).
    fn capture(
        &mut self,
        plan: MinMaxPlan,
        consumer_id: I::ConsumerId,
        session: Option<I::SessionId>,
    ) -> Registered {
        let MinMaxPlan {
            table_id,
            kind,
            agg_column,
            column_type,
            dependency_columns,
            where_program,
            reexec_sql,
        } = plan;

        // The materializer is responsible for the initial value; subql holds
        // `Cell::Null` ("empty set / unknown") until `install` arrives.
        let runtime = QueryRuntime::Partial(MinMaxQuery::new(
            kind,
            agg_column,
            where_program,
            dependency_columns,
            Cell::Null,
        ));

        let query_id = self.next_id;
        self.next_id += 1;

        self.table_deps
            .entry(table_id)
            .or_default()
            .insert(query_id);
        if let Some(s) = session {
            self.session_index.entry(s).or_default().push(query_id);
        }
        self.reexec.insert(
            query_id,
            ReExecEntry {
                consumer_id,
                session,
                table_id,
                runtime,
            },
        );

        Registered::ReExec {
            query_id,
            sql: reexec_sql,
            column_type,
        }
    }

    /// Install a value computed by the materializer (initial bootstrap, or
    /// the refreshed value after servicing a [`ReExecutionTrigger`]). Returns
    /// `true` if the query exists. Calls are idempotent: repeated `install`s
    /// unconditionally overwrite the stored value.
    pub fn install(&mut self, query_id: ReExecQueryId, value: Cell) -> bool {
        if let Some(entry) = self.reexec.get_mut(&query_id) {
            entry.runtime.install(value);
            true
        } else {
            false
        }
    }

    /// Unregister everything for a session: delegates to the core engine, then
    /// removes every captured re-execution query owned by the session.
    pub fn unregister_session(&mut self, session_id: I::SessionId) -> ReExecUnregisterReport {
        let engine = self.inner.unregister_session(session_id);

        let query_ids = self
            .session_index
            .get(&session_id)
            .cloned()
            .unwrap_or_default();
        let mut reexec_queries_removed = 0;
        for query_id in query_ids {
            if self.remove_reexec_entry(query_id) {
                reexec_queries_removed += 1;
            }
        }

        ReExecUnregisterReport {
            engine,
            reexec_queries_removed,
        }
    }

    /// Unregister a single captured re-execution query by the id returned from
    /// [`register`](Self::register). Returns `false` if no such query exists.
    pub fn unregister_reexec_query(&mut self, query_id: ReExecQueryId) -> bool {
        self.remove_reexec_entry(query_id)
    }

    /// Unregister a core-engine subscription by `(consumer_id, sql)`. Does not
    /// affect captured re-execution queries, which are removed by id via
    /// [`unregister_reexec_query`](Self::unregister_reexec_query).
    pub fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        self.inner.unregister_query(consumer_id, sql)
    }

    /// Dispatch a CDC event.
    ///
    /// Delegates to the core engine for per-consumer notifications, then runs
    /// the maintenance state machine for every captured query reading the
    /// event's table. The result carries in-process [`ScalarUpdate`]s and any
    /// [`ReExecutionTrigger`]s the materializer must service.
    pub fn consumers(&mut self, event: &WalEvent) -> Result<ReExecNotifications<I>, DispatchError> {
        let engine = match self.inner.consumers(event) {
            Ok(notifs) => notifs,
            // A table with only re-execution queries has no engine partition;
            // there are no per-consumer notifications, but the re-execution
            // queries still need to run.
            Err(DispatchError::UnknownTableId(_))
                if self.table_deps.contains_key(&event.table_id()) =>
            {
                ConsumerNotifications::empty()
            }
            Err(e) => return Err(e),
        };

        let (scalar_updates, triggers) = self.dispatch_reexec(event);

        Ok(ReExecNotifications {
            engine,
            scalar_updates,
            triggers,
        })
    }

    /// Feed `event` to each captured query reading its table. Returns
    /// (in-process scalar updates, re-execution triggers).
    fn dispatch_reexec(
        &mut self,
        event: &WalEvent,
    ) -> (Vec<ScalarUpdate<I>>, Vec<ReExecutionTrigger<I>>) {
        // Snapshot affected ids before borrowing `reexec`/`vm` mutably.
        let query_ids: Vec<ReExecQueryId> = match self.table_deps.get(&event.table_id()) {
            Some(ids) => ids.iter().copied().collect(),
            None => return (Vec::new(), Vec::new()),
        };

        // Split-borrow disjoint fields so `on_event` can take `&mut vm` while we
        // mutate `reexec`.
        let Self { reexec, vm, .. } = self;

        let mut scalar_updates = Vec::new();
        let mut triggers = Vec::new();
        for query_id in query_ids {
            let Some(entry) = reexec.get_mut(&query_id) else {
                continue;
            };

            // UPDATE that changes no column the query depends on can't affect
            // it: skip without running the machine.
            if event.kind() == EventKind::Update {
                let changed = event.changed_columns();
                if !changed.is_empty()
                    && !changed
                        .iter()
                        .any(|c| entry.runtime.dependency_columns().contains(c))
                {
                    continue;
                }
            }

            let consumer_id = entry.consumer_id;
            match entry.runtime.on_event(event, vm) {
                Maintenance::Unchanged => {}
                Maintenance::Updated(value) => {
                    scalar_updates.push(ScalarUpdate {
                        query_id,
                        consumer_id,
                        value,
                    });
                }
                Maintenance::NeedsReexecution => {
                    triggers.push(ReExecutionTrigger {
                        query_id,
                        consumer_id,
                    });
                }
            }
        }
        (scalar_updates, triggers)
    }

    /// Remove a captured query and prune it from the routing and session
    /// indexes. Returns whether it was present.
    fn remove_reexec_entry(&mut self, query_id: ReExecQueryId) -> bool {
        let Some(entry) = self.reexec.remove(&query_id) else {
            return false;
        };

        if let Some(set) = self.table_deps.get_mut(&entry.table_id) {
            set.remove(&query_id);
            if set.is_empty() {
                self.table_deps.remove(&entry.table_id);
            }
        }

        if let Some(session) = entry.session {
            if let Some(ids) = self.session_index.get_mut(&session) {
                ids.retain(|&id| id != query_id);
                if ids.is_empty() {
                    self.session_index.remove(&session);
                }
            }
        }

        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{ColumnId, DefaultIds, RowImage};
    use alloc::sync::Arc;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    type TestEngine = ReExecEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

    fn engine() -> TestEngine {
        let inner = SubscriptionEngine::new(Arc::new(catalog()), PostgreSqlDialect {});
        ReExecEngine::new(inner)
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

    fn insert_event(price: f64) -> WalEvent {
        WalEvent::builder(0)
            .insert()
            .pk_cell(0, Cell::Int(0))
            .new_row(row(price))
            .build()
            .unwrap()
    }

    fn update_event(changed: &[ColumnId], old: f64, new: f64) -> WalEvent {
        WalEvent::builder(0)
            .update()
            .new_row(row(new))
            .pk_cell(0, Cell::Int(0))
            .maybe_old_row(Some(row(old)))
            .changed_columns(Arc::from(changed))
            .build()
            .unwrap()
    }

    fn delete_event(price: f64) -> WalEvent {
        WalEvent::builder(0)
            .delete()
            .pk_cell(0, Cell::Int(2))
            .old_row(row(price))
            .build()
            .unwrap()
    }

    /// Register a MIN(price) subscription and bootstrap an initial value.
    fn registered_min(e: &mut TestEngine, initial: Cell) -> ReExecQueryId {
        let r = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders",
            ))
            .unwrap();
        let (qid, sql, ct) = match r {
            Registered::ReExec {
                query_id,
                sql,
                column_type,
            } => (query_id, sql, column_type),
            Registered::Engine(_) => panic!("expected ReExec"),
        };
        assert!(sql.contains("AS v") || sql.contains("AS \"v\""));
        assert_eq!(ct, ColumnType::Float);
        assert!(e.install(qid, initial));
        qid
    }

    #[test]
    fn register_returns_bootstrap_sql_and_type() {
        let mut e = engine();
        let r = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders",
            ))
            .unwrap();
        match r {
            Registered::ReExec {
                sql, column_type, ..
            } => {
                assert!(sql.contains("MIN"));
                assert!(sql.contains("orders"));
                assert_eq!(column_type, ColumnType::Float);
            }
            Registered::Engine(_) => panic!("expected ReExec"),
        }
        assert_eq!(e.reexec_query_count(), 1);
    }

    /// A `MIN(price)` registration must hard-reject when `orders` has RLS
    /// enabled. Per-viewer auth makes a shared in-process IVM state unsafe;
    /// the wrapper surfaces `RegisterError::AggregatorOnRlsTable` until
    /// per-consumer total re-execution lands.
    #[test]
    fn aggregator_on_rls_table_is_rejected() {
        let ddl =
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);\n\
                   ALTER TABLE orders ENABLE ROW LEVEL SECURITY;";
        let rls_catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap();
        let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            Arc::new(rls_catalog),
            PostgreSqlDialect {},
        );
        let mut e = ReExecEngine::new(inner);
        let err = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders",
            ))
            .unwrap_err();
        match err {
            RegisterError::AggregatorOnRlsTable { table_id } => {
                assert_eq!(table_id, 0, "orders is the only declared table");
            }
            other => panic!("expected AggregatorOnRlsTable, got {other:?}"),
        }
        assert_eq!(e.reexec_query_count(), 0);
    }

    #[test]
    fn supported_query_goes_to_engine() {
        let mut e = engine();
        let r = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE quantity > 5",
            ))
            .unwrap();
        assert!(matches!(r, Registered::Engine(_)));
        assert_eq!(e.reexec_query_count(), 0);
    }

    #[test]
    fn join_surfaces_engine_rejection() {
        let mut e = engine();
        let err = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders JOIN users ON orders.id = users.id",
            ))
            .unwrap_err();
        assert!(matches!(err, RegisterError::UnsupportedSql(_)));
    }

    #[test]
    fn install_unknown_id_returns_false() {
        let mut e = engine();
        assert!(!e.install(9999, Cell::Float(1.0)));
    }

    #[test]
    fn insert_below_min_emits_scalar_update_no_trigger() {
        let mut e = engine();
        let qid = registered_min(&mut e, Cell::Float(4.0));
        let n = e.consumers(&insert_event(1.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].query_id, qid);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(1.0));
        assert!(n.triggers.is_empty());
    }

    #[test]
    fn insert_above_min_emits_nothing() {
        let mut e = engine();
        let _qid = registered_min(&mut e, Cell::Float(4.0));
        let n = e.consumers(&insert_event(99.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
    }

    #[test]
    fn delete_of_extreme_emits_trigger_no_update() {
        let mut e = engine();
        let qid = registered_min(&mut e, Cell::Float(4.0));
        let n = e.consumers(&delete_event(4.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(n.triggers.len(), 1);
        assert_eq!(n.triggers[0].query_id, qid);
        assert_eq!(n.triggers[0].consumer_id, 1u64);
    }

    #[test]
    fn install_after_trigger_updates_current() {
        let mut e = engine();
        let qid = registered_min(&mut e, Cell::Float(4.0));
        let n = e.consumers(&delete_event(4.0)).unwrap();
        assert_eq!(n.triggers.len(), 1);
        // Materializer re-runs the query and installs the new MIN.
        assert!(e.install(qid, Cell::Float(10.5)));
        // A subsequent insert above the new MIN does not emit; below does.
        let n = e.consumers(&insert_event(15.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        let n = e.consumers(&insert_event(2.0)).unwrap();
        assert_eq!(n.scalar_updates[0].value, Cell::Float(2.0));
    }

    #[test]
    fn update_unrelated_column_skipped() {
        let mut e = engine();
        let _qid = registered_min(&mut e, Cell::Float(4.0));
        // Status is column 3; the query depends on price (1). Skip.
        let n = e.consumers(&update_event(&[3], 4.0, 4.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
    }

    #[test]
    fn update_lowering_non_extreme_emits_scalar_update() {
        let mut e = engine();
        let _qid = registered_min(&mut e, Cell::Float(4.0));
        // old 9.0 is non-extreme; new 1.0 becomes the new MIN, in-process.
        let n = e.consumers(&update_event(&[1], 9.0, 1.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(1.0));
        assert!(n.triggers.is_empty());
    }

    #[test]
    fn update_displacing_extreme_emits_trigger() {
        let mut e = engine();
        let qid = registered_min(&mut e, Cell::Float(4.0));
        // old 4.0 IS the current extreme; deleting-it half forces a trigger.
        let n = e.consumers(&update_event(&[1], 4.0, 10.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(n.triggers.len(), 1);
        assert_eq!(n.triggers[0].query_id, qid);
    }

    #[test]
    fn unregister_reexec_query_removes_it() {
        let mut e = engine();
        // Keep the table's partition alive with an engine subscription, so
        // post-removal dispatch still succeeds (returns empty).
        e.register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE quantity > 0",
        ))
        .unwrap();
        let qid = registered_min(&mut e, Cell::Float(4.0));
        assert!(e.unregister_reexec_query(qid));
        assert_eq!(e.reexec_query_count(), 0);
        let n = e.consumers(&insert_event(1.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
    }

    #[test]
    fn unregister_session_removes_session_scoped_queries() {
        let mut e = engine();
        let r = e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders")
                    .scope(SubscriptionScope::Session(7u64)),
            )
            .unwrap();
        assert!(matches!(r, Registered::ReExec { .. }));
        assert_eq!(e.reexec_query_count(), 1);

        let report = e.unregister_session(7u64);
        assert_eq!(report.reexec_queries_removed, 1);
        assert_eq!(e.reexec_query_count(), 0);
    }

    #[test]
    fn unregister_other_session_keeps_query() {
        let mut e = engine();
        e.register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders")
                .scope(SubscriptionScope::Session(7u64)),
        )
        .unwrap();
        let report = e.unregister_session(99u64);
        assert_eq!(report.reexec_queries_removed, 0);
        assert_eq!(e.reexec_query_count(), 1);
    }
}
