//! Front-door wrapper that delegates to the core engine and captures the
//! queries it rejects (single-table scalar `MIN` / `MAX`) for re-execution.
//!
//! subql is **DB-free**: it classifies and maintains in-process. When the
//! maintenance state machine cannot resolve a result on its own (extreme
//! removal, or future Total / aggregate triggers), the engine emits a
//! [`ReExecutionTrigger`] for the caller. The caller (a downstream
//! subscription materializer) services the trigger: re-runs the query,
//! authorizes, builds a patchset, then feeds the recomputed value back via
//! [`ReExecEngine::install`].

use super::maintain::{Maintenance, MinMaxQuery, QueryRuntime};
use super::plan::{build_plan, MinMaxPlan, QueryPlan};
use crate::backend::{Backend, CdcEvent, Value};
use crate::catalog_helpers::table_has_rls;
use crate::compiler::literals::SqlLiteralParse;
use crate::compiler::Vm;
use crate::{
    ConsumerNotifications, DispatchError, EventKind, IdTypes, RegisterError, RegisterResult,
    SubscriptionEngine, SubscriptionRequest, SubscriptionScope, TableId, UnregisterReport,
};
use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use sql_traits::prelude::DatabaseLike;

/// Identifier for a captured re-execution query, assigned by the wrapper.
pub type ReExecQueryId = u64;

/// A captured re-execution query: the table it reads (for cleanup
/// routing), who owns it, and the maintenance state machine that tracks
/// its value.
struct ReExecEntry<I: IdTypes, B: Backend> {
    /// Consumer that registered the query.
    consumer_id: I::ConsumerId,
    /// Session owning the query, if session-scoped (for cleanup).
    session: Option<I::SessionId>,
    /// Table the query reads from (routing + cleanup).
    /// Every table whose changes route to this query. One for a scalar
    /// aggregate, one per referenced table for a whole re-read, and removal
    /// walks all of them so a multi-table capture leaves no stale routing.
    tables: Vec<TableId>,
    /// In-process maintenance state machine.
    runtime: QueryRuntime<B>,
}

/// Outcome of registering a query through the wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Registered {
    /// The core engine accepted the query (rows or a delta-composable
    /// aggregate).
    Engine(RegisterResult),
    /// The query was captured for re-execution. The materializer must
    /// bootstrap (run `sql`, decode the scalar as `column_kind`) and call
    /// [`ReExecEngine::install`] with the initial value. Until that
    /// happens the wrapper holds `Value::Null` for this query.
    ReExec {
        /// Wrapper-assigned id for the captured query.
        query_id: ReExecQueryId,
        /// SQL to run for the initial value and for any later trigger.
        sql: String,
        /// Decode hint for the scalar result.
        column_kind: crate::backend::BuiltinKind,
    },
    /// The query was captured to be re-read whole. Nothing about its answer
    /// is maintained in process, so every change to a table it reads produces
    /// a fresh read delivered as pages.
    ///
    /// Reported rather than folded into [`Self::ReExec`] because the two cost
    /// wildly different things per change, and a caller metering its database
    /// needs to know which it got.
    Captured {
        /// Wrapper-assigned id for the captured query.
        query_id: ReExecQueryId,
        /// The statement, unchanged: this tier promises exactly the rows the
        /// caller asked for.
        sql: String,
        /// Tables whose changes trigger a re-read.
        tables: Vec<TableId>,
    },
}

/// A re-executed scalar whose value changed, to be delivered to its
/// consumer.
///
/// Emitted when the in-process maintenance state machine produced a new
/// value from the event (no DB round-trip needed). Carries the originating
/// event's [`Checkpoint`](crate::Checkpoint) when known.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarUpdate<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query whose value changed.
    pub query_id: ReExecQueryId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// The new scalar value (`Value::Null` if the aggregate is now empty).
    pub value: Value<B>,
    /// Position of the event that produced this update, when known.
    pub checkpoint: Option<C>,
}

/// One page of a re-read captured query, delivered to its consumer.
///
/// A whole-re-read capture holds no answer, so a change does not produce a
/// delta: it produces the answer again, in pages. `generation` says which
/// re-read a page belongs to and `more` says whether the re-read is finished,
/// which together are what let a consumer replace what it had without ever
/// showing a half-replaced answer.
#[derive(Debug, Clone, PartialEq)]
pub struct RowsUpdate<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query these rows answer.
    pub query_id: ReExecQueryId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Which re-read this page belongs to, increasing by one per re-read.
    ///
    /// Pages of one re-read share a generation. A page carrying a higher one
    /// starts a new answer, which is the signal to discard the previous.
    pub generation: u64,
    /// Column names as the database reported them, in projection order.
    pub columns: Vec<String>,
    /// This page's rows, in `columns` order.
    pub rows: Vec<Vec<Value<B>>>,
    /// Whether further pages of this same re-read follow.
    pub more: bool,
    /// Position of the event that triggered the re-read, when known.
    pub checkpoint: Option<C>,
}

/// Signal that a captured query needs to be re-executed.
///
/// Emitted when the in-process maintenance state machine cannot resolve
/// the result for an event. The caller (materializer) must re-execute the
/// query and call [`ReExecEngine::install`] with the recomputed value.
///
/// Triggers are designed to be **idempotent and coalescible**: emitting
/// the same trigger twice is safe (a single re-execution serves any
/// number of pending triggers), and `install` unconditionally overwrites
/// the stored value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReExecutionTrigger<I: IdTypes, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query needing re-execution.
    pub query_id: ReExecQueryId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Position of the event that triggered this re-execution, when known.
    pub checkpoint: Option<C>,
}

/// Combined dispatch result: the core engine's per-consumer notifications,
/// any in-process scalar updates, and any re-execution triggers for the
/// materializer to service.
pub struct ReExecNotifications<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// View-relative notifications from the core engine.
    pub engine: ConsumerNotifications<I, C, B>,
    /// Scalar values that changed in-process (no DB round-trip).
    pub scalar_updates: Vec<ScalarUpdate<I, B, C>>,
    /// Pages of re-read captured queries, in delivery order.
    pub rows_updates: Vec<RowsUpdate<I, B, C>>,
    /// Queries whose maintenance could not resolve in-process. The
    /// materializer must re-execute and call [`ReExecEngine::install`].
    pub triggers: Vec<ReExecutionTrigger<I, C>>,
}

/// Result of [`ReExecEngine::consumers_batch`].
///
/// Carries per-event engine notifications in input order plus coalesced
/// scalar updates and triggers across the whole batch. Also returned by
/// the auto-resolving engines' batch methods (with `triggers` always
/// empty after resolution).
pub struct BatchOutcome<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// View-relative engine notifications, one entry per input event in
    /// the order they were supplied.
    pub per_event: Vec<ConsumerNotifications<I, C, B>>,
    /// Scalar updates produced in-process during the batch.
    pub scalar_updates: Vec<ScalarUpdate<I, B, C>>,
    /// Re-execution triggers, deduplicated by `query_id` across the
    /// batch (last occurrence's checkpoint wins).
    pub triggers: Vec<ReExecutionTrigger<I, C>>,
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
/// Registration flows through this type: queries the core engine
/// supports pass straight through, and single-table scalar `MIN` / `MAX`
/// queries it rejects are captured here and maintained incrementally.
/// The wrapper never opens a database connection. The materializer
/// services [`ReExecutionTrigger`]s and calls
/// [`install`](Self::install).
pub struct ReExecEngine<E: CdcEvent, I: IdTypes, DB: DatabaseLike>
where
    E::Backend: SqlLiteralParse,
{
    inner: SubscriptionEngine<E, I, DB>,
    reexec: HashMap<ReExecQueryId, ReExecEntry<I, E::Backend>>,
    /// Table -> captured queries reading from it (CDC event routing).
    table_deps: HashMap<TableId, HashSet<ReExecQueryId>>,
    /// Session -> its captured queries (session-scoped cleanup).
    session_index: HashMap<I::SessionId, Vec<ReExecQueryId>>,
    /// VM reused for in-process WHERE-membership evaluation during
    /// dispatch.
    vm: Vm<E::Backend>,
    next_id: ReExecQueryId,
}

impl<E, I, DB> ReExecEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Wrap an existing engine. No database connection is taken or
    /// required.
    pub fn new(inner: SubscriptionEngine<E, I, DB>) -> Self {
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
    pub const fn inner(&self) -> &SubscriptionEngine<E, I, DB> {
        &self.inner
    }

    /// Mutable access to the wrapped core engine.
    pub const fn inner_mut(&mut self) -> &mut SubscriptionEngine<E, I, DB> {
        &mut self.inner
    }

    /// Number of captured re-execution queries.
    pub fn reexec_query_count(&self) -> usize {
        self.reexec.len()
    }

    /// Register a subscription.
    ///
    /// Queries the core engine supports are delegated to it
    /// ([`Registered::Engine`]). A single-table scalar `MIN` / `MAX` it
    /// rejects is captured for re-execution and returned as
    /// [`Registered::ReExec`] — the materializer must then bootstrap the
    /// initial value (run `sql`) and call [`install`](Self::install).
    /// Any other engine rejection surfaces as a [`RegisterError`].
    pub fn register(
        &mut self,
        spec: SubscriptionRequest<I, E::Backend>,
    ) -> Result<Registered, RegisterError> {
        // Capture what tracking needs before `spec` is moved into the
        // inner engine (which consumes it).
        let sql = spec.sql.clone();
        let consumer_id = spec.consumer_id;
        let session = match &spec.scope {
            SubscriptionScope::Session(s) => Some(*s),
            SubscriptionScope::Durable => None,
        };

        match self.inner.register(spec) {
            Ok(result) => Ok(Registered::Engine(result)),
            Err(RegisterError::UnsupportedSql(msg)) => {
                match build_plan::<E::Backend, DB>(
                    &sql,
                    self.inner.dialect(),
                    self.inner.database(),
                ) {
                    Ok(QueryPlan::Partial(plan)) => {
                        // Per-viewer RLS makes a shared in-process IVM
                        // state unsafe: different consumers observe
                        // different rows. Hard-reject the registration;
                        // total re-execution with per-consumer state is
                        // a planned follow-on.
                        if table_has_rls(self.inner.database(), plan.table_id).unwrap_or(false) {
                            return Err(RegisterError::AggregatorOnRlsTable {
                                table_id: plan.table_id,
                            });
                        }
                        Ok(self.capture(plan, consumer_id, session))
                    }
                    Ok(QueryPlan::Total(plan)) => {
                        // Row-level security makes a shared answer unsafe for
                        // the same reason it does for an aggregate: consumers
                        // see different rows, and this tier holds one answer
                        // per query rather than one per viewer.
                        if let Some(table) = plan
                            .tables
                            .iter()
                            .copied()
                            .find(|t| table_has_rls(self.inner.database(), *t).unwrap_or(false))
                        {
                            return Err(RegisterError::AggregatorOnRlsTable { table_id: table });
                        }
                        Ok(self.capture_total(plan, consumer_id, session))
                    }
                    // Not a re-executable query (build_plan rejected it):
                    // surface the engine's original rejection.
                    Err(_) => Err(RegisterError::UnsupportedSql(msg)),
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Build the maintenance runtime, populate the routing / session
    /// indexes, and return the `ReExec` registration. The initial value
    /// is `Value::Null` until the materializer bootstraps via
    /// [`install`](Self::install).
    fn capture(
        &mut self,
        plan: MinMaxPlan<E::Backend>,
        consumer_id: I::ConsumerId,
        session: Option<I::SessionId>,
    ) -> Registered {
        let MinMaxPlan {
            table_id,
            kind,
            agg_column,
            agg_kind,
            dependency_columns,
            where_program,
            reexec_sql,
        } = plan;

        // The materializer is responsible for the initial value; subql
        // holds `Value::Null` ("empty set / unknown") until `install`
        // arrives.
        let runtime = QueryRuntime::Partial(MinMaxQuery::new(
            kind,
            agg_column,
            where_program,
            dependency_columns,
            Value::Null,
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
                tables: alloc::vec![table_id],
                runtime,
            },
        );

        Registered::ReExec {
            query_id,
            sql: reexec_sql,
            column_kind: agg_kind,
        }
    }

    /// Register a whole-re-read capture: route every table it reads to it, and
    /// report the tables so a caller knows what will trigger a read.
    fn capture_total(
        &mut self,
        plan: crate::reexec::plan::TotalPlan,
        consumer_id: I::ConsumerId,
        session: Option<I::SessionId>,
    ) -> Registered {
        let crate::reexec::plan::TotalPlan {
            tables,
            dependency_columns,
            reexec_sql,
        } = plan;

        let query_id = self.next_id;
        self.next_id += 1;

        for table in &tables {
            self.table_deps.entry(*table).or_default().insert(query_id);
        }
        if let Some(s) = session {
            self.session_index.entry(s).or_default().push(query_id);
        }
        self.reexec.insert(
            query_id,
            ReExecEntry {
                consumer_id,
                session,
                tables: tables.clone(),
                runtime: QueryRuntime::Total(crate::reexec::maintain::TotalQuery::new(
                    dependency_columns,
                )),
            },
        );

        Registered::Captured {
            query_id,
            sql: reexec_sql,
            tables,
        }
    }

    /// Install a value computed by the materializer (initial bootstrap,
    /// or the refreshed value after servicing a
    /// [`ReExecutionTrigger`]). Returns `true` if the query exists.
    /// Calls are idempotent: repeated `install`s unconditionally
    /// overwrite the stored value.
    pub fn install(&mut self, query_id: ReExecQueryId, value: Value<E::Backend>) -> bool {
        if let Some(entry) = self.reexec.get_mut(&query_id) {
            entry.runtime.install(value);
            true
        } else {
            false
        }
    }

    /// Unregister everything for a session: delegates to the core
    /// engine, then removes every captured re-execution query owned by
    /// the session.
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

    /// Unregister a single captured re-execution query by the id
    /// returned from [`register`](Self::register). Returns `false` if no
    /// such query exists.
    pub fn unregister_reexec_query(&mut self, query_id: ReExecQueryId) -> bool {
        self.remove_reexec_entry(query_id)
    }

    /// Unregister a core-engine subscription by `(consumer_id, sql)`.
    /// Does not affect captured re-execution queries, which are removed
    /// by id via
    /// [`unregister_reexec_query`](Self::unregister_reexec_query).
    pub fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        self.inner.unregister_query(consumer_id, sql)
    }

    /// Dispatch a batch of CDC events at once, coalescing triggers by
    /// `query_id` across the whole batch.
    ///
    /// Per-event engine notifications are returned in input order in
    /// [`BatchOutcome::per_event`]. In-process scalar updates accumulate
    /// across the batch into [`BatchOutcome::scalar_updates`]. Triggers
    /// are deduplicated by `query_id` (last occurrence's checkpoint
    /// wins) into [`BatchOutcome::triggers`].
    pub fn consumers_batch(
        &mut self,
        events: &[E],
    ) -> Result<BatchOutcome<I, E::Backend, E::Checkpoint>, DispatchError> {
        let mut per_event = Vec::with_capacity(events.len());
        let mut scalar_updates = Vec::new();
        // HashMap dedup by query_id; insertion-order across the batch is
        // discarded, which is correct: the connector reads the
        // cumulative state at the end of the batch, not any intermediate
        // state.
        let mut triggers: HashMap<ReExecQueryId, ReExecutionTrigger<I, E::Checkpoint>> =
            HashMap::new();
        for event in events {
            let notifs = self.consumers(event)?;
            per_event.push(notifs.engine);
            scalar_updates.extend(notifs.scalar_updates);
            for trigger in notifs.triggers {
                // Last write wins: overwrites the previous trigger's
                // checkpoint with the most recent one, matching the
                // semantics of "the connector reads at the latest
                // point".
                triggers.insert(trigger.query_id, trigger);
            }
        }
        Ok(BatchOutcome {
            per_event,
            scalar_updates,
            triggers: triggers.into_values().collect(),
        })
    }

    /// Dispatch a CDC event.
    ///
    /// Delegates to the core engine for per-consumer notifications, then
    /// runs the maintenance state machine for every captured query
    /// reading the event's table. The result carries in-process
    /// [`ScalarUpdate`]s and any [`ReExecutionTrigger`]s the
    /// materializer must service.
    pub fn consumers(
        &mut self,
        event: &E,
    ) -> Result<ReExecNotifications<I, E::Backend, E::Checkpoint>, DispatchError> {
        let engine = match self.inner.consumers(event) {
            Ok(notifs) => notifs,
            // A table with only re-execution queries has no engine
            // partition; there are no per-consumer notifications, but
            // the re-execution queries still need to run.
            Err(DispatchError::UnknownTableId(_))
                if self
                    .table_deps
                    .contains_key(&event.table_id(self.inner.database())) =>
            {
                ConsumerNotifications::empty().with_checkpoint(event.checkpoint())
            }
            Err(e) => return Err(e),
        };

        let (scalar_updates, triggers) = self.dispatch_reexec(event);

        Ok(ReExecNotifications {
            engine,
            scalar_updates,
            // The trigger-emitting engine never reads the database, so a
            // re-read's pages can only come from a resolving engine above it.
            rows_updates: Vec::new(),
            triggers,
        })
    }

    /// Feed `event` to each captured query reading its table. Returns
    /// (in-process scalar updates, re-execution triggers).
    #[allow(clippy::type_complexity)]
    fn dispatch_reexec(
        &mut self,
        event: &E,
    ) -> (
        Vec<ScalarUpdate<I, E::Backend, E::Checkpoint>>,
        Vec<ReExecutionTrigger<I, E::Checkpoint>>,
    ) {
        // Snapshot affected ids before borrowing `reexec` / `vm` mutably.
        let table_id = event.table_id(self.inner.database());
        let query_ids: Vec<ReExecQueryId> = match self.table_deps.get(&table_id) {
            Some(ids) => ids.iter().copied().collect(),
            None => return (Vec::new(), Vec::new()),
        };

        // Split-borrow disjoint fields so `on_event` can take `&mut vm`
        // and the catalog while we mutate `reexec`.
        let Self {
            reexec, vm, inner, ..
        } = self;
        let database = inner.database();

        let mut scalar_updates = Vec::new();
        let mut triggers = Vec::new();
        for query_id in query_ids {
            let Some(entry) = reexec.get_mut(&query_id) else {
                continue;
            };

            // UPDATE that changes no column the query depends on can't
            // affect it: skip without running the machine.
            if event.kind() == EventKind::Update {
                let changed = event.changed_columns(database);
                if !changed.is_empty()
                    && !changed
                        .iter()
                        .any(|c| entry.runtime.dependency_columns().contains(c))
                {
                    continue;
                }
            }

            let consumer_id = entry.consumer_id;
            let checkpoint = event.checkpoint();
            match entry.runtime.on_event(event, vm, database) {
                Maintenance::Unchanged => {}
                Maintenance::Updated(value) => {
                    scalar_updates.push(ScalarUpdate {
                        query_id,
                        consumer_id,
                        value,
                        checkpoint: checkpoint.clone(),
                    });
                }
                Maintenance::NeedsReexecution => {
                    triggers.push(ReExecutionTrigger {
                        query_id,
                        consumer_id,
                        checkpoint: checkpoint.clone(),
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

        for table in &entry.tables {
            if let Some(set) = self.table_deps.get_mut(table) {
                set.remove(&query_id);
                if set.is_empty() {
                    self.table_deps.remove(table);
                }
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

impl<E, I, DB> crate::SubscriptionDispatch<I, E> for ReExecEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    type Notifications = ReExecNotifications<I, E::Backend, E::Checkpoint>;
    type Error = DispatchError;

    fn consumers(&mut self, event: &E) -> Result<Self::Notifications, Self::Error> {
        Self::consumers(self, event)
    }
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.
