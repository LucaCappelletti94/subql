//! Front-door wrapper that delegates to the core engine and captures the
//! queries it rejects (single-table scalar `MIN`/`MAX`) for re-execution.

use super::executor::{ConnectionProvider, ReExecError, ScalarSource};
use super::maintain::{Maintenance, MinMaxQuery, QueryRuntime};
use super::plan::{build_plan, MinMaxPlan, QueryPlan};
use crate::compiler::Vm;
use crate::{
    Cell, ConsumerNotifications, DispatchError, EventKind, IdTypes, RegisterError, RegisterResult,
    SubscriptionEngine, SubscriptionRequest, SubscriptionScope, TableId, UnregisterReport,
    WalEvent,
};
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
    /// Layer 2 state machine maintaining the query's value.
    runtime: QueryRuntime,
}

/// Outcome of registering a query through the wrapper.
#[derive(Debug, Clone, PartialEq)]
pub enum Registered {
    /// The core engine accepted the query (rows or a delta-composable
    /// aggregate).
    Engine(RegisterResult),
    /// The query was captured for re-execution.
    ReExec {
        /// Wrapper-assigned id for the captured query.
        query_id: ReExecQueryId,
        /// Scalar value computed at registration time.
        initial_value: Cell,
    },
}

/// Error registering a query through the wrapper.
#[derive(Debug, thiserror::Error)]
pub enum ReExecRegisterError<E> {
    /// The core engine rejected the query for a reason other than being a
    /// re-executable aggregate (parse error, unknown table, an unsupported
    /// projection the wrapper also cannot handle, ...).
    #[error(transparent)]
    Register(RegisterError),
    /// Computing the initial scalar value against the database failed.
    #[error("re-execution failed: {0}")]
    Execute(ReExecError<E>),
}

/// A re-executed scalar whose value changed, to be delivered to its consumer.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarUpdate<I: IdTypes> {
    /// The captured query whose value changed.
    pub query_id: ReExecQueryId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// The new scalar value (`Cell::Null` if the aggregate is now empty).
    pub value: Cell,
}

/// Combined dispatch result: the core engine's per-consumer notifications plus
/// any re-executed scalar values that changed.
pub struct ReExecNotifications<I: IdTypes> {
    /// View-relative notifications from the core engine.
    pub engine: ConsumerNotifications<I>,
    /// Re-executed `MIN`/`MAX` values that changed for this event.
    pub scalar_updates: Vec<ScalarUpdate<I>>,
}

/// Counts from unregistering through the wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReExecUnregisterReport {
    /// Core-engine unregistration counts.
    pub engine: UnregisterReport,
    /// Number of captured re-execution queries removed.
    pub reexec_queries_removed: usize,
}

/// Error dispatching a CDC event through the wrapper.
#[derive(Debug, thiserror::Error)]
pub enum ReExecDispatchError<E> {
    /// The core engine failed to dispatch the event.
    #[error(transparent)]
    Dispatch(DispatchError),
    /// Re-executing a captured query against the database failed.
    #[error("re-execution failed: {0}")]
    Execute(ReExecError<E>),
}

/// Front-door wrapper over [`SubscriptionEngine`].
///
/// Registration flows through this type: queries the core engine supports pass
/// straight through, and single-table scalar `MIN`/`MAX` queries it rejects are
/// captured here and served by re-querying the database via a
/// [`ConnectionProvider`].
pub struct ReExecEngine<D: Dialect, I: IdTypes, DB: DatabaseLike, P: ConnectionProvider> {
    inner: SubscriptionEngine<D, I, DB>,
    provider: P,
    reexec: HashMap<ReExecQueryId, ReExecEntry<I>>,
    /// Table -> captured queries reading from it (CDC event routing).
    table_deps: HashMap<TableId, HashSet<ReExecQueryId>>,
    /// Session -> its captured queries (session-scoped cleanup).
    session_index: HashMap<I::SessionId, Vec<ReExecQueryId>>,
    /// VM reused for in-process WHERE-membership evaluation during dispatch.
    vm: Vm,
    next_id: ReExecQueryId,
}

impl<D: Dialect, I: IdTypes, DB: DatabaseLike + 'static, P: ConnectionProvider>
    ReExecEngine<D, I, DB, P>
{
    /// Wrap an existing engine with a connection provider for re-execution.
    pub fn new(inner: SubscriptionEngine<D, I, DB>, provider: P) -> Self {
        Self {
            inner,
            provider,
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

    /// Unregister a core-engine subscription by `(consumer_id, sql)`. This does
    /// not affect captured re-execution queries, which are removed by id via
    /// [`unregister_reexec_query`](Self::unregister_reexec_query).
    pub fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        self.inner.unregister_query(consumer_id, sql)
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

impl<D: Dialect, I: IdTypes, DB: DatabaseLike + 'static, P: ScalarSource>
    ReExecEngine<D, I, DB, P>
{
    /// Register a subscription.
    ///
    /// Queries the core engine supports are delegated to it
    /// ([`Registered::Engine`]). A single-table scalar `MIN`/`MAX` it rejects
    /// is captured for re-execution: its value is computed immediately and
    /// returned ([`Registered::ReExec`]). Any other rejection surfaces as
    /// [`ReExecRegisterError::Register`].
    pub fn register(
        &mut self,
        spec: SubscriptionRequest<I>,
    ) -> Result<Registered, ReExecRegisterError<P::Error>> {
        // Capture what the classifier and tracking need before `spec` is moved
        // into the inner engine (which consumes it).
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
                    Ok(QueryPlan::Partial(plan)) => self.capture(plan, consumer_id, session),
                    // Not a re-executable query (build_plan rejected it):
                    // surface the engine's original rejection.
                    Err(_) => Err(ReExecRegisterError::Register(
                        RegisterError::UnsupportedSql(msg),
                    )),
                }
            }
            Err(other) => Err(ReExecRegisterError::Register(other)),
        }
    }

    /// Compute a captured query's initial value, build its maintenance state
    /// machine, and record it in the routing and session indexes.
    fn capture(
        &mut self,
        plan: MinMaxPlan,
        consumer_id: I::ConsumerId,
        session: Option<I::SessionId>,
    ) -> Result<Registered, ReExecRegisterError<P::Error>> {
        let MinMaxPlan {
            table_id,
            kind,
            agg_column,
            column_type,
            dependency_columns,
            where_program,
            reexec_sql,
        } = plan;

        let initial = self
            .provider
            .load_scalar(&reexec_sql, column_type)
            .map_err(ReExecRegisterError::Execute)?;

        let runtime = QueryRuntime::Partial(MinMaxQuery::new(
            kind,
            agg_column,
            column_type,
            where_program,
            dependency_columns,
            reexec_sql,
            initial.clone(),
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

        Ok(Registered::ReExec {
            query_id,
            initial_value: initial,
        })
    }

    /// Dispatch a CDC event.
    ///
    /// Delegates to the core engine for per-consumer notifications, then
    /// re-executes every captured query that reads the event's table whose
    /// result the event could have changed, emitting a [`ScalarUpdate`] for
    /// each whose value actually moved. The two results are returned together.
    pub fn consumers(
        &mut self,
        event: &WalEvent,
    ) -> Result<ReExecNotifications<I>, ReExecDispatchError<P::Error>> {
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
            Err(e) => return Err(ReExecDispatchError::Dispatch(e)),
        };

        let scalar_updates = self
            .dispatch_reexec(event)
            .map_err(ReExecDispatchError::Execute)?;

        Ok(ReExecNotifications {
            engine,
            scalar_updates,
        })
    }

    /// Feed `event` to each captured query reading its table, running the
    /// Layer 2 state machine and re-querying (Layer 3) only when the machine
    /// cannot decide in-process. Collects the queries whose value changed.
    fn dispatch_reexec(
        &mut self,
        event: &WalEvent,
    ) -> Result<Vec<ScalarUpdate<I>>, ReExecError<P::Error>> {
        // Snapshot affected ids before borrowing `reexec`/`vm` mutably.
        let query_ids: Vec<ReExecQueryId> = match self.table_deps.get(&event.table_id()) {
            Some(ids) => ids.iter().copied().collect(),
            None => return Ok(Vec::new()),
        };

        // Split-borrow disjoint fields so `on_event` can take `&mut vm` while we
        // mutate `reexec`, and Layer 3 can read `provider`.
        let Self {
            reexec,
            vm,
            provider,
            ..
        } = self;

        let mut updates = Vec::new();
        for query_id in query_ids {
            let Some(entry) = reexec.get_mut(&query_id) else {
                continue;
            };

            // Routing optimization: an UPDATE that changes no column the query
            // depends on cannot affect it; skip without running the machine.
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
                    updates.push(ScalarUpdate {
                        query_id,
                        consumer_id,
                        value,
                    });
                }
                Maintenance::NeedsReexecution => {
                    let new_value = Self::reexecute(provider, &entry.runtime)?;
                    // Suppress no-op re-queries (e.g. extreme deleted but the
                    // recomputed value is identical).
                    let changed = entry.runtime.current() != &new_value;
                    entry.runtime.install(new_value.clone());
                    if changed {
                        updates.push(ScalarUpdate {
                            query_id,
                            consumer_id,
                            value: new_value,
                        });
                    }
                }
            }
        }
        Ok(updates)
    }

    /// Layer 3: re-run a captured query against the database. Partial queries
    /// load a scalar; the future `Total` variant will load a row set here.
    fn reexecute(provider: &P, runtime: &QueryRuntime) -> Result<Cell, ReExecError<P::Error>> {
        provider.load_scalar(runtime.reexec_sql(), runtime.column_type())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{ColumnId, DefaultIds, RowImage};
    use alloc::rc::Rc;
    use alloc::sync::Arc;
    use core::cell::RefCell;
    use core::convert::Infallible;
    use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    /// Shared single connection so a test can mutate the live DB after the
    /// engine has taken its own clone of the provider.
    #[derive(Clone)]
    struct TestProvider {
        conn: Rc<RefCell<SqliteConnection>>,
    }

    impl ConnectionProvider for TestProvider {
        type Connection = SqliteConnection;
        type Error = Infallible;

        fn with_connection<R>(
            &self,
            f: impl FnOnce(&mut SqliteConnection) -> R,
        ) -> Result<R, Self::Error> {
            Ok(f(&mut self.conn.borrow_mut()))
        }
    }

    impl TestProvider {
        fn exec(&self, sql: &str) {
            self.with_connection(|c| sql_query(sql).execute(c).unwrap())
                .unwrap();
        }
    }

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    fn provider() -> TestProvider {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, price REAL, \
             quantity INTEGER, status TEXT)",
        )
        .execute(&mut conn)
        .unwrap();
        sql_query(
            "INSERT INTO orders (id, price, quantity, status) VALUES \
             (1, 10.5, 3, 'paid'), (2, 4.0, 7, 'paid')",
        )
        .execute(&mut conn)
        .unwrap();
        TestProvider {
            conn: Rc::new(RefCell::new(conn)),
        }
    }

    type TestEngine = ReExecEngine<PostgreSqlDialect, DefaultIds, ParserDB, TestProvider>;

    fn engine() -> (TestEngine, TestProvider) {
        let db = provider();
        let inner = SubscriptionEngine::new(Arc::new(catalog()), PostgreSqlDialect {});
        (ReExecEngine::new(inner, db.clone()), db)
    }

    // The `orders` table is table 0 in the single-table catalog. Column
    // ordinals: id=0, price=1, quantity=2, status=3.
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

    fn update_event(changed: &[ColumnId]) -> WalEvent {
        WalEvent::builder(0)
            .update()
            .new_row(row(1.0))
            .pk_cell(0, Cell::Int(0))
            .maybe_old_row(Some(row(2.0)))
            .changed_columns(Arc::from(changed))
            .build()
            .unwrap()
    }

    #[test]
    fn min_is_captured_with_initial_value() {
        let (mut e, _db) = engine();
        let r = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders",
            ))
            .unwrap();
        match r {
            Registered::ReExec { initial_value, .. } => {
                assert_eq!(initial_value, Cell::Float(4.0));
            }
            Registered::Engine(_) => panic!("expected ReExec, not Engine"),
        }
        assert_eq!(e.reexec_query_count(), 1);
    }

    #[test]
    fn supported_query_goes_to_engine() {
        let (mut e, _db) = engine();
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
        let (mut e, _db) = engine();
        let err = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders JOIN users ON orders.id = users.id",
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            ReExecRegisterError::Register(RegisterError::UnsupportedSql(_))
        ));
        assert_eq!(e.reexec_query_count(), 0);
    }

    #[test]
    fn max_int_captured() {
        let (mut e, _db) = engine();
        let r = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MAX(quantity) FROM orders",
            ))
            .unwrap();
        match r {
            Registered::ReExec { initial_value, .. } => assert_eq!(initial_value, Cell::Int(7)),
            Registered::Engine(_) => panic!("expected ReExec, not Engine"),
        }
    }

    /// Register `SELECT MIN(price)` (initial MIN = 4.0) and return the engine,
    /// the shared DB handle, and the captured query id.
    fn registered_min() -> (TestEngine, TestProvider, ReExecQueryId) {
        let (mut e, db) = engine();
        let r = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders",
            ))
            .unwrap();
        let Registered::ReExec {
            query_id,
            initial_value,
        } = r
        else {
            panic!("expected ReExec");
        };
        assert_eq!(initial_value, Cell::Float(4.0));
        (e, db, query_id)
    }

    #[test]
    fn insert_lowering_min_emits_update() {
        let (mut e, _db, qid) = registered_min();
        // Maintained in-process from the event's row image; no DB round-trip.
        let n = e.consumers(&insert_event(1.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].query_id, qid);
        assert_eq!(n.scalar_updates[0].consumer_id, 1u64);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(1.0));
        // No engine partition for this table: engine notifications are empty.
        assert!(n.engine.inserted().is_empty());
    }

    #[test]
    fn insert_above_min_emits_nothing() {
        let (mut e, _db, _qid) = registered_min();
        // 99.0 > current min 4.0: handled in-process, no update, no DB round-trip.
        let n = e.consumers(&insert_event(99.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
    }

    #[test]
    fn delete_min_row_emits_update() {
        let (mut e, db, _qid) = registered_min();
        db.exec("DELETE FROM orders WHERE id = 2"); // removes the 4.0 row
        let event = WalEvent::builder(0)
            .delete()
            .pk_cell(0, Cell::Int(2))
            .old_row(row(4.0))
            .build()
            .unwrap();
        let n = e.consumers(&event).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(10.5));
    }

    #[test]
    fn update_unrelated_column_skips_reexec() {
        let (mut e, db, _qid) = registered_min();
        // Lower the DB min, but report that only `status` (col 3) changed.
        // MIN(price) depends on col 1, so the wrapper must not re-execute and
        // must emit nothing despite the (unreported) change.
        db.exec("UPDATE orders SET price = 1.0 WHERE id = 2");
        let n = e.consumers(&update_event(&[3])).unwrap();
        assert!(
            n.scalar_updates.is_empty(),
            "unrelated-column UPDATE must skip re-execution"
        );
    }

    #[test]
    fn update_dependency_column_reexecs() {
        let (mut e, db, _qid) = registered_min();
        db.exec("UPDATE orders SET price = 1.0 WHERE id = 2");
        let n = e.consumers(&update_event(&[1])).unwrap(); // price (col 1) changed
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Cell::Float(1.0));
    }

    #[test]
    fn unregister_reexec_query_removes_and_stops_dispatch() {
        let (mut e, _db) = engine();
        // An engine subscription keeps the table's partition alive so dispatch
        // still succeeds (returns empty) once the reexec query is removed.
        e.register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE quantity > 0",
        ))
        .unwrap();
        let Registered::ReExec { query_id, .. } = e
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT MIN(price) FROM orders",
            ))
            .unwrap()
        else {
            panic!("expected ReExec");
        };

        assert!(e.unregister_reexec_query(query_id));
        assert_eq!(e.reexec_query_count(), 0);

        // The captured query is gone: an event yields no scalar updates.
        let n = e.consumers(&insert_event(1.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
    }

    #[test]
    fn unregister_unknown_reexec_query_is_false() {
        let (mut e, _db, _qid) = registered_min();
        assert!(!e.unregister_reexec_query(9999));
        assert_eq!(e.reexec_query_count(), 1);
    }

    #[test]
    fn unregister_session_removes_session_scoped_queries() {
        let (mut e, _db) = engine();
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
        let (mut e, _db) = engine();
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
