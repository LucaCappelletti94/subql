//! Tests for the asynchronous engine, moved out of `async_auto.rs` verbatim.
//!
//! They mirror the synchronous suite on purpose: a mutation showed the async
//! wrapper is held to its behaviour only because this suite repeats it.

#![allow(clippy::unwrap_used)]

use super::super::connector::Snapshot;
use super::super::test_fixtures::{catalog, delete_event, insert_event, row, update_status_only};
use super::*;
use crate::backend::{Postgres, ScalarFamily};
use crate::testing::{block_on, TestEvent, YieldOnce};
use crate::{
    DefaultIds, NoCheckpoint, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};
use core::future::Future;
use core::pin::pin;
use core::task::Context;
use parking_lot::Mutex;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

/// `parking_lot::Mutex`-backed mock so the futures are `Send`.
struct MockAsyncConnector {
    values: Mutex<Vec<Value<Postgres>>>,
    call_count: Mutex<usize>,
    scalar_queries: Mutex<Vec<super::super::ReadQuery<'static, Postgres>>>,
    page_queries: Mutex<Vec<super::super::ReadQuery<'static, Postgres>>>,
    cursor_queries: Mutex<Vec<super::super::ReadQuery<'static, Postgres>>>,
    /// When set, the next scalar read suspends once before answering, so
    /// a test can drop a resolve future mid-read.
    pend_next_read: Mutex<bool>,
    /// Pages a whole re-read serves, front first. Empty means the mock
    /// holds no cursors and `open_cursor` refuses.
    cursor_pages: Mutex<Vec<crate::reexec::RowPage<Postgres>>>,
    /// Pages `read_page` serves, popped from the back like `values`.
    /// Empty keeps the historic refusal, which the scalar tests rely on.
    pages: Mutex<Vec<crate::reexec::RowPage<Postgres>>>,
    /// Fetch index that suspends once before serving, so a test can drop
    /// a resolve future between pages.
    pend_fetch_at: Mutex<Option<usize>>,
    /// Fetches served so far.
    fetch_count: Mutex<usize>,
    /// Interleaving log shared with the test's sink.
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl MockAsyncConnector {
    fn new(values: Vec<Value<Postgres>>) -> Self {
        Self {
            values: Mutex::new(values),
            call_count: Mutex::new(0),
            scalar_queries: Mutex::new(Vec::new()),
            page_queries: Mutex::new(Vec::new()),
            cursor_queries: Mutex::new(Vec::new()),
            pend_next_read: Mutex::new(false),
            cursor_pages: Mutex::new(Vec::new()),
            pages: Mutex::new(Vec::new()),
            pend_fetch_at: Mutex::new(None),
            fetch_count: Mutex::new(0),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn call_count(&self) -> usize {
        *self.call_count.lock()
    }
    fn push_page(&self, page: crate::reexec::RowPage<Postgres>) {
        self.pages.lock().push(page);
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
        query: &super::super::ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        async move {
            if core::mem::take(&mut *self.pend_next_read.lock()) {
                YieldOnce(false).await;
            }
            *self.call_count.lock() += 1;
            self.scalar_queries.lock().push(query.clone().into_owned());
            let value = self.values.lock().pop().ok_or(MockError("queue empty"))?;
            Ok((value, None))
        }
    }

    fn read_page(
        &self,
        query: &super::super::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> impl Future<
        Output = Result<Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>, Self::Error>,
    > + Send {
        async move {
            self.page_queries.lock().push(query.clone().into_owned());
            let popped = self.pages.lock().pop();
            let Some(page) = popped else {
                return Err(MockError("read_page is not exercised by the scalar tests"));
            };
            Ok(Snapshot {
                value: page,
                checkpoint: None,
            })
        }
    }

    fn open_cursor(
        &self,
        query: &super::super::ReadQuery<'_, Postgres>,
        _auth: &(),
    ) -> impl Future<Output = Result<super::super::CursorId, super::super::CursorError<Self::Error>>>
           + Send {
        self.cursor_queries.lock().push(query.clone().into_owned());
        if self.cursor_pages.lock().is_empty() {
            return core::future::ready(Err(super::super::CursorError::Unsupported));
        }
        self.log.lock().push("open");
        core::future::ready(Ok(super::super::CursorId(1)))
    }

    fn fetch_cursor(
        &self,
        _cursor: super::super::CursorId,
        _max_bytes: usize,
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>,
            super::super::CursorError<Self::Error>,
        >,
    > + Send {
        async move {
            let index = {
                let mut count = self.fetch_count.lock();
                let index = *count;
                *count += 1;
                index
            };
            if *self.pend_fetch_at.lock() == Some(index) {
                YieldOnce(false).await;
            }
            self.log.lock().push("fetch");
            let page = self.cursor_pages.lock().remove(0);
            Ok(Snapshot {
                value: page,
                checkpoint: None,
            })
        }
    }

    fn close_cursor(
        &self,
        _cursor: super::super::CursorId,
    ) -> impl Future<Output = Result<(), super::super::CursorError<Self::Error>>> + Send {
        self.log.lock().push("close");
        core::future::ready(Ok(()))
    }
}

fn engine_with_values(
    values: Vec<Value<Postgres>>,
) -> (
    AutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, AsyncMode<MockAsyncConnector>>,
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
        AutoResolvingEngine::new(inner, AsyncMode::new(MockAsyncConnector::new(values))),
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };

    // Snapshot bootstraps. Future is Send-bound and ready immediately.
    let snap = block_on(e.snapshot(qid))
        .unwrap()
        .expect("subscription_id exists");
    match snap {
        SnapshotResult::Scalar(Value::Float(v), None) => {
            assert!((v - 5.0).abs() < f64::EPSILON);
        }
        other => panic!("expected Scalar(5.0, None), got {other:?}"),
    }
    assert_eq!(e.connector().call_count(), 1);

    // Insert above the extreme: in-process Unchanged, no connector call.
    let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(e.connector().call_count(), 1);

    // Delete the extreme: trigger -> connector -> ScalarUpdate.
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let n = block_on(e.resolve_collect()).unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
    assert_eq!(e.connector().call_count(), 2);
}

/// A queued read must not outlive its subscription, the async twin:
/// unregistering purges it, so the next resolve is a clean no-op rather
/// than a panic on the missing resolve context in `plan_job`.
#[test]
fn unregister_subscription_drops_the_queued_read() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(5.0)]);
    let captured = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        captured,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert_eq!(
        e.pending_read_count(),
        1,
        "the displacement queues one read"
    );
    assert!(e.unregister_subscription(captured));
    assert_eq!(
        e.pending_read_count(),
        0,
        "the queued read left with its subscription"
    );
    block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no read runs for a dead subscription"
    );
}

#[test]
fn async_scalar_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(vec![Value::Float(5.0)]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(vec![Value::Int(2)]),
            (),
        )
        .expect("scalar registers")
        .subscription_id;

    block_on(engine.snapshot(subscription))
        .expect("snapshot succeeds")
        .expect("snapshot exists");
    let queries = engine.connector().scalar_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(2)]);
    drop(queries);
}

#[test]
fn async_scalar_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![Value::Float(9.0)]);

    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(vec![Value::Int(0)]),
            (),
        )
        .expect("scalar registers")
        .subscription_id;
    crate::Install::install(
        &mut engine,
        subscription,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .expect("scalar installs");

    engine
        .apply(&delete_event(table, 1, 5.0))
        .expect("apply succeeds");
    let _ = block_on(engine.resolve_collect()).expect("resolve succeeds");
    let queries = engine.connector().scalar_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
    drop(queries);
}

/// The async twin of the sync drop-on-install-failure contract: a
/// grouped re-read answering the wrong row count can never install, so
/// the failing read is dropped in phase three rather than staying queued
/// and repeating the same malformed read on every resolve.
#[test]
fn a_failed_install_drops_the_read_instead_of_requeueing_it() {
    let (mut engine, table) = engine_with_values(Vec::new());
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders GROUP BY status",
            ),
            (),
        )
        .expect("grouped minimum registers")
        .subscription_id;
    crate::Install::install(
        &mut engine.inner,
        subscription,
        crate::GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(2),
            ]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("group map installs");
    engine
        .apply(
            &TestEvent::<Postgres>::delete(
                table,
                vec![
                    Value::Int(1),
                    Value::Float(5.0),
                    Value::Int(1),
                    Value::String("paid".into()),
                ],
            )
            .with_pk_columns([0u16]),
        )
        .expect("the displacing delete dispatches");
    assert_eq!(
        engine.pending_read_count(),
        1,
        "the displacement queues one read"
    );
    // The re-read answers two rows for a one-group read, which can never
    // install: retrying it would return the same malformed answer.
    engine.connector().push_page(crate::reexec::RowPage {
        columns: vec!["min".into(), "n".into()],
        rows: vec![
            vec![Value::Float(6.0), Value::Int(1)],
            vec![Value::Float(7.0), Value::Int(1)],
        ],
        more: false,
    });
    let error = block_on(engine.resolve_collect()).unwrap_err();
    assert!(
        matches!(error, super::super::ReExecError::AggregateInstall(_)),
        "the row count mismatch reports as an aggregate install failure, got {error:?}"
    );
    assert_eq!(
        engine.pending_read_count(),
        0,
        "a non-retryable read is dropped, never requeued"
    );
    block_on(engine.resolve_collect()).expect("the next resolve is a clean no-op");
}

#[test]
fn async_keyed_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");
    let Tier::KeyedRows { ref query, .. } = registered.tier else {
        panic!("expected keyed rows")
    };

    let _ = block_on(engine.snapshot(registered.subscription_id));
    let queries = engine.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), query.sql());
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
    drop(queries);
}

#[test]
fn async_keyed_event_scopes_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = block_on(engine.resolve_collect());
    let queries = engine.connector().page_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE (lower(status) = $1) AND \"id\" IN (1)"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
    drop(queries);
}

#[test]
fn async_grouped_bootstrap_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers");
    let Tier::GroupedScalar { ref bootstrap } = registered.tier else {
        panic!("expected grouped scalar")
    };

    let _ = block_on(engine.snapshot(registered.subscription_id));
    let queries = engine.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), bootstrap.query.sql());
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
    drop(queries);
}

#[test]
fn async_grouped_scoped_read_orders_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers")
        .subscription_id;
    crate::Install::install(
        &mut engine.inner,
        subscription,
        crate::GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(2),
            ]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("grouped seed installs");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = block_on(engine.resolve_collect());
    let queries = engine.connector().page_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT MIN(\"price\") AS v, COUNT(*) AS c1 FROM orders WHERE (quantity > $1) AND \"status\" = $2"
    );
    assert_eq!(
        queries[0].binds(),
        &[Value::Int(0), Value::String("paid".into())]
    );
    drop(queries);
}

#[test]
fn async_whole_snapshot_forwards_registration_binds() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let qid = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(vec![Value::Int(3)]),
            (),
        )
        .unwrap()
        .subscription_id;

    let _ = block_on(e.snapshot(qid));

    let queries = e.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
    drop(queries);
}

#[test]
fn async_whole_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(vec![Value::Int(3)]),
            (),
        )
        .expect("whole read registers");

    engine.apply(&insert_event(table, 2, 9.0)).unwrap();
    let _ = block_on(engine.resolve_collect());
    let queries = engine.connector().cursor_queries.lock();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders ORDER BY id DESC LIMIT $1"
    );
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
    drop(queries);
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(10.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    let event = update_status_only(tid, 1, 10.0);

    let n = e.apply(&event).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(n.outstanding, 0, "nothing was queued either");
    assert_eq!(e.connector().call_count(), 0);
}

/// Mirrors `auto::tests::a_dispatch_reports_the_reads_it_queued`,
/// because a shared line tested on one side only is correct for the
/// other by luck.
#[test]
fn async_dispatch_reports_the_reads_it_queued() {
    let (mut e, tid) = engine_with_values(vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");

    let queued = e
        .apply(&insert_event(tid, 1, 5.0))
        .expect("the event applies");
    assert_eq!(
        queued.outstanding,
        e.pending_read_count(),
        "the report is the depth of the queue, not a guess about it"
    );
    assert!(
        queued.outstanding > 0,
        "a whole-result subscription queues a read for this event"
    );

    let again = e
        .apply(&insert_event(tid, 2, 6.0))
        .expect("the event applies");
    assert_eq!(
        again.outstanding, queued.outstanding,
        "a burst coalesces, so the report does not count events"
    );

    // Depth two, so a count collapsed to `min(1)` cannot pass.
    e.register(
        SubscriptionRequest::new(2u64, "SELECT DISTINCT quantity FROM orders"),
        (),
    )
    .expect("a second whole read registers");
    let two = e
        .apply(&insert_event(tid, 4, 8.0))
        .expect("the event applies");
    assert_eq!(
        two.outstanding, 2,
        "two subscriptions each queued one read, so the depth is two"
    );

    // The mock refuses a cursor it has no page for.
    e.connector().cursor_pages.lock().extend([
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: false,
        },
        crate::reexec::RowPage {
            columns: vec![String::from("quantity")],
            rows: vec![vec![Value::Int(1)]],
            more: false,
        },
    ]);
    block_on(e.resolve_collect()).expect("the reads run");
    let settled = e
        .apply(&update_status_only(tid, 3, 7.0))
        .expect("the event applies");
    assert_eq!(
        e.pending_read_count(),
        settled.outstanding,
        "and it still agrees with the queue after a drain"
    );
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match block_on(e.resolve_collect()) {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}

/// Async batch coalesces repeated triggers for the same query into a
/// single connector call. Mirrors the sync engine's T4.1 assertion.
#[test]
fn async_applied_burst_coalesces_repeated_triggers() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(99.0)]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    let events = [
        delete_event(tid, 1, 5.0),
        delete_event(tid, 2, 5.0),
        delete_event(tid, 3, 5.0),
    ];

    let per_event: Vec<_> = events.iter().map(|ev| e.apply(ev).unwrap()).collect();
    assert_eq!(per_event.len(), 3, "per_event positional alignment");
    let outcome = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        1,
        "three displacing events collapse to one connector call"
    );
    assert_eq!(outcome.scalar_updates.len(), 1);
    assert_eq!(outcome.scalar_updates[0].value, Value::Float(99.0));
}

/// `with_max_concurrent_reexecutions` does not change the result of
/// one resolve of a burst. Correctness is preserved. The cap is a
/// throughput / fairness knob, not a semantic one.
#[test]
#[allow(clippy::similar_names)]
fn async_applied_burst_respects_max_concurrent_cap() {
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec for MIN, got {other:?}"),
    };
    let qid2 = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec for MAX, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid1,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    assert!(crate::Install::install(
        &mut e,
        qid2,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    let events = vec![delete_event(tid, 1, 7.0)];
    for event in &events {
        e.apply(event).unwrap();
    }
    let outcome = block_on(e.resolve_collect()).unwrap();
    assert_eq!(e.connector().call_count(), 2);
    assert_eq!(outcome.scalar_updates.len(), 2);
    let qids: std::collections::BTreeSet<_> = outcome
        .scalar_updates
        .iter()
        .map(|u| u.subscription_id)
        .collect();
    assert!(qids.contains(&qid1));
    assert!(qids.contains(&qid2));
}

/// `unregister_subscription` drops the stored auth context.
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(qid));
    assert_eq!(e.contexts.len(), 0);
}

// Re-execution concurrency throttle
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

/// Cleanup invariant: after a successful resolve of a burst the
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    let qid2 = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid1,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    assert!(crate::Install::install(
        &mut e,
        qid2,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    e.apply(&delete_event(tid, 1, 7.0)).unwrap();
    let _ = block_on(e.resolve_collect()).unwrap();
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
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    let qid2 = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(crate::Install::install(
        &mut e,
        qid1,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    assert!(crate::Install::install(
        &mut e,
        qid2,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    e.apply(&delete_event(tid, 1, 7.0)).unwrap();
    assert!(block_on(e.resolve_collect()).is_err());
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
                    SubscriptionRequest::new(c, "SELECT MIN(price) FROM orders WHERE quantity = 1"),
                    (),
                )
                .unwrap()
            {
                Registered {
                    subscription_id,
                    tier: Tier::Scalar { .. },
                    ..
                } => subscription_id,
                other => panic!("expected ReExec, got {other:?}"),
            }
        })
        .collect();
    for q in &qids {
        assert!(crate::Install::install(
            &mut e,
            *q,
            crate::ScalarInstall {
                value: Value::Float(7.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());
    }
    e.apply(&delete_event(tid, 1, 7.0)).unwrap();
    let outcome = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        3,
        "three distinct queries each get one connector call regardless of cap"
    );
    assert_eq!(outcome.scalar_updates.len(), 3);
}

#[test]
fn grouped_debounce_is_scoped_by_group_key_async() {
    let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
    let engine_clock: crate::ClockHandle = clock;
    let (engine, _) = engine_with_values(Vec::new());
    let mut engine = engine
        .with_clock(engine_clock)
        .with_debounce_per_query(core::time::Duration::from_secs(1));
    let first = super::super::ReExecutionRead::GroupedScalar {
        group: vec![1],
        query: super::super::BoundQuery::new(String::new(), Vec::new()),
        column_kinds: [ScalarFamily::Int, ScalarFamily::Int],
    };
    let second = super::super::ReExecutionRead::GroupedScalar {
        group: vec![2],
        query: super::super::BoundQuery::new(String::new(), Vec::new()),
        column_kinds: [ScalarFamily::Int, ScalarFamily::Int],
    };
    engine.stamp_reexec(7, &first);
    assert!(engine.debounce_skip(7, &first));
    assert!(!engine.debounce_skip(7, &second));
}

#[test]
fn async_unregister_subscription_resolves_either_registry() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let captured = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    let in_process = match e
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE price > 100"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    assert!(
        e.unregister_subscription(captured),
        "read registry id resolves"
    );
    assert!(
        e.unregister_subscription(in_process),
        "in-process registry id resolves"
    );
    assert!(
        !e.unregister_subscription(999u64),
        "an unknown id resolves to neither registry"
    );
}

#[test]
fn async_unregister_subscription_drops_the_resolve_context() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
    let captured = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        captured,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(captured));
    assert_eq!(e.contexts.len(), 0, "the resolve context is dropped");
    let n = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(
        e.connector().call_count(),
        0,
        "no connector call after unregister"
    );
}

#[test]
fn async_cursor_state_is_reachable_through_the_wrapper() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let session = 1u64;
    let sub = 7u64;
    let cp = crate::OpaqueCheckpoint(vec![1, 2, 3]);
    assert_eq!(e.advance_cursor(session, sub, cp.clone()), Ok(None));
    assert_eq!(e.cursor_for(session, sub), Some(&cp));
    let older = crate::OpaqueCheckpoint(vec![0]);
    assert_eq!(e.force_set_cursor(session, sub, older.clone()), Some(cp));
    assert_eq!(e.cursor_for(session, sub), Some(&older));
    let listed: Vec<_> = e
        .cursors_for_session(session)
        .map(|(s, c)| (s, c.clone()))
        .collect();
    assert_eq!(listed, vec![(sub, older.clone())]);
    assert_eq!(e.drop_cursor(session, sub), Some(older));
    assert_eq!(e.cursor_for(session, sub), None);
}

#[test]
fn async_match_rows_replays_without_reading_or_folding() {
    // match_rows must be a plain sync call here: the inner match does no
    // I/O, so no block_on wraps it. One value, for the single live read.
    let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
    let min_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        min_id,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    e.register(
        SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE price < 100"),
        (),
    )
    .unwrap();

    // A delete of the current extreme, replayed against the seeded model
    // before any live dispatch has moved it, so it is still displacing.
    let ev = delete_event(tid, 1, 5.0);
    let replay = e.match_rows(&ev).unwrap();
    assert!(
        !replay.deleted().is_empty(),
        "match_rows matched the row subscription"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "match_rows read nothing from the connector even for a displacing delete"
    );

    // The live dispatch of the same delete is still the first read: proof
    // match_rows left the re-execution model untouched.
    e.apply(&ev).unwrap();
    let live = block_on(e.resolve_collect()).unwrap();
    assert_eq!(
        e.connector().call_count(),
        1,
        "the re-execution model was untouched, so the live read is the first"
    );
    assert_eq!(live.scalar_updates.len(), 1);
    assert_eq!(live.scalar_updates[0].value, Value::Float(7.0));
}

#[test]
fn async_describe_terms_is_reachable_through_the_wrapper() {
    // Sync method even on the async wrapper: it only reads the engine's
    // compiler, no I/O, so no block_on.
    let (e, _tid) = engine_with_values(vec![]);
    let plain = e
        .describe_terms(&SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE price > 100",
        ))
        .expect("a plain filter is describable");
    assert!(plain.is_empty(), "a plain filter has no membership terms");
    let refused = e.describe_terms(&SubscriptionRequest::new(
        2u64,
        "SELECT * FROM orders WHERE nonexistent_column > 5",
    ));
    assert!(
        refused.is_err(),
        "an unknown-column filter is refused, got {refused:?}"
    );
}

#[test]
fn async_connector_error_names_its_subscription() {
    let (mut e, tid) = engine_with_values(vec![]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match block_on(e.resolve_collect()) {
        Ok(_) => panic!("expected the triggered read to fail"),
        Err(ReExecError::Connector {
            subscription,
            error: MockError(msg),
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert_eq!(msg, "queue empty");
        }
        Err(other) => panic!("expected Connector naming its subscription, got {other:?}"),
    }
}

#[test]
fn async_cursor_error_names_its_subscription() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::WholeRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected WholeRows, got {other:?}"),
    };
    match block_on(e.snapshot(qid)) {
        Ok(_) => panic!("expected the cursorless read to fail"),
        Err(ReExecError::Cursor {
            subscription,
            error,
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert!(matches!(error, super::super::CursorError::Unsupported));
        }
        Err(other) => panic!("expected Cursor naming its subscription, got {other:?}"),
    }
}

#[test]
fn async_ungrouped_aggregate_folds_through_the_wrapper() {
    let (mut e, tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(5)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    let n = e.apply(&insert_event(tid, 1, 5.0)).unwrap();
    assert_eq!(
        n.aggregate_updates.len(),
        1,
        "one aggregate update through the wrapper"
    );
    assert_eq!(
        n.aggregate_updates[0].folded_value(),
        Some(crate::AggValue::CountStar(6)),
        "the incremented total"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "an in-process fold reads nothing"
    );
}

#[test]
fn async_ungrouped_aggregate_folds_across_an_applied_burst() {
    let (mut e, tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(5)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    let aggregate_updates: Vec<_> = [insert_event(tid, 1, 5.0), insert_event(tid, 2, 6.0)]
        .iter()
        .flat_map(|ev| e.apply(ev).unwrap().aggregate_updates)
        .collect();
    assert_eq!(aggregate_updates.len(), 2, "each insert folds");
    assert_eq!(
        aggregate_updates.last().unwrap().folded_value(),
        Some(crate::AggValue::CountStar(7)),
        "the running total after both inserts"
    );
}

#[test]
fn async_ungrouped_aggregate_demotion_resolves_through_the_wrapper() {
    let (mut e, tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders WHERE status = 'paid'"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    let missing_old = TestEvent::<Postgres>::update(tid, vec![], row(1, 5.0))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16]);
    e.apply(&missing_old).unwrap();
    match block_on(e.resolve_collect()) {
        Err(ReExecError::Cursor { subscription, .. }) => {
            assert_eq!(
                subscription, count_id,
                "the demoted aggregate attempts its whole read"
            );
        }
        Ok(_) => panic!("expected the demotion to attempt a whole read"),
        Err(other) => panic!("expected a Cursor error naming the aggregate, got {other:?}"),
    }
}

#[test]
fn async_snapshot_of_a_folding_aggregate_is_none() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(5)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        block_on(e.snapshot(count_id)).unwrap().is_none(),
        "no bootstrap for a fold"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "snapshot reads nothing for a fold"
    );
}

#[test]
fn async_a_seed_that_demotes_at_install_serves_the_whole_read() {
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        catalog(),
        PostgreSqlDialect {},
    )
    .with_max_groups_per_aggregate(1);
    let mut e = AutoResolvingEngine::new(inner, AsyncMode::new(MockAsyncConnector::new(vec![])));
    let grouped = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT status, COUNT(*) FROM orders GROUP BY status"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    let seeded = crate::Install::install(
        &mut e,
        grouped,
        crate::AggregateSeedInstall {
            rows: vec![
                vec![Value::String("open".into()), Value::Int(2), Value::Int(2)],
                vec![Value::String("done".into()), Value::Int(1), Value::Int(1)],
            ],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        !seeded.transitions.is_empty(),
        "the install-time demotion carries a transition"
    );
    match block_on(e.snapshot(grouped)) {
        Err(ReExecError::Cursor { subscription, .. }) => {
            assert_eq!(
                subscription, grouped,
                "the demoted subscription attempts its whole read"
            );
        }
        Ok(answer) => panic!("expected the whole read to be attempted, got {answer:?}"),
        Err(other) => panic!("expected a Cursor error naming the subscription, got {other:?}"),
    }
}

#[test]
fn async_ordered_row_query_folds_in_process() {
    let (mut e, tid) = engine_with_values(vec![]);
    match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY price"),
            (),
        )
        .unwrap()
    {
        Registered {
            tier: Tier::InProcess(_),
            ..
        } => {}
        other => panic!("expected InProcess for an ordered row query, got {other:?}"),
    }
    let n = e.apply(&insert_event(tid, 1, 5.0)).unwrap();
    assert!(
        n.engine.inserted().contains(&1),
        "the ordered row list is notified of the insert"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "an ordered row list reads nothing"
    );
}

/// Dropping a resolve mid-read loses nothing: the read stays queued, a
/// fresh resolve completes it, and the event is never reapplied.
#[test]
fn dropped_resolve_keeps_the_read_queued() {
    let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
    let qid = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected Scalar, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .unwrap();

    let applied = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert!(
        applied.scalar_updates.is_empty(),
        "the read is queued, not run"
    );
    assert_eq!(e.pending_read_count(), 1);

    *e.connector().pend_next_read.lock() = true;
    {
        let mut ctx = Context::from_waker(core::task::Waker::noop());
        let mut sink =
            |_delivery: super::super::ReadDelivery<DefaultIds, Postgres, NoCheckpoint>| {};
        let fut = e.resolve(&mut sink);
        let mut pinned = pin!(fut);
        assert!(
            pinned.as_mut().poll(&mut ctx).is_pending(),
            "the resolve suspends inside the connector read"
        );
        // Dropped here, mid-read.
    }
    assert_eq!(e.pending_read_count(), 1, "the dropped read stayed queued");
    assert_eq!(e.connector().call_count(), 0, "the read never completed");

    let resolved = block_on(e.resolve_collect()).unwrap();
    assert_eq!(resolved.scalar_updates.len(), 1);
    assert_eq!(resolved.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.pending_read_count(), 0);
    assert_eq!(
        e.connector().call_count(),
        1,
        "one completed read, no redispatch"
    );
}

/// Each page reaches the sink before the next page is fetched, so
/// retained memory tracks one page rather than the whole answer.
#[test]
fn async_pages_reach_the_sink_before_the_next_fetch() {
    let (mut e, tid) = engine_with_values(vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.lock().extend([
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: true,
        },
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();

    let log = Arc::clone(&e.connector().log);
    block_on(e.resolve(move |delivery| {
        if matches!(delivery, crate::reexec::ReadDelivery::Rows(_)) {
            log.lock().push("deliver");
        }
    }))
    .unwrap();
    assert_eq!(
        *e.connector().log.lock(),
        ["open", "fetch", "deliver", "fetch", "deliver", "close"],
        "a page is delivered before the next one is fetched"
    );
}

/// A resolve dropped between pages leaves the read queued, and the retry
/// streams a complete answer under a higher generation, which is the
/// consumer's signal to discard the partial one.
#[test]
fn dropped_stream_is_superseded_by_a_higher_generation() {
    let (mut e, tid) = engine_with_values(vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.lock().extend([
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: true,
        },
        crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();

    // The second fetch suspends, and the future is dropped there: one
    // partial page was already delivered.
    *e.connector().pend_fetch_at.lock() = Some(1);
    let partial = Arc::new(Mutex::new(Vec::new()));
    {
        let partial = Arc::clone(&partial);
        let mut ctx = Context::from_waker(core::task::Waker::noop());
        let fut = e.resolve(move |delivery| {
            if let crate::reexec::ReadDelivery::Rows(page) = delivery {
                partial.lock().push(page.generation);
            }
        });
        let mut pinned = pin!(fut);
        assert!(
            pinned.as_mut().poll(&mut ctx).is_pending(),
            "the resolve suspends between pages"
        );
    }
    assert_eq!(partial.lock().len(), 1, "one partial page was delivered");
    assert_eq!(e.pending_read_count(), 1, "the dropped read stayed queued");

    // The retry streams a complete answer under a higher generation.
    e.connector()
        .cursor_pages
        .lock()
        .push(crate::reexec::RowPage {
            columns: vec![String::from("status")],
            rows: vec![vec![Value::String("paid".into())]],
            more: false,
        });
    let retried = block_on(e.resolve_collect()).unwrap();
    assert_eq!(e.pending_read_count(), 0);
    assert!(!retried.rows_updates.is_empty());
    let partial_generation = partial.lock()[0];
    assert!(
        retried
            .rows_updates
            .iter()
            .all(|page| page.generation > partial_generation),
        "the complete answer supersedes the partial generation"
    );
    assert!(
        !retried.rows_updates.last().unwrap().more,
        "the retry ends its generation"
    );
}
