//! Tests for the synchronous engine, moved out of `auto.rs` verbatim.

#![allow(clippy::unwrap_used)]

use super::super::test_fixtures::{catalog, delete_event, insert_event, row, update_status_only};
use super::*;
use crate::backend::Postgres;
use crate::testing::TestEvent;
use crate::TableId;
use crate::{DefaultIds, SubscriptionEngine};
use core::cell::RefCell;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

/// Records every call and serves a programmed value queue. Errors are
/// modeled by leaving the queue empty when `panic_on_empty` is false.
struct MockConnector {
    values: RefCell<alloc::vec::Vec<Value<Postgres>>>,
    calls: RefCell<alloc::vec::Vec<(String, ScalarFamily)>>,
    scalar_queries: RefCell<alloc::vec::Vec<super::super::ReadQuery<'static, Postgres>>>,
    page_queries: RefCell<alloc::vec::Vec<super::super::ReadQuery<'static, Postgres>>>,
    cursor_queries: RefCell<alloc::vec::Vec<super::super::ReadQuery<'static, Postgres>>>,
    /// Pages a whole re-read serves, front first. Empty means the mock
    /// holds no cursors and `open_cursor` refuses.
    cursor_pages: RefCell<alloc::vec::Vec<super::super::RowPage<Postgres>>>,
    /// Pages `read_page` serves, popped from the back like `values`.
    /// Empty keeps the historic refusal, which the scalar tests rely on.
    pages: RefCell<alloc::vec::Vec<super::super::RowPage<Postgres>>>,
    /// Which `fetch_cursor` call fails, zero-based, if any. The async
    /// mock suspends at a fetch to model an abandoned read; the sync
    /// path has no suspension, so a read is left part way by a fetch
    /// that raises.
    fail_fetch_at: RefCell<Option<usize>>,
    /// How many times `fetch_cursor` has been called.
    fetches: RefCell<usize>,
    /// Interleaving log shared with the test's sink.
    log: alloc::rc::Rc<RefCell<alloc::vec::Vec<&'static str>>>,
}

impl MockConnector {
    fn new(values: alloc::vec::Vec<Value<Postgres>>) -> Self {
        Self {
            values: RefCell::new(values),
            calls: RefCell::new(alloc::vec::Vec::new()),
            scalar_queries: RefCell::new(alloc::vec::Vec::new()),
            page_queries: RefCell::new(alloc::vec::Vec::new()),
            cursor_queries: RefCell::new(alloc::vec::Vec::new()),
            cursor_pages: RefCell::new(alloc::vec::Vec::new()),
            pages: RefCell::new(alloc::vec::Vec::new()),
            fail_fetch_at: RefCell::new(None),
            fetches: RefCell::new(0),
            log: alloc::rc::Rc::new(RefCell::new(alloc::vec::Vec::new())),
        }
    }
    fn call_count(&self) -> usize {
        self.calls.borrow().len()
    }
    fn push_page(&self, page: super::super::RowPage<Postgres>) {
        self.pages.borrow_mut().push(page);
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
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &super::super::ReadQuery<'_, Postgres>,
        column_kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error> {
        self.calls
            .borrow_mut()
            .push((String::from(query.sql()), column_kind));
        self.scalar_queries
            .borrow_mut()
            .push(query.clone().into_owned());
        let value = self
            .values
            .borrow_mut()
            .pop()
            .ok_or(MockError("queue empty"))?;
        Ok((value, None))
    }

    fn read_page(
        &self,
        query: &super::super::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<
        super::super::connector::Snapshot<
            super::super::connector::RowPage<Postgres>,
            Self::Checkpoint,
        >,
        Self::Error,
    > {
        self.page_queries
            .borrow_mut()
            .push(query.clone().into_owned());
        let popped = self.pages.borrow_mut().pop();
        let Some(page) = popped else {
            return Err(MockError("read_page is not exercised by the scalar tests"));
        };
        Ok(super::super::connector::Snapshot {
            value: page,
            checkpoint: None,
        })
    }

    fn open_cursor(
        &self,
        query: &super::super::ReadQuery<'_, Postgres>,
        _auth: &(),
    ) -> Result<super::super::CursorId, super::super::CursorError<Self::Error>> {
        self.cursor_queries
            .borrow_mut()
            .push(query.clone().into_owned());
        if self.cursor_pages.borrow().is_empty() {
            return Err(super::super::CursorError::Unsupported);
        }
        self.log.borrow_mut().push("open");
        Ok(super::super::CursorId(1))
    }

    fn fetch_cursor(
        &self,
        _cursor: super::super::CursorId,
        _max_bytes: usize,
    ) -> Result<
        super::super::connector::Snapshot<
            super::super::connector::RowPage<Postgres>,
            Self::Checkpoint,
        >,
        super::super::CursorError<Self::Error>,
    > {
        self.log.borrow_mut().push("fetch");
        let fetch = *self.fetches.borrow();
        *self.fetches.borrow_mut() = fetch + 1;
        if *self.fail_fetch_at.borrow() == Some(fetch) {
            return Err(super::super::CursorError::Unsupported);
        }
        let page = self.cursor_pages.borrow_mut().remove(0);
        Ok(super::super::connector::Snapshot {
            value: page,
            checkpoint: None,
        })
    }

    fn close_cursor(
        &self,
        _cursor: super::super::CursorId,
    ) -> Result<(), super::super::CursorError<Self::Error>> {
        self.log.borrow_mut().push("close");
        Ok(())
    }
}

struct DisagreeingRegistrationRequest(SubscriptionRequest<DefaultIds, Postgres>);

impl crate::RegistrationRequest<DefaultIds, Postgres> for DisagreeingRegistrationRequest {
    const DATABASE_READS_PER_CONSUMER: bool = false;

    fn into_request(self) -> SubscriptionRequest<DefaultIds, Postgres> {
        self.0
    }
}

fn engine_with_values(
    values: alloc::vec::Vec<Value<Postgres>>,
) -> (
    AutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, SyncMode<MockConnector>>,
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
        AutoResolvingEngine::new(inner, SyncMode(MockConnector::new(values))),
        orders_id,
    )
}

/// Full path: register, bootstrap install, insert that does not displace
/// the extreme (in-process scalar update, no connector call), delete of
/// the current extreme (trigger -> connector -> ScalarUpdate). The
/// returned notifications carry no triggers under AutoResolvingEngine.
#[test]
fn delete_of_extreme_resolves_via_connector() {
    // Connector returns 7.0 when re-run after the extreme is removed.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);

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
    // Bootstrap: model = {1=>5.0}. Current MIN = 5.0.
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    // Insert price=9.0 (>5.0): in-process Unchanged, no scalar update, no trigger.
    let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
    assert!(n.scalar_updates.is_empty(), "insert above extreme");
    assert_eq!(e.connector().call_count(), 0, "no re-execution yet");

    // Delete id=1, price=5.0 (the current extreme): trigger -> connector -> 7.0.
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].subscription_id, qid);
    assert_eq!(n.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.connector().call_count(), 1);
    let (sql, kind) = e.connector().calls.borrow()[0].clone();
    assert!(sql.contains("MIN"));
    assert_eq!(kind, ScalarFamily::Float);
}

#[test]
fn unrelated_column_update_does_not_call_connector() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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

    let n = e.apply(&update_status_only(tid, 1, 10.0)).unwrap();
    assert!(n.scalar_updates.is_empty());
    assert_eq!(e.connector().call_count(), 0);
}

#[test]
fn connector_error_aborts_batch() {
    // Empty queue: the connector errors on first call.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
    match e.resolve_collect() {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}

/// `snapshot(subscription_id)` reads through the connector and installs the
/// value so subsequent dispatches see it as the current state.
#[test]
fn snapshot_installs_via_connector() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(12.5)]);
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

    // No bootstrap install: snapshot does it.
    let snap = e.snapshot(qid).unwrap().expect("subscription_id exists");
    match snap {
        SnapshotResult::Scalar(value, checkpoint) => {
            assert_eq!(value, Value::Float(12.5));
            // MockConnector returns checkpoint = None.
            assert!(checkpoint.is_none());
        }
        other => panic!("a scalar capture snapshots as a scalar, got {other:?}"),
    }
    assert_eq!(e.connector().call_count(), 1);

    // After snapshot, the engine treats 12.5 as the current MIN.
    // An insert below it (e.g. 9.0) becomes the new in-process MIN.
    let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
    assert_eq!(e.connector().call_count(), 1);
}

#[test]
fn sync_scalar_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(alloc::vec![Value::Float(5.0)]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(alloc::vec![Value::Int(2)]),
            (),
        )
        .expect("scalar registers")
        .subscription_id;

    engine
        .snapshot(subscription)
        .expect("snapshot succeeds")
        .expect("snapshot exists");
    let queries = engine.connector().scalar_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(2)]);
}

#[test]
fn sync_scalar_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![Value::Float(9.0)]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                .binds(alloc::vec![Value::Int(0)]),
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

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    engine.resolve_collect().expect("delete resolves");
    let queries = engine.connector().scalar_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
}

#[test]
fn sync_keyed_initial_snapshot_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(alloc::vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(alloc::vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");
    let Tier::KeyedRows { ref query, .. } = registered.tier else {
        panic!("expected keyed rows")
    };

    let _ = engine.snapshot(registered.subscription_id);
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), query.sql());
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

#[test]
fn sync_keyed_event_scopes_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(alloc::vec![Value::String("paid".into())]),
            (),
        )
        .expect("keyed read registers");

    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = engine.resolve_collect();
    let queries = engine.connector().page_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE (lower(status) = $1) AND \"id\" IN (1)"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

/// `debounced` counts the unanswered-cell path, not only the trigger
/// path.
#[expect(
    clippy::clone_on_ref_ptr,
    reason = "the clone below performs the Arc<ManualClock> to Arc<dyn Clock> \
              unsize coercion at the assignment site, which Arc::clone cannot \
              do from an uncoerced source"
)]
#[test]
fn a_debounced_unanswered_read_is_reported() {
    let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
    let engine_clock: crate::ClockHandle = clock.clone();
    let (e0, tid) = engine_with_values(alloc::vec![]);
    let mut engine = e0
        .with_clock(engine_clock)
        .with_debounce_per_query(core::time::Duration::from_millis(100));
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
            (),
        )
        .expect("the filter is served in process");

    // The row image omits `status`, which the predicate reads, so the
    // event cannot answer it and the read is queued.
    let unanswerable = |id: i64| {
        let mut cells = row(id, 5.0);
        cells[3] = Value::Missing;
        TestEvent::<Postgres>::update(tid, row(id, 5.0), cells)
            .with_pk_columns([0u16])
            .with_changed_columns([1u16])
    };

    engine
        .connector()
        .cursor_pages
        .borrow_mut()
        .push(super::super::RowPage {
            columns: alloc::vec![String::from("id"), String::from("status")],
            rows: alloc::vec![alloc::vec![Value::Int(1), Value::String("paid".into())]],
            more: false,
        });
    let first = engine.apply(&unanswerable(1)).expect("the event applies");
    assert_eq!(
        first.debounced, 0,
        "nothing has run yet, so nothing is dropped"
    );
    assert!(first.outstanding > 0, "the unanswered cell queued a read");
    engine.resolve_collect().expect("the read runs and stamps");

    // Inside the window, the same subscription's read is discarded.
    clock.advance(core::time::Duration::from_millis(50));
    let second = engine.apply(&unanswerable(2)).expect("the event applies");
    assert_eq!(
        second.debounced, 1,
        "the window dropped the unanswered-cell read and the report says so"
    );
    assert_eq!(
        second.outstanding, 0,
        "which the queue depth alone cannot say"
    );
}

/// `outstanding` is the queue's depth now: not a constant, not one per
/// event, and not the number ever queued.
#[test]
fn a_dispatch_reports_the_reads_it_queued() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .expect("whole read registers");

    // An event no subscription cares about queues nothing.
    let quiet = engine
        .apply(&insert_event(table, 1, 5.0))
        .expect("the event applies");
    let after_first = quiet.outstanding;

    engine
        .connector()
        .cursor_pages
        .borrow_mut()
        .push(super::super::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: false,
        });
    assert_eq!(
        after_first,
        engine.pending_read_count(),
        "the report is the depth of the queue, not a guess about it"
    );
    assert!(
        after_first > 0,
        "a whole-result subscription queues a read for this event"
    );

    // A second event for the same subscription coalesces: the queue is
    // keyed by subscription, so the depth does not grow.
    let again = engine
        .apply(&insert_event(table, 2, 6.0))
        .expect("the event applies");
    assert_eq!(
        again.outstanding, after_first,
        "a burst coalesces, so the report does not count events"
    );

    // Depth two, so a count collapsed to `min(1)` cannot pass.
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT DISTINCT quantity FROM orders"),
            (),
        )
        .expect("a second whole read registers");
    let two = engine
        .apply(&insert_event(table, 4, 8.0))
        .expect("the event applies");
    assert_eq!(
        two.outstanding, 2,
        "two subscriptions each queued one read, so the depth is two"
    );

    // The mock refuses a cursor it has no page for.
    engine
        .connector()
        .cursor_pages
        .borrow_mut()
        .push(super::super::RowPage {
            columns: alloc::vec![String::from("quantity")],
            rows: alloc::vec![alloc::vec![Value::Int(1)]],
            more: false,
        });
    engine.resolve_collect().expect("the reads run");
    let settled = engine
        .apply(&update_status_only(table, 3, 7.0))
        .expect("the event applies");
    assert_eq!(
        engine.pending_read_count(),
        settled.outstanding,
        "and it still agrees with the queue after a drain"
    );
}

/// A keyed read asks about the row the event says is its own.
///
/// The key is built from the columns the event declares as its
/// primary key, in `KeyedQuery::on_event`, so a declaration naming
/// the wrong column asks the database about the wrong row. Two
/// inserts are used rather than one, and their ids differ, because
/// one insert cannot tell a correct declaration from one naming a
/// column that happens to hold the same value in every fixture row:
/// `quantity` is always 1 here, so pointing the key at it collapses
/// both rows onto a single key and the read asks about half of what
/// changed.
///
/// The delete path was already covered by
/// `sync_keyed_event_scopes_registration_binds`. This is the insert
/// path, which nothing depended on: pointing the shared fixture's
/// insert at another column reddened no test in the suite.
#[test]
fn sync_keyed_insert_asks_about_the_declared_key() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = 'paid'"),
            (),
        )
        .expect("keyed read registers");

    engine.apply(&insert_event(table, 1, 5.0)).unwrap();
    engine.apply(&insert_event(table, 2, 6.0)).unwrap();
    let _ = engine.resolve_collect();

    let queries = engine.connector().page_queries.borrow();
    assert_eq!(queries.len(), 1, "both keys are asked in one read");
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE (lower(status) = 'paid') AND \"id\" IN (1, 2)",
        "the read names the id column and both ids"
    );
}

#[test]
fn sync_grouped_bootstrap_forwards_registration_binds() {
    let (mut engine, _) = engine_with_values(alloc::vec![]);
    let registered = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(alloc::vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers");
    let Tier::GroupedScalar { ref bootstrap } = registered.tier else {
        panic!("expected grouped scalar")
    };

    let _ = engine.snapshot(registered.subscription_id);
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql(), bootstrap.query.sql());
    assert_eq!(queries[0].binds(), &[Value::Int(0)]);
}

#[test]
fn sync_grouped_scoped_read_orders_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
            )
            .binds(alloc::vec![Value::Int(0)]),
            (),
        )
        .expect("grouped read registers")
        .subscription_id;
    crate::Install::install(
        &mut engine.inner,
        subscription,
        crate::GroupedScalarSeedInstall {
            rows: alloc::vec![alloc::vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(2),
            ]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("grouped seed installs");
    engine.apply(&delete_event(table, 1, 5.0)).unwrap();
    let _ = engine.resolve_collect();
    let queries = engine.connector().page_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT MIN(\"price\") AS v, COUNT(*) AS c1 FROM orders WHERE (quantity > $1) AND \"status\" = $2"
    );
    assert_eq!(
        queries[0].binds(),
        &[Value::Int(0), Value::String("paid".into())]
    );
}

#[test]
fn whole_snapshot_forwards_registration_binds() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    let qid = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(alloc::vec![Value::Int(3)]),
            (),
        )
        .unwrap()
        .subscription_id;

    let _ = e.snapshot(qid);

    let queries = e.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
}

#[test]
fn sync_whole_event_forwards_registration_binds() {
    let (mut engine, table) = engine_with_values(alloc::vec![]);
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                .binds(alloc::vec![Value::Int(3)]),
            (),
        )
        .expect("whole read registers");

    engine.apply(&insert_event(table, 2, 9.0)).unwrap();
    let _ = engine.resolve_collect();
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders ORDER BY id DESC LIMIT $1"
    );
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
}

#[test]
fn registration_request_metadata_comes_from_consumed_request() {
    let session = 91u64;
    let request = DisagreeingRegistrationRequest(
        SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
            .binds(alloc::vec![Value::Int(3)])
            .scope(SubscriptionScope::Session(session)),
    );
    let (mut engine, _) = engine_with_values(alloc::vec![]);
    let subscription = engine
        .register(request, ())
        .expect("request registers")
        .subscription_id;

    let _ = engine.snapshot(subscription);
    let queries = engine.connector().cursor_queries.borrow();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].binds(), &[Value::Int(3)]);
    drop(queries);

    let _ = engine.unregister_session(session);
    assert!(engine.contexts.is_empty());
}

/// `snapshot(subscription_id)` on an unknown id returns `Ok(None)` rather
/// than panicking so callers can race snapshot against unregister.
#[test]
fn snapshot_unknown_query_returns_none() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    assert!(e.snapshot(99999).unwrap().is_none());
    // Connector was never called.
    assert_eq!(e.connector().call_count(), 0);
}

/// T4.1 + T4.2: a batch of 3 events that displace the same captured
/// query's extreme produces ONE connector call (dedup), and engine
/// notifications come back in input order.
#[test]
fn applied_burst_coalesces_repeated_triggers() {
    // Connector serves a single value, which is what we expect since
    // the trigger should be deduplicated to one call.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(99.0)]);
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
    // Bootstrap: extreme is 5.0.
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());

    // Three DELETEs of the current extreme. Each one in isolation
    // would emit a trigger, the batch should collapse them.
    let events = alloc::vec![
        delete_event(tid, 1, 5.0),
        delete_event(tid, 2, 5.0),
        delete_event(tid, 3, 5.0),
    ];

    let per_event: alloc::vec::Vec<_> = events.iter().map(|ev| e.apply(ev).unwrap()).collect();
    let resolve_outcome = e.resolve_collect().unwrap();
    assert_eq!(
        per_event.len(),
        3,
        "per_event must align positionally with input"
    );
    assert_eq!(
        e.connector().call_count(),
        1,
        "three displacing events must collapse to one connector call"
    );
    assert_eq!(resolve_outcome.scalar_updates.len(), 1);
    assert_eq!(resolve_outcome.scalar_updates[0].value, Value::Float(99.0));
    assert_eq!(e.pending_read_count(), 0, "auto-resolving drains reads");
}

/// T4.3: a connector failure mid-batch aborts the whole batch with
/// `ReExecError::Connector` and the caller is expected to retry.
#[test]
fn applied_burst_error_surfaces_from_resolve() {
    // Empty value queue: connector errors on first call.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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

    let events = alloc::vec![delete_event(tid, 1, 5.0)];
    for ev in &events {
        e.apply(ev).unwrap();
    }
    match e.resolve_collect() {
        Ok(_) => panic!("expected Connector error, got Ok"),
        Err(ReExecError::Connector {
            error: MockError(msg),
            ..
        }) => assert_eq!(msg, "queue empty"),
        Err(other) => panic!("expected Connector error, got {other:?}"),
    }
}
/// Coalescing only collapses the **same** `subscription_id`. Distinct captured
/// queries each trigger their own connector call.
#[test]
#[allow(clippy::similar_names)]
fn applied_burst_keeps_distinct_queries_apart() {
    // Two captured queries on the same table. Connector returns 11.0
    // (popped first) for one and 22.0 (popped second) for the other.
    // MockConnector pops from the back, so push values in reverse:
    // first pop = 22.0, second pop = 11.0.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(22.0), Value::Float(11.0)]);
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
    // Bootstrap both at 7.0 so deleting price=7.0 displaces both.
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

    let events = alloc::vec![delete_event(tid, 1, 7.0)];
    for ev in &events {
        e.apply(ev).unwrap();
    }
    let resolve_outcome = e.resolve_collect().unwrap();
    assert_eq!(e.connector().call_count(), 2, "one call per distinct query");
    assert_eq!(resolve_outcome.scalar_updates.len(), 2);
    let qids: alloc::collections::BTreeSet<_> = resolve_outcome
        .scalar_updates
        .iter()
        .map(|u| u.subscription_id)
        .collect();
    assert!(qids.contains(&qid1));
    assert!(qids.contains(&qid2));
}

/// T6.1: a second displacing event within the debounce window is
/// dropped: connector is not called and no ScalarUpdate is emitted.
/// T6.2 in the same test: after the clock ticks past the window the
/// next trigger fires normally.
#[test]
// The `.clone()` below needs the Arc<ManualClock> -> Arc<dyn Clock>
// unsize coercion at the assignment site. `Arc::clone(&clock)` would
// need an already-coerced source. Allow the clippy lint here.
#[allow(clippy::clone_on_ref_ptr)]
fn debounce_skips_within_window_and_fires_after() {
    let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
    let engine_clock: crate::ClockHandle = clock.clone();
    // Two values in the connector queue: one for the first re-exec,
    // one for the post-window re-exec. The "within window" re-exec
    // is debounced and never reaches the connector.
    let (e0, tid) = engine_with_values(alloc::vec![Value::Float(20.0), Value::Float(7.0)]);
    let mut e = e0
        .with_clock(engine_clock)
        .with_debounce_per_query(core::time::Duration::from_millis(100));

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

    // First displacing event: re-exec proceeds (no prior stamp).
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1);
    assert_eq!(n.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.connector().call_count(), 1);

    // Second displacing event within 50ms (window is 100ms): skipped.
    clock.advance(core::time::Duration::from_millis(50));
    // The engine's MIN is currently 7.0 from the prior re-exec. To
    // force a second trigger we delete a row matching 7.0.
    let dispatched = e.apply(&delete_event(tid, 2, 7.0)).unwrap();
    // The queue is empty because the trigger was discarded.
    assert_eq!(
        dispatched.debounced, 1,
        "the window dropped this event's read and the report says so"
    );
    assert_eq!(
        dispatched.outstanding, 0,
        "and nothing is queued, which on its own would look answered"
    );
    let n = e.resolve_collect().unwrap();
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
    // displacement is well-defined. The test exercises debounce,
    // not the state machine.
    assert!(crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(7.0),
            checkpoint: None::<crate::NoCheckpoint>
        }
    )
    .is_ok());
    e.apply(&delete_event(tid, 3, 7.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1, "post-window trigger must fire");
    assert_eq!(n.scalar_updates[0].value, Value::Float(20.0));
    assert_eq!(e.connector().call_count(), 2);
}

/// Without a configured clock, `with_debounce_per_query` is a no-op:
/// triggers fire as normal.
#[test]
fn debounce_without_clock_is_a_noop() {
    let (e0, tid) = engine_with_values(alloc::vec![Value::Float(9.0), Value::Float(7.0)]);
    let mut e = e0.with_debounce_per_query(core::time::Duration::from_secs(3600));

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
    let result = e.resolve_collect().unwrap();
    assert!(!result.scalar_updates.is_empty());
    // trigger. The debounce-without-clock case must NOT skip it.
    e.apply(&delete_event(tid, 2, 7.0)).unwrap();
    let n = e.resolve_collect().unwrap();
    assert_eq!(n.scalar_updates.len(), 1, "no clock -> no debounce");
    assert_eq!(e.connector().call_count(), 2);
}

#[test]
fn unregister_drops_auth_context() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
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
    assert!(!e.unregister_subscription(qid), "second drop is a no-op");
}
#[test]
fn grouped_batch_keeps_one_trigger_per_displaced_group() {
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
            rows: vec![
                vec![
                    Value::String("paid".into()),
                    Value::Float(5.0),
                    Value::Int(2),
                ],
                vec![
                    Value::String("void".into()),
                    Value::Float(7.0),
                    Value::Int(2),
                ],
            ],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .expect("group map installs");
    let delete = |id, price, status: &str| {
        TestEvent::<Postgres>::delete(
            table,
            vec![
                Value::Int(id),
                Value::Float(price),
                Value::Int(1),
                Value::String(status.into()),
            ],
        )
        .with_pk_columns([0u16])
    };
    engine
        .apply(&delete(1, 5.0, "paid"))
        .expect("first delete dispatches");
    engine
        .apply(&delete(2, 7.0, "void"))
        .expect("second delete dispatches");
    assert_eq!(
        engine.pending_read_count(),
        2,
        "one queued read per displaced group, never coalesced across groups"
    );
}

/// An install failure is deterministic: the database answer does not
/// match the subscription, and the same read returns the same answer.
/// The failing read is dropped rather than requeued, exactly as the
/// resolve contract documents, so it cannot block every read behind it.
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
    engine.connector().push_page(super::super::RowPage {
        columns: vec!["min".into(), "n".into()],
        rows: vec![
            vec![Value::Float(6.0), Value::Int(1)],
            vec![Value::Float(7.0), Value::Int(1)],
        ],
        more: false,
    });
    let error = engine.resolve_collect().unwrap_err();
    assert!(
        matches!(error, super::super::ReExecError::AggregateInstall(_)),
        "the row count mismatch reports as an aggregate install failure, got {error:?}"
    );
    assert_eq!(
        engine.pending_read_count(),
        0,
        "a non-retryable read is dropped, never requeued"
    );
    engine
        .resolve_collect()
        .expect("the next resolve is a clean no-op");
}

/// Removing reads by key must not leave storage behind: the async
/// resolver drains exclusively through key removal and never pops, so
/// consumed entries have to be reclaimed or the queue grows for the
/// process lifetime and every snapshot rescans dead history.
#[test]
fn key_removal_reclaims_queue_storage() {
    let mut queue: super::ReadQueue<DefaultIds, crate::NoCheckpoint, Postgres> =
        super::ReadQueue::new();
    for round in 0..64u64 {
        queue.enqueue(super::super::ReExecutionTrigger {
            subscription_id: round,
            consumer_id: 1u64,
            read: super::super::ReExecutionRead::Subscription,
            checkpoint: None,
        });
        queue.remove(round, None);
    }
    assert!(queue.is_empty(), "every queued read was removed");
    assert!(
        queue.entry_slots() <= 1,
        "consumed entries are reclaimed, got {} slots for an empty queue",
        queue.entry_slots()
    );
}

#[test]
fn grouped_debounce_is_scoped_by_group_key() {
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
fn unregister_subscription_resolves_either_registry() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    // Captured re-execution query: lands in the read registry with a context.
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
    // In-process row subscription: lands in the in-process registry, no context.
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
fn unregister_subscription_drops_the_resolve_context() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);
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
    assert_eq!(e.contexts.len(), 1);
    assert!(e.unregister_subscription(captured));
    assert_eq!(e.contexts.len(), 0, "the resolve context is dropped");
    // A later delete of the former extreme must not reach the connector.
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no connector call after unregister"
    );
}

/// A queued read must not outlive its subscription: unregistering purges
/// it, so the next resolve is a clean no-op rather than a panic on the
/// missing resolve context.
#[test]
fn unregister_subscription_drops_the_queued_read() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(5.0)]);
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
    e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no read runs for a dead subscription"
    );
}

/// The session twin: unregistering a session purges the queued reads of
/// every subscription it carried.
#[test]
fn unregister_session_drops_the_queued_reads() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(5.0)]);
    let session = 9u64;
    let captured = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders")
                .scope(crate::SubscriptionScope::Session(session)),
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
    e.unregister_session(session);
    assert_eq!(
        e.pending_read_count(),
        0,
        "the session took its queued reads with it"
    );
    e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        0,
        "no read runs for a dead session"
    );
}

#[test]
fn cursor_state_is_reachable_through_the_wrapper() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    let session = 1u64;
    let sub = 7u64;
    let cp = crate::OpaqueCheckpoint(alloc::vec![1, 2, 3]);
    assert_eq!(e.advance_cursor(session, sub, cp.clone()), Ok(None));
    assert_eq!(e.cursor_for(session, sub), Some(&cp));
    // force_set bypasses the monotonic rule and returns the previous value.
    let older = crate::OpaqueCheckpoint(alloc::vec![0]);
    assert_eq!(e.force_set_cursor(session, sub, older.clone()), Some(cp));
    assert_eq!(e.cursor_for(session, sub), Some(&older));
    let listed: Vec<_> = e
        .cursors_for_session(session)
        .map(|(s, c)| (s, c.clone()))
        .collect();
    assert_eq!(listed, alloc::vec![(sub, older.clone())]);
    assert_eq!(e.drop_cursor(session, sub), Some(older));
    assert_eq!(e.cursor_for(session, sub), None);
}

#[test]
fn match_rows_replays_without_reading_or_folding() {
    // One value, for the single live re-execution below. match_rows reads
    // nothing, so it must never consume it.
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);
    // Captured MIN: a delete of its extreme is a read on the live path, and
    // the connector call that read needs is the guard that match_rows stays
    // off the resolving path.
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
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    // In-process row subscription: gives match_rows a non-empty verdict.
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

    // The live dispatch of the same delete is still the first read, which
    // proves match_rows left the re-execution model untouched: it resolves
    // MIN once, to 7.0.
    e.apply(&ev).unwrap();
    let live = e.resolve_collect().unwrap();
    assert_eq!(
        e.connector().call_count(),
        1,
        "the re-execution model was untouched, so the live read is the first"
    );
    assert_eq!(live.scalar_updates.len(), 1);
    assert_eq!(live.scalar_updates[0].value, Value::Float(7.0));
}

#[test]
fn describe_terms_is_reachable_through_the_wrapper() {
    let (e, _tid) = engine_with_values(alloc::vec![]);
    // A filter naming no membership subquery describes as empty.
    let plain = e
        .describe_terms(&SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders WHERE price > 100",
        ))
        .expect("a plain filter is describable");
    assert!(plain.is_empty(), "a plain filter has no membership terms");
    // A filter subql cannot compile is refused, which proves the call
    // reaches the engine's compiler rather than returning a stub.
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
fn connector_error_names_its_subscription() {
    // Empty queue: the connector errors on the triggered read.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    match e.resolve_collect() {
        Ok(_) => panic!("expected the triggered read to fail"),
        Err(ReExecError::Connector {
            subscription,
            error,
        }) => {
            assert_eq!(subscription, qid, "the failing subscription is named");
            assert_eq!(error, MockError("queue empty"));
        }
        Err(other) => panic!("expected Connector naming its subscription, got {other:?}"),
    }
}

#[test]
fn cursor_error_names_its_subscription() {
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
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
    // snapshot reads the whole result through a cursor. The mock connector
    // holds none, so the read fails, and the error must name the query.
    match e.snapshot(qid) {
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
fn ungrouped_aggregate_folds_through_the_wrapper() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    // The seeded fold updates through the facade rather than being absorbed.
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
fn ungrouped_aggregate_folds_across_an_applied_burst() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    let events = &[insert_event(tid, 1, 5.0), insert_event(tid, 2, 6.0)];
    let folds: alloc::vec::Vec<_> = events
        .iter()
        .flat_map(|ev| e.apply(ev).unwrap().aggregate_updates)
        .collect();
    let last_fold = folds.last();
    assert_eq!(
        last_fold.unwrap().folded_value(),
        Some(crate::AggValue::CountStar(7)),
        "the running total after both inserts"
    );
}

#[test]
fn ungrouped_aggregate_demotion_resolves_through_the_wrapper() {
    // A filtered count needs the old row to know whether it was matching;
    // an UPDATE missing its old image demotes the aggregate to a whole
    // re-read. The wrapper must resolve that with the caller's auth, not
    // panic for want of a stored context.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(1)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    let missing_old = TestEvent::<Postgres>::update(tid, alloc::vec![], row(1, 5.0))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16]);
    // The mock connector holds no cursor, so the demoted whole re-read
    // surfaces as a Cursor error naming the aggregate rather than a panic.
    e.apply(&missing_old).unwrap();
    match e.resolve_collect() {
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
fn snapshot_of_a_folding_aggregate_is_none() {
    // An aggregate seeds through Install, so the wrapper has nothing to
    // bootstrap for it: snapshot returns None rather than mistaking the
    // stored aggregate context for a scalar re-read.
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        e.snapshot(count_id).unwrap().is_none(),
        "no bootstrap for a fold"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "snapshot reads nothing for a fold"
    );
}

#[test]
fn a_seed_that_demotes_at_install_serves_the_whole_read() {
    // A grouped fold whose seed already exceeds the group budget demotes
    // at install time. The demotion rides the install output as a
    // transition, so the facade must apply it to its own context.
    // Otherwise the context stays a still-folding aggregate and snapshot
    // answers None instead of serving the whole read the demotion asked
    // for.
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        catalog(),
        PostgreSqlDialect {},
    )
    .with_max_groups_per_aggregate(1);
    let mut e = AutoResolvingEngine::new(inner, SyncMode(MockConnector::new(alloc::vec![])));
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
    // Two groups against a budget of one: the install demotes.
    let seeded = crate::Install::install(
        &mut e,
        grouped,
        crate::AggregateSeedInstall {
            rows: alloc::vec![
                alloc::vec![Value::String("open".into()), Value::Int(2), Value::Int(2)],
                alloc::vec![Value::String("done".into()), Value::Int(1), Value::Int(1)],
            ],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        !seeded.transitions.is_empty(),
        "the install-time demotion carries a transition"
    );
    // The mock connector holds no cursor, so a served whole read surfaces
    // as a Cursor error naming the subscription. The bug returns Ok(None)
    // instead, never reaching the connector.
    match e.snapshot(grouped) {
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
fn ordered_row_query_folds_in_process() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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

#[test]
fn ordered_row_query_with_a_window_stays_a_read_tier() {
    // A window changes membership, so ordering plus LIMIT/OFFSET stays a
    // whole-answer read tier rather than an in-process row list.
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
    for sql in [
        "SELECT * FROM orders ORDER BY price LIMIT 3",
        "SELECT * FROM orders ORDER BY price OFFSET 5",
    ] {
        let reg = e.register(SubscriptionRequest::new(1u64, sql), ()).unwrap();
        assert!(
            !matches!(reg.tier, Tier::InProcess(_)),
            "a windowed order stays a read tier: {sql} classified {:?}",
            reg.tier
        );
    }
}

/// The two-stage contract: one event applies exactly once however its
/// reads fare. The fused dispatch this replaced could only retry a
/// failed read by redispatching the event, which folded the delete into
/// the count a second time.
#[test]
fn applied_event_survives_failed_resolve() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    let count = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
        .subscription_id;
    crate::Install::install(
        &mut e,
        count,
        crate::AggregateSeedInstall {
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    let minimum = e
        .register(
            SubscriptionRequest::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
        .subscription_id;
    crate::Install::install(
        &mut e,
        minimum,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();

    // The delete folds the count in memory and queues the MIN re-read.
    let applied = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    assert_eq!(applied.aggregate_updates.len(), 1);
    assert_eq!(
        applied.aggregate_updates[0].folded_value(),
        Some(crate::AggValue::CountStar(4)),
        "the delete folds exactly once, at apply time"
    );
    assert_eq!(e.pending_read_count(), 1, "the displaced MIN queues a read");

    // The read fails: the fold stands, the read stays queued.
    assert!(matches!(
        e.resolve_collect(),
        Err(ReExecError::Connector { subscription, .. }) if subscription == minimum
    ));
    assert_eq!(e.pending_read_count(), 1, "a failed read stays queued");

    // Retrying resolves the read alone: no second application.
    e.connector().values.borrow_mut().push(Value::Float(7.0));
    let resolved = e.resolve_collect().unwrap();
    assert_eq!(resolved.scalar_updates.len(), 1);
    assert_eq!(resolved.scalar_updates[0].value, Value::Float(7.0));
    assert_eq!(e.pending_read_count(), 0);
    assert_eq!(
        e.connector().call_count(),
        2,
        "one failed try, one successful retry, never a redispatch"
    );
}

/// A burst of displacements queues one read per subscription: the queue
/// dedup is what the deleted batch entry point implemented separately.
#[test]
fn burst_of_displacements_costs_one_read() {
    let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(9.0)]);
    let qid = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .unwrap()
        .subscription_id;
    crate::Install::install(
        &mut e,
        qid,
        crate::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();

    e.apply(&delete_event(tid, 1, 5.0)).unwrap();
    e.apply(&delete_event(tid, 2, 5.0)).unwrap();
    assert_eq!(e.pending_read_count(), 1, "same subscription, one read");

    let resolved = e.resolve_collect().unwrap();
    assert_eq!(e.connector().call_count(), 1, "the burst costs one read");
    assert_eq!(resolved.scalar_updates.len(), 1);
    assert_eq!(resolved.scalar_updates[0].value, Value::Float(9.0));
}

/// Each page reaches the sink before the next page is fetched, so
/// retained memory tracks one page rather than the whole answer.
#[test]
fn pages_reach_the_sink_before_the_next_fetch() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.borrow_mut().extend([
        super::super::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: true,
        },
        super::super::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();
    assert_eq!(e.pending_read_count(), 1);

    let log = alloc::rc::Rc::clone(&e.connector().log);
    e.resolve(|delivery| {
        if matches!(delivery, crate::reexec::ReadDelivery::Rows(_)) {
            log.borrow_mut().push("deliver");
        }
    })
    .unwrap();
    assert_eq!(
        *e.connector().log.borrow(),
        ["open", "fetch", "deliver", "fetch", "deliver", "close"],
        "a page is delivered before the next one is fetched"
    );
}

/// A whole read that fails part way delivers its retry under a higher
/// generation, exactly as the async path does.
///
/// The async side has pinned this since it was written
/// (`dropped_stream_is_superseded_by_a_higher_generation`), and the
/// sync side has carried the same bump since the read tier was added
/// without a test naming it. That is the drift this phase is about:
/// two copies of one rule, one of them unpinned, so a change to the
/// sync copy is caught by nothing. The consumer contract is the same
/// on both: a generation with no final page is partial, and a higher
/// generation is the signal to discard it.
#[test]
fn sync_whole_read_bumps_the_generation_like_the_async_path() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
        (),
    )
    .expect("whole read registers");
    e.connector().cursor_pages.borrow_mut().extend([
        super::super::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: true,
        },
        super::super::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("void".into())]],
            more: false,
        },
    ]);
    e.apply(&insert_event(tid, 1, 5.0)).unwrap();

    // The second fetch raises, so one partial page was delivered and
    // the generation it carried has no final page.
    *e.connector().fail_fetch_at.borrow_mut() = Some(1);
    let partial = alloc::rc::Rc::new(RefCell::new(alloc::vec::Vec::new()));
    {
        let partial = alloc::rc::Rc::clone(&partial);
        let outcome = e.resolve(move |delivery| {
            if let crate::reexec::ReadDelivery::Rows(page) = delivery {
                partial.borrow_mut().push(page.generation);
            }
        });
        assert!(outcome.is_err(), "the read failed part way through");
    }
    assert_eq!(partial.borrow().len(), 1, "one partial page was delivered");

    // The retry serves a complete answer under a higher generation.
    *e.connector().fail_fetch_at.borrow_mut() = None;
    e.connector()
        .cursor_pages
        .borrow_mut()
        .push(super::super::RowPage {
            columns: alloc::vec![String::from("status")],
            rows: alloc::vec![alloc::vec![Value::String("paid".into())]],
            more: false,
        });
    e.apply(&insert_event(tid, 2, 6.0)).unwrap();
    let retried = e.resolve_collect().expect("the retry reads");
    assert!(!retried.rows_updates.is_empty(), "the retry delivered rows");
    let partial_generation = partial.borrow()[0];
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

/// An in-process row subscription whose predicate read a cell the event
/// did not carry is re-executed against the database, which is the half
/// of the missing-cell work the core cannot do: it holds no connector.
///
/// The core reports; the wrapper owns the connector and turns the report
/// into a read, which is the same ladder every other unresolvable
/// maintenance takes.
#[test]
fn an_unanswered_cell_is_re_executed_by_the_auto_engine() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
    e.register(
        SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
        (),
    )
    .expect("the filter is served in process");
    e.connector()
        .cursor_pages
        .borrow_mut()
        .push(super::super::RowPage {
            columns: alloc::vec![String::from("id"), String::from("status")],
            rows: alloc::vec![alloc::vec![Value::Int(1), Value::String("paid".into())]],
            more: false,
        });

    // The row image omits `status`, which the predicate reads, so the
    // event cannot answer it: an unchanged TOASTed column arrives this
    // way.
    let mut cells = row(1, 5.0);
    cells[3] = Value::Missing;
    let event = TestEvent::<Postgres>::update(tid, row(1, 5.0), cells)
        .with_pk_columns([0u16])
        .with_changed_columns([1u16]);

    let applied = e.apply(&event).expect("the event applies");
    assert_eq!(
        applied.engine.unanswered().len(),
        1,
        "the core reports the subscription it could not answer"
    );
    assert_eq!(
        e.pending_read_count(),
        1,
        "and the wrapper queues a read for it"
    );

    let resolved = e.resolve_collect().expect("the read resolves");
    assert_eq!(
        resolved.rows_updates.len(),
        1,
        "the subscriber is given the answer the database holds"
    );
    assert_eq!(
        resolved.rows_updates[0].subscription_id,
        applied.engine.unanswered()[0].subscription_id
    );
}

/// Retaining the query does not make an in-process filter snapshottable.
/// The stream is how that answer is produced, so priming it from the
/// database was never part of its contract and still reads nothing.
#[test]
fn a_stream_served_filter_is_not_snapshotted() {
    let (mut e, _) = engine_with_values(alloc::vec![]);
    let id = e
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE status = 'paid'"),
            (),
        )
        .expect("the filter is served in process")
        .subscription_id;

    assert!(
        e.snapshot(id).unwrap().is_none(),
        "the stream answers this subscription, so there is nothing to prime"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "and no read is issued for it"
    );
}
