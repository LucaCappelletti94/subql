//! Shared fixture for the two concurrency-throttle tests.
//!
//! `reexec_throttle` drives it deterministically and pins the cap-1
//! serialisation floor. `proptest_throttle` drives it over 32 randomised
//! cases. Only the fixture is shared: the drivers measure different things
//! and both are kept.
//!
//! The connector delays every `execute_scalar` long enough that calls
//! overlap under a multi-task runtime, bumps an `inflight` counter and
//! updates `peak` with `fetch_max`. The result queue is FIFO via
//! `Mutex<Vec>::remove(0)`.

#![allow(
    clippy::unwrap_used,
    clippy::missing_const_for_fn,
    clippy::type_complexity,
    clippy::cast_precision_loss
)]

use core::future::Future;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use parking_lot::Mutex;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{AsyncConnector, AsyncMode, AutoResolvingEngine, RowPage, Snapshot};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, NoCheckpoint, Registered, SubscriptionEngine, SubscriptionRequest,
    TableId, Tier,
};

pub type Engine = AutoResolvingEngine<
    TestEvent<Postgres>,
    DefaultIds,
    ParserDB,
    AsyncMode<ConcurrencyProbingConnector>,
>;

/// Async connector that delays every `execute_scalar` and exposes
/// peak-inflight + total-call counters. Mirrors the shape used by
/// `tests/it/reexec_throttle.rs`.
pub struct ConcurrencyProbingConnector {
    values: Mutex<Vec<Value<Postgres>>>,
    inflight: AtomicUsize,
    peak: AtomicUsize,
    total_calls: AtomicUsize,
    delay: Duration,
}

impl ConcurrencyProbingConnector {
    fn new(values: Vec<Value<Postgres>>, delay: Duration) -> Self {
        Self {
            values: Mutex::new(values),
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            total_calls: AtomicUsize::new(0),
            delay,
        }
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }

    pub fn total_calls(&self) -> usize {
        self.total_calls.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct ProbeError(&'static str);

impl core::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[allow(clippy::manual_async_fn)]
impl AsyncConnector for ConcurrencyProbingConnector {
    type AuthContext = ();
    type Error = ProbeError;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &subql::reexec::ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        async move {
            let now = self.inflight.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak.fetch_max(now, Ordering::AcqRel);
            self.total_calls.fetch_add(1, Ordering::AcqRel);
            tokio::time::sleep(self.delay).await;
            let value = {
                let mut q = self.values.lock();
                if q.is_empty() {
                    self.inflight.fetch_sub(1, Ordering::Release);
                    return Err(ProbeError("queue empty"));
                }
                q.remove(0)
            };
            self.inflight.fetch_sub(1, Ordering::Release);
            Ok((value, None))
        }
    }

    fn read_page(
        &self,
        _query: &subql::reexec::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> impl Future<Output = Result<Snapshot<RowPage<Postgres>, Self::Checkpoint>, Self::Error>> + Send
    {
        async move { Err(ProbeError("read_page is not exercised by this test")) }
    }
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn orders_id(database: &ParserDB) -> TableId {
    catalog_helpers::table_id::<subql::backend::Postgres, _>(database, "orders")
        .expect("orders table")
}

fn row(id: i64, price: f64, quantity: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(quantity),
        Value::String("paid".into()),
    ]
}

pub fn delete_event(tid: TableId, id: i64, price: f64, quantity: i64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::delete(tid, row(id, price, quantity)).with_pk_columns([0u16])
}

/// The six captured aggregates, each installed with the value the test's
/// delete removes, so one delete of `(id=1, price=7.0, quantity=1)` displaces
/// every one of them and triggers six concurrent re-executions.
pub const QUERIES: &[(&str, fn() -> Value<Postgres>)] = &[
    ("SELECT MIN(id) FROM orders", || Value::Int(1)),
    ("SELECT MAX(id) FROM orders", || Value::Int(1)),
    ("SELECT MIN(price) FROM orders", || Value::Float(7.0)),
    ("SELECT MAX(price) FROM orders", || Value::Float(7.0)),
    ("SELECT MIN(quantity) FROM orders", || Value::Int(1)),
    ("SELECT MAX(quantity) FROM orders", || Value::Int(1)),
];

pub fn engine_with_first_n_queries(n: usize, cap: usize, delay: Duration) -> (Engine, TableId) {
    assert!(
        n <= QUERIES.len(),
        "the fixture has {} queries, asked for {n}",
        QUERIES.len()
    );
    let seeded_values: Vec<Value<Postgres>> =
        (0..n).map(|i| Value::Float(100.0 + i as f64)).collect();
    let connector = ConcurrencyProbingConnector::new(seeded_values, delay);
    let database = catalog();
    let tid = orders_id(&database);
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        database,
        PostgreSqlDialect {},
    );
    let mut engine = AutoResolvingEngine::new(inner, AsyncMode::new(connector))
        .with_max_concurrent_reexecutions(cap);

    for (i, (sql, install_value)) in QUERIES.iter().take(n).enumerate() {
        let registered = engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(i as u64 + 1, *sql),
                (),
            )
            .unwrap();
        let qid = match registered {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec capture for `{sql}`, got {other:?}"),
        };
        assert!(subql::Install::install(
            &mut engine,
            qid,
            subql::ScalarInstall {
                value: install_value(),
                checkpoint: None::<subql::NoCheckpoint>
            }
        )
        .is_ok());
    }
    (engine, tid)
}
