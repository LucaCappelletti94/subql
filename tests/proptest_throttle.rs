//! Property-based extension of `tests/reexec_throttle.rs`.
//!
//! The hand-written integration test exercises `cap = 1`, `cap = 2`,
//! and `cap = QUERIES.len()`. This proptest broadens the matrix to
//! arbitrary `(num_triggers, cap, per-call delay)` tuples and asserts
//! the same load-bearing invariants on every case:
//!
//! 1. **Cap respected.** `peak inflight <= cap` for every schedule the
//!    runtime produces.
//! 2. **No dropped work.** `total_calls == num_triggers` exactly. The
//!    throttle must not skip or double-count.
//! 3. **Clean shutdown.** `inflight` returns to 0 after the batch.
//! 4. **Per-trigger outcome.** `scalar_updates.len() == num_triggers`.
//!
//! Each case spins up its own `current_thread` tokio runtime. Proptest
//! itself is synchronous so we use `block_on`. To keep wall-clock
//! reasonable we cap the delay at 8 ms and run 32 cases.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::missing_const_for_fn,
    clippy::type_complexity,
    clippy::cast_precision_loss
)]

use core::future::Future;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use parking_lot::Mutex;

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::reexec::{AsyncAutoResolvingEngine, AsyncConnector, ReExecEngine, Registered, Snapshot};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, NoCheckpoint, SubscriptionEngine, SubscriptionRequest, TableId,
};

type Engine = AsyncAutoResolvingEngine<
    TestEvent<Postgres>,
    DefaultIds,
    ParserDB,
    ConcurrencyProbingConnector,
>;

/// Async connector that delays every `execute_scalar` and exposes
/// peak-inflight + total-call counters. Mirrors the shape used by
/// `tests/reexec_throttle.rs`.
struct ConcurrencyProbingConnector {
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

    fn peak(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }

    fn total_calls(&self) -> usize {
        self.total_calls.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct ProbeError(&'static str);

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
        _sql: &str,
        _kind: BuiltinKind,
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

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> impl Future<
        Output = Result<Snapshot<Vec<Vec<Value<Postgres>>>, Self::Checkpoint>, Self::Error>,
    > + Send {
        async move { Err(ProbeError("execute_rows not exercised in this test")) }
    }
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn orders_id(database: &ParserDB) -> TableId {
    catalog_helpers::table_id(database, "orders").expect("orders table")
}

fn row(id: i64, price: f64, quantity: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(quantity),
        Value::String("paid".into()),
    ]
}

fn delete_event(tid: TableId, id: i64, price: f64, quantity: i64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::delete(tid, row(id, price, quantity)).with_pk_columns([0u16])
}

/// Same six queries the hand-written throttle test uses.
const QUERIES: &[(&str, fn() -> Value<Postgres>)] = &[
    ("SELECT MIN(id) FROM orders", || Value::Int(1)),
    ("SELECT MAX(id) FROM orders", || Value::Int(1)),
    ("SELECT MIN(price) FROM orders", || Value::Float(7.0)),
    ("SELECT MAX(price) FROM orders", || Value::Float(7.0)),
    ("SELECT MIN(quantity) FROM orders", || Value::Int(1)),
    ("SELECT MAX(quantity) FROM orders", || Value::Int(1)),
];

fn engine_with_first_n_queries(n: usize, cap: usize, delay: Duration) -> (Engine, TableId) {
    let seeded_values: Vec<Value<Postgres>> =
        (0..n).map(|i| Value::Float(100.0 + i as f64)).collect();
    let connector = ConcurrencyProbingConnector::new(seeded_values, delay);
    let database = catalog();
    let tid = orders_id(&database);
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        database,
        PostgreSqlDialect {},
    );
    let mut engine = AsyncAutoResolvingEngine::new(ReExecEngine::new(inner), connector)
        .with_max_concurrent_reexecutions(cap);

    for (i, (sql, install_value)) in QUERIES.iter().take(n).enumerate() {
        let registered = engine
            .register(
                SubscriptionRequest::<DefaultIds, Postgres>::new(i as u64 + 1, *sql),
                (),
            )
            .unwrap();
        let qid = match registered {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec capture for `{sql}`"),
        };
        assert!(engine.install(qid, install_value()));
    }
    (engine, tid)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        ..ProptestConfig::default()
    })]

    /// For every `(num_triggers, cap, delay)`:
    ///   peak inflight <= cap, total_calls == num_triggers,
    ///   scalar_updates.len() == num_triggers, inflight returns to 0.
    #[test]
    fn throttle_invariants_hold(
        num_triggers in 1usize..=QUERIES.len(),
        cap in 1usize..=QUERIES.len(),
        delay_ms in 1u64..=8,
    ) {
        let delay = Duration::from_millis(delay_ms);
        let (mut engine, tid) = engine_with_first_n_queries(num_triggers, cap, delay);
        let events = vec![delete_event(tid, 1, 7.0, 1)];

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let outcome = runtime.block_on(engine.consumers_batch(&events)).unwrap();

        let peak = engine.connector().peak();
        let total = engine.connector().total_calls();

        prop_assert!(
            peak <= cap,
            "peak ({peak}) exceeded cap ({cap}) for num_triggers={num_triggers} delay={delay:?}",
        );
        prop_assert_eq!(
            total,
            num_triggers,
            "total_calls ({}) != num_triggers ({})",
            total,
            num_triggers,
        );
        prop_assert_eq!(
            outcome.scalar_updates.len(),
            num_triggers,
            "scalar_updates count ({}) != num_triggers ({})",
            outcome.scalar_updates.len(),
            num_triggers,
        );
        prop_assert_eq!(
            engine.inflight(),
            0,
            "inflight ({}) did not return to 0 after batch",
            engine.inflight(),
        );
    }
}
