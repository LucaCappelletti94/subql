//! Integration test: peak concurrent `execute_scalar` calls during a
//! single resolve of an applied burst never exceed
//! `with_max_concurrent_reexecutions(cap)`.
//!
//! The unit tests in `src/reexec/async_auto.rs` cannot observe this
//! invariant because their `MockAsyncConnector` futures complete in one
//! poll (no parking, no real concurrency). This test wires up a tokio
//! `current_thread` runtime plus a `ConcurrencyProbingConnector` whose
//! `execute_scalar` actually awaits a `tokio::time::sleep`. Each call
//! bumps an `inflight` counter and updates a `peak` via `fetch_max`.
//! After the batch completes we assert `peak <= cap` and that every
//! captured query did get its update.

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
use std::time::Instant;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{AsyncConnector, AsyncMode, AutoResolvingEngine, RowPage, Snapshot};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, NoCheckpoint, Registered, SubscriptionEngine, SubscriptionRequest,
    TableId, Tier,
};

type Engine = AutoResolvingEngine<
    TestEvent<Postgres>,
    DefaultIds,
    ParserDB,
    AsyncMode<ConcurrencyProbingConnector>,
>;

/// Async connector that waits a fixed delay on every `execute_scalar`
/// (long enough that many calls can overlap under a multi-task
/// runtime). Tracks current and peak in-flight counts. The result
/// queue is FIFO with `Mutex<Vec>::remove(0)`. We pre-load with as many
/// values as there are triggers.
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
        _query: &subql::reexec::ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        async move {
            let now = self.inflight.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak.fetch_max(now, Ordering::AcqRel);
            self.total_calls.fetch_add(1, Ordering::AcqRel);
            // Yield long enough that the runtime polls the next future
            // before we return, so a real concurrency cap actually
            // gates the schedule.
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

/// The six captured aggregates we use as distinct queries: every one
/// re-executes on a delete of row `(id=1, price=7.0, quantity=1)`
/// because its installed extremum equals that column's deleted value.
const QUERIES: &[(&str, fn() -> Value<Postgres>)] = &[
    ("SELECT MIN(id) FROM orders", || Value::Int(1)),
    ("SELECT MAX(id) FROM orders", || Value::Int(1)),
    ("SELECT MIN(price) FROM orders", || Value::Float(7.0)),
    ("SELECT MAX(price) FROM orders", || Value::Float(7.0)),
    ("SELECT MIN(quantity) FROM orders", || Value::Int(1)),
    ("SELECT MAX(quantity) FROM orders", || Value::Int(1)),
];

/// Build an async engine, register every query in `QUERIES`, install
/// the extremum so the upcoming delete will displace it, and pre-load
/// the connector with one reply value per query.
fn engine_with_all_queries(cap: usize, delay: Duration) -> (Engine, TableId) {
    let seeded_values: Vec<Value<Postgres>> = (0..QUERIES.len())
        .map(|i| Value::Float(100.0 + i as f64))
        .collect();
    let connector = ConcurrencyProbingConnector::new(seeded_values, delay);
    let database = catalog();
    let tid = orders_id(&database);
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        database,
        PostgreSqlDialect {},
    );
    let mut engine = AutoResolvingEngine::new(inner, AsyncMode::new(connector))
        .with_max_concurrent_reexecutions(cap);

    for (i, (sql, install_value)) in QUERIES.iter().enumerate() {
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

#[tokio::test(flavor = "current_thread")]
async fn throttle_peak_inflight_under_cap_for_cap_1() {
    run_peak_inflight_assertion(1).await;
}

#[tokio::test(flavor = "current_thread")]
async fn throttle_peak_inflight_under_cap_for_cap_2() {
    run_peak_inflight_assertion(2).await;
}

#[tokio::test(flavor = "current_thread")]
async fn throttle_peak_inflight_at_cap_equals_trigger_count() {
    run_peak_inflight_assertion(QUERIES.len()).await;
}

async fn run_peak_inflight_assertion(cap: usize) {
    let delay = Duration::from_millis(20);
    let n_triggers = QUERIES.len();
    let (mut engine, tid) = engine_with_all_queries(cap, delay);

    let events = vec![delete_event(tid, 1, 7.0, 1)];

    let started = Instant::now();
    for event in &events {
        engine.apply(event).unwrap();
    }
    let outcome = engine.resolve_collect().await.unwrap();
    let elapsed = started.elapsed();

    let peak = engine.connector().peak();
    let total = engine.connector().total_calls();
    println!("cap={cap} n_triggers={n_triggers} peak={peak} total={total} elapsed={elapsed:?}");

    assert!(
        peak <= cap,
        "peak concurrent connector calls ({peak}) exceeded cap ({cap})"
    );
    assert_eq!(
        total, n_triggers,
        "every captured query should get exactly one connector call"
    );
    assert_eq!(
        outcome.scalar_updates.len(),
        n_triggers,
        "every captured query should emit one ScalarUpdate"
    );
    assert_eq!(
        engine.inflight(),
        0,
        "inflight must return to 0 after the batch completes"
    );

    if cap == 1 {
        let expected_min = delay.saturating_mul(n_triggers.try_into().unwrap_or(u32::MAX)) / 2;
        assert!(
            elapsed >= expected_min,
            "with cap=1 and {n_triggers} triggers, expected at least {expected_min:?}, got {elapsed:?}"
        );
    }
}
