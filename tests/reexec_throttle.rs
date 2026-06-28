//! Integration test: peak concurrent `execute_scalar` calls during a
//! single `consumers_batch` never exceed
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
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::reexec::{AsyncAutoResolvingEngine, AsyncConnector, ReExecEngine, Registered, Snapshot};
use subql::{
    Cell, ColumnType, DefaultIds, NoCheckpoint, RowImage, SubscriptionEngine, SubscriptionRequest,
    WalEvent,
};

/// Async connector that waits a fixed delay on every `execute_scalar`
/// (long enough that many calls can overlap under a multi-task
/// runtime). Tracks current and peak in-flight counts. The result
/// queue is FIFO with `Mutex<Vec>::remove(0)`. We pre-load with as many
/// values as there are triggers.
struct ConcurrencyProbingConnector {
    values: Mutex<Vec<Cell>>,
    inflight: AtomicUsize,
    peak: AtomicUsize,
    total_calls: AtomicUsize,
    delay: Duration,
}

impl ConcurrencyProbingConnector {
    fn new(values: Vec<Cell>, delay: Duration) -> Self {
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

    fn execute_scalar(
        &self,
        _sql: &str,
        _column_type: ColumnType,
        _auth: &(),
    ) -> impl Future<Output = Result<(Cell, Option<Self::Checkpoint>), Self::Error>> + Send {
        async move {
            let now = self.inflight.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak.fetch_max(now, Ordering::AcqRel);
            self.total_calls.fetch_add(1, Ordering::AcqRel);
            // Yield long enough that the runtime polls the next future
            // before we return, so a real concurrency cap actually
            // gates the schedule.
            tokio::time::sleep(self.delay).await;
            let cell = {
                let mut q = self.values.lock().unwrap();
                if q.is_empty() {
                    self.inflight.fetch_sub(1, Ordering::Release);
                    return Err(ProbeError("queue empty"));
                }
                q.remove(0)
            };
            self.inflight.fetch_sub(1, Ordering::Release);
            Ok((cell, None))
        }
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> impl Future<Output = Result<Snapshot<Vec<RowImage>, Self::Checkpoint>, Self::Error>> + Send
    {
        async move { Err(ProbeError("execute_rows not exercised in this test")) }
    }
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .unwrap()
}

fn row(id: i64, price: f64, quantity: i64) -> RowImage {
    RowImage {
        cells: Arc::from([
            Cell::Int(id),
            Cell::Float(price),
            Cell::Int(quantity),
            Cell::String(Arc::from("paid")),
        ]),
    }
}

fn delete_event(id: i64, price: f64, quantity: i64) -> WalEvent {
    WalEvent::builder(0)
        .delete()
        .pk_cell(0, Cell::Int(id))
        .old_row(row(id, price, quantity))
        .build()
        .unwrap()
}

/// The six captured aggregates we use as distinct queries: every one
/// re-executes on a delete of row `(id=1, price=7.0, quantity=1)`
/// because its installed extremum equals that column's deleted value.
const QUERIES: &[(&str, fn() -> Cell)] = &[
    ("SELECT MIN(id) FROM orders", || Cell::Int(1)),
    ("SELECT MAX(id) FROM orders", || Cell::Int(1)),
    ("SELECT MIN(price) FROM orders", || Cell::Float(7.0)),
    ("SELECT MAX(price) FROM orders", || Cell::Float(7.0)),
    ("SELECT MIN(quantity) FROM orders", || Cell::Int(1)),
    ("SELECT MAX(quantity) FROM orders", || Cell::Int(1)),
];

/// Build an async engine, register every query in `QUERIES`, install
/// the extremum so the upcoming delete will displace it, and pre-load
/// the connector with one reply value per query.
fn engine_with_all_queries(
    cap: usize,
    delay: Duration,
) -> AsyncAutoResolvingEngine<PostgreSqlDialect, DefaultIds, ParserDB, ConcurrencyProbingConnector>
{
    let seeded_values: Vec<Cell> = (0..QUERIES.len())
        .map(|i| Cell::Float(100.0 + i as f64))
        .collect();
    let connector = ConcurrencyProbingConnector::new(seeded_values, delay);
    let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
        catalog(),
        PostgreSqlDialect {},
    );
    let mut engine = AsyncAutoResolvingEngine::new(ReExecEngine::new(inner), connector)
        .with_max_concurrent_reexecutions(cap);

    for (i, (sql, install_value)) in QUERIES.iter().enumerate() {
        let registered = engine
            .register(SubscriptionRequest::new(i as u64 + 1, *sql), ())
            .unwrap();
        let qid = match registered {
            Registered::ReExec { query_id, .. } => query_id,
            Registered::Engine(_) => panic!("expected ReExec capture for `{sql}`"),
        };
        assert!(engine.install(qid, install_value()));
    }
    engine
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
    // Cap equals the trigger count: every trigger can run concurrently.
    run_peak_inflight_assertion(QUERIES.len()).await;
}

async fn run_peak_inflight_assertion(cap: usize) {
    let delay = Duration::from_millis(20);
    let n_triggers = QUERIES.len();
    let mut engine = engine_with_all_queries(cap, delay);

    // One event that displaces every captured query's stored extremum
    // (id=1, price=7.0, quantity=1 match all six installed extrema).
    let events = vec![delete_event(1, 7.0, 1)];

    let started = Instant::now();
    let outcome = engine.consumers_batch(&events).await.unwrap();
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

    // With cap=1 the batch must serialise: at least n_triggers * delay
    // wall-clock. CI timing is noisy so we leave a generous lower bound.
    if cap == 1 {
        let expected_min = delay.saturating_mul(n_triggers.try_into().unwrap_or(u32::MAX)) / 2;
        assert!(
            elapsed >= expected_min,
            "with cap=1 and {n_triggers} triggers, expected at least {expected_min:?}, got {elapsed:?}"
        );
    }
}
