//! Property-based extension of `tests/reexec_throttle.rs`.
//!
//! The hand-written integration test exercises `cap = 1`, `cap = 2`,
//! and `cap = QUERIES.len()`. This proptest broadens the matrix to
//! arbitrary `(num_triggers, cap, per-call delay)` tuples and asserts
//! the same load-bearing invariants on every case:
//!
//! 1. **Cap respected.** `peak inflight ≤ cap` for every schedule the
//!    runtime produces.
//! 2. **No dropped work.** `total_calls == num_triggers` exactly. The
//!    throttle must not skip or double-count.
//! 3. **Clean shutdown.** `inflight` returns to 0 after the batch.
//! 4. **Per-trigger outcome.** `scalar_updates.len() == num_triggers`.
//!
//! Each case spins up its own `current_thread` tokio runtime; proptest
//! itself is synchronous so we use `block_on`. To keep wall-clock
//! reasonable we cap the delay at 8 ms and run 32 cases, giving an
//! upper bound around `32 * 6 * 8 ms = 1.5 s` for the slowest path
//! (cap=1 + max triggers + max delay), well under CI limits.

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

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::reexec::{AsyncAutoResolvingEngine, AsyncConnector, ReExecEngine, Registered, Snapshot};
use subql::{
    Cell, ColumnType, DefaultIds, NoCheckpoint, RowImage, SubscriptionEngine, SubscriptionRequest,
    WalEvent,
};

/// Async connector that delays every `execute_scalar` and exposes
/// peak-inflight + total-call counters. Mirrors the shape used by
/// `tests/reexec_throttle.rs`.
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

/// Same six queries the hand-written throttle test uses, so the
/// trigger-firing schedule (a single delete that displaces every
/// captured extremum) is identical and well understood.
const QUERIES: &[(&str, fn() -> Cell)] = &[
    ("SELECT MIN(id) FROM orders", || Cell::Int(1)),
    ("SELECT MAX(id) FROM orders", || Cell::Int(1)),
    ("SELECT MIN(price) FROM orders", || Cell::Float(7.0)),
    ("SELECT MAX(price) FROM orders", || Cell::Float(7.0)),
    ("SELECT MIN(quantity) FROM orders", || Cell::Int(1)),
    ("SELECT MAX(quantity) FROM orders", || Cell::Int(1)),
];

fn engine_with_first_n_queries(
    n: usize,
    cap: usize,
    delay: Duration,
) -> AsyncAutoResolvingEngine<PostgreSqlDialect, DefaultIds, ParserDB, ConcurrencyProbingConnector>
{
    let seeded_values: Vec<Cell> = (0..n).map(|i| Cell::Float(100.0 + i as f64)).collect();
    let connector = ConcurrencyProbingConnector::new(seeded_values, delay);
    let inner = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
        Arc::new(catalog()),
        PostgreSqlDialect {},
    );
    let mut engine = AsyncAutoResolvingEngine::new(ReExecEngine::new(inner), connector)
        .with_max_concurrent_reexecutions(cap);

    for (i, (sql, install_value)) in QUERIES.iter().take(n).enumerate() {
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

proptest! {
    #![proptest_config(ProptestConfig {
        // 32 cases is enough to broaden coverage past the three caps
        // exercised by the hand-written integration test without
        // pushing wall-clock past CI sanity. Each case constructs a
        // fresh tokio runtime, so the per-case overhead is non-trivial.
        cases: 32,
        ..ProptestConfig::default()
    })]

    /// For every `(num_triggers, cap, delay)`:
    ///   peak inflight ≤ cap, total_calls == num_triggers,
    ///   scalar_updates.len() == num_triggers, inflight returns to 0.
    #[test]
    fn throttle_invariants_hold(
        num_triggers in 1usize..=QUERIES.len(),
        cap in 1usize..=QUERIES.len(),
        delay_ms in 1u64..=8,
    ) {
        let delay = Duration::from_millis(delay_ms);
        let mut engine = engine_with_first_n_queries(num_triggers, cap, delay);
        let events = vec![delete_event(1, 7.0, 1)];

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
