//! Demo-wide state. Holds the sqlite harness, the diesel-hook capture, the
//! SubQL `SubscriptionEngine`, and the list of registered consumers.
//!
//! Single-threaded (the entire app runs in the wasm main thread). State is
//! held as `Rc<RefCell<DemoState>>` and shared via Dioxus context.

use std::collections::VecDeque;

use rand::rngs::SmallRng;
use rand::SeedableRng;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{AggSpec, AggValue, DefaultIds, EventKind, RegisterError, SubscriptionEngine};

use crate::presets::{self, PresetSchema};
use crate::sqlite::capture::{CapturedHook, EventCapture};
use crate::sqlite::{HarnessError, SqliteHarness};

const EVENT_LOG_CAP: usize = 50;

pub type DemoEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

#[derive(Debug, thiserror::Error)]
pub enum DemoError {
    #[error(transparent)]
    Harness(#[from] HarnessError),
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error(transparent)]
    Dispatch(#[from] subql::DispatchError),
    #[error(transparent)]
    AggregateInstall(#[from] subql::AggregateInstallError),
    #[error("unknown preset `{0}`")]
    UnknownPreset(String),
}

#[derive(Clone, Debug, Default)]
pub struct ConsumerCounters {
    pub inserted: u64,
    pub deleted: u64,
    pub updated: u64,
}

#[derive(Clone, Debug)]
pub struct ConsumerEntry {
    pub consumer_id: u64,
    pub sql: String,
    pub counters: ConsumerCounters,
    /// `Some` for aggregate subscriptions, holding the value the engine last
    /// reported.
    pub agg: Option<AggValue>,
    /// The subscription id the engine files this consumer's total under.
    pub subscription_id: subql::SubscriptionId,
}

#[derive(Clone, Debug)]
pub enum LogEntry {
    Event {
        kind: &'static str,
        summary: String,
        notified: Vec<u64>,
    },
    Note(String),
}

pub struct DemoState {
    pub preset: &'static PresetSchema,
    pub harness: SqliteHarness,
    pub capture: EventCapture,
    pub engine: DemoEngine,
    pub consumers: Vec<ConsumerEntry>,
    pub event_log: VecDeque<LogEntry>,
    pub rng: SmallRng,
    next_consumer_id: u64,
    pub auto_running: bool,
    pub auto_rate_per_sec: f32,
}

impl DemoState {
    pub fn new(preset_name: &str) -> Result<Self, DemoError> {
        let preset = presets::by_name(preset_name)
            .ok_or_else(|| DemoError::UnknownPreset(preset_name.into()))?;
        let mut harness = SqliteHarness::open(preset)?;
        let capture = EventCapture::install(&mut harness);

        // Seed: re-emit insert events through the capture so subscribers
        // registered after this point still see the initial state. (No
        // consumers exist yet, so this just primes the rowid->RowImage
        // cache via `apply_insert`'s sqlite write... but harness already
        // did that. Instead just hydrate the cache by reissuing the
        // staging-side writes through `apply_insert` AFTER tearing down
        // the harness's own initial inserts? That's wasteful. Simpler:
        // build the cache from harness's seed rowids retrospectively.)
        //
        // Pragmatic v1: re-run the preset's seed_rows through capture so
        // both the sqlite contents and the cache match. The harness was
        // already loaded above; truncate and re-insert via capture.
        capture.apply_truncate(&mut harness)?;
        // `apply_truncate` produced one WalEvent we don't care about
        // (no consumers yet). Discard it.
        let _ = capture.drain_events(usize::MAX);
        for row in (preset.seed_rows)() {
            capture.apply_insert(&mut harness, row)?;
        }
        let _ = capture.drain_events(usize::MAX);

        let engine = SubscriptionEngine::new(
            ParserDB::parse::<PostgreSqlDialect>(preset.pg_ddl).expect("preset DDL parses"),
            PostgreSqlDialect {},
        );

        Ok(Self {
            preset,
            harness,
            capture,
            engine,
            consumers: Vec::new(),
            event_log: VecDeque::with_capacity(EVENT_LOG_CAP),
            rng: SmallRng::seed_from_u64(0xc0ffee),
            next_consumer_id: 1,
            auto_running: false,
            auto_rate_per_sec: 2.0,
        })
    }

    pub fn switch_preset(&mut self, preset_name: &str) -> Result<(), DemoError> {
        let fresh = Self::new(preset_name)?;
        *self = fresh;
        Ok(())
    }

    /// Register a new consumer with the given SELECT statement. Returns the
    /// freshly allocated consumer id.
    pub fn register_consumer(&mut self, sql: impl Into<String>) -> Result<u64, DemoError> {
        let consumer_id = self.next_consumer_id;
        self.next_consumer_id += 1;
        let sql = sql.into();
        let result = self.engine.register_select(consumer_id, sql.clone())?;
        let subscription_id = result.subscription_id;

        // The engine holds the running value, so an aggregate needs its
        // starting numbers before it reports anything. A real caller runs
        // `Served::aggregate_bootstrap`'s SQL against its database. This demo
        // is its own database, so it computes the same component row from the
        // rows it is holding. Nothing has been folded for a subscription that
        // was created a line ago, so the read cannot have raced a change and
        // needs no stream position.
        let agg = if let Some(spec) = result.aggregate_spec() {
            let row = seed_components(spec, &self.capture.snapshot_rows());
            let updates = subql::Install::install(
                &mut self.engine,
                subscription_id,
                subql::AggregateSeedInstall {
                    rows: vec![row],
                    read_at: None,
                },
            )?;
            updates
                .first()
                .and_then(subql::AggregateValueUpdate::folded_value)
        } else {
            None
        };

        self.consumers.push(ConsumerEntry {
            consumer_id,
            sql,
            counters: ConsumerCounters::default(),
            agg,
            subscription_id,
        });
        Ok(consumer_id)
    }

    /// Drain pending WalEvents from capture, dispatch each through the
    /// engine, and update consumer counters / event log.
    pub fn pump(&mut self) -> Result<usize, DemoError> {
        let events = self.capture.drain_events(usize::MAX);
        let n = events.len();
        for event in events {
            self.dispatch_one(&event)?;
        }
        Ok(n)
    }

    fn dispatch_one(&mut self, event: &TestEvent<Postgres>) -> Result<(), DemoError> {
        let out = self.engine.dispatch(event)?;

        let notifications = out.notifications();
        for &cid in notifications.inserted() {
            if let Some(c) = self.find_consumer_mut(cid) {
                c.counters.inserted += 1;
            }
        }
        for &cid in notifications.deleted() {
            if let Some(c) = self.find_consumer_mut(cid) {
                c.counters.deleted += 1;
            }
        }
        for &cid in notifications.updated() {
            if let Some(c) = self.find_consumer_mut(cid) {
                c.counters.updated += 1;
            }
        }
        for update in out.aggregate_updates() {
            if let Some(entry) = self
                .consumers
                .iter_mut()
                .find(|c| c.subscription_id == update.subscription)
            {
                entry.agg = match &update.change {
                    subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(
                        value,
                    )) => Some(*value),
                    subql::AggregateValueChange::Set(subql::AggregateResultValue::Scalar(_))
                    | subql::AggregateValueChange::Remove => None,
                };
            }
        }

        self.push_log(LogEntry::Event {
            kind: event_kind_label(event),
            summary: summarize_event(event),
            notified: out.notified(),
        });
        Ok(())
    }

    fn find_consumer_mut(&mut self, cid: u64) -> Option<&mut ConsumerEntry> {
        self.consumers.iter_mut().find(|c| c.consumer_id == cid)
    }

    fn push_log(&mut self, entry: LogEntry) {
        if self.event_log.len() >= EVENT_LOG_CAP {
            self.event_log.pop_front();
        }
        self.event_log.push_back(entry);
    }

    pub fn note(&mut self, text: impl Into<String>) {
        self.push_log(LogEntry::Note(text.into()));
    }

    pub fn snapshot_hooks(&self, max: usize) -> Vec<CapturedHook> {
        self.capture.snapshot_hook_log(max)
    }
}

fn event_kind_label(event: &TestEvent<Postgres>) -> &'static str {
    match event.kind {
        EventKind::Insert => "INSERT",
        EventKind::Update => "UPDATE",
        EventKind::Delete => "DELETE",
        EventKind::Truncate => "TRUNCATE",
    }
}

fn summarize_event(event: &TestEvent<Postgres>) -> String {
    if event.kind == EventKind::Truncate {
        return "table wiped".into();
    }
    let row = if event.kind == EventKind::Delete {
        &event.old_row
    } else {
        &event.new_row
    };
    let pk: Vec<_> = event
        .pk_columns
        .iter()
        .filter_map(|&c| row.get(usize::from(c)))
        .collect();
    format!("pk={pk:?}")
}

/// The bootstrap component row for `spec` over `rows`, in the column order
/// `AggregateBootstrap` projects: `[c]` for COUNT, `[s]` for SUM, `[s, c]` for
/// AVG, and `[s, sq, c]` for the variance and stddev family.
///
/// A caller with a database runs the bootstrap SQL instead. The demo is its own
/// database, small enough to add the columns up in memory.
fn seed_components(spec: &AggSpec, rows: &[(i64, presets::Row)]) -> Vec<Value<Postgres>> {
    let column = match spec {
        AggSpec::CountStar => {
            return vec![Value::Int(i64::try_from(rows.len()).unwrap_or(i64::MAX))]
        }
        AggSpec::CountColumn { column }
        | AggSpec::Sum { column }
        | AggSpec::Avg { column }
        | AggSpec::VarPop { column }
        | AggSpec::VarSamp { column }
        | AggSpec::StddevPop { column }
        | AggSpec::StddevSamp { column } => usize::from(*column),
        // `AggSpec` is non-exhaustive, so a variant added upstream lands here
        // rather than breaking the demo's build. It seeds as empty until the
        // demo learns the new shape.
        _ => return Vec::new(),
    };

    let numbers: Vec<f64> = rows
        .iter()
        .filter_map(|(_, row)| match row.get(column) {
            Some(Value::Int(i)) =>
            {
                #[allow(clippy::cast_precision_loss)]
                Some(*i as f64)
            }
            Some(Value::Float(f)) if f.is_finite() => Some(*f),
            _ => None,
        })
        .collect();
    let count = i64::try_from(numbers.len()).unwrap_or(i64::MAX);
    let sum: f64 = numbers.iter().sum();
    let sum_sq: f64 = numbers.iter().map(|v| v * v).sum();

    match spec {
        AggSpec::CountStar | AggSpec::CountColumn { .. } => vec![Value::Int(count)],
        AggSpec::Sum { .. } => vec![Value::Float(sum)],
        AggSpec::Avg { .. } => vec![Value::Float(sum), Value::Int(count)],
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => {
            vec![Value::Float(sum), Value::Float(sum_sq), Value::Int(count)]
        }
        _ => Vec::new(),
    }
}
