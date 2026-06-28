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

use subql::{
    AggAccumulator, DefaultIds, QueryProjection, RegisterError, SubscriptionEngine,
    SubscriptionRequest, WalEvent,
};

use crate::presets::{self, PresetSchema};
use crate::sqlite::capture::{CapturedHook, EventCapture};
use crate::sqlite::{HarnessError, SqliteHarness};

const EVENT_LOG_CAP: usize = 50;

pub type DemoEngine = SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

#[derive(Debug, thiserror::Error)]
pub enum DemoError {
    #[error(transparent)]
    Harness(#[from] HarnessError),
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error(transparent)]
    Dispatch(#[from] subql::DispatchError),
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
    /// `Some` for aggregate subscriptions, holding the running value.
    pub agg: Option<AggAccumulator>,
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
        let req = SubscriptionRequest::<DefaultIds>::new(consumer_id, sql.clone());
        let result = self.engine.register(req)?;

        let agg = match &result.projection {
            QueryProjection::Aggregate(spec) => Some(AggAccumulator::from_spec(spec)),
            _ => None,
        };

        self.consumers.push(ConsumerEntry {
            consumer_id,
            sql,
            counters: ConsumerCounters::default(),
            agg,
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

    fn dispatch_one(&mut self, event: &WalEvent) -> Result<(), DemoError> {
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
        for (cid, delta) in out.aggregate_deltas() {
            if let Some(c) = self.find_consumer_mut(*cid) {
                if let Some(acc) = c.agg.as_mut() {
                    acc.apply(delta);
                }
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

fn event_kind_label(event: &WalEvent) -> &'static str {
    match event {
        WalEvent::Insert { .. } => "INSERT",
        WalEvent::Update { .. } => "UPDATE",
        WalEvent::Delete { .. } => "DELETE",
        WalEvent::Truncate { .. } => "TRUNCATE",
    }
}

fn summarize_event(event: &WalEvent) -> String {
    match event {
        WalEvent::Insert { pk, .. } => format!("pk={:?}", pk.values()),
        WalEvent::Update { pk, .. } => format!("pk={:?}", pk.values()),
        WalEvent::Delete { pk, .. } => format!("pk={:?}", pk.values()),
        WalEvent::Truncate { .. } => "table wiped".into(),
    }
}
