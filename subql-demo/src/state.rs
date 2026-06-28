//! Demo-wide state: the in-browser `SqliteCdcSource`, the SubQL
//! `SubscriptionEngine`, the current table rows reconstructed from the CDC
//! stream, and the registered consumers.
//!
//! Single-threaded (the whole app runs on the wasm main thread). Held as
//! `Rc<RefCell<DemoState>>` and shared via Dioxus context.

use std::collections::VecDeque;

use diesel::{Connection, SqliteConnection};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::{
    catalog_helpers, AggAccumulator, Cell, ColumnId, DefaultIds, RegisterError, RowImage,
    SqliteCdcConfig, SqliteCdcError, SqliteCdcSource, SubscriptionEngine, TableId, WalEvent,
};

use crate::presets::{self, PresetSchema};
use crate::sqlite;

const EVENT_LOG_CAP: usize = 50;

pub type DemoEngine = SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

#[derive(Debug, thiserror::Error)]
pub enum DemoError {
    #[error("sqlite cdc: {0}")]
    Cdc(#[from] SqliteCdcError),
    #[error("diesel connection: {0}")]
    Connection(#[from] diesel::ConnectionError),
    #[error("catalog parse: {0}")]
    Catalog(String),
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error(transparent)]
    Dispatch(#[from] subql::DispatchError),
    #[error("unknown preset `{0}`")]
    UnknownPreset(String),
    #[error("table or column `{0}` not found in catalog")]
    NotInCatalog(String),
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
    pub source: SqliteCdcSource,
    pub engine: DemoEngine,
    /// Resolved table id. Held for the upcoming pgoutput round-trip wiring.
    #[allow(dead_code)]
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<String>,
    pub column_ids: Vec<ColumnId>,
    pub pk_column: ColumnId,
    pub pk_name: String,
    /// Current table contents, reconstructed from the drained CDC stream.
    pub rows: Vec<RowImage>,
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

        // One catalog parsed from the preset DDL, shared by cloning: the source
        // takes a clone, the engine takes the original. Same parse, so their
        // table/column ids match by construction.
        let catalog = ParserDB::parse::<PostgreSqlDialect>(preset.pg_ddl)
            .map_err(|e| DemoError::Catalog(format!("{e}")))?;
        let resolved = catalog_helpers::resolve_table(&catalog, preset.table_name, preset.columns)
            .ok_or_else(|| DemoError::NotInCatalog(preset.table_name.into()))?;
        let table_id = resolved.table_id;
        let column_ids = resolved.column_ids;
        let pk_column = resolved
            .primary_key
            .first()
            .copied()
            .unwrap_or(column_ids[0]);
        let pk_idx = column_ids.iter().position(|&c| c == pk_column).unwrap_or(0);
        let pk_name = preset.columns[pk_idx].to_string();

        let conn = SqliteConnection::establish(":memory:")?;
        let source = SqliteCdcSource::new(conn, catalog.clone(), SqliteCdcConfig::default())?;
        let engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let mut state = Self {
            preset,
            source,
            engine,
            table_id,
            table_name: preset.table_name.into(),
            columns: preset.columns.iter().map(|s| (*s).to_string()).collect(),
            column_ids,
            pk_column,
            pk_name,
            rows: Vec::new(),
            consumers: Vec::new(),
            event_log: VecDeque::with_capacity(EVENT_LOG_CAP),
            rng: SmallRng::seed_from_u64(0xc0ffee),
            next_consumer_id: 1,
            auto_running: false,
            auto_rate_per_sec: 2.0,
        };

        // Seed: execute the preset's inserts and prime the row view. No
        // consumers exist yet, so we drain the events into `rows` only.
        for row in (preset.seed_rows)() {
            let sql = sqlite::insert_sql(&state.table_name, &state.columns, &row);
            state.source.execute(&sql)?;
        }
        state.prime_rows()?;

        Ok(state)
    }

    pub fn switch_preset(&mut self, preset_name: &str) -> Result<(), DemoError> {
        *self = Self::new(preset_name)?;
        Ok(())
    }

    /// Index of the primary-key column within the preset's column list.
    #[must_use]
    pub fn pk_index(&self) -> usize {
        self.column_ids
            .iter()
            .position(|&c| c == self.pk_column)
            .unwrap_or(0)
    }

    /// Register a new consumer for the given SELECT. Returns its id.
    pub fn register_consumer(&mut self, sql: impl Into<String>) -> Result<u64, DemoError> {
        let consumer_id = self.next_consumer_id;
        self.next_consumer_id += 1;
        let sql = sql.into();
        let result = self.engine.register_select(consumer_id, sql.clone())?;
        let agg = result.aggregate_spec().map(AggAccumulator::from_spec);

        self.consumers.push(ConsumerEntry {
            consumer_id,
            sql,
            counters: ConsumerCounters::default(),
            agg,
        });
        Ok(consumer_id)
    }

    /// Run a DML statement through the source and dispatch the events it
    /// produces. Returns the number of events dispatched.
    pub fn execute_dml(&mut self, sql: &str) -> Result<usize, DemoError> {
        self.source.execute(sql)?;
        self.pump()
    }

    /// Drain every pending event, apply it to the row view, and dispatch it
    /// through the engine, updating consumer counters and the log.
    pub fn pump(&mut self) -> Result<usize, DemoError> {
        let mut n = 0;
        while let Some(event) = self.source.poll_next_event()? {
            self.apply_to_rows(&event);
            self.dispatch_one(&event)?;
            n += 1;
        }
        Ok(n)
    }

    /// Drain pending events into the row view only (used while seeding,
    /// before any consumer is registered).
    fn prime_rows(&mut self) -> Result<(), DemoError> {
        while let Some(event) = self.source.poll_next_event()? {
            self.apply_to_rows(&event);
        }
        Ok(())
    }

    fn apply_to_rows(&mut self, event: &WalEvent) {
        let pk_col = self.pk_column;
        match event {
            WalEvent::Insert { new_row, .. } => {
                let key = cell_at(new_row, pk_col);
                self.rows.retain(|r| cell_at(r, pk_col) != key);
                self.rows.push(new_row.clone());
            }
            WalEvent::Update { new_row, .. } => {
                let key = cell_at(new_row, pk_col);
                if let Some(slot) = self.rows.iter_mut().find(|r| cell_at(r, pk_col) == key) {
                    *slot = new_row.clone();
                } else {
                    self.rows.push(new_row.clone());
                }
            }
            WalEvent::Delete { pk, .. } => {
                let key = pk.values().first().cloned();
                self.rows.retain(|r| cell_at(r, pk_col) != key);
            }
            WalEvent::Truncate { .. } => self.rows.clear(),
        }
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
}

fn cell_at(row: &RowImage, col: ColumnId) -> Option<Cell> {
    row.cell(col)
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
        WalEvent::Insert { pk, .. } | WalEvent::Update { pk, .. } | WalEvent::Delete { pk, .. } => {
            format!("pk={:?}", pk.values())
        }
        WalEvent::Truncate { .. } => "table wiped".into(),
    }
}
