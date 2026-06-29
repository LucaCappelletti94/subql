//! Demo-wide state: the in-browser `SqliteCdcSource`, the SubQL
//! `SubscriptionEngine`, one `TableView` per table in the schema (rows
//! reconstructed from the CDC stream), and the registered consumers.
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

use crate::presets::{self, PresetSchema, PresetTable, Row};
use crate::sqlite;

const EVENT_LOG_CAP: usize = 50;

pub type DemoEngine = SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

#[derive(Debug, thiserror::Error)]
pub enum DemoError {
    #[error("sqlite cdc: {0}")]
    Cdc(#[from] SqliteCdcError),
    #[error("diesel connection: {0}")]
    Connection(#[from] diesel::ConnectionError),
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

/// One table in the active schema: its resolved ids, display metadata, the
/// rows reconstructed from the CDC stream, and the row generator that drives
/// the simulation for this table.
pub struct TableView {
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<String>,
    pub column_types: Vec<String>,
    pub column_ids: Vec<ColumnId>,
    pub pk_column: ColumnId,
    pub pk_name: String,
    pub rows: Vec<RowImage>,
    pub starter_queries: Vec<String>,
    pub generator: fn(&mut SmallRng) -> Row,
}

impl TableView {
    /// Index of the primary-key column within this table's column list.
    #[must_use]
    pub fn pk_index(&self) -> usize {
        self.column_ids
            .iter()
            .position(|&c| c == self.pk_column)
            .unwrap_or(0)
    }

    /// Primary-key value of a random current row, or `None` when empty.
    fn random_pk(&self, rng: &mut SmallRng) -> Option<Cell> {
        use rand::Rng;
        if self.rows.is_empty() {
            return None;
        }
        let i = rng.random_range(0..self.rows.len());
        self.rows[i].cell(self.pk_column)
    }
}

pub struct DemoState {
    pub preset: &'static PresetSchema,
    pub source: SqliteCdcSource,
    pub engine: DemoEngine,
    pub tables: Vec<TableView>,
    /// Index into `tables`: the table the manual sim controls act on and the
    /// schema view highlights.
    pub selected: usize,
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

        // `with_pg_ddl` creates the user tables in the sqlite connection (via
        // pg2sqlite), installs the CDC triggers, and parses the matching
        // catalog in lockstep. `new` alone would not create the tables, so the
        // trigger install would fail. The engine reuses a clone of that same
        // catalog, so their table/column ids match by construction.
        let conn = SqliteConnection::establish(":memory:")?;
        let source = SqliteCdcSource::with_pg_ddl(conn, preset.pg_ddl, SqliteCdcConfig::default())?;
        let catalog = source.catalog().clone();

        let tables = preset
            .tables
            .iter()
            .map(|pt| resolve_table_view(&catalog, pt))
            .collect::<Result<Vec<_>, _>>()?;

        let engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let mut state = Self {
            preset,
            source,
            engine,
            tables,
            selected: 0,
            consumers: Vec::new(),
            event_log: VecDeque::with_capacity(EVENT_LOG_CAP),
            rng: SmallRng::seed_from_u64(0xc0ffee),
            next_consumer_id: 1,
            auto_running: false,
            auto_rate_per_sec: 2.0,
        };

        // Seed every table, then prime the row views from the drained stream.
        // No consumers exist yet, so events go to `rows` only.
        for (ti, pt) in state.preset.tables.iter().enumerate() {
            let table_name = state.tables[ti].table_name.clone();
            let columns = state.tables[ti].columns.clone();
            for row in (pt.seed_rows)() {
                let sql = sqlite::insert_sql(&table_name, &columns, &row);
                state.source.execute(&sql)?;
            }
        }
        state.prime_rows()?;

        Ok(state)
    }

    pub fn switch_preset(&mut self, preset_name: &str) -> Result<(), DemoError> {
        *self = Self::new(preset_name)?;
        Ok(())
    }

    #[must_use]
    pub fn selected_table(&self) -> &TableView {
        &self.tables[self.selected]
    }

    /// Resolve a SELECT and register a consumer for it. Returns its id.
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

    /// Pick a random current primary key for the table at `ti`, or `None`.
    pub fn random_pk(&mut self, ti: usize) -> Option<Cell> {
        let table = self.tables.get(ti)?;
        table.random_pk(&mut self.rng)
    }

    /// Drain every pending event, apply it to the row views, and dispatch it
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

    /// Drain pending events into the row views only (used while seeding,
    /// before any consumer is registered).
    fn prime_rows(&mut self) -> Result<(), DemoError> {
        while let Some(event) = self.source.poll_next_event()? {
            self.apply_to_rows(&event);
        }
        Ok(())
    }

    fn table_index(&self, table_id: TableId) -> Option<usize> {
        self.tables.iter().position(|t| t.table_id == table_id)
    }

    fn apply_to_rows(&mut self, event: &WalEvent) {
        let Some(ti) = self.table_index(event.table_id()) else {
            return;
        };
        let pk_col = self.tables[ti].pk_column;
        let rows = &mut self.tables[ti].rows;
        match event {
            WalEvent::Insert { new_row, .. } => {
                let key = cell_at(new_row, pk_col);
                rows.retain(|r| cell_at(r, pk_col) != key);
                rows.push(new_row.clone());
            }
            WalEvent::Update { new_row, .. } => {
                let key = cell_at(new_row, pk_col);
                if let Some(slot) = rows.iter_mut().find(|r| cell_at(r, pk_col) == key) {
                    *slot = new_row.clone();
                } else {
                    rows.push(new_row.clone());
                }
            }
            WalEvent::Delete { pk, .. } => {
                let key = pk.values().first().cloned();
                rows.retain(|r| cell_at(r, pk_col) != key);
            }
            WalEvent::Truncate { .. } => rows.clear(),
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

        let table_label = self
            .table_index(event.table_id())
            .map_or("?", |ti| self.tables[ti].table_name.as_str());
        self.push_log(LogEntry::Event {
            kind: event_kind_label(event),
            summary: format!("{table_label} {}", summarize_event(event)),
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

/// Build a [`TableView`] by resolving a preset table against the catalog.
fn resolve_table_view(catalog: &ParserDB, pt: &PresetTable) -> Result<TableView, DemoError> {
    let resolved = catalog_helpers::resolve_table(catalog, pt.table_name, pt.columns)
        .ok_or_else(|| DemoError::NotInCatalog(pt.table_name.into()))?;
    let column_ids = resolved.column_ids;
    let pk_column = resolved
        .primary_key
        .first()
        .copied()
        .unwrap_or(column_ids[0]);
    let pk_idx = column_ids.iter().position(|&c| c == pk_column).unwrap_or(0);
    Ok(TableView {
        table_id: resolved.table_id,
        table_name: pt.table_name.to_string(),
        columns: pt.columns.iter().map(|s| (*s).to_string()).collect(),
        column_types: pt.column_types.iter().map(|s| (*s).to_string()).collect(),
        column_ids,
        pk_column,
        pk_name: pt.columns[pk_idx].to_string(),
        rows: Vec::new(),
        starter_queries: pt
            .starter_queries
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
        generator: pt.generator,
    })
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::sim;

    #[test]
    fn new_seeds_every_table_in_the_schema() {
        let state = DemoState::new("orders").expect("init orders schema");
        let names: Vec<&str> = state.tables.iter().map(|t| t.table_name.as_str()).collect();
        assert_eq!(names, ["customers", "orders"]);
        // Seeds: 4 customers, 5 orders, materialized from the CDC stream.
        assert_eq!(state.tables[0].rows.len(), 4);
        assert_eq!(state.tables[1].rows.len(), 5);
        // Distinct table ids so row routing can tell them apart.
        assert_ne!(state.tables[0].table_id, state.tables[1].table_id);
    }

    #[test]
    fn manual_insert_targets_the_selected_table_only() {
        let mut state = DemoState::new("orders").unwrap();
        // Select `orders` (index 1) and insert; only that table grows.
        state.selected = 1;
        sim::do_insert(&mut state).unwrap();
        assert_eq!(state.tables[0].rows.len(), 4, "customers untouched");
        assert_eq!(state.tables[1].rows.len(), 6, "orders gained a row");
    }

    #[test]
    fn dispatch_routes_per_table_to_matching_consumers() {
        let mut state = DemoState::new("orders").unwrap();
        // A consumer on each table. Mutating one must not move the other's
        // counters.
        let cust = state
            .register_consumer("SELECT * FROM customers WHERE tier = 'pro'")
            .unwrap();
        let ord = state
            .register_consumer("SELECT * FROM orders WHERE amount > 100")
            .unwrap();

        state
            .execute_dml(
                "INSERT INTO orders (id, customer_id, amount, status) VALUES (900, 1, 999, 'open')",
            )
            .unwrap();

        let cust_e = state
            .consumers
            .iter()
            .find(|c| c.consumer_id == cust)
            .unwrap();
        let ord_e = state
            .consumers
            .iter()
            .find(|c| c.consumer_id == ord)
            .unwrap();
        assert_eq!(ord_e.counters.inserted, 1, "orders consumer saw the insert");
        assert_eq!(cust_e.counters.inserted, 0, "customers consumer did not");
    }

    #[test]
    fn auto_step_spreads_across_tables_without_panicking() {
        let mut state = DemoState::new("readings").unwrap();
        for _ in 0..200 {
            sim::step_auto(&mut state).unwrap();
        }
        // Both tables should have seen activity over 200 random steps.
        assert!(state.tables[0].rows.len() >= 4);
        assert!(state.tables[1].rows.len() >= 5);
    }
}
