//! Diesel-hook-driven event capture for the SubQL demo.
//!
//! Diesel's sqlite update hook exposes `sqlite3_update_hook` as
//! `on_update(SqliteUpdateRouter)`, where a route (here `on_any`) takes an
//! `FnMut(SqliteChangeEvent)`. The callback receives `(op, table_name, rowid)`
//! only, **not the row data**, and per the sqlite C contract the callback
//! **cannot use the connection**. Recovering full row images for dynamic
//! table schemas would require either `diesel_dynamic_schema` plumbing per
//! preset or raw libsqlite3-sys queries.
//!
//! So we take a simpler honest path:
//!
//!  * The hook is installed and every firing is recorded in `hook_log` for
//!    the UI to display, proving the hook is wired and live.
//!  * The simulation driver owns the row data it issues. It calls
//!    `apply_insert / apply_update / apply_delete / apply_truncate` which
//!    issue the diesel DML, update an internal `rowid -> row` side cache,
//!    and enqueue a [`TestEvent<Postgres>`] for dispatch. The demo models a
//!    Postgres source running on a SQLite substrate, so the events it emits
//!    are Postgres-backed CDC events.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use diesel::sqlite::{SqliteChangeEvent, SqliteChangeOp, SqliteChangeOps, SqliteUpdateRouter};

use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{ColumnId, TableId};

use super::{HarnessError, SqliteHarness};

/// One row image indexed by column ordinal.
type Row = Vec<Value<Postgres>>;
/// The Postgres-shaped CDC event the demo synthesizes and dispatches.
type DemoEvent = TestEvent<Postgres>;

#[derive(Clone, Debug)]
pub struct CapturedHook {
    pub op: SqliteChangeOp,
    pub table_name: String,
    pub rowid: i64,
}

#[derive(Default)]
struct CaptureInner {
    rowid_to_row: HashMap<i64, Row>,
    pending_events: VecDeque<DemoEvent>,
    hook_log: VecDeque<CapturedHook>,
}

#[derive(Clone)]
pub struct EventCapture {
    inner: Arc<Mutex<CaptureInner>>,
    table_id: TableId,
    table_name: String,
    column_ids: Arc<[ColumnId]>,
    pk_column: ColumnId,
}

impl EventCapture {
    /// Install the diesel sqlite hook on `harness.conn` and return a capture
    /// handle. The hook records firings into `hook_log`. The `apply_*`
    /// methods are the ones that synthesize [`TestEvent`]s for dispatch.
    pub fn install(harness: &mut SqliteHarness) -> Self {
        let pk_column = harness.column_ids.first().copied().unwrap_or(0);
        let cap = Self {
            inner: Arc::new(Mutex::new(CaptureInner::default())),
            table_id: harness.table_id,
            table_name: harness.table_name.clone(),
            column_ids: Arc::from(harness.column_ids.as_slice()),
            pk_column,
        };

        let table_name = cap.table_name.clone();
        let inner_for_hook = Arc::clone(&cap.inner);
        harness.conn.on_update(SqliteUpdateRouter::new().on_any(
            SqliteChangeOps::ALL,
            move |ev: SqliteChangeEvent<'_>| {
                if ev.table_name == table_name {
                    if let Ok(mut g) = inner_for_hook.lock() {
                        g.hook_log.push_back(CapturedHook {
                            op: ev.op,
                            table_name: ev.table_name.to_owned(),
                            rowid: ev.rowid,
                        });
                    }
                }
            },
        ));

        cap
    }

    /// Insert a row in sqlite, cache it, and enqueue an insert event.
    pub fn apply_insert(&self, harness: &mut SqliteHarness, row: Row) -> Result<(), HarnessError> {
        let rowid = harness.exec_insert(&row)?;
        let event = TestEvent::insert(self.table_id, row.clone()).with_pk_columns([self.pk_column]);

        let mut g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.insert(rowid, row);
        g.pending_events.push_back(event);
        Ok(())
    }

    /// Replace the row at `rowid` and enqueue an update event. The old
    /// image is read from the cache, and `changed_columns` is a
    /// cell-by-cell diff.
    pub fn apply_update(
        &self,
        harness: &mut SqliteHarness,
        rowid: i64,
        new_row: Row,
    ) -> Result<(), HarnessError> {
        let old_row = {
            let g = self.inner.lock().expect("capture poisoned");
            g.rowid_to_row.get(&rowid).cloned()
        };

        harness.exec_update(rowid, &new_row)?;

        let old = old_row.unwrap_or_default();
        let changed = changed_columns(&old, &new_row, &self.column_ids);
        let event = TestEvent::update(self.table_id, old, new_row.clone())
            .with_pk_columns([self.pk_column])
            .with_changed_columns(changed);

        let mut g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.insert(rowid, new_row);
        g.pending_events.push_back(event);
        Ok(())
    }

    /// Delete the row at `rowid` and enqueue a delete event carrying the
    /// cached old-row image.
    pub fn apply_delete(
        &self,
        harness: &mut SqliteHarness,
        rowid: i64,
    ) -> Result<(), HarnessError> {
        let old_row = {
            let mut g = self.inner.lock().expect("capture poisoned");
            g.rowid_to_row.remove(&rowid)
        };

        harness.exec_delete(rowid)?;

        if let Some(old) = old_row {
            let event = TestEvent::delete(self.table_id, old).with_pk_columns([self.pk_column]);
            let mut g = self.inner.lock().expect("capture poisoned");
            g.pending_events.push_back(event);
        }
        Ok(())
    }

    /// Wipe the table and enqueue a truncate event.
    pub fn apply_truncate(&self, harness: &mut SqliteHarness) -> Result<(), HarnessError> {
        harness.exec_truncate()?;
        let mut g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.clear();
        g.pending_events
            .push_back(TestEvent::truncate(self.table_id));
        Ok(())
    }

    /// Drain at most `limit` pending events for dispatch.
    pub fn drain_events(&self, limit: usize) -> Vec<DemoEvent> {
        let mut g = self.inner.lock().expect("capture poisoned");
        let take = limit.min(g.pending_events.len());
        g.pending_events.drain(..take).collect()
    }

    /// Snapshot of the most recent hook firings, newest last.
    pub fn snapshot_hook_log(&self, max: usize) -> Vec<CapturedHook> {
        let g = self.inner.lock().expect("capture poisoned");
        let n = g.hook_log.len();
        let start = n.saturating_sub(max);
        g.hook_log.iter().skip(start).cloned().collect()
    }

    /// All currently cached rowids. Useful for the UPDATE / DELETE buttons
    /// to pick a target.
    pub fn known_rowids(&self) -> Vec<i64> {
        let g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.keys().copied().collect()
    }

    /// Snapshot of the currently cached rows, sorted by rowid.
    pub fn snapshot_rows(&self) -> Vec<(i64, Row)> {
        let g = self.inner.lock().expect("capture poisoned");
        let mut pairs: Vec<(i64, Row)> = g
            .rowid_to_row
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        pairs.sort_by_key(|(k, _)| *k);
        pairs
    }
}

/// The column ordinals whose cell differs between the old and new image.
fn changed_columns(
    old: &[Value<Postgres>],
    new: &[Value<Postgres>],
    column_ids: &[ColumnId],
) -> Vec<ColumnId> {
    column_ids
        .iter()
        .copied()
        .filter(|&cid| old.get(usize::from(cid)) != new.get(usize::from(cid)))
        .collect()
}
