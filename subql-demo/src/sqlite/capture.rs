//! Diesel-hook-driven event capture for the SubQL demo.
//!
//! Architecture (v1 first draft):
//!
//! Diesel's sqlite update hook exposes `sqlite3_update_hook` as
//! `on_update(SqliteUpdateRouter)`, where a route (here `on_any`) takes an
//! `FnMut(SqliteChangeEvent)`. The callback receives `(op, table_name, rowid)`
//! only - **not the row data**,
//! and per the sqlite C contract the callback **cannot use the connection**.
//! Recovering full row images for dynamic table schemas would require either
//! `diesel_dynamic_schema` plumbing per preset or raw libsqlite3-sys queries.
//!
//! For the first draft we take a simpler honest path:
//!
//!  * The hook is installed and every firing is recorded in `hook_log` for
//!    the UI to display, proving the hook is wired and live.
//!  * The simulation driver owns the row data it issues. It calls
//!    `apply_insert / apply_update / apply_delete / apply_truncate` which
//!    issue the diesel DML, update an internal `rowid -> RowImage` side
//!    cache, and emit a [`WalEvent`] into the pending queue.
//!
//! A future v2 can flip the direction so the hook itself drives WalEvent
//! construction (fetching the new row via diesel_dynamic_schema), once the
//! per-preset column-type dispatcher exists.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use diesel::sqlite::{SqliteChangeEvent, SqliteChangeOp, SqliteChangeOps, SqliteUpdateRouter};

use subql::{Cell, ColumnId, PrimaryKey, RowImage, TableId, WalEvent};

use super::{HarnessError, SqliteHarness};

#[derive(Clone, Debug)]
pub struct CapturedHook {
    pub op: SqliteChangeOp,
    pub table_name: String,
    pub rowid: i64,
}

#[derive(Default)]
struct CaptureInner {
    rowid_to_row: HashMap<i64, RowImage>,
    pending_events: VecDeque<WalEvent>,
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
    /// methods are the ones that synthesize `WalEvent`s for dispatch.
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

    /// Insert a row in sqlite, update the cache, and emit a [`WalEvent::Insert`]
    /// into the pending queue.
    pub fn apply_insert(
        &self,
        harness: &mut SqliteHarness,
        row: Vec<Cell>,
    ) -> Result<(), HarnessError> {
        let rowid = harness.exec_insert(&row)?;
        let image = row_image(&row);
        let pk = self.primary_key_from(&row);

        let mut g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.insert(rowid, image.clone());

        let event = WalEvent::builder(self.table_id)
            .insert()
            .pk(pk)
            .new_row(image)
            .build()
            .map_err(|e| HarnessError::ParserDb(format!("WalEvent build: {e}")))?;
        g.pending_events.push_back(event);
        Ok(())
    }

    /// Replace the row at `rowid` and emit a [`WalEvent::Update`]. The old
    /// image is read from the cache. `changed_columns` is computed by
    /// diffing cell-by-cell.
    pub fn apply_update(
        &self,
        harness: &mut SqliteHarness,
        rowid: i64,
        new_row: Vec<Cell>,
    ) -> Result<(), HarnessError> {
        let old_image = {
            let g = self.inner.lock().expect("capture poisoned");
            g.rowid_to_row.get(&rowid).cloned()
        };

        harness.exec_update(rowid, &new_row)?;
        let new_image = row_image(&new_row);
        let pk = self.primary_key_from(&new_row);

        let mut g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.insert(rowid, new_image.clone());

        let mut builder = WalEvent::builder(self.table_id)
            .update()
            .pk(pk)
            .new_row(new_image.clone());
        if let Some(old) = old_image {
            let changed = changed_columns(&old, &new_image, &self.column_ids);
            builder = builder.old_row(old).changed_columns(Arc::from(changed));
        }
        let event = builder
            .build()
            .map_err(|e| HarnessError::ParserDb(format!("WalEvent build: {e}")))?;
        g.pending_events.push_back(event);
        Ok(())
    }

    /// Delete the row at `rowid` and emit a [`WalEvent::Delete`].
    pub fn apply_delete(
        &self,
        harness: &mut SqliteHarness,
        rowid: i64,
    ) -> Result<(), HarnessError> {
        let old_image = {
            let mut g = self.inner.lock().expect("capture poisoned");
            g.rowid_to_row.remove(&rowid)
        };

        harness.exec_delete(rowid)?;

        if let Some(old) = old_image {
            let pk_value = old.cell(self.pk_column).unwrap_or(Cell::Missing);
            let pk = PrimaryKey::new(
                Arc::from([self.pk_column].as_slice()),
                Arc::from([pk_value].as_slice()),
            )
            .map_err(|e| HarnessError::ParserDb(format!("PK: {e}")))?;
            let event = WalEvent::builder(self.table_id)
                .delete()
                .pk(pk)
                .old_row(old)
                .build()
                .map_err(|e| HarnessError::ParserDb(format!("WalEvent build: {e}")))?;
            let mut g = self.inner.lock().expect("capture poisoned");
            g.pending_events.push_back(event);
        }
        Ok(())
    }

    /// Wipe the table and emit a [`WalEvent::Truncate`].
    pub fn apply_truncate(&self, harness: &mut SqliteHarness) -> Result<(), HarnessError> {
        harness.exec_truncate()?;
        let mut g = self.inner.lock().expect("capture poisoned");
        g.rowid_to_row.clear();
        let event = WalEvent::builder(self.table_id)
            .truncate()
            .build()
            .map_err(|e| HarnessError::ParserDb(format!("WalEvent build: {e}")))?;
        g.pending_events.push_back(event);
        Ok(())
    }

    /// Drain at most `limit` pending events for dispatch.
    pub fn drain_events(&self, limit: usize) -> Vec<WalEvent> {
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
    pub fn snapshot_rows(&self) -> Vec<(i64, RowImage)> {
        let g = self.inner.lock().expect("capture poisoned");
        let mut pairs: Vec<(i64, RowImage)> = g
            .rowid_to_row
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        pairs.sort_by_key(|(k, _)| *k);
        pairs
    }

    fn primary_key_from(&self, row: &[Cell]) -> PrimaryKey {
        let pk_value = row
            .get(self.pk_column as usize)
            .cloned()
            .unwrap_or(Cell::Missing);
        PrimaryKey::new(
            Arc::from([self.pk_column].as_slice()),
            Arc::from([pk_value].as_slice()),
        )
        .expect("single-column PK is always non-empty")
    }
}

fn row_image(row: &[Cell]) -> RowImage {
    RowImage {
        cells: Arc::from(row),
    }
}

fn changed_columns(old: &RowImage, new: &RowImage, column_ids: &[ColumnId]) -> Vec<ColumnId> {
    column_ids
        .iter()
        .copied()
        .filter(|&cid| old.get(cid) != new.get(cid))
        .collect()
}
