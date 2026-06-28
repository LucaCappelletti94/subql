//! Simulation driver.
//!
//! - **Manual**: button-triggered single actions (`do_insert`,
//!   `do_update_random`, `do_delete_random`, `do_truncate`). Each builds a
//!   DML statement, runs it through `SqliteCdcSource::execute`, and dispatches
//!   the resulting `WalEvent`s through the engine.
//! - **Auto**: a one-shot `step_auto` that picks a random action. The
//!   component layer drives this on a `gloo_timers` interval.

use rand::seq::IndexedRandom;
use rand::Rng;

use subql::Cell;

use crate::sqlite;
use crate::state::{DemoError, DemoState};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Insert,
    Update,
    Delete,
}

const AUTO_ACTIONS: &[Action] = &[
    Action::Insert,
    Action::Insert,
    Action::Insert,
    Action::Update,
    Action::Update,
    Action::Delete,
];

/// Pick one of `AUTO_ACTIONS`, weighted toward INSERTs so the table does not
/// empty itself under sustained random load.
pub fn pick_auto_action(state: &mut DemoState) -> Action {
    AUTO_ACTIONS
        .choose(&mut state.rng)
        .copied()
        .unwrap_or(Action::Insert)
}

pub fn do_insert(state: &mut DemoState) -> Result<(), DemoError> {
    let row = (state.preset.generator)(&mut state.rng);
    let sql = sqlite::insert_sql(&state.table_name, &state.columns, &row);
    state.execute_dml(&sql)?;
    Ok(())
}

pub fn do_update_random(state: &mut DemoState) -> Result<(), DemoError> {
    let Some(pk_val) = pick_random_pk(state) else {
        state.note("update skipped: table empty");
        return Ok(());
    };
    // Generate a fresh row but keep the existing PK so we update in place
    // rather than effectively re-keying the row.
    let mut new_row = (state.preset.generator)(&mut state.rng);
    let pk_idx = state.pk_index();
    if let Some(slot) = new_row.get_mut(pk_idx) {
        *slot = pk_val.clone();
    }
    let sql = sqlite::update_sql(
        &state.table_name,
        &state.columns,
        &state.pk_name,
        &pk_val,
        &new_row,
    );
    state.execute_dml(&sql)?;
    Ok(())
}

pub fn do_delete_random(state: &mut DemoState) -> Result<(), DemoError> {
    let Some(pk_val) = pick_random_pk(state) else {
        state.note("delete skipped: table empty");
        return Ok(());
    };
    let sql = sqlite::delete_sql(&state.table_name, &state.pk_name, &pk_val);
    state.execute_dml(&sql)?;
    Ok(())
}

pub fn do_truncate(state: &mut DemoState) -> Result<(), DemoError> {
    let sql = sqlite::truncate_sql(&state.table_name);
    state.execute_dml(&sql)?;
    Ok(())
}

pub fn step_auto(state: &mut DemoState) -> Result<(), DemoError> {
    match pick_auto_action(state) {
        Action::Insert => do_insert(state),
        Action::Update => do_update_random(state),
        Action::Delete => do_delete_random(state),
    }
}

/// Pick the primary-key value of a random current row, or `None` when empty.
fn pick_random_pk(state: &mut DemoState) -> Option<Cell> {
    let len = state.rows.len();
    if len == 0 {
        return None;
    }
    let i = state.rng.random_range(0..len);
    state.rows[i].cell(state.pk_column)
}
