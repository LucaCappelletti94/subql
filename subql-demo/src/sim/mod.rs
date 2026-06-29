//! Simulation driver.
//!
//! - **Manual**: button-triggered single actions act on the currently selected
//!   table (`do_insert`, `do_update_random`, `do_delete_random`,
//!   `do_truncate`). Each builds a DML statement, runs it through
//!   `SqliteCdcSource::execute`, and dispatches the resulting `WalEvent`s.
//! - **Auto**: a one-shot `step_auto` that picks a random table and a random
//!   action, so sustained load spreads across the whole schema. The component
//!   layer drives this on a `gloo_timers` interval.

use rand::seq::IndexedRandom;
use rand::Rng;

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

pub fn do_insert(state: &mut DemoState) -> Result<(), DemoError> {
    insert_on(state, state.selected)
}

pub fn do_update_random(state: &mut DemoState) -> Result<(), DemoError> {
    update_on(state, state.selected)
}

pub fn do_delete_random(state: &mut DemoState) -> Result<(), DemoError> {
    delete_on(state, state.selected)
}

pub fn do_truncate(state: &mut DemoState) -> Result<(), DemoError> {
    truncate_on(state, state.selected)
}

/// One auto step: random table, random action (insert-weighted).
pub fn step_auto(state: &mut DemoState) -> Result<(), DemoError> {
    let ti = state.rng.random_range(0..state.tables.len());
    let action = AUTO_ACTIONS
        .choose(&mut state.rng)
        .copied()
        .unwrap_or(Action::Insert);
    match action {
        Action::Insert => insert_on(state, ti),
        Action::Update => update_on(state, ti),
        Action::Delete => delete_on(state, ti),
    }
}

fn insert_on(state: &mut DemoState, ti: usize) -> Result<(), DemoError> {
    let table = &state.tables[ti];
    let row = (table.generator)(&mut state.rng);
    let sql = sqlite::insert_sql(&table.table_name, &table.columns, &row);
    state.execute_dml(&sql)?;
    Ok(())
}

fn update_on(state: &mut DemoState, ti: usize) -> Result<(), DemoError> {
    let Some(pk_val) = state.random_pk(ti) else {
        let name = state.tables[ti].table_name.clone();
        state.note(format!("update skipped: {name} empty"));
        return Ok(());
    };
    // Generate a fresh row but keep the existing PK so we update in place
    // rather than effectively re-keying the row.
    let table = &state.tables[ti];
    let mut new_row = (table.generator)(&mut state.rng);
    let pk_idx = table.pk_index();
    if let Some(slot) = new_row.get_mut(pk_idx) {
        *slot = pk_val.clone();
    }
    let sql = sqlite::update_sql(
        &table.table_name,
        &table.columns,
        &table.pk_name,
        &pk_val,
        &new_row,
    );
    state.execute_dml(&sql)?;
    Ok(())
}

fn delete_on(state: &mut DemoState, ti: usize) -> Result<(), DemoError> {
    let Some(pk_val) = state.random_pk(ti) else {
        let name = state.tables[ti].table_name.clone();
        state.note(format!("delete skipped: {name} empty"));
        return Ok(());
    };
    let table = &state.tables[ti];
    let sql = sqlite::delete_sql(&table.table_name, &table.pk_name, &pk_val);
    state.execute_dml(&sql)?;
    Ok(())
}

fn truncate_on(state: &mut DemoState, ti: usize) -> Result<(), DemoError> {
    let sql = sqlite::truncate_sql(&state.tables[ti].table_name);
    state.execute_dml(&sql)?;
    Ok(())
}
