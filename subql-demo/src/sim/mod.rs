//! Simulation driver. Two flavours:
//!
//! - **Manual**: button-triggered single actions (`do_insert`, `do_update_random`,
//!   `do_delete_random`, `do_truncate`). Each issues a DML through the
//!   capture and dispatches the resulting events through the engine.
//! - **Auto**: a one-shot `step` that picks a random action and runs it.
//!   The component layer drives this on a `gloo_timers` interval.

use rand::Rng;
use rand::seq::IndexedRandom;

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

/// Pick one of `AUTO_ACTIONS` weighted toward INSERTs (so the table doesn't
/// empty itself out under sustained random load).
pub fn pick_auto_action(state: &mut DemoState) -> Action {
    AUTO_ACTIONS
        .choose(&mut state.rng)
        .copied()
        .unwrap_or(Action::Insert)
}

pub fn do_insert(state: &mut DemoState) -> Result<(), DemoError> {
    let row = (state.preset.generator)(&mut state.rng);
    state.capture.apply_insert(&mut state.harness, row)?;
    state.pump()?;
    Ok(())
}

pub fn do_update_random(state: &mut DemoState) -> Result<(), DemoError> {
    let Some(rowid) = pick_random_rowid(state) else {
        state.note("update skipped: table empty");
        return Ok(());
    };
    let new_row = (state.preset.generator)(&mut state.rng);
    state.capture.apply_update(&mut state.harness, rowid, new_row)?;
    state.pump()?;
    Ok(())
}

pub fn do_delete_random(state: &mut DemoState) -> Result<(), DemoError> {
    let Some(rowid) = pick_random_rowid(state) else {
        state.note("delete skipped: table empty");
        return Ok(());
    };
    state.capture.apply_delete(&mut state.harness, rowid)?;
    state.pump()?;
    Ok(())
}

pub fn do_truncate(state: &mut DemoState) -> Result<(), DemoError> {
    state.capture.apply_truncate(&mut state.harness)?;
    state.pump()?;
    Ok(())
}

pub fn step_auto(state: &mut DemoState) -> Result<(), DemoError> {
    match pick_auto_action(state) {
        Action::Insert => do_insert(state),
        Action::Update => do_update_random(state),
        Action::Delete => do_delete_random(state),
    }
}

fn pick_random_rowid(state: &mut DemoState) -> Option<i64> {
    let rowids = state.capture.known_rowids();
    if rowids.is_empty() {
        return None;
    }
    let i = state.rng.random_range(0..rowids.len());
    Some(rowids[i])
}
