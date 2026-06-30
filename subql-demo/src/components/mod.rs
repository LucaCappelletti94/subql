use std::cell::RefCell;
use std::rc::Rc;

use dioxus::prelude::*;

use crate::state::DemoState;

pub mod consumer_panel;
pub mod event_log;
pub mod header;
pub mod presentation;
pub mod query_console;
pub mod schema_picker;
pub mod schema_view;
pub mod sim_controls;

pub type SharedState = Rc<RefCell<DemoState>>;
pub type TickSignal = Signal<u64>;

/// Helper for components: bump the render tick after mutating state.
pub fn bump(tick: &mut TickSignal) {
    let v = *tick.read();
    tick.set(v.wrapping_add(1));
}
