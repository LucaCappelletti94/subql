use dioxus::prelude::*;
use dioxus_free_icons::{Icon, icons::fa_solid_icons::{FaClockRotateLeft, FaPlug}};

use crate::state::LogEntry;

use super::{SharedState, TickSignal};

#[component]
pub fn EventLog() -> Element {
    let state = use_context::<SharedState>();
    let tick = use_context::<TickSignal>();
    let _ = tick.read();

    let s = state.borrow();
    let entries: Vec<LogEntry> = s.event_log.iter().rev().cloned().collect();
    let hooks = s.snapshot_hooks(20);
    drop(s);

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaClockRotateLeft, class: "h-icon".to_string() }
            " Event log"
        }
        div { class: "log",
            for entry in entries.iter() {
                match entry {
                    LogEntry::Event { kind, summary, notified } => {
                        let notified_str = if notified.is_empty() {
                            String::new()
                        } else {
                            format!(" -> consumers: {notified:?}")
                        };
                        rsx! {
                            div {
                                strong { "{kind} " }
                                "{summary}"
                                if !notified.is_empty() {
                                    span { class: "muted", "{notified_str}" }
                                }
                            }
                        }
                    }
                    LogEntry::Note(text) => rsx! {
                        div { class: "muted", "[note] {text}" }
                    },
                }
            }
        }
        h3 {
            Icon { width: 14, height: 14, icon: FaPlug, class: "h-icon".to_string() }
            " Diesel hook firings (raw)"
        }
        div { class: "log",
            for hook in hooks.iter().rev() {
                {
                    let op = format!("{:?}", hook.op);
                    let table = hook.table_name.clone();
                    let rowid = hook.rowid;
                    rsx! {
                        div {
                            span { class: "muted",
                                "{op} table={table} rowid={rowid}"
                            }
                        }
                    }
                }
            }
        }
    }
}
