use dioxus::prelude::*;
use dioxus_free_icons::{icons::fa_solid_icons::FaClockRotateLeft, Icon};

use crate::state::LogEntry;

use super::{SharedState, TickSignal};

#[component]
pub fn EventLog() -> Element {
    let state = use_context::<SharedState>();
    let tick = use_context::<TickSignal>();
    let _ = tick.read();

    let s = state.borrow();
    let entries: Vec<LogEntry> = s.event_log.iter().rev().cloned().collect();
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
    }
}
