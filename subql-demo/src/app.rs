use std::cell::RefCell;
use std::rc::Rc;

use dioxus::prelude::*;

use crate::components::{
    consumer_panel::ConsumerPanel,
    event_log::EventLog,
    header::Header,
    presentation::{Features, Hero, HowItWorks, Problem},
    query_console::QueryConsole,
    schema_picker::SchemaPicker,
    schema_view::SchemaView,
    sim_controls::SimControls,
    SharedState, TickSignal,
};
use crate::state::DemoState;

const DEFAULT_PRESET: &str = "orders";

#[component]
pub fn App() -> Element {
    let state: SharedState = use_hook(|| {
        let demo = DemoState::new(DEFAULT_PRESET)
            .unwrap_or_else(|e| panic!("init demo state ({DEFAULT_PRESET}): {e}"));
        Rc::new(RefCell::new(demo))
    });
    use_context_provider(|| state.clone());
    let tick: TickSignal = use_signal(|| 0u64);
    use_context_provider(|| tick);

    rsx! {
        span { id: "top" }
        Header {}
        main { class: "page",
            Hero {}
            Problem {}
            Features {}
            HowItWorks {}
            Demo {}
        }
        footer { class: "sq-footer",
            "SubQL - the general engine for real-time SQL subscriptions."
        }
    }
}

/// The live demo: the whole CDC path running in the browser.
#[component]
fn Demo() -> Element {
    rsx! {
        section { class: "sq-section demo", id: "demo",
            p { class: "sq-eyebrow", "Live demo" }
            h2 { "Try it in the browser" }
            p { class: "sq-lead",
                "Postgres DDL is translated to SQLite, captured through "
                "SqliteCdcSource, and dispatched by SubQL, all in this tab. "
                "Register queries, mutate any table, watch the deltas."
            }
            div { class: "layout",
                div {
                    section { class: "panel", SchemaPicker {} }
                    section { class: "panel", SimControls {} }
                    section { class: "panel", QueryConsole {} }
                }
                div {
                    section { class: "panel", SchemaView {} }
                    section { class: "panel", ConsumerPanel {} }
                    section { class: "panel", EventLog {} }
                }
            }
        }
    }
}
