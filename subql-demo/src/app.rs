use std::cell::RefCell;
use std::rc::Rc;

use dioxus::prelude::*;
use dioxus_free_icons::{icons::fa_solid_icons::FaBolt, Icon};

use crate::components::{
    consumer_panel::ConsumerPanel, event_log::EventLog, query_console::QueryConsole,
    schema_picker::SchemaPicker, schema_view::SchemaView, sim_controls::SimControls, SharedState,
    TickSignal,
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
        h1 {
            Icon { width: 22, height: 22, icon: FaBolt, class: "h-icon".to_string() }
            " SubQL demo"
        }
        p { class: "muted",
            "PG schema -> pg2sqlite -> SQLite -> SqliteCdcSource -> SubQL dispatch."
        }
        div { class: "layout",
            div {
                section { SchemaPicker {} }
                section { SimControls {} }
                section { QueryConsole {} }
            }
            div {
                section { SchemaView {} }
                section { ConsumerPanel {} }
                section { EventLog {} }
            }
        }
    }
}
