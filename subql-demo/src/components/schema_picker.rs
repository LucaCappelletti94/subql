use dioxus::prelude::*;
use dioxus_free_icons::{icons::fa_solid_icons::FaDatabase, Icon};

use crate::presets;

use super::{bump, SharedState, TickSignal};

#[component]
pub fn SchemaPicker() -> Element {
    let state = use_context::<SharedState>();
    let mut tick = use_context::<TickSignal>();
    let _ = tick.read();

    let current_name = state.borrow().preset.name.to_string();
    let presets = presets::all();

    let on_change = {
        let state = state.clone();
        move |evt: FormEvent| {
            let new_name = evt.value();
            let mut s = state.borrow_mut();
            if s.preset.name != new_name {
                if let Err(e) = s.switch_preset(&new_name) {
                    s.note(format!("switch failed: {e}"));
                }
            }
            drop(s);
            bump(&mut tick);
        }
    };

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaDatabase, class: "h-icon".to_string() }
            " Schema"
        }
        select { value: "{current_name}", onchange: on_change,
            for p in presets {
                option { value: "{p.name}", selected: p.name == current_name, "{p.name}" }
            }
        }
        p { class: "muted", "Switching schema resets the engine and clears all consumers." }
    }
}
