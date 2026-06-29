use dioxus::prelude::*;
use dioxus_code::{Language, Theme};
use dioxus_code_editor::CodeEditor;
use dioxus_free_icons::{
    icons::fa_solid_icons::{FaCirclePlus, FaTerminal},
    Icon,
};

use super::{bump, SharedState, TickSignal};

#[component]
pub fn QueryConsole() -> Element {
    let state = use_context::<SharedState>();
    let mut tick = use_context::<TickSignal>();
    let _ = tick.read();

    let mut current = use_signal(String::new);
    // Gather starter queries from every table in the active schema.
    let starter_queries: Vec<String> = state
        .borrow()
        .tables
        .iter()
        .flat_map(|t| t.starter_queries.iter().cloned())
        .collect();

    let on_register = {
        let state = state.clone();
        move |_| {
            let sql = current.read().trim().to_string();
            if sql.is_empty() {
                return;
            }
            let result = state.borrow_mut().register_consumer(&sql);
            match result {
                Ok(id) => {
                    state
                        .borrow_mut()
                        .note(format!("registered consumer #{id}"));
                    current.set(String::new());
                }
                Err(e) => {
                    state.borrow_mut().note(format!("register failed: {e}"));
                }
            }
            bump(&mut tick);
        }
    };

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaTerminal, class: "h-icon".to_string() }
            " Query console"
        }
        p { class: "muted", "Click a badge below to load a starter query, then Register." }
        div { class: "badges",
            for q in starter_queries.iter() {
                {
                    let q = q.clone();
                    let mut current = current;
                    rsx! {
                        button {
                            class: "badge",
                            r#type: "button",
                            title: "{q}",
                            onclick: move |_| current.set(q.clone()),
                            "{q}"
                        }
                    }
                }
            }
        }
        CodeEditor {
            value: current(),
            language: Language::Sql,
            theme: Theme::TOKYO_NIGHT,
            oninput: move |value: String| current.set(value),
        }
        div { style: "margin-top: .5rem;",
            button { onclick: on_register,
                Icon { width: 12, height: 12, icon: FaCirclePlus, class: "btn-icon".to_string() }
                " Register"
            }
        }
    }
}
