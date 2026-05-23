use dioxus::prelude::*;
use dioxus_code::{Code, Language, SourceCode, Theme};
use dioxus_free_icons::{Icon, icons::fa_solid_icons::FaUsers};

use crate::state::AggState;

use super::{SharedState, TickSignal};

#[component]
pub fn ConsumerPanel() -> Element {
    let state = use_context::<SharedState>();
    let tick = use_context::<TickSignal>();
    let _ = tick.read();

    let s = state.borrow();
    let consumers: Vec<_> = s.consumers.iter().cloned().collect();
    drop(s);

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaUsers, class: "h-icon".to_string() }
            " Consumers"
        }
        if consumers.is_empty() {
            p { class: "muted", "No consumers registered. Use the query console below to register one." }
        } else {
            table {
                thead {
                    tr {
                        th { "id" }
                        th { "query" }
                        th { "ins" }
                        th { "upd" }
                        th { "del" }
                        th { "agg" }
                    }
                }
                tbody {
                    for c in consumers {
                        tr {
                            td { "#{c.consumer_id}" }
                            td {
                                Code {
                                    src: SourceCode::new(Language::Sql, c.sql.clone()),
                                    theme: Theme::TOKYO_NIGHT,
                                }
                            }
                            td { "{c.counters.inserted}" }
                            td { "{c.counters.updated}" }
                            td { "{c.counters.deleted}" }
                            td {
                                match &c.agg {
                                    AggState::None => rsx! { span { class: "muted", "-" } },
                                    _ => rsx! { "{c.agg.display()}" },
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
