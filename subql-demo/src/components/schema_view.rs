use dioxus::prelude::*;
use dioxus_free_icons::{icons::fa_solid_icons::FaTable, Icon};

use subql::{Cell, RowImage};

use super::{bump, SharedState, TickSignal};

const MAX_VISIBLE: usize = 12;

struct TableSnapshot {
    name: String,
    columns: Vec<(String, String)>,
    rows: Vec<RowImage>,
    row_count: usize,
}

#[component]
pub fn SchemaView() -> Element {
    let state = use_context::<SharedState>();
    let mut tick = use_context::<TickSignal>();
    let _ = tick.read();

    let s = state.borrow();
    let selected = s.selected;
    let tables: Vec<TableSnapshot> = s
        .tables
        .iter()
        .map(|t| TableSnapshot {
            name: t.table_name.clone(),
            columns: t
                .columns
                .iter()
                .cloned()
                .zip(t.column_types.iter().cloned())
                .collect(),
            rows: t.rows.iter().take(MAX_VISIBLE).cloned().collect(),
            row_count: t.rows.len(),
        })
        .collect();
    drop(s);

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaTable, class: "h-icon".to_string() }
            " Tables"
        }
        for (ti, t) in tables.into_iter().enumerate() {
            {
                let state = state.clone();
                let is_selected = ti == selected;
                let select = move |_| {
                    state.borrow_mut().selected = ti;
                    bump(&mut tick);
                };
                rsx! {
                    div { class: "table-block",
                        h3 {
                            button {
                                r#type: "button",
                                class: if is_selected { "table-tab selected" } else { "table-tab" },
                                onclick: select,
                                code { "{t.name}" }
                                if is_selected {
                                    span { class: "muted", " (sim target)" }
                                }
                            }
                        }
                        p { class: "muted",
                            "{t.row_count} row(s) materialized from the CDC stream. ",
                            if t.row_count > MAX_VISIBLE {
                                "Showing first {MAX_VISIBLE}."
                            }
                        }
                        table { class: "schema-table",
                            thead {
                                tr {
                                    for (col, ty) in t.columns.iter() {
                                        th {
                                            "{col}"
                                            br {}
                                            span { class: "muted", "{ty}" }
                                        }
                                    }
                                }
                            }
                            tbody {
                                for image in t.rows.iter() {
                                    tr {
                                        for cell in image.iter() {
                                            td { "{cell_display(cell)}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn cell_display(cell: &Cell) -> String {
    match cell {
        Cell::Null | Cell::Missing => "NULL".into(),
        Cell::Bool(b) => b.to_string(),
        Cell::Int(i) => i.to_string(),
        Cell::Float(f) => format!("{f:.3}"),
        Cell::String(s) => format!("'{s}'"),
        _ => "?".into(),
    }
}
