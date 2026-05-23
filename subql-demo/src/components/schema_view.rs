use dioxus::prelude::*;
use dioxus_free_icons::{Icon, icons::fa_solid_icons::FaTable};

use subql::Cell;

use super::{SharedState, TickSignal};

#[component]
pub fn SchemaView() -> Element {
    let state = use_context::<SharedState>();
    let tick = use_context::<TickSignal>();
    let _ = tick.read();

    let s = state.borrow();
    let table_name = s.preset.table_name;
    let columns: Vec<(&'static str, &'static str)> = s
        .preset
        .columns
        .iter()
        .zip(s.preset.column_types.iter())
        .map(|(c, t)| (*c, *t))
        .collect();
    let rows = s.capture.snapshot_rows();
    let row_count = rows.len();
    drop(s);

    let max_visible = 20;
    let visible_rows: Vec<_> = rows.into_iter().take(max_visible).collect();

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaTable, class: "h-icon".to_string() }
            " Table "
            code { "{table_name}" }
        }
        p { class: "muted",
            "{row_count} row(s) currently in sqlite. ",
            if row_count > max_visible {
                "Showing first {max_visible}."
            }
        }
        table { class: "schema-table",
            thead {
                tr {
                    th { "rowid" }
                    for (col, ty) in columns.iter() {
                        th {
                            "{col}"
                            br {}
                            span { class: "muted", "{ty}" }
                        }
                    }
                }
            }
            tbody {
                for (rowid, image) in visible_rows {
                    tr {
                        td { class: "muted", "{rowid}" }
                        for cell in image.cells.iter() {
                            td { "{cell_display(cell)}" }
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
