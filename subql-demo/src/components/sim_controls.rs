use dioxus::prelude::*;
use dioxus_free_icons::{
    icons::fa_solid_icons::{
        FaBroom, FaForwardStep, FaPause, FaPenToSquare, FaPlay, FaPlus, FaSliders, FaTrash,
    },
    Icon,
};

use crate::sim;

use super::{bump, SharedState, TickSignal};

#[component]
pub fn SimControls() -> Element {
    let state = use_context::<SharedState>();
    let mut tick = use_context::<TickSignal>();
    let _ = tick.read();

    let running = state.borrow().auto_running;
    let rate = state.borrow().auto_rate_per_sec;
    let selected = state.borrow().selected;
    let table_names: Vec<String> = state
        .borrow()
        .tables
        .iter()
        .map(|t| t.table_name.clone())
        .collect();

    let on_insert = {
        let state = state.clone();
        move |_| {
            let result = sim::do_insert(&mut state.borrow_mut());
            if let Err(e) = result {
                state.borrow_mut().note(format!("insert failed: {e}"));
            }
            bump(&mut tick);
        }
    };
    let on_update = {
        let state = state.clone();
        move |_| {
            let result = sim::do_update_random(&mut state.borrow_mut());
            if let Err(e) = result {
                state.borrow_mut().note(format!("update failed: {e}"));
            }
            bump(&mut tick);
        }
    };
    let on_delete = {
        let state = state.clone();
        move |_| {
            let result = sim::do_delete_random(&mut state.borrow_mut());
            if let Err(e) = result {
                state.borrow_mut().note(format!("delete failed: {e}"));
            }
            bump(&mut tick);
        }
    };
    let on_truncate = {
        let state = state.clone();
        move |_| {
            let result = sim::do_truncate(&mut state.borrow_mut());
            if let Err(e) = result {
                state.borrow_mut().note(format!("truncate failed: {e}"));
            }
            bump(&mut tick);
        }
    };
    let on_step = {
        let state = state.clone();
        move |_| {
            let result = sim::step_auto(&mut state.borrow_mut());
            if let Err(e) = result {
                state.borrow_mut().note(format!("step failed: {e}"));
            }
            bump(&mut tick);
        }
    };
    let on_toggle_auto = {
        let state = state.clone();
        move |_| {
            let now = {
                let mut s = state.borrow_mut();
                s.auto_running = !s.auto_running;
                s.auto_running
            };
            bump(&mut tick);
            if now {
                start_auto_loop(state.clone(), tick);
            }
        }
    };
    let on_rate = {
        let state = state.clone();
        move |evt: FormEvent| {
            if let Ok(v) = evt.value().parse::<f32>() {
                state.borrow_mut().auto_rate_per_sec = v.clamp(0.5, 20.0);
            }
            bump(&mut tick);
        }
    };
    let on_select_table = {
        let state = state.clone();
        move |evt: FormEvent| {
            if let Ok(idx) = evt.value().parse::<usize>() {
                if idx < state.borrow().tables.len() {
                    state.borrow_mut().selected = idx;
                }
            }
            bump(&mut tick);
        }
    };

    rsx! {
        h2 {
            Icon { width: 16, height: 16, icon: FaSliders, class: "h-icon".to_string() }
            " Simulation"
        }
        div {
            button { onclick: on_toggle_auto,
                if running {
                    Icon { width: 12, height: 12, icon: FaPause, class: "btn-icon".to_string() }
                    " Pause"
                } else {
                    Icon { width: 12, height: 12, icon: FaPlay, class: "btn-icon".to_string() }
                    " Play"
                }
            }
            button { onclick: on_step,
                Icon { width: 12, height: 12, icon: FaForwardStep, class: "btn-icon".to_string() }
                " Step"
            }
            label { class: "muted", style: "margin-left: 1rem;",
                "rate: {rate:.1}/s"
                input {
                    r#type: "range",
                    min: "0.5", max: "20", step: "0.5",
                    value: "{rate}",
                    oninput: on_rate,
                }
            }
        }
        h3 { "Manual actions" }
        label { class: "muted",
            "on table "
            select { value: "{selected}", onchange: on_select_table,
                for (i, name) in table_names.iter().enumerate() {
                    option { value: "{i}", selected: i == selected, "{name}" }
                }
            }
        }
        div {
            button { onclick: on_insert,
                Icon { width: 12, height: 12, icon: FaPlus, class: "btn-icon".to_string() }
                " INSERT random"
            }
            button { onclick: on_update,
                Icon { width: 12, height: 12, icon: FaPenToSquare, class: "btn-icon".to_string() }
                " UPDATE random"
            }
            button { class: "danger", onclick: on_delete,
                Icon { width: 12, height: 12, icon: FaTrash, class: "btn-icon".to_string() }
                " DELETE random"
            }
            button { class: "danger", onclick: on_truncate,
                Icon { width: 12, height: 12, icon: FaBroom, class: "btn-icon".to_string() }
                " TRUNCATE"
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn start_auto_loop(state: SharedState, mut tick: TickSignal) {
    use gloo_timers::future::TimeoutFuture;
    spawn(async move {
        loop {
            let (running, rate) = {
                let s = state.borrow();
                (s.auto_running, s.auto_rate_per_sec)
            };
            if !running {
                break;
            }
            let interval_ms = (1000.0 / rate.max(0.5)).max(50.0) as u32;
            TimeoutFuture::new(interval_ms).await;
            if !state.borrow().auto_running {
                break;
            }
            let result = crate::sim::step_auto(&mut state.borrow_mut());
            if let Err(e) = result {
                state.borrow_mut().note(format!("auto step failed: {e}"));
            }
            bump(&mut tick);
        }
    });
}

#[cfg(not(target_arch = "wasm32"))]
fn start_auto_loop(_state: SharedState, _tick: TickSignal) {
    // Native build: cargo check only. Auto-loop has no driver outside wasm.
}
