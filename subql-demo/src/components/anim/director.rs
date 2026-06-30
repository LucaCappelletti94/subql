//! Scripted phase clock for the animation scenes. A scene hands `use_director`
//! its `&'static [Phase]` timeline and gets back a signal holding the current
//! phase index; the scene maps that index to CSS classes / data-attributes on a
//! stable element tree, and CSS does the per-frame motion.
//!
//! The driving loop reuses the `gloo_timers` + async-task pattern from
//! `sim_controls::start_auto_loop`: wasm-only, with a native no-op so the
//! check-only native build still compiles.

use dioxus::prelude::*;

/// One step of a scene's timeline.
#[derive(Clone, Copy)]
#[allow(clippy::struct_field_names)] // `phase` reads naturally here
pub struct Phase {
    /// Scenario this step belongs to (`"insert" | "update" | "delete"`),
    /// surfaced as `data-scenario` so CSS/markup can vary verdicts per scenario.
    pub scenario: &'static str,
    /// Motion phase: `"idle" | "write" | "commit" | "cdc" | "fanout" |
    /// "verdict"`, surfaced as `data-phase`.
    pub phase: &'static str,
    /// Dwell time in milliseconds before advancing to the next step. Only read
    /// by the wasm driving loop; the native build is check-only.
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub ms: u32,
}

/// Drive `timeline` on a continuous loop, returning the signal of the current
/// index. The loop starts once on mount and never stops (the scene is always
/// mounted); motion itself is CSS, gated on `prefers-reduced-motion`.
pub fn use_director(timeline: &'static [Phase]) -> Signal<usize> {
    let step = use_signal(|| 0usize);

    #[cfg(target_arch = "wasm32")]
    {
        let mut step = step;
        use_future(move || async move {
            use gloo_timers::future::TimeoutFuture;
            let mut i = 0usize;
            loop {
                TimeoutFuture::new(timeline[i].ms).await;
                i = (i + 1) % timeline.len();
                step.set(i);
            }
        });
    }
    #[cfg(not(target_arch = "wasm32"))]
    let _ = timeline;

    step
}
