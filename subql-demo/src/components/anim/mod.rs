//! Animated, scripted SVG illustrations of how SubQL routes changes. Each scene
//! runs a deterministic phase timeline on a local signal (see [`director`]) and
//! expresses all motion in CSS, gated on `prefers-reduced-motion`. Decoupled
//! from the live demo engine. Built from shared [`primitives`] so a new scene is
//! one file plus a `pub use` here and one mount line in `presentation.rs`.

mod director;
mod primitives;
mod system_flow;

pub use system_flow::SystemFlowScene;
