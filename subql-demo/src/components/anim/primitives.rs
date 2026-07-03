//! Shared SVG building blocks for the animation scenes. Inline SVG with
//! `currentColor` (theme + dark-mode flip for free), same convention as the
//! `Logo` in `header.rs`. Reused by every scene so a new scene is mostly a
//! timeline plus a layout.

use dioxus::prelude::*;
use dioxus_free_icons::{icons::fa_solid_icons::FaDatabase, Icon, IconShape};

use crate::components::header::{SUBQL_LOGO_PATH, SUBQL_LOGO_VIEWBOX};

/// A labelled topology node: a Font Awesome icon centered at `(x, y)` with a
/// caption below it. `state` is appended to the class (e.g. `"is-active"`); the
/// node group's `color` drives both the icon (`fill="currentColor"`) and label.
pub fn node<T>(icon: T, label: &str, x: i32, y: i32, state: &str) -> Element
where
    T: IconShape + Clone + PartialEq + 'static,
{
    rsx! {
        g { class: "sq-node {state}", transform: "translate({x}, {y})",
            g { transform: "translate(-14, -22)",
                Icon { icon, width: 28, height: 28 }
            }
            text { class: "sq-node-label", x: "0", y: "26", "{label}" }
        }
    }
}

/// Like [`node`], but renders the real SubQL logo (the brand mark) as the core
/// node instead of a Font Awesome icon. Sized larger than the client/backend
/// icons so the hub reads as the centre of the topology.
pub fn logo_node(label: &str, x: i32, y: i32, state: &str) -> Element {
    const SIZE: i32 = 60;
    rsx! {
        g { class: "sq-node {state}", transform: "translate({x}, {y})",
            svg {
                x: "{-SIZE / 2}",
                y: "{-SIZE / 2 - 6}",
                width: "{SIZE}",
                height: "{SIZE}",
                view_box: SUBQL_LOGO_VIEWBOX,
                path { d: SUBQL_LOGO_PATH, fill: "currentColor" }
            }
            text { class: "sq-node-label", x: "0", y: "36", "{label}" }
        }
    }
}

/// A database node (no label), the icon centered on its node point. The
/// replication mesh endpoints are inset (see the `M_*` consts) so the dashed
/// lines stop just outside the icon rather than running under it.
pub fn db_node(x: i32, y: i32, state: &str) -> Element {
    rsx! {
        g { class: "sq-node {state}", transform: "translate({x}, {y})",
            g { transform: "translate(-14, -14)",
                Icon { icon: FaDatabase, width: 28, height: 28 }
            }
        }
    }
}

/// A payload glyph whose shape encodes the operation: `"add"` is an up-triangle,
/// `"remove"` a down-triangle, anything else (modify) a diamond. Centered at the
/// origin; it inherits its fill (content) and stroke (sender) from the token's
/// `.sq-token` styling.
pub fn payload_shape(kind: &str) -> Element {
    match kind {
        "add" => rsx! { polygon { points: "0,-8 7,5 -7,5" } },
        "remove" => rsx! { polygon { points: "0,8 7,-5 -7,-5" } },
        _ => rsx! { polygon { points: "0,-8 8,0 0,8 -8,0" } },
    }
}

/// A faint static connector path drawn in the topology skeleton.
pub fn edge(d: &str) -> Element {
    rsx! {
        path { class: "sq-edge", d: "{d}" }
    }
}
