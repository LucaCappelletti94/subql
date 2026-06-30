//! Scene 1: the "system flow". Clients (writers and subscribers, deliberately on
//! the same side) talk to the SubQL core, which sits in front of one-or-more
//! Postgres backends. A write glides client -> SubQL -> Postgres, the CDC change
//! returns Postgres -> SubQL, then SubQL fans out per-consumer deltas to the
//! subscribed clients, each lighting up with its view-relative verdict
//! (IN / OUT / UPDATE).
//!
//! All elements exist in every phase; the director only flips `data-scenario` /
//! `data-phase` on the root and toggles `is-active` / `is-flying` / `is-shown`
//! classes on stable nodes. CSS does the motion. Decoupled from the live engine.

use dioxus::prelude::*;
use dioxus_free_icons::icons::fa_solid_icons::{FaBrain, FaCircleNodes, FaRobot, FaServer, FaUser};

use super::director::{use_director, Phase};
use super::primitives::{db_node, edge, logo_node, node};

// Faint topology skeleton (always visible). Coordinates are in the 800x360
// viewBox; clients at x=120, SubQL at x=400, the PostgreSQL cluster around
// x=700. SubQL wires to two of the three backends (top + bottom); the third
// (right apex) is reached only through the replication mesh.
const E_USER: &str = "M150,52 C 270,66 300,150 372,168";
const E_BOT: &str = "M150,116 C 270,120 300,158 372,172";
const E_S1: &str = "M150,198 C 270,192 320,182 372,180";
const E_S2: &str = "M150,262 C 270,250 320,200 372,184";
const E_S3: &str = "M150,326 C 270,300 320,212 372,188";
const E_PG_TOP: &str = "M428,168 C 540,138 590,118 632,116";
const E_PG_BOT: &str = "M428,182 C 540,212 590,236 632,236";

// Dashed replication mesh between the three PostgreSQL instances: an equilateral
// triangle with a vertical left edge (top + bottom) and the apex on the right.
// Vertices (icon centers): top (645,114), bottom (645,238), apex (752,176) - a
// 124-unit side, apex one altitude (~107) to the right at the vertical midpoint.
// Each endpoint is inset ~17 units from its vertex along the edge so the dashes
// stop just outside the ~14-unit-radius icons instead of running under them.
const M_TOP_APEX: &str = "M660,123 L737,168";
const M_APEX_BOT: &str = "M737,185 L660,230";
const M_TOP_BOT: &str = "M645,131 L645,221";

// Token motion paths (reused as `offset-path`). The write token rides client ->
// SubQL -> the top backend in one glide; the CDC token returns; the delta tokens
// fan out to each subscriber.
const P_WRITE: &str = "M150,116 C 280,116 300,175 400,175 C 520,175 575,135 632,116";
const P_CDC: &str = "M632,116 C 575,135 520,175 408,175";
const P_FAN1: &str = "M400,175 C 300,180 250,198 156,198";
const P_FAN2: &str = "M400,175 C 300,205 250,262 156,262";
const P_FAN3: &str = "M400,175 C 300,238 250,326 156,326";

// One full loop: the UPDATE scenario first (the showcase: IN for one subscriber,
// OUT for another, UPDATE for a third), then INSERT, then DELETE.
const TIMELINE: &[Phase] = &[
    Phase {
        scenario: "update",
        phase: "idle",
        ms: 700,
    },
    Phase {
        scenario: "update",
        phase: "write",
        ms: 1100,
    },
    Phase {
        scenario: "update",
        phase: "commit",
        ms: 1000,
    },
    Phase {
        scenario: "update",
        phase: "cdc",
        ms: 950,
    },
    Phase {
        scenario: "update",
        phase: "fanout",
        ms: 950,
    },
    Phase {
        scenario: "update",
        phase: "verdict",
        ms: 1500,
    },
    Phase {
        scenario: "insert",
        phase: "idle",
        ms: 550,
    },
    Phase {
        scenario: "insert",
        phase: "write",
        ms: 1100,
    },
    Phase {
        scenario: "insert",
        phase: "commit",
        ms: 1000,
    },
    Phase {
        scenario: "insert",
        phase: "cdc",
        ms: 950,
    },
    Phase {
        scenario: "insert",
        phase: "fanout",
        ms: 850,
    },
    Phase {
        scenario: "insert",
        phase: "verdict",
        ms: 1300,
    },
    Phase {
        scenario: "delete",
        phase: "idle",
        ms: 550,
    },
    Phase {
        scenario: "delete",
        phase: "write",
        ms: 1100,
    },
    Phase {
        scenario: "delete",
        phase: "commit",
        ms: 1000,
    },
    Phase {
        scenario: "delete",
        phase: "cdc",
        ms: 950,
    },
    Phase {
        scenario: "delete",
        phase: "fanout",
        ms: 850,
    },
    Phase {
        scenario: "delete",
        phase: "verdict",
        ms: 1300,
    },
];

fn verdict_class(v: Option<&str>) -> &'static str {
    match v {
        Some("IN") => "sq-vl--in",
        Some("OUT") => "sq-vl--out",
        Some("UPDATE") => "sq-vl--update",
        _ => "",
    }
}

#[component]
pub fn SystemFlowScene() -> Element {
    let step = use_director(TIMELINE);
    let p = TIMELINE[*step.read() % TIMELINE.len()];
    let scenario = p.scenario;
    let phase = p.phase;

    let verb = match scenario {
        "insert" => "INSERT",
        "delete" => "DELETE",
        _ => "UPDATE",
    };

    // Per-subscriber verdict for this scenario (browser, service, agent).
    let (v1, v2, v3) = match scenario {
        "insert" => (Some("IN"), None, None),
        "delete" => (None, Some("OUT"), None),
        _ => (Some("IN"), Some("OUT"), Some("UPDATE")),
    };

    let on = |b: bool, class: &'static str| if b { class } else { "" };

    // Writes in this scene originate from the bot/agent row (the write token's
    // path starts there); the user node stays a passive client.
    let bot_writes = phase == "write";
    let subql_active = matches!(phase, "write" | "cdc" | "fanout");
    let pg_active = matches!(phase, "commit" | "cdc");
    // The PostgreSQL cluster keeps its brand blue; `is-active` only changes its
    // brightness (see `.sq-pg` in the stylesheet).
    let pg_state = if pg_active {
        "sq-pg is-active"
    } else {
        "sq-pg"
    };
    // Replication fires the instant the user's write lands at the primary (the
    // commit phase): payloads flow out across the mesh to the other instances.
    let replicate = on(phase == "commit", "is-flying");

    let write_flying = phase == "write";
    let cdc_flying = phase == "cdc";
    let fan = phase == "fanout";
    let verdict = phase == "verdict";

    // Delta tokens fly (and badges light) only for the subscribers this change
    // actually affects.
    let d1 = on(fan && v1.is_some(), "is-flying");
    let d2 = on(fan && v2.is_some(), "is-flying");
    let d3 = on(fan && v3.is_some(), "is-flying");
    let show1 = on(verdict && v1.is_some(), "is-shown");
    let show2 = on(verdict && v2.is_some(), "is-shown");
    let show3 = on(verdict && v3.is_some(), "is-shown");

    rsx! {
        div {
            class: "sq-anim",
            "data-scenario": scenario,
            "data-phase": phase,
            aria_hidden: "true",
            svg {
                class: "sq-anim-svg",
                view_box: "0 0 800 360",
                preserve_aspect_ratio: "xMidYMid meet",

                // Skeleton
                {edge(E_USER)}
                {edge(E_BOT)}
                {edge(E_S1)}
                {edge(E_S2)}
                {edge(E_S3)}
                {edge(E_PG_TOP)}
                {edge(E_PG_BOT)}

                // PostgreSQL replication mesh (dashed, brand blue). When the
                // write lands at the primary (top), payloads replicate out to
                // the other two instances, in sync with the arriving write.
                path { class: "sq-mesh", d: M_TOP_APEX }
                path { class: "sq-mesh", d: M_APEX_BOT }
                path { class: "sq-mesh", d: M_TOP_BOT }
                circle { class: "sq-pg-payload {replicate}", r: "4", style: "offset-path: path('{M_TOP_APEX}')" }
                circle { class: "sq-pg-payload {replicate}", r: "4", style: "offset-path: path('{M_TOP_BOT}')" }

                // Nodes: clients (writers + subscribers) on the left, core, backends.
                {node(FaUser, "user", 120, 52, "")}
                {node(FaRobot, "bot / agent", 120, 116, on(bot_writes, "is-active"))}
                {node(FaCircleNodes, "browser", 120, 198, show1)}
                {node(FaServer, "service", 120, 262, show2)}
                {node(FaBrain, "LLM agent", 120, 326, show3)}
                {logo_node("SubQL", 400, 175, on(subql_active, "is-active"))}
                {db_node(645, 114, pg_state)}
                {db_node(752, 176, pg_state)}
                {db_node(645, 238, pg_state)}
                text { class: "sq-node-label sq-pg", x: "694", y: "300", "PostgreSQL" }

                // Tokens
                g { class: "sq-token sq-token--write {on(write_flying, \"is-flying\")}",
                    style: "offset-path: path('{P_WRITE}')",
                    circle { r: "9" }
                    text { class: "sq-token-label", x: "0", y: "-13", "{verb}" }
                }
                g { class: "sq-token sq-token--cdc {on(cdc_flying, \"is-flying\")}",
                    style: "offset-path: path('{P_CDC}')",
                    circle { r: "6" }
                    text { class: "sq-token-label", x: "0", y: "-11", "CDC" }
                }
                g { class: "sq-token sq-token--delta {d1}", style: "offset-path: path('{P_FAN1}')",
                    circle { r: "6" }
                }
                g { class: "sq-token sq-token--delta {d2}", style: "offset-path: path('{P_FAN2}')",
                    circle { r: "6" }
                }
                g { class: "sq-token sq-token--delta {d3}", style: "offset-path: path('{P_FAN3}')",
                    circle { r: "6" }
                }

                // Verdict badges, anchored to the right of each subscriber.
                g { class: "sq-vl {verdict_class(v1)} {show1}", transform: "translate(150, 198)",
                    rect { class: "sq-vl-bg", x: "8", y: "-10", width: "58", height: "20", rx: "10" }
                    text { class: "sq-vl-text", x: "37", y: "4", "{v1.unwrap_or(\"\")}" }
                }
                g { class: "sq-vl {verdict_class(v2)} {show2}", transform: "translate(150, 262)",
                    rect { class: "sq-vl-bg", x: "8", y: "-10", width: "58", height: "20", rx: "10" }
                    text { class: "sq-vl-text", x: "37", y: "4", "{v2.unwrap_or(\"\")}" }
                }
                g { class: "sq-vl {verdict_class(v3)} {show3}", transform: "translate(150, 326)",
                    rect { class: "sq-vl-bg", x: "8", y: "-10", width: "58", height: "20", rx: "10" }
                    text { class: "sq-vl-text", x: "37", y: "4", "{v3.unwrap_or(\"\")}" }
                }
            }
            p { class: "sq-visually-hidden",
                "Animated diagram: a client write flows through SubQL to a Postgres "
                "backend, then SubQL pushes each subscriber only the change to their "
                "query, a row entering, leaving, or updating their result."
            }
        }
    }
}
