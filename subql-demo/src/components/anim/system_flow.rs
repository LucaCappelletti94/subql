//! Scene 1: the "system flow". Three client archetypes (a user, a backend
//! service, an LLM agent) sit on one side. Each both writes and subscribes: to
//! SubQL a client is just a connection. SubQL sits in front of a SQL database
//! (engine-agnostic: SubQL works against Postgres, MySQL, etc.). Per scenario a
//! different client issues a write, it lands in the database, then SubQL fans
//! the change back out as per-consumer deltas, and each affected client lights
//! up with its
//! view-relative verdict (IN / OUT / UPDATE). A caption narrates each step.
//!
//! Every payload rides one of the drawn connections: the spokes and links below
//! are each used both as the visible line and as a token's `offset-path` (in
//! reverse for the return directions, via `is-flying-rev`). All elements exist
//! in every phase; the director only flips `data-scenario` / `data-phase` on the
//! root and toggles classes. CSS does the motion. Decoupled from the live engine.

use dioxus::prelude::*;
use dioxus_free_icons::icons::fa_solid_icons::{FaRobot, FaServer, FaUser};

use super::director::{use_director, Phase};
use super::primitives::{db_node, edge, logo_node, node, payload_shape};

// Every line stops a uniform ~10px gap from its icon: 24px from the 28px client
// and database icon centers, 33px from the larger SubQL logo center (400,176).
//
// Spokes: each client <-> SubQL, drawn once (client -> SubQL direction) and
// reused for token motion. Writes ride them forward; deltas ride them reversed.
// Clients sit at x=175 (pulled in from the edge to tighten both sides equally).
const SPOKE_USER: &str = "M199,92 C 270,104 330,160 367,172";
const SPOKE_SVC: &str = "M199,176 C 270,176 330,176 367,176";
const SPOKE_AGENT: &str = "M199,260 C 270,248 330,190 367,180";

// SubQL <-> the SQL database (one node at (625,176), mirroring the client column
// about x=400 so the logo stays centered). The write leg rides this link forward;
// the CDC return rides it reversed.
const LINK_DB: &str = "M433,176 L601,176";

// The write payload rides one continuous path (client -> through SubQL -> the
// database) as a single glide spanning the write and commit phases, so it
// crosses the hub smoothly instead of teleporting from the spoke's end to the
// link's start. Each follows its drawn spoke to (367,~176), bridges the logo
// gap through (400,176), then runs the link to the database.
const WRITE_USER: &str = "M199,92 C 270,104 330,160 367,172 C 385,176 415,176 433,176 L601,176";
const WRITE_SVC: &str = "M199,176 C 270,176 330,176 367,176 C 385,176 415,176 433,176 L601,176";
const WRITE_AGENT: &str = "M199,260 C 270,248 330,190 367,180 C 385,176 415,176 433,176 L601,176";

// The CDC change returns along the same link but keeps going across the logo to
// the spoke junction (367,176), so it transits SubQL smoothly instead of
// stopping on the database side and teleporting to the client side. Ridden
// reversed: database (601) -> through the logo -> the fan-out point (367), where
// it multiplies into the per-consumer delta tokens that ride the spokes out.
const CDC_IN: &str = "M367,176 C 385,176 415,176 433,176 L601,176";

// One full loop: UPDATE first (the IN/OUT/UPDATE showcase), then INSERT, DELETE.
// Durations are generous so the captions are readable and the points are easy
// to follow.
const TIMELINE: &[Phase] = &[
    Phase {
        scenario: "update",
        phase: "idle",
        ms: 900,
    },
    Phase {
        scenario: "update",
        phase: "write",
        ms: 2800,
    },
    Phase {
        scenario: "update",
        phase: "commit",
        ms: 2400,
    },
    Phase {
        scenario: "update",
        phase: "cdc",
        ms: 2400,
    },
    Phase {
        scenario: "update",
        phase: "fanout",
        ms: 2800,
    },
    Phase {
        scenario: "update",
        phase: "verdict",
        ms: 3600,
    },
    Phase {
        scenario: "insert",
        phase: "idle",
        ms: 800,
    },
    Phase {
        scenario: "insert",
        phase: "write",
        ms: 2800,
    },
    Phase {
        scenario: "insert",
        phase: "commit",
        ms: 2400,
    },
    Phase {
        scenario: "insert",
        phase: "cdc",
        ms: 2400,
    },
    Phase {
        scenario: "insert",
        phase: "fanout",
        ms: 2600,
    },
    Phase {
        scenario: "insert",
        phase: "verdict",
        ms: 3200,
    },
    Phase {
        scenario: "delete",
        phase: "idle",
        ms: 800,
    },
    Phase {
        scenario: "delete",
        phase: "write",
        ms: 2800,
    },
    Phase {
        scenario: "delete",
        phase: "commit",
        ms: 2400,
    },
    Phase {
        scenario: "delete",
        phase: "cdc",
        ms: 2400,
    },
    Phase {
        scenario: "delete",
        phase: "fanout",
        ms: 2600,
    },
    Phase {
        scenario: "delete",
        phase: "verdict",
        ms: 3200,
    },
];

/// The content (fill) color for a payload carrying verdict `v`: green for IN,
/// red for OUT, blue for UPDATE. Falls back to the inherited color.
fn verdict_fill(v: Option<&str>) -> &'static str {
    match v {
        Some("IN") => "var(--op-in)",
        Some("OUT") => "var(--danger)",
        Some("UPDATE") => "var(--op-upd)",
        _ => "currentColor",
    }
}

/// The payload shape for verdict `v`: up-triangle for IN, down-triangle for OUT,
/// diamond for UPDATE (see [`payload_shape`]).
fn verdict_shape(v: Option<&str>) -> &'static str {
    match v {
        Some("IN") => "add",
        Some("OUT") => "remove",
        _ => "modify",
    }
}

#[component]
pub fn SystemFlowScene() -> Element {
    let step = use_director(TIMELINE);
    let idx = *step.read() % TIMELINE.len();
    let p = TIMELINE[idx];
    let scenario = p.scenario;
    let phase = p.phase;

    let verb = match scenario {
        "insert" => "INSERT",
        "delete" => "DELETE",
        _ => "UPDATE",
    };

    // Per-scenario: which client writes, its spoke, its color, and the verdict
    // each client's subscription gets (user, service, agent).
    let (writer, write_path, write_color) = match scenario {
        "insert" => ("user", WRITE_USER, "sq-c-user"),
        "delete" => ("service", WRITE_SVC, "sq-c-service"),
        _ => ("agent", WRITE_AGENT, "sq-c-agent"),
    };
    let (vu, vs, va) = match scenario {
        "insert" => (None, Some("IN"), None),
        "delete" => (None, None, Some("OUT")),
        _ => (Some("IN"), Some("OUT"), Some("UPDATE")),
    };

    let on = |b: bool, class: &'static str| if b { class } else { "" };

    let writing = phase == "write";
    let fan = phase == "fanout";
    let verdict = phase == "verdict";

    // Each client carries its identity color (sq-c-*) and lights (is-active)
    // when it is writing or when its verdict is showing.
    let user_state = if (writing && writer == "user") || (verdict && vu.is_some()) {
        "sq-c-user is-active"
    } else {
        "sq-c-user"
    };
    let svc_state = if (writing && writer == "service") || (verdict && vs.is_some()) {
        "sq-c-service is-active"
    } else {
        "sq-c-service"
    };
    let agent_state = if (writing && writer == "agent") || (verdict && va.is_some()) {
        "sq-c-agent is-active"
    } else {
        "sq-c-agent"
    };

    let subql_state = on(matches!(phase, "write" | "cdc" | "fanout"), "is-active");
    let db_state = if matches!(phase, "commit" | "cdc") {
        "sq-db is-active"
    } else {
        "sq-db"
    };

    // The write travels in two legs on real lines, each its own token so that
    // `is-flying` is added fresh in its phase (which reliably (re)starts the
    // glide): the writer's spoke (client -> SubQL) in `write`, then the link to
    // the database in `commit`.
    // One continuous glide across both write and commit: the class stays put
    // across the phase change (same class, same path, same style), so the
    // animation is never restarted and the payload crosses SubQL without a jump.
    let write_fly = on(matches!(phase, "write" | "commit"), "is-writing");
    // CDC rides the link back (reversed).
    let cdc_rev = on(phase == "cdc", "is-flying-rev");

    // Delta tokens ride each spoke reversed (SubQL -> client) during `fanout`,
    // then stay landed beside the client through `verdict`, where the verdict
    // label fades in beside the shape.
    let du = on(fan && vu.is_some(), "is-flying-rev");
    let ds = on(fan && vs.is_some(), "is-flying-rev");
    let da = on(fan && va.is_some(), "is-flying-rev");
    let lu = on(verdict && vu.is_some(), "is-landed");
    let ls = on(verdict && vs.is_some(), "is-landed");
    let la = on(verdict && va.is_some(), "is-landed");
    let su = on(verdict && vu.is_some(), "is-shown");
    let ss = on(verdict && vs.is_some(), "is-shown");
    let sa = on(verdict && va.is_some(), "is-shown");

    // Payload colors: content (fill) is the operation; sender/recipient (border)
    // is an identity color. The verb takes the operation color too.
    let op_fill = match scenario {
        "insert" => "var(--op-in)",
        "delete" => "var(--danger)",
        _ => "var(--op-upd)",
    };
    let op_shape = match scenario {
        "insert" => "add",
        "delete" => "remove",
        _ => "modify",
    };
    let writer_stroke = match writer {
        "user" => "var(--c-user)",
        "service" => "var(--c-service)",
        _ => "var(--c-agent)",
    };
    let verb_hl = match scenario {
        "insert" => "sq-cap-hl sq-vl--in",
        "delete" => "sq-cap-hl sq-vl--out",
        _ => "sq-cap-hl sq-vl--update",
    };

    // A brief, color-coded description of the current step. Each segment is
    // (text, class): "" inherits the muted caption color; otherwise `sq-cap-hl`
    // plus an accent class colors the word (client name = sender color, verb =
    // operation color).
    let writer_hl = format!("sq-cap-hl {write_color}");
    let article = if scenario == "delete" { "a" } else { "an" };
    let cap_segs: Vec<(&str, &str)> = match phase {
        "write" => vec![
            ("The ", ""),
            (writer, writer_hl.as_str()),
            (" issues ", ""),
            (article, ""),
            (" ", ""),
            (verb, verb_hl),
        ],
        "commit" => vec![("Written to the ", ""), ("database", "sq-cap-hl sq-db")],
        "cdc" => vec![
            ("SubQL", "sq-cap-subql"),
            (" captures the ", ""),
            ("change", "sq-cap-hl sq-db"),
        ],
        "fanout" => vec![("Matched against every standing subscription", "")],
        "verdict" => match scenario {
            "insert" => vec![
                ("A new row enters the ", ""),
                ("service", "sq-cap-hl sq-c-service"),
                ("'s query", ""),
            ],
            "delete" => vec![
                ("A row ", ""),
                ("leaves", "sq-cap-hl sq-vl--out"),
                (" the ", ""),
                ("agent", "sq-cap-hl sq-c-agent"),
                ("'s query", ""),
            ],
            _ => vec![
                ("In", "sq-cap-hl sq-vl--in"),
                (" for one query, ", ""),
                ("out", "sq-cap-hl sq-vl--out"),
                (" for another, ", ""),
                ("updated", "sq-cap-hl sq-vl--update"),
                (" for a third", ""),
            ],
        },
        _ => vec![],
    };

    rsx! {
        div {
            class: "sq-anim",
            "data-scenario": scenario,
            "data-phase": phase,
            aria_hidden: "true",
            svg {
                class: "sq-anim-svg",
                // Cropped to the content bounds (diagram ~70..286 y, caption at
                // ~291) with a small margin, so there is no excess space.
                view_box: "140 58 520 237",
                preserve_aspect_ratio: "xMidYMid meet",

                // Drawn connections (reused as token paths).
                {edge(SPOKE_USER)}
                {edge(SPOKE_SVC)}
                {edge(SPOKE_AGENT)}
                {edge(LINK_DB)}

                // Clients (each writes and subscribes), core, the database.
                {node(FaUser, "User", 175, 92, user_state)}
                {node(FaServer, "Service", 175, 176, svc_state)}
                {node(FaRobot, "Agent", 175, 260, agent_state)}
                {logo_node("SubQL", 400, 176, subql_state)}
                {db_node(625, 176, db_state)}
                text { class: "sq-node-label sq-db", x: "625", y: "204", "SQL DB" }

                // Write token: a single payload that glides client -> through
                // SubQL -> the database in one continuous motion (write + commit).
                // Filled by the operation, bordered by the writer.
                g { class: "sq-token {write_fly}",
                    style: "offset-path: path('{write_path}'); color: {op_fill}; --stroke: {writer_stroke}",
                    {payload_shape(op_shape)}
                    text { class: "sq-token-label", x: "0", y: "-15", "{verb}" }
                }
                // CDC change returning to SubQL: same operation (shape + fill),
                // bordered by the database (sender). Rides the return path
                // reversed, transiting the logo to the fan-out point.
                g { class: "sq-token {cdc_rev}",
                    style: "offset-path: path('{CDC_IN}'); color: {op_fill}; --stroke: var(--db)",
                    {payload_shape(op_shape)}
                    text { class: "sq-token-label", x: "0", y: "-13", "CDC" }
                }
                // Per-consumer delta tokens: shaped + filled by the consumer's
                // verdict, bordered by the recipient client. They ride the spoke
                // in during fanout, stay landed during verdict, and the verdict
                // label fades in beside the shape (this replaces a separate badge).
                g { class: "sq-token {du} {lu}",
                    style: "offset-path: path('{SPOKE_USER}'); color: {verdict_fill(vu)}; --stroke: var(--c-user)",
                    {payload_shape(verdict_shape(vu))}
                    text { class: "sq-dl-label {su}", x: "13", y: "4", "{vu.unwrap_or(\"\")}" }
                }
                g { class: "sq-token {ds} {ls}",
                    style: "offset-path: path('{SPOKE_SVC}'); color: {verdict_fill(vs)}; --stroke: var(--c-service)",
                    {payload_shape(verdict_shape(vs))}
                    text { class: "sq-dl-label {ss}", x: "13", y: "4", "{vs.unwrap_or(\"\")}" }
                }
                g { class: "sq-token {da} {la}",
                    style: "offset-path: path('{SPOKE_AGENT}'); color: {verdict_fill(va)}; --stroke: var(--c-agent)",
                    {payload_shape(verdict_shape(va))}
                    text { class: "sq-dl-label {sa}", x: "13", y: "4", "{va.unwrap_or(\"\")}" }
                }

                // Per-phase caption, as SVG text in the band at the bottom of the
                // box (the viewBox is cropped to just below it). One-item keyed
                // loop so a phase change remounts it (keys only reconcile inside an
                // iterator), replaying the fade; duration matches the phase length.
                for cap_key in [idx] {
                    text {
                        key: "{cap_key}",
                        class: "sq-anim-caption-text",
                        x: "400",
                        y: "291",
                        style: "animation-duration: {p.ms}ms",
                        for (seg, cls) in cap_segs.iter() {
                            tspan { class: "{cls}", "{seg}" }
                        }
                    }
                }
            }
            p { class: "sq-visually-hidden",
                "Animated diagram: a client (a user, a service, or an agent) writes "
                "to a SQL database through SubQL, then SubQL captures the change and "
                "pushes each subscribed client only the change to their query, a "
                "row entering, leaving, or updating their result."
            }
        }
    }
}
