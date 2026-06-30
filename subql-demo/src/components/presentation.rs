//! The presentation that sits above the live demo: hero, the problem SubQL
//! solves, what you get, and how it works. Copy follows `subql-demo/WEBSITE.md`
//! (hero + problem are the approved wording; features and how-it-works are
//! drafted against the same positioning).

use dioxus::prelude::*;
use dioxus_free_icons::{
    icons::fa_solid_icons::{FaCalculator, FaGears, FaShareNodes, FaTableList},
    Icon,
};

use crate::components::anim::SystemFlowScene;

const REPO_URL: &str = "https://github.com/LucaCappelletti94/subql";

#[component]
pub fn Hero() -> Element {
    rsx! {
        section { class: "sq-hero",
            h1 { "SQL subscriptions, without the plumbing" }
            p { class: "sq-hero-sub",
                "A general engine that tracks who's affected by every change, "
                "instead of the code you'd hand-write per query."
            }
            div { class: "sq-hero-cta",
                a { class: "sq-btn sq-btn--primary", href: "#demo", "Try the live demo" }
                a {
                    class: "sq-btn",
                    href: REPO_URL,
                    target: "_blank",
                    rel: "noopener",
                    "View on GitHub"
                }
            }
        }
    }
}

#[component]
pub fn Problem() -> Element {
    rsx! {
        section { class: "sq-section",
            p { class: "sq-eyebrow", "What it is" }
            div { class: "sq-prose",
                p {
                    strong { "The problem. " }
                    "Anything that needs to stay current on a slice of your data, "
                    "a live dashboard, a service, an LLM agent watching a topic, "
                    "needs the same backend: when data changes, work out who cares "
                    "and how their view changed. Today you hand-write that, per app "
                    "and per query."
                }
                p {
                    strong { "SubQL. " }
                    "The general engine for it. A consumer registers a SQL SELECT, "
                    "a standing interest, and on every change SubQL says, per "
                    "consumer, exactly how their result changed: rows in/out, "
                    "aggregate deltas. A browser, a service, or a bot kept up to "
                    "date automatically. Bring your own transport (WebSocket, SSE). "
                    "SubQL is the \"what to push, to whom\" core. One setup, any schema."
                }
            }
        }
    }
}

struct Feature {
    icon: Element,
    title: &'static str,
    body: &'static str,
}

#[component]
pub fn Features() -> Element {
    let features = vec![
        Feature {
            icon: rsx! { Icon { width: 22, height: 22, icon: FaTableList } },
            title: "Row deltas, per consumer",
            body: "Each consumer registers a SELECT. On every change SubQL reports \
                   how that consumer's result set moved: a row entering, leaving, or \
                   updating in place. The same base change is an INSERT for one \
                   consumer and a DELETE for another, decided by their WHERE clause.",
        },
        Feature {
            icon: rsx! { Icon { width: 22, height: 22, icon: FaCalculator } },
            title: "Aggregate deltas, incremental",
            body: "COUNT, SUM, AVG, variance and stddev are maintained with signed \
                   updates as rows move through a subscription. No re-scan of the \
                   table when a single row changes.",
        },
        Feature {
            icon: rsx! { Icon { width: 22, height: 22, icon: FaShareNodes } },
            title: "One change, fanned out",
            body: "Identical predicates across consumers are deduplicated, so a \
                   single event is matched once and delivered to everyone whose \
                   query it affects.",
        },
        Feature {
            icon: rsx! { Icon { width: 22, height: 22, icon: FaGears } },
            title: "Set it up and forget it",
            body: "Durable shards, automatic eviction, and multiple SQL dialects, \
                   handled inside the engine. The per-app plumbing you would \
                   otherwise rewrite every time, done once.",
        },
    ];

    rsx! {
        section { class: "sq-section",
            p { class: "sq-eyebrow", "Features" }
            h2 { "What you get" }
            div { class: "sq-features",
                for f in features {
                    div { class: "sq-feature",
                        div { class: "sq-feature-icon", {f.icon} }
                        h3 { "{f.title}" }
                        p { "{f.body}" }
                    }
                }
            }
        }
    }
}

#[component]
pub fn HowItWorks() -> Element {
    rsx! {
        section { class: "sq-section",
            p { class: "sq-eyebrow", "How it works" }
            h2 { "One change in, the right deltas out" }
            p { class: "sq-lead",
                "A change enters as a CDC event. SubQL matches it against every "
                "standing subscription and emits, per consumer, only what their "
                "view needs."
            }

            div { class: "sq-how-block sq-anim-block",
                SystemFlowScene {}
            }

            div { class: "sq-how-block",
                Pipeline {}
            }

            div { class: "sq-how-block",
                h3 { "View-relative deltas" }
                p {
                    "One base change lands differently in each consumer's result "
                    "set. Take a row whose "
                    code { "total" }
                    " drops from 120 to 40:"
                }
                Fanout {}
            }

            div { class: "sq-how-block",
                h3 { "Aggregate deltas" }
                p {
                    "A subscription on "
                    code { "SELECT COUNT(*), AVG(total) ..." }
                    " does not re-run on every change. As a row enters or leaves the "
                    "filtered set, SubQL applies a signed update to the running "
                    "COUNT, SUM, AVG, variance and stddev, so the aggregate is always "
                    "current at the cost of one increment."
                }
            }

            div { class: "sq-how-block",
                h3 { "Deduped fan-out" }
                p {
                    "Many consumers ask the same question. SubQL collapses identical "
                    "predicates so each incoming event is evaluated once, then routed "
                    "to every consumer whose subscription shares that predicate. The "
                    "cost scales with distinct queries, not with consumers."
                }
            }

            p { class: "muted",
                "The live demo below runs the whole path in your browser. Subscribe "
                "to queries on any table, mutate the data, and watch each consumer's "
                "deltas land."
            }
        }
    }
}

#[component]
fn Pipeline() -> Element {
    let stages = [
        "Postgres DDL",
        "SQLite (wasm)",
        "SqliteCdcSource",
        "SubQL dispatch",
        "consumers",
    ];
    rsx! {
        div { class: "sq-pipeline",
            for (i, stage) in stages.iter().enumerate() {
                if i > 0 {
                    span { class: "sq-arrow", "->" }
                }
                span { class: "sq-stage", "{stage}" }
            }
        }
    }
}

#[component]
fn Fanout() -> Element {
    rsx! {
        div { class: "sq-fanout",
            div { class: "sq-fanout-source", "UPDATE orders" br {} "total 120 -> 40" }
            div { class: "sq-fanout-list",
                div { class: "sq-fanout-row",
                    span { class: "sq-verdict sq-verdict--in", "IN" }
                    span { class: "sq-fanout-q", "WHERE total < 50" }
                    span { class: "muted", "row enters: INSERT" }
                }
                div { class: "sq-fanout-row",
                    span { class: "sq-verdict sq-verdict--out", "OUT" }
                    span { class: "sq-fanout-q", "WHERE total > 100" }
                    span { class: "muted", "row leaves: DELETE" }
                }
                div { class: "sq-fanout-row",
                    span { class: "sq-verdict", "UPDATE" }
                    span { class: "sq-fanout-q", "WHERE customer_id = 7" }
                    span { class: "muted", "still matches: UPDATE" }
                }
            }
        }
    }
}
