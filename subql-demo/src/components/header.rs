use dioxus::prelude::*;
use dioxus_free_icons::{
    icons::{fa_brands_icons::FaGithub, fa_solid_icons::FaBook, fa_solid_icons::FaHeart},
    Icon,
};

const REPO_URL: &str = "https://github.com/LucaCappelletti94/subql";
const CRATES_URL: &str = "https://crates.io/crates/subql";
const SPONSOR_URL: &str = "https://github.com/sponsors/LucaCappelletti94";

/// Floating top bar: brand left, monochrome icon links right. Mirrors the
/// tsne.luca.phd header (chromeless icon buttons, dim on hover, sponsor heart
/// hovers red).
#[component]
pub fn Header() -> Element {
    rsx! {
        header { class: "sq-topbar",
            div { class: "sq-topbar-inner",
                a { class: "sq-brand", href: "#top", aria_label: "SubQL home",
                    Logo {}
                    "SubQL"
                }
                nav { class: "sq-topbar-actions",
                    a {
                        class: "sq-iconbtn",
                        href: REPO_URL,
                        target: "_blank",
                        rel: "noopener",
                        title: "Source on GitHub",
                        aria_label: "Source on GitHub",
                        Icon { width: 18, height: 18, icon: FaGithub }
                    }
                    a {
                        class: "sq-iconbtn",
                        href: CRATES_URL,
                        target: "_blank",
                        rel: "noopener",
                        title: "Docs and crate",
                        aria_label: "Docs and crate",
                        Icon { width: 18, height: 18, icon: FaBook }
                    }
                    a {
                        class: "sq-iconbtn sq-heartbtn",
                        href: SPONSOR_URL,
                        target: "_blank",
                        rel: "noopener",
                        title: "Sponsor",
                        aria_label: "Sponsor",
                        Icon { width: 18, height: 18, icon: FaHeart }
                    }
                }
            }
        }
    }
}

/// The SubQL mark, inlined so `fill="currentColor"` tracks the text color and
/// flips with dark mode without a `filter: invert` hack.
#[component]
fn Logo() -> Element {
    rsx! {
        svg {
            class: "sq-logo",
            view_box: "0 0 2048 2048",
            fill: "currentColor",
            "aria-hidden": "true",
            path { d: "M916.0,216.0 L920.0,690.0 L865.0,737.0 L743.0,326.0 Z M705.0,297.0 L855.0,749.0 L831.0,814.0 L576.0,442.0 Z M829.0,899.0 L462.0,616.0 L530.0,440.0 L813.0,820.0 Z M434.0,632.0 L823.0,918.0 L870.0,990.0 L400.0,801.0 Z M1638.0,1233.0 L1625.0,1430.0 L1210.0,1103.0 L1154.0,1026.0 Z M1201.0,1121.0 L1576.0,1433.0 L1482.0,1627.0 L1215.0,1211.0 Z M1169.0,1287.0 L1209.0,1231.0 L1437.0,1612.0 L1279.0,1774.0 Z M1156.0,1300.0 L1239.0,1754.0 L1044.0,1832.0 L1101.0,1331.0 Z" }
        }
    }
}
