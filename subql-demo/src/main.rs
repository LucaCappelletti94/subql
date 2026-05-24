// Clippy allows scoped to this demo binary. Dioxus state plumbing uses
// patterns flagged by several `restriction`-level lints (`Arc::clone`
// written as `.clone()`, RefCell borrow guards held across statements,
// by-value props, identical match arms). Style fixes here would
// obscure the framework idioms without changing behaviour. The demo
// is not a published library.
#![allow(
    clippy::clone_on_ref_ptr,
    clippy::significant_drop_in_scrutinee,
    clippy::significant_drop_tightening,
    clippy::needless_pass_by_value,
    clippy::redundant_clone,
    clippy::match_same_arms,
    clippy::missing_const_for_fn,
    clippy::needless_collect,
    clippy::iter_cloned_collect,
    clippy::cast_precision_loss,
    clippy::unreadable_literal
)]

mod app;
mod components;
mod presets;
mod sim;
mod sqlite;
mod state;

fn main() {
    #[cfg(target_arch = "wasm32")]
    console_error_panic_hook::set_once();

    dioxus::launch(app::App);
}
