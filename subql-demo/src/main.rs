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
