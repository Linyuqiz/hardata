#![allow(non_snake_case)]
// Dioxus RSX uses format-like interpolation for native text and attributes.
#![allow(clippy::useless_format)]

mod api;
mod app;
mod components;

use app::App;

fn main() {
    dioxus::launch(App);
}
