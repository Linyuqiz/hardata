#![allow(non_snake_case)]
// Dioxus RSX interpolation expands `"{value}"` inside the macro. Clippy sees
// the expansion as a regular format call even though this is the framework's
// native text/attribute syntax.
#![allow(clippy::useless_format)]

mod api;
mod app;
mod components;

use app::App;

fn main() {
    dioxus::launch(App);
}
