#![allow(non_snake_case)]

mod api;
mod app;
mod components;

use app::App;

fn main() {
    dioxus::launch(App);
}
