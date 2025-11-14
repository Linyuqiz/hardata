use dioxus::prelude::*;
use crate::api::Stats;

#[component]
pub fn StatsCards(stats: ReadSignal<Stats>) -> Element {
    let stats_val = stats();
    rsx! {
        div { class: "stats",
            StatCard { label: "Total Jobs", value: stats_val.total }
            StatCard { label: "Pending", value: stats_val.pending }
            StatCard { label: "Running", value: stats_val.running }
            StatCard { label: "Completed", value: stats_val.completed }
            StatCard { label: "Failed", value: stats_val.failed }
        }
    }
}

#[component]
fn StatCard(label: String, value: i64) -> Element {
    rsx! {
        div { class: "stat-card",
            div { class: "stat-label", "{label}" }
            div { class: "stat-value", "{value}" }
        }
    }
}
