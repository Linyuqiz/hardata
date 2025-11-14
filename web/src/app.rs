use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;
use crate::api::{Stats, JobDetail, fetch_stats, fetch_jobs, fetch_job_detail};
use crate::components::{StatsCards, JobsList};

#[component]
pub fn App() -> Element {
    let mut stats = use_signal(|| Stats::default());
    let mut jobs = use_signal(|| Vec::<JobDetail>::new());
    let mut connected = use_signal(|| false);

    let load_stats = move || {
        spawn(async move {
            match fetch_stats().await {
                Ok(s) => {
                    stats.set(s);
                    connected.set(true);
                }
                Err(e) => {
                    web_sys::console::error_1(&format!("Failed to load stats: {:?}", e).into());
                    connected.set(false);
                }
            }
        });
    };

    let load_jobs = move || {
        spawn(async move {
            match fetch_jobs(100).await {
                Ok(response) => {
                    let mut job_details = Vec::new();
                    for job_summary in response.jobs {
                        if let Ok(detail) = fetch_job_detail(&job_summary.job_id).await {
                            job_details.push(detail);
                        }
                    }
                    jobs.set(job_details);
                }
                Err(e) => {
                    web_sys::console::error_1(&format!("Failed to load jobs: {:?}", e).into());
                }
            }
        });
    };

    let refresh_data = move |_| {
        load_stats();
        load_jobs();
    };

    use_effect(move || {
        load_stats();
        load_jobs();
    });

    use_effect(move || {
        spawn(async move {
            loop {
                TimeoutFuture::new(5_000).await;
                load_stats();
                load_jobs();
            }
        });
    });

    let conn_class = if connected() { "connected" } else { "disconnected" };
    let conn_text = if connected() { "Connected" } else { "Disconnected" };

    rsx! {
        div { class: "connection-status {conn_class}",
            "{conn_text}"
        }

        div { class: "container",
            header {
                h1 { "HarData Dashboard" }
                p { class: "subtitle", "High-Performance Data Transfer Service" }
            }

            StatsCards { stats: stats }

            div { class: "jobs-container",
                div { class: "jobs-header",
                    h2 { class: "jobs-title", "Job List" }
                    button {
                        class: "refresh-btn",
                        onclick: refresh_data,
                        "Refresh"
                    }
                }
                JobsList { jobs: jobs }
            }
        }
    }
}
