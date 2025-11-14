use dioxus::prelude::*;
use crate::api::{JobDetail, format_bytes, cancel_job};

#[component]
pub fn ConfirmDialog(
    visible: bool,
    job_id: String,
    on_confirm: EventHandler<String>,
    on_cancel: EventHandler<()>,
) -> Element {
    if !visible {
        return rsx! {};
    }

    rsx! {
        div { class: "modal-overlay",
            onclick: move |_| on_cancel.call(()),
            div { class: "modal-content",
                onclick: move |e| e.stop_propagation(),
                h3 { class: "modal-title", "Confirm Cancel" }
                p { class: "modal-message",
                    "Are you sure you want to cancel this job?"
                }
                p { class: "modal-job-id", "{job_id}" }
                div { class: "modal-actions",
                    button {
                        class: "modal-btn modal-btn-cancel",
                        onclick: move |_| on_cancel.call(()),
                        "No"
                    }
                    button {
                        class: "modal-btn modal-btn-confirm",
                        onclick: {
                            let job_id = job_id.clone();
                            move |_| on_confirm.call(job_id.clone())
                        },
                        "Yes, Cancel"
                    }
                }
            }
        }
    }
}

fn format_datetime(datetime: &str) -> String {
    if datetime.is_empty() {
        return "-".to_string();
    }
    if let Some(t_pos) = datetime.find('T') {
        let date = &datetime[..t_pos];
        let time = datetime[t_pos + 1..].split('.').next().unwrap_or("");
        format!("{} {}", date, time)
    } else {
        datetime.to_string()
    }
}

fn calculate_duration(start: &str, end: &str, status: &str) -> String {
    if start.is_empty() {
        return "-".to_string();
    }

    let parse_timestamp = |s: &str| -> Option<i64> {
        let s = s.replace("Z", "").replace("z", "");
        let parts: Vec<&str> = s.split('T').collect();
        if parts.len() != 2 {
            return None;
        }

        let date_parts: Vec<i32> = parts[0].split('-').filter_map(|p| p.parse().ok()).collect();
        let time_str = parts[1].split('.').next()?;
        let time_parts: Vec<i32> = time_str.split(':').filter_map(|p| p.parse().ok()).collect();

        if date_parts.len() != 3 || time_parts.len() != 3 {
            return None;
        }

        let days = date_parts[0] as i64 * 365 + date_parts[1] as i64 * 30 + date_parts[2] as i64;
        let secs = time_parts[0] as i64 * 3600 + time_parts[1] as i64 * 60 + time_parts[2] as i64;
        Some(days * 86400 + secs)
    };

    let start_ts = match parse_timestamp(start) {
        Some(ts) => ts,
        None => return "-".to_string(),
    };

    let end_ts = if status == "completed" || status == "failed" {
        match parse_timestamp(end) {
            Some(ts) => ts,
            None => return "-".to_string(),
        }
    } else {
        return "In Progress".to_string();
    };

    let duration_secs = (end_ts - start_ts).abs();
    format_duration(duration_secs as u64)
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        let mins = secs / 60;
        let s = secs % 60;
        if s > 0 {
            format!("{}m {}s", mins, s)
        } else {
            format!("{}m", mins)
        }
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        if mins > 0 {
            format!("{}h {}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}

#[component]
pub fn JobsList(jobs: ReadSignal<Vec<JobDetail>>) -> Element {
    let mut show_confirm = use_signal(|| false);
    let mut pending_cancel_job_id = use_signal(|| String::new());

    let jobs_val = jobs();
    if jobs_val.is_empty() {
        return rsx! {
            div { class: "empty-state",
                div { class: "empty-icon", "📦" }
                h3 { "No Active Jobs" }
                p { "No transfer jobs currently running" }
            }
        };
    }

    rsx! {
        ConfirmDialog {
            visible: show_confirm(),
            job_id: pending_cancel_job_id(),
            on_confirm: move |job_id: String| {
                show_confirm.set(false);
                spawn(async move {
                    if let Err(e) = cancel_job(&job_id).await {
                        web_sys::console::error_1(&format!("Failed to cancel job: {:?}", e).into());
                    }
                });
            },
            on_cancel: move |_| {
                show_confirm.set(false);
                pending_cancel_job_id.set(String::new());
            }
        }

        div { class: "jobs-list",
            for job in jobs_val.iter() {
                JobCard {
                    job: job.clone(),
                    on_cancel_click: move |job_id: String| {
                        pending_cancel_job_id.set(job_id);
                        show_confirm.set(true);
                    }
                }
            }
        }
    }
}

#[component]
fn JobCard(job: JobDetail, on_cancel_click: EventHandler<String>) -> Element {
    let status_class = format!("status-{}", job.status.to_lowercase());
    let status_lower = job.status.to_lowercase();
    let can_cancel = status_lower == "pending" || status_lower == "syncing";
    let job_id_for_cancel = job.job_id.clone();

    let size_info = if job.total_size > 0 {
        format!("{} / {}",
            format_bytes(job.current_size),
            format_bytes(job.total_size)
        )
    } else {
        "Calculating...".to_string()
    };

    let started_at = format_datetime(&job.created_at);
    let ended_at = if status_lower == "completed" || status_lower == "failed" {
        format_datetime(&job.updated_at)
    } else {
        "-".to_string()
    };
    let duration = calculate_duration(&job.created_at, &job.updated_at, &status_lower);

    rsx! {
        div { class: "job-card",
            div { class: "job-header",
                div { class: "job-header-left",
                    span { class: "job-id", "{job.job_id}" }
                }
                div { class: "job-header-center",
                    span { class: "region-badge", "{job.region}" }
                    span { class: "type-badge type-{job.job_type}", "{job.job_type}" }
                    span { class: "status-badge {status_class}", "{job.status}" }
                    if job.job_type == "sync" {
                        span { class: "round-badge", "Round {job.round_id}" }
                    }
                }
                div { class: "job-header-right",
                    if can_cancel {
                        button {
                            class: "action-btn action-btn-cancel",
                            onclick: move |_| {
                                on_cancel_click.call(job_id_for_cancel.clone());
                            },
                            "Cancel"
                        }
                    }
                }
            }

            div { class: "progress-bar",
                div {
                    class: "progress-fill",
                    style: "width: {job.progress}%"
                }
            }

            div { class: "job-main-info",
                div { class: "info-row",
                    div { class: "info-item",
                        span { class: "info-label", "Progress" }
                        span { class: "info-value", "{job.progress}%" }
                    }
                    div { class: "info-item",
                        span { class: "info-label", "Size" }
                        span { class: "info-value", "{size_info}" }
                    }
                    div { class: "info-item",
                        span { class: "info-label", "Duration" }
                        span { class: "info-value duration", "{duration}" }
                    }
                }

                div { class: "info-row time-row",
                    div { class: "info-item",
                        span { class: "info-label", "Started" }
                        span { class: "info-value time", "{started_at}" }
                    }
                    div { class: "info-item",
                        span { class: "info-label", "Ended" }
                        span { class: "info-value time", "{ended_at}" }
                    }
                }
            }

            div { class: "job-paths",
                div { class: "path-item",
                    span { class: "path-label", "Source" }
                    span { class: "path-value", title: "{job.source.path}", "{job.source.path}" }
                }
                div { class: "path-item",
                    span { class: "path-label", "Dest" }
                    span { class: "path-value", title: "{job.dest.path}", "{job.dest.path}" }
                }
            }
        }
    }
}
