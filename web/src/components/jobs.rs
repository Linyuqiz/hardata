use super::job_actions::{
    cancel_action_label, finalize_action_label, format_cancel_error, format_finalize_error,
    prune_cancelling_job_ids, prune_finalizing_job_ids, should_clear_cancel_error,
    should_clear_finalize_error,
};
use crate::api::{cancel_job, finalize_job, format_bytes, JobDetail};
use dioxus::prelude::*;
use std::collections::HashSet;

#[derive(Clone, PartialEq, Eq)]
struct CancelErrorState {
    job_id: String,
    message: String,
}

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

#[component]
fn ActionErrorBanner(message: String, on_dismiss: EventHandler<()>) -> Element {
    rsx! {
        div { class: "error-banner error-banner-dismissible",
            span { class: "error-banner-message", "{message}" }
            button {
                class: "error-banner-dismiss",
                r#type: "button",
                onclick: move |_| on_dismiss.call(()),
                "Dismiss"
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

fn non_terminal_duration_label(status: &str) -> Option<&'static str> {
    match status {
        "completed" | "failed" | "cancelled" => None,
        "pending" => Some("Queued"),
        "paused" => Some("Paused"),
        _ => None,
    }
}

fn calculate_duration(start: &str, end: &str, status: &str) -> String {
    if let Some(label) = non_terminal_duration_label(status) {
        return label.to_string();
    }

    if start.is_empty() {
        return "-".to_string();
    }

    let parse_timestamp = |s: &str| -> Option<f64> {
        let timestamp = js_sys::Date::parse(s);
        if timestamp.is_nan() {
            None
        } else {
            Some(timestamp)
        }
    };

    let start_ts = match parse_timestamp(start) {
        Some(ts) => ts,
        None => return "-".to_string(),
    };

    let is_terminal = matches!(status, "completed" | "failed" | "cancelled");
    let end_ts = if is_terminal {
        match parse_timestamp(end) {
            Some(ts) => ts,
            None => return "-".to_string(),
        }
    } else {
        js_sys::Date::now()
    };

    let duration_secs = ((end_ts - start_ts).abs() / 1000.0) as u64;
    format_duration(duration_secs)
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

fn round_badge_label(job: &JobDetail) -> Option<String> {
    if job.job_type != "sync" {
        return None;
    }

    let round_id = job.round_id.max(i64::from(job.is_last_round));
    if round_id <= 0 {
        return None;
    }

    if job.is_last_round {
        Some(format!("Final Round {}", round_id))
    } else {
        Some(format!("Round {}", round_id))
    }
}

#[component]
pub fn JobsList(jobs: ReadSignal<Vec<JobDetail>>, on_jobs_changed: EventHandler<()>) -> Element {
    let mut show_confirm = use_signal(|| false);
    let mut pending_cancel_job_id = use_signal(String::new);
    let mut cancelling_job_ids = use_signal(HashSet::<String>::new);
    let mut finalizing_job_ids = use_signal(HashSet::<String>::new);
    let mut cancel_error = use_signal(|| Option::<CancelErrorState>::None);
    let mut finalize_error = use_signal(|| Option::<CancelErrorState>::None);

    use_effect(move || {
        let jobs_snapshot = jobs();
        let current_ids = cancelling_job_ids();
        let next_ids = prune_cancelling_job_ids(&current_ids, &jobs_snapshot);
        if next_ids != current_ids {
            cancelling_job_ids.set(next_ids);
        }

        let current_finalizing_ids = finalizing_job_ids();
        let next_finalizing_ids = prune_finalizing_job_ids(&current_finalizing_ids, &jobs_snapshot);
        if next_finalizing_ids != current_finalizing_ids {
            finalizing_job_ids.set(next_finalizing_ids);
        }

        if let Some(current_error) = cancel_error() {
            if should_clear_cancel_error(&current_error.job_id, &jobs_snapshot) {
                cancel_error.set(None);
            }
        }

        if let Some(current_error) = finalize_error() {
            if should_clear_finalize_error(&current_error.job_id, &jobs_snapshot) {
                finalize_error.set(None);
            }
        }
    });

    let jobs_val = jobs();
    let cancelling_job_ids_val = cancelling_job_ids();
    let finalizing_job_ids_val = finalizing_job_ids();
    let cancel_error_message = cancel_error().map(|error| error.message);
    let finalize_error_message = finalize_error().map(|error| error.message);
    if jobs_val.is_empty() {
        return rsx! {
            if let Some(message) = finalize_error_message.clone() {
                ActionErrorBanner {
                    message,
                    on_dismiss: move |_| finalize_error.set(None),
                }
            }

            if let Some(message) = cancel_error_message.clone() {
                ActionErrorBanner {
                    message,
                    on_dismiss: move |_| cancel_error.set(None),
                }
            }

            div { class: "empty-state",
                div { class: "empty-icon", "📦" }
                h3 { "No Jobs Found" }
                p { "No transfer jobs are available for this page" }
            }
        };
    }

    rsx! {
        ConfirmDialog {
            visible: show_confirm(),
            job_id: pending_cancel_job_id(),
            on_confirm: move |job_id: String| {
                let on_jobs_changed = on_jobs_changed;
                let mut cancelling_job_ids = cancelling_job_ids;
                let mut cancel_error = cancel_error;
                let mut finalizing_job_ids = finalizing_job_ids;
                show_confirm.set(false);
                pending_cancel_job_id.set(String::new());
                cancel_error.set(None);
                finalizing_job_ids.write().remove(&job_id);
                cancelling_job_ids.write().insert(job_id.clone());
                spawn(async move {
                    if let Err(error) = cancel_job(&job_id).await {
                        let message = format_cancel_error(&job_id, &error);
                        cancelling_job_ids.write().remove(&job_id);
                        web_sys::console::error_1(&message.clone().into());
                        cancel_error.set(Some(CancelErrorState {
                            job_id: job_id.clone(),
                            message,
                        }));
                    }
                    on_jobs_changed.call(());
                });
            },
            on_cancel: move |_| {
                show_confirm.set(false);
                pending_cancel_job_id.set(String::new());
            }
        }

        if let Some(message) = finalize_error_message {
            ActionErrorBanner {
                message,
                on_dismiss: move |_| finalize_error.set(None),
            }
        }

        if let Some(message) = cancel_error_message {
            ActionErrorBanner {
                message,
                on_dismiss: move |_| cancel_error.set(None),
            }
        }

        div { class: "jobs-list",
            for job in jobs_val.iter() {
                JobCard {
                    job: job.clone(),
                    is_cancelling: cancelling_job_ids_val.contains(&job.job_id),
                    is_finalizing: finalizing_job_ids_val.contains(&job.job_id),
                    on_cancel_click: move |job_id: String| {
                        pending_cancel_job_id.set(job_id);
                        show_confirm.set(true);
                    },
                    on_finalize_click: move |job_id: String| {
                        let on_jobs_changed = on_jobs_changed;
                        let mut finalizing_job_ids = finalizing_job_ids;
                        let mut finalize_error = finalize_error;
                        finalize_error.set(None);
                        finalizing_job_ids.write().insert(job_id.clone());
                        spawn(async move {
                            if let Err(error) = finalize_job(&job_id).await {
                                let message = format_finalize_error(&job_id, &error);
                                finalizing_job_ids.write().remove(&job_id);
                                web_sys::console::error_1(&message.clone().into());
                                finalize_error.set(Some(CancelErrorState {
                                    job_id: job_id.clone(),
                                    message,
                                }));
                            }
                            on_jobs_changed.call(());
                        });
                    }
                }
            }
        }
    }
}

#[component]
fn JobCard(
    job: JobDetail,
    is_cancelling: bool,
    is_finalizing: bool,
    on_cancel_click: EventHandler<String>,
    on_finalize_click: EventHandler<String>,
) -> Element {
    let status_class = format!("status-{}", job.status.to_lowercase());
    let status_lower = job.status.to_lowercase();
    let can_cancel = job.can_cancel;
    let can_finalize = job.can_finalize;
    let show_cancel = can_cancel || is_cancelling;
    let show_finalize = can_finalize || is_finalizing;
    let cancel_label = cancel_action_label(is_cancelling);
    let finalize_label = finalize_action_label(is_finalizing);
    let job_id_for_cancel = job.job_id.clone();
    let job_id_for_finalize = job.job_id.clone();

    let size_info = if job.total_size > 0 {
        format!(
            "{} / {}",
            format_bytes(job.current_size),
            format_bytes(job.total_size)
        )
    } else {
        "Calculating...".to_string()
    };

    let started_at = format_datetime(&job.created_at);
    let ended_at =
        if status_lower == "completed" || status_lower == "failed" || status_lower == "cancelled" {
            format_datetime(&job.updated_at)
        } else {
            "-".to_string()
        };
    let duration = calculate_duration(&job.created_at, &job.updated_at, &status_lower);
    let error_message = job.error_message.clone().unwrap_or_default();
    let round_badge = round_badge_label(&job);

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
                    if let Some(round_badge) = round_badge {
                        span { class: "round-badge", "{round_badge}" }
                    }
                }
                div { class: "job-header-right",
                    if show_finalize {
                        button {
                            class: "action-btn action-btn-primary",
                            disabled: is_finalizing || is_cancelling,
                            onclick: move |_| {
                                if is_finalizing || is_cancelling {
                                    return;
                                }
                                on_finalize_click.call(job_id_for_finalize.clone());
                            },
                            "{finalize_label}"
                        }
                    }
                    if show_cancel {
                        button {
                            class: "action-btn action-btn-cancel",
                            disabled: is_cancelling || is_finalizing,
                            onclick: move |_| {
                                if is_cancelling || is_finalizing {
                                    return;
                                }
                                on_cancel_click.call(job_id_for_cancel.clone());
                            },
                            "{cancel_label}"
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

                if status_lower == "failed" && !error_message.is_empty() {
                    div { class: "job-error-box",
                        div { class: "job-error-label", "Error" }
                        div { class: "job-error-message", "{error_message}" }
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
#[cfg(test)]
mod tests {
    use super::{format_duration, non_terminal_duration_label, round_badge_label};
    use crate::api::{JobDetail, JobPath};

    fn sample_job() -> JobDetail {
        JobDetail {
            job_id: "job-1".to_string(),
            status: "pending".to_string(),
            error_message: None,
            can_cancel: true,
            can_finalize: true,
            progress: 0,
            current_size: 0,
            total_size: 0,
            source: JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            dest: JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
            region: "local".to_string(),
            job_type: "sync".to_string(),
            round_id: 0,
            is_last_round: false,
            priority: 1,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn non_terminal_duration_label_marks_pending_jobs_as_queued() {
        assert_eq!(non_terminal_duration_label("pending"), Some("Queued"));
    }

    #[test]
    fn non_terminal_duration_label_marks_paused_jobs_as_paused() {
        assert_eq!(non_terminal_duration_label("paused"), Some("Paused"));
    }

    #[test]
    fn non_terminal_duration_label_returns_none_for_syncing_jobs() {
        assert_eq!(non_terminal_duration_label("syncing"), None);
    }

    #[test]
    fn format_duration_formats_terminal_elapsed_time() {
        assert_eq!(format_duration(90), "1m 30s");
    }

    #[test]
    fn round_badge_label_hides_zero_round_sync_jobs() {
        let job = sample_job();

        assert_eq!(round_badge_label(&job), None);
    }

    #[test]
    fn round_badge_label_marks_regular_sync_rounds() {
        let mut job = sample_job();
        job.round_id = 3;

        assert_eq!(round_badge_label(&job).as_deref(), Some("Round 3"));
    }

    #[test]
    fn round_badge_label_marks_final_sync_rounds() {
        let mut job = sample_job();
        job.round_id = 4;
        job.is_last_round = true;

        assert_eq!(round_badge_label(&job).as_deref(), Some("Final Round 4"));
    }

    #[test]
    fn round_badge_label_normalizes_final_round_without_persisted_round_id() {
        let mut job = sample_job();
        job.is_last_round = true;

        assert_eq!(round_badge_label(&job).as_deref(), Some("Final Round 1"));
    }
}
