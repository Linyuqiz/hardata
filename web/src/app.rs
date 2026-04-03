use crate::api::{
    fetch_jobs, fetch_stats, load_api_token, store_api_token, try_load_api_token, ApiError,
    JobDetail, Stats,
};
use crate::components::{JobsList, StatsCards};
use dioxus::dioxus_core::{use_drop, Task};
use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;
use std::{cell::Cell, rc::Rc};

const JOBS_PAGE_SIZE: usize = 100;

#[derive(Default)]
struct RefreshCoordinator {
    latest_request_id: Cell<u64>,
}

impl RefreshCoordinator {
    fn begin_request(&self) -> u64 {
        let next_request_id = self.latest_request_id.get().saturating_add(1);
        self.latest_request_id.set(next_request_id);
        next_request_id
    }

    fn is_current(&self, request_id: u64) -> bool {
        self.latest_request_id.get() == request_id
    }
}

fn format_load_error(resource: &str, error: &ApiError) -> String {
    if error.is_unauthorized() {
        format!(
            "Failed to load {}: authentication failed, update API token",
            resource
        )
    } else {
        format!("Failed to load {}: {}", resource, error)
    }
}

fn should_pause_background_polling(error: &ApiError) -> bool {
    error.is_unauthorized()
}

fn should_clear_cached_view(error: &ApiError) -> bool {
    should_pause_background_polling(error)
}

fn refresh_keeps_connection(stats_loaded: bool, jobs_loaded: bool) -> bool {
    stats_loaded || jobs_loaded
}

fn format_token_storage_error(action: &str, error: &str) -> String {
    format!("Failed to {} API token: {}", action, error)
}

fn token_controls_state(persisted_token: &str) -> (&'static str, bool) {
    if persisted_token.trim().is_empty() {
        ("Save Token", false)
    } else {
        ("Update Token", true)
    }
}

fn resolve_loaded_token(load_result: Result<String, String>) -> (String, Option<String>) {
    match load_result {
        Ok(token) => (token, None),
        Err(error) => (
            String::new(),
            Some(format_token_storage_error("load", &error)),
        ),
    }
}

fn can_change_jobs_page(has_target_page: bool, refresh_in_flight: bool) -> bool {
    has_target_page && !refresh_in_flight
}

fn refresh_button_label(refresh_in_flight: bool) -> &'static str {
    if refresh_in_flight {
        "Refreshing..."
    } else {
        "Refresh"
    }
}

fn should_run_poll_tick(auth_poll_paused: bool, refresh_in_flight: bool) -> bool {
    !auth_poll_paused && !refresh_in_flight
}

fn can_manage_token(refresh_in_flight: bool) -> bool {
    !refresh_in_flight
}

fn can_submit_token_draft(token_draft: &str, refresh_in_flight: bool) -> bool {
    !token_draft.trim().is_empty() && can_manage_token(refresh_in_flight)
}

fn page_error_message(
    refresh_error: Option<String>,
    token_storage_error: Option<String>,
) -> Option<String> {
    summarize_refresh_errors(
        [token_storage_error, refresh_error]
            .into_iter()
            .flatten()
            .collect(),
    )
}

fn summarize_refresh_errors(errors: Vec<String>) -> Option<String> {
    let mut unique_errors = Vec::new();

    for error in errors {
        if !unique_errors.iter().any(|existing| existing == &error) {
            unique_errors.push(error);
        }
    }

    match unique_errors.len() {
        0 => None,
        1 => unique_errors.into_iter().next(),
        _ => Some(unique_errors.join(" | ")),
    }
}

fn last_jobs_page(total: usize, limit: usize) -> usize {
    if total == 0 {
        0
    } else {
        total.saturating_sub(1) / limit.max(1)
    }
}

fn resolve_jobs_page(requested_page: usize, total: usize, limit: usize) -> usize {
    requested_page.min(last_jobs_page(total, limit))
}

fn total_jobs_pages(total: usize, limit: usize) -> usize {
    last_jobs_page(total, limit).saturating_add(1)
}

fn visible_job_range(
    total: usize,
    page: usize,
    limit: usize,
    jobs_on_page: usize,
) -> Option<(usize, usize)> {
    if total == 0 || jobs_on_page == 0 {
        return None;
    }

    let start = page
        .saturating_mul(limit.max(1))
        .saturating_add(1)
        .min(total);
    let end = start
        .saturating_add(jobs_on_page.saturating_sub(1))
        .min(total);
    Some((start, end))
}

fn jobs_summary_label(total: usize, page: usize, limit: usize, jobs_on_page: usize) -> String {
    visible_job_range(total, page, limit, jobs_on_page)
        .map(|(start, end)| format!("Showing {}-{} of {}", start, end, total))
        .unwrap_or_else(|| {
            if total == 0 {
                "No jobs".to_string()
            } else {
                format!("Showing 0 of {}", total)
            }
        })
}

#[component]
pub fn App() -> Element {
    let mut stats = use_signal(Stats::default);
    let mut jobs = use_signal(Vec::<JobDetail>::new);
    let mut jobs_total = use_signal(|| 0usize);
    let mut jobs_page = use_signal(|| 0usize);
    let requested_jobs_page = use_signal(|| 0usize);
    let (initial_token, initial_token_error) = resolve_loaded_token(try_load_api_token());
    let mut connected = use_signal(|| false);
    let mut refresh_error = use_signal(|| Option::<String>::None);
    let mut token_storage_error = use_signal(|| initial_token_error.clone());
    let refresh_in_flight = use_signal(|| false);
    let mut api_token = use_signal(|| initial_token.clone());
    let mut persisted_api_token = use_signal(|| initial_token.clone());
    let mut auth_poll_paused = use_signal(|| false);
    let mut token_panel_open = use_signal(|| false);
    let poll_started = use_hook(Cell::default);
    let mut poll_task = use_signal(|| Option::<Task>::None);
    let refresh_coordinator = use_hook(|| Rc::new(RefreshCoordinator::default()));

    let refresh_data = Rc::new(move |requested_page: usize| {
        let mut requested_jobs_page = requested_jobs_page;
        let mut refresh_in_flight = refresh_in_flight;
        // 记录当前目标页，避免轮询在翻页请求完成前回退到旧页。
        requested_jobs_page.set(requested_page);
        refresh_in_flight.set(true);
        let request_id = refresh_coordinator.begin_request();
        let refresh_coordinator = refresh_coordinator.clone();
        spawn(async move {
            let mut requested_jobs_page = requested_jobs_page;
            let mut refresh_in_flight = refresh_in_flight;
            let mut errors = Vec::new();
            let mut stats_loaded = false;
            let mut jobs_loaded = false;

            match fetch_stats().await {
                Ok(fetched_stats) => {
                    if !refresh_coordinator.is_current(request_id) {
                        return;
                    }
                    stats.set(fetched_stats);
                    stats_loaded = true;
                }
                Err(error) => {
                    if !refresh_coordinator.is_current(request_id) {
                        return;
                    }

                    let message = format_load_error("stats", &error);
                    web_sys::console::error_1(&message.clone().into());
                    if should_clear_cached_view(&error) {
                        let cleared_page =
                            resolve_jobs_page(requested_jobs_page(), 0, JOBS_PAGE_SIZE);
                        connected.set(false);
                        stats.set(Stats::default());
                        jobs.set(Vec::new());
                        jobs_total.set(0);
                        jobs_page.set(cleared_page);
                        requested_jobs_page.set(cleared_page);
                        auth_poll_paused.set(true);
                        refresh_in_flight.set(false);
                        refresh_error.set(Some(message));
                        return;
                    }
                    errors.push(message);
                }
            }

            if auth_poll_paused() {
                connected.set(false);
                refresh_in_flight.set(false);
                refresh_error.set(summarize_refresh_errors(errors));
                return;
            }

            let jobs_response = match fetch_jobs(requested_page, JOBS_PAGE_SIZE).await {
                Ok(response) => response,
                Err(error) => {
                    if !refresh_coordinator.is_current(request_id) {
                        return;
                    }

                    let message = format_load_error("jobs", &error);
                    web_sys::console::error_1(&message.clone().into());
                    if should_clear_cached_view(&error) {
                        let cleared_page =
                            resolve_jobs_page(requested_jobs_page(), 0, JOBS_PAGE_SIZE);
                        connected.set(false);
                        stats.set(Stats::default());
                        jobs.set(Vec::new());
                        jobs_total.set(0);
                        jobs_page.set(cleared_page);
                        requested_jobs_page.set(cleared_page);
                        auth_poll_paused.set(true);
                        refresh_in_flight.set(false);
                        refresh_error.set(Some(message));
                        return;
                    }
                    errors.push(message);
                    connected.set(refresh_keeps_connection(stats_loaded, jobs_loaded));
                    refresh_in_flight.set(false);
                    refresh_error.set(summarize_refresh_errors(errors));
                    return;
                }
            };

            if !refresh_coordinator.is_current(request_id) {
                return;
            }

            // 服务端已经返回实际页码，这里只做防御性夹取，避免本地状态越界。
            let resolved_page =
                resolve_jobs_page(jobs_response.page, jobs_response.total, jobs_response.limit);
            jobs_total.set(jobs_response.total);
            jobs_page.set(resolved_page);
            requested_jobs_page.set(resolved_page);
            jobs.set(jobs_response.jobs);
            jobs_loaded = true;
            auth_poll_paused.set(false);
            connected.set(refresh_keeps_connection(stats_loaded, jobs_loaded));
            refresh_in_flight.set(false);
            refresh_error.set(summarize_refresh_errors(errors));
        });
    });

    let refresh_data_for_effect = refresh_data.clone();
    use_effect(move || {
        if poll_started.get() {
            return;
        }

        poll_started.set(true);
        refresh_data_for_effect(jobs_page());

        let refresh_data = refresh_data_for_effect.clone();
        let task = spawn(async move {
            loop {
                TimeoutFuture::new(5_000).await;
                if !should_run_poll_tick(auth_poll_paused(), refresh_in_flight()) {
                    continue;
                }
                refresh_data(requested_jobs_page());
            }
        });
        poll_task.set(Some(task));
    });

    use_drop(move || {
        if let Some(task) = poll_task() {
            task.cancel();
        }
    });

    let conn_class = if connected() {
        "connected"
    } else {
        "disconnected"
    };
    let conn_text = if connected() {
        "Connected"
    } else {
        "Disconnected"
    };
    let (token_submit_label, show_clear_token) = token_controls_state(&persisted_api_token());
    let refresh_data_for_save = refresh_data.clone();
    let refresh_data_for_clear = refresh_data.clone();
    let refresh_data_for_button = refresh_data.clone();
    let refresh_data_for_job_change = refresh_data.clone();
    let refresh_data_for_previous_page = refresh_data.clone();
    let refresh_data_for_next_page = refresh_data.clone();
    let jobs_total_value = jobs_total();
    let jobs_page_value = jobs_page();
    let jobs_count_on_page = jobs().len();
    let page_count = total_jobs_pages(jobs_total_value, JOBS_PAGE_SIZE);
    let refresh_in_flight_value = refresh_in_flight();
    let can_manage_token_value = can_manage_token(refresh_in_flight_value);
    let can_submit_token_value = can_submit_token_draft(&api_token(), refresh_in_flight_value);
    let has_previous_page = jobs_page_value > 0;
    let has_next_page = jobs_page_value.saturating_add(1) < page_count;
    let can_previous_page = can_change_jobs_page(has_previous_page, refresh_in_flight_value);
    let can_next_page = can_change_jobs_page(has_next_page, refresh_in_flight_value);
    let refresh_label = refresh_button_label(refresh_in_flight_value);
    let jobs_summary = jobs_summary_label(
        jobs_total_value,
        jobs_page_value,
        JOBS_PAGE_SIZE,
        jobs_count_on_page,
    );
    let page_label = format!(
        "Page {} / {}",
        jobs_page_value.saturating_add(1),
        page_count
    );
    let has_token = !persisted_api_token().trim().is_empty();
    let token_panel_open_value = token_panel_open();
    let token_toggle_class = if has_token {
        "token-toggle-btn authenticated"
    } else {
        "token-toggle-btn"
    };

    rsx! {
        div { class: "connection-status {conn_class}",
            "{conn_text}"
        }

        div { class: "container",
            if let Some(message) = page_error_message(refresh_error(), token_storage_error()) {
                div { class: "error-banner", "{message}" }
            }

            header {
                h1 { "HarData Dashboard" }
                div { class: "header-subtitle-row",
                    p { class: "subtitle", "High-Performance Data Transfer Service" }
                    button {
                        class: "{token_toggle_class}",
                        title: if has_token { "Token configured" } else { "Set API token" },
                        onclick: move |_| token_panel_open.set(!token_panel_open_value),
                        if has_token { "\u{1F512}" } else { "\u{1F513}" }
                    }
                }
                if token_panel_open_value {
                    div { class: "token-panel",
                        input {
                            class: "token-panel-input",
                            r#type: "password",
                            placeholder: "Enter API token",
                            value: "{api_token()}",
                            oninput: move |event| api_token.set(event.value())
                        }
                        div { class: "token-panel-actions",
                            button {
                                class: "token-panel-btn token-panel-btn-primary",
                                disabled: !can_submit_token_value,
                                onclick: move |_| {
                                    if !can_submit_token_value {
                                        return;
                                    }
                                    let normalized_token = api_token().trim().to_string();
                                    if let Err(error) = store_api_token(&normalized_token) {
                                        let message = format_token_storage_error("store", &error);
                                        let persisted_token = load_api_token();
                                        web_sys::console::error_1(&message.clone().into());
                                        api_token.set(persisted_token.clone());
                                        persisted_api_token.set(persisted_token);
                                        token_storage_error.set(Some(message));
                                        return;
                                    }
                                    api_token.set(normalized_token.clone());
                                    persisted_api_token.set(normalized_token);
                                    token_storage_error.set(None);
                                    auth_poll_paused.set(false);
                                    token_panel_open.set(false);
                                    refresh_data_for_save(requested_jobs_page());
                                },
                                "{token_submit_label}"
                            }
                            if show_clear_token {
                                button {
                                    class: "token-panel-btn token-panel-btn-danger",
                                    disabled: !can_manage_token_value,
                                    onclick: move |_| {
                                        if !can_manage_token_value {
                                            return;
                                        }
                                        if let Err(error) = store_api_token("") {
                                            let message = format_token_storage_error("clear", &error);
                                            let persisted_token = load_api_token();
                                            web_sys::console::error_1(&message.clone().into());
                                            api_token.set(persisted_token.clone());
                                            persisted_api_token.set(persisted_token);
                                            token_storage_error.set(Some(message));
                                            return;
                                        }
                                        api_token.set(String::new());
                                        persisted_api_token.set(String::new());
                                        token_storage_error.set(None);
                                        auth_poll_paused.set(false);
                                        token_panel_open.set(false);
                                        refresh_data_for_clear(requested_jobs_page());
                                    },
                                    "Clear"
                                }
                            }
                        }
                    }
                }
            }

            StatsCards { stats: stats }

            div { class: "jobs-container",
                div { class: "jobs-header",
                    div { class: "jobs-header-main",
                        h2 { class: "jobs-title", "Job List" }
                        p { class: "jobs-summary", "{jobs_summary}" }
                    }
                    div { class: "jobs-header-actions",
                        div { class: "jobs-pagination",
                            button {
                                class: "pagination-btn",
                                disabled: !can_previous_page,
                                onclick: move |_| {
                                    if !can_previous_page {
                                        return;
                                    }
                                    auth_poll_paused.set(false);
                                    refresh_data_for_previous_page(jobs_page_value.saturating_sub(1));
                                },
                                "Previous"
                            }
                            span { class: "jobs-page-indicator", "{page_label}" }
                            button {
                                class: "pagination-btn",
                                disabled: !can_next_page,
                                onclick: move |_| {
                                    if !can_next_page {
                                        return;
                                    }
                                    auth_poll_paused.set(false);
                                    refresh_data_for_next_page(jobs_page_value.saturating_add(1));
                                },
                                "Next"
                            }
                        }
                        button {
                            class: "refresh-btn",
                            disabled: refresh_in_flight_value,
                            onclick: move |_| {
                                if refresh_in_flight_value {
                                    return;
                                }
                                auth_poll_paused.set(false);
                                refresh_data_for_button(requested_jobs_page());
                            },
                            "{refresh_label}"
                        }
                    }
                }
                JobsList {
                    jobs: jobs,
                    on_jobs_changed: move |_| refresh_data_for_job_change(requested_jobs_page()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        can_change_jobs_page, can_manage_token, can_submit_token_draft, format_load_error,
        format_token_storage_error, jobs_summary_label, last_jobs_page, page_error_message,
        refresh_button_label, refresh_keeps_connection, resolve_jobs_page, resolve_loaded_token,
        should_clear_cached_view, should_pause_background_polling, should_run_poll_tick,
        summarize_refresh_errors, token_controls_state, visible_job_range, RefreshCoordinator,
        JOBS_PAGE_SIZE,
    };
    use crate::api::ApiError;

    #[test]
    fn refresh_coordinator_only_accepts_latest_request() {
        let coordinator = RefreshCoordinator::default();
        let first = coordinator.begin_request();
        let second = coordinator.begin_request();

        assert!(!coordinator.is_current(first));
        assert!(coordinator.is_current(second));
    }

    #[test]
    fn refresh_coordinator_saturates_request_counter() {
        let coordinator = RefreshCoordinator::default();
        coordinator.latest_request_id.set(u64::MAX);

        let request_id = coordinator.begin_request();

        assert_eq!(request_id, u64::MAX);
        assert!(coordinator.is_current(u64::MAX));
    }

    #[test]
    fn format_load_error_uses_auth_specific_guidance() {
        let message = format_load_error("stats", &ApiError::http(401, "HTTP 401"));

        assert!(message.contains("authentication failed"));
        assert!(message.contains("update API token"));
    }

    #[test]
    fn format_token_storage_error_describes_failing_action() {
        assert_eq!(
            format_token_storage_error("store", "Browser storage is unavailable"),
            "Failed to store API token: Browser storage is unavailable"
        );
        assert_eq!(
            format_token_storage_error("clear", "Browser storage is unavailable"),
            "Failed to clear API token: Browser storage is unavailable"
        );
    }

    #[test]
    fn page_error_message_keeps_token_storage_error_visible_across_refresh_success() {
        assert_eq!(
            page_error_message(
                None,
                Some("Failed to store API token: Browser storage is unavailable".to_string()),
            ),
            Some("Failed to store API token: Browser storage is unavailable".to_string())
        );
    }

    #[test]
    fn page_error_message_combines_token_and_refresh_errors_in_order() {
        assert_eq!(
            page_error_message(
                Some("Failed to load jobs: timeout".to_string()),
                Some("Failed to store API token: Browser storage is unavailable".to_string()),
            ),
            Some(
                "Failed to store API token: Browser storage is unavailable | Failed to load jobs: timeout"
                    .to_string(),
            )
        );
    }

    #[test]
    fn token_controls_state_follows_persisted_token_presence() {
        assert_eq!(token_controls_state("saved-token"), ("Update Token", true));
        assert_eq!(token_controls_state("   "), ("Save Token", false));
    }

    #[test]
    fn resolve_loaded_token_keeps_loaded_value_without_error() {
        assert_eq!(
            resolve_loaded_token(Ok("saved-token".to_string())),
            ("saved-token".to_string(), None)
        );
    }

    #[test]
    fn resolve_loaded_token_surfaces_storage_failure() {
        assert_eq!(
            resolve_loaded_token(Err("Browser storage is unavailable".to_string())),
            (
                String::new(),
                Some("Failed to load API token: Browser storage is unavailable".to_string())
            )
        );
    }

    #[test]
    fn can_manage_token_blocks_storage_updates_while_refreshing() {
        assert!(can_manage_token(false));
        assert!(!can_manage_token(true));
    }

    #[test]
    fn should_run_poll_tick_requires_idle_authorized_state() {
        assert!(should_run_poll_tick(false, false));
        assert!(!should_run_poll_tick(true, false));
        assert!(!should_run_poll_tick(false, true));
    }

    #[test]
    fn can_submit_token_draft_requires_non_empty_input_and_idle_refresh() {
        assert!(can_submit_token_draft("new-token", false));
        assert!(!can_submit_token_draft("   ", false));
        assert!(!can_submit_token_draft("new-token", true));
    }

    #[test]
    fn can_change_jobs_page_blocks_navigation_while_refreshing() {
        assert!(can_change_jobs_page(true, false));
        assert!(!can_change_jobs_page(true, true));
        assert!(!can_change_jobs_page(false, false));
    }

    #[test]
    fn refresh_button_label_marks_active_refresh() {
        assert_eq!(refresh_button_label(false), "Refresh");
        assert_eq!(refresh_button_label(true), "Refreshing...");
    }

    #[test]
    fn unauthorized_error_pauses_background_polling() {
        assert!(should_pause_background_polling(&ApiError::http(
            401, "HTTP 401"
        )));
    }

    #[test]
    fn non_unauthorized_error_keeps_background_polling() {
        assert!(!should_pause_background_polling(&ApiError::http(
            500, "HTTP 500"
        )));
    }

    #[test]
    fn unauthorized_error_clears_cached_view() {
        assert!(should_clear_cached_view(&ApiError::http(401, "HTTP 401")));
        assert!(!should_clear_cached_view(&ApiError::http(500, "HTTP 500")));
    }

    #[test]
    fn connection_stays_up_when_any_refresh_branch_succeeds() {
        assert!(refresh_keeps_connection(true, false));
        assert!(refresh_keeps_connection(false, true));
        assert!(!refresh_keeps_connection(false, false));
    }

    #[test]
    fn summarize_refresh_errors_returns_none_when_empty() {
        assert_eq!(summarize_refresh_errors(Vec::new()), None);
    }

    #[test]
    fn summarize_refresh_errors_keeps_single_error_unchanged() {
        assert_eq!(
            summarize_refresh_errors(vec!["Failed to load stats: timeout".to_string()]),
            Some("Failed to load stats: timeout".to_string())
        );
    }

    #[test]
    fn summarize_refresh_errors_joins_distinct_errors_in_order() {
        assert_eq!(
            summarize_refresh_errors(vec![
                "Failed to load stats: timeout".to_string(),
                "Failed to load jobs: connection reset".to_string(),
            ]),
            Some(
                "Failed to load stats: timeout | Failed to load jobs: connection reset".to_string()
            )
        );
    }

    #[test]
    fn summarize_refresh_errors_deduplicates_repeated_messages() {
        assert_eq!(
            summarize_refresh_errors(vec![
                "Failed to load stats: timeout".to_string(),
                "Failed to load stats: timeout".to_string(),
            ]),
            Some("Failed to load stats: timeout".to_string())
        );
    }

    #[test]
    fn last_jobs_page_returns_zero_based_tail_page() {
        assert_eq!(last_jobs_page(0, JOBS_PAGE_SIZE), 0);
        assert_eq!(last_jobs_page(250, JOBS_PAGE_SIZE), 2);
    }

    #[test]
    fn resolve_jobs_page_clamps_requested_page_to_available_tail() {
        assert_eq!(resolve_jobs_page(0, 0, JOBS_PAGE_SIZE), 0);
        assert_eq!(resolve_jobs_page(4, 0, JOBS_PAGE_SIZE), 0);
        assert_eq!(resolve_jobs_page(1, JOBS_PAGE_SIZE + 1, JOBS_PAGE_SIZE), 1);
        assert_eq!(resolve_jobs_page(4, JOBS_PAGE_SIZE + 1, JOBS_PAGE_SIZE), 1);
    }

    #[test]
    fn visible_job_range_reports_current_page_slice() {
        assert_eq!(
            visible_job_range(250, 1, JOBS_PAGE_SIZE, JOBS_PAGE_SIZE),
            Some((101, 200))
        );
    }

    #[test]
    fn jobs_summary_label_handles_empty_results() {
        assert_eq!(jobs_summary_label(0, 0, JOBS_PAGE_SIZE, 0), "No jobs");
    }
}
