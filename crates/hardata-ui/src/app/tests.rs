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
