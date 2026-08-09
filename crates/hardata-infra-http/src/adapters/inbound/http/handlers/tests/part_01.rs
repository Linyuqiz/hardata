    #[test]
    fn pagination_bounds_handles_regular_page() {
        assert_eq!(pagination_bounds(10, 1, 3), (3, 6));
    }

    #[test]
    fn pagination_bounds_saturates_on_overflowing_page_product() {
        assert_eq!(pagination_bounds(10, usize::MAX, 2), (10, 10));
    }

    #[test]
    fn resolve_list_jobs_page_clamps_out_of_range_request_to_tail() {
        assert_eq!(resolve_list_jobs_page(0, 9, 100), 0);
        assert_eq!(resolve_list_jobs_page(201, 9, 100), 2);
        assert_eq!(resolve_list_jobs_page(201, 1, 100), 1);
    }

    #[test]
    fn validate_destination_scope_rejects_absolute_path_outside_data_dir() {
        let err = validate_destination_scope("/tmp/outside", "/srv/sync", false).unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
        assert!(err.1.contains("outside sync.data_dir"));
    }

    #[test]
    fn validate_destination_scope_allows_relative_path_inside_data_dir() {
        validate_destination_scope("jobs/output.bin", "/srv/sync", false).unwrap();
    }

    #[test]
    fn validate_destination_scope_allows_external_path_when_enabled() {
        validate_destination_scope("/tmp/outside", "/srv/sync", true).unwrap();
    }

    #[test]
    fn summarize_statuses_counts_cancelled_separately_from_failed() {
        let stats = summarize_statuses([
            JobStatus::Pending,
            JobStatus::Failed,
            JobStatus::Cancelled,
            JobStatus::Completed,
            JobStatus::Cancelled,
        ]);

        assert_eq!(stats.total, 5);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.cancelled, 2);
    }

    #[test]
    fn stats_from_counts_preserves_sparse_status_map() {
        let counts = HashMap::from([(JobStatus::Paused, 2), (JobStatus::Completed, 1)]);
        let stats = stats_from_counts(&counts);

        assert_eq!(stats.total, 3);
        assert_eq!(stats.paused, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.pending, 0);
    }

    #[test]
    fn extract_bearer_token_accepts_case_insensitive_scheme() {
        assert_eq!(extract_bearer_token("Bearer secret"), Some("secret"));
        assert_eq!(extract_bearer_token("bearer secret"), Some("secret"));
        assert_eq!(extract_bearer_token("BEARER secret"), Some("secret"));
    }

    #[test]
    fn extract_bearer_token_rejects_malformed_values() {
        assert_eq!(extract_bearer_token("Token secret"), None);
        assert_eq!(extract_bearer_token("Bearer"), None);
        assert_eq!(extract_bearer_token("Bearer secret extra"), None);
    }

    #[test]
    fn token_matches_requires_exact_token() {
        assert!(super::token_matches("secret", "secret"));
        assert!(!super::token_matches("Secret", "secret"));
        assert!(!super::token_matches("secret-extra", "secret"));
    }

    #[test]
    fn extract_create_job_idempotency_key_rejects_mismatched_header_and_body() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("header-key"),
        );

        let err = extract_create_job_idempotency_key(&headers, Some("body-key")).unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("must match request_id"));
    }

    #[test]
    fn create_job_request_fingerprint_ignores_request_id_field() {
        let request = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec!["ignored".to_string()],
            include_regex: vec!["included".to_string()],
            request_id: Some("request-a".to_string()),
        };
        let mut same_request = CreateJobRequest {
            request_id: Some("request-b".to_string()),
            ..request
        };

        let first = create_job_request_fingerprint(&same_request, JobType::Sync).unwrap();
        same_request.request_id = None;
        let second = create_job_request_fingerprint(&same_request, JobType::Sync).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn create_job_request_fingerprint_normalizes_filter_order_and_duplicates() {
        let first = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec!["b".to_string(), "a".to_string(), "a".to_string()],
            include_regex: vec!["keep-2".to_string(), "keep-1".to_string()],
            request_id: None,
        };
        let second = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec!["a".to_string(), "b".to_string()],
            include_regex: vec![
                "keep-1".to_string(),
                "keep-2".to_string(),
                "keep-1".to_string(),
            ],
            request_id: None,
        };

        let first_fingerprint = create_job_request_fingerprint(&first, JobType::Sync).unwrap();
        let second_fingerprint = create_job_request_fingerprint(&second, JobType::Sync).unwrap();

        assert_eq!(first_fingerprint, second_fingerprint);
    }

    #[test]
    fn create_job_request_fingerprint_normalizes_equivalent_path_syntax() {
        let first = CreateJobRequest {
            source_path: "/tmp//source.bin".to_string(),
            dest_path: "mirror/./nested//output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: None,
        };
        let second = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/nested/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: None,
        };

        let first_fingerprint = create_job_request_fingerprint(&first, JobType::Sync).unwrap();
        let second_fingerprint = create_job_request_fingerprint(&second, JobType::Sync).unwrap();

        assert_eq!(first_fingerprint, second_fingerprint);
    }

    #[test]
    fn resolve_public_job_id_rejects_internal_final_suffix() {
        let error = resolve_public_job_id("job-1_final").unwrap_err();
        assert_eq!(error.0, axum::http::StatusCode::NOT_FOUND);
        assert_eq!(error.1, "Job job-1_final not found");
    }

    #[test]
    fn resolve_public_job_id_keeps_regular_public_id() {
        assert_eq!(resolve_public_job_id("job-1").unwrap(), "job-1");
    }

    #[test]
    fn is_internal_final_job_id_only_matches_internal_suffix() {
        assert!(is_internal_final_job_id("job-1_final"));
        assert!(!is_internal_final_job_id("job-1"));
    }

    #[test]
    fn can_finalize_job_from_snapshot_allows_pending_sync_without_active_final() {
        let job = Job::new(
            "job-finalize-ready".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);

        assert!(can_finalize_job_from_snapshot(
            &job,
            &HashMap::new(),
            &HashSet::new()
        ));
    }

    #[test]
    fn can_finalize_job_from_snapshot_rejects_active_final_transfer() {
        let mut job = Job::new(
            "job-finalize-blocked".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Failed;

        let statuses = HashMap::from([(internal_final_job_id(&job.job_id), JobStatus::Pending)]);

        assert!(!can_finalize_job_from_snapshot(
            &job,
            &statuses,
            &HashSet::new()
        ));
    }

    #[test]
    fn can_finalize_job_from_snapshot_allows_retrying_terminal_failed_final_transfer() {
        let mut job = Job::new(
            "job-finalize-retry-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Failed;

        let statuses = HashMap::from([(internal_final_job_id(&job.job_id), JobStatus::Failed)]);

        assert!(can_finalize_job_from_snapshot(
            &job,
            &statuses,
            &HashSet::new()
        ));
    }

    #[test]
    fn internal_final_job_id_appends_internal_suffix() {
        assert_eq!(internal_final_job_id("job-1"), "job-1_final");
    }

    #[test]
    fn map_cancel_error_status_uses_internal_error_for_unexpected_failures() {
        let status = map_cancel_error_status(&HarDataError::Database(sqlx::Error::PoolClosed));
        assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn map_cancel_error_status_keeps_conflict_for_terminal_jobs() {
        let status = map_cancel_error_status(&HarDataError::Unknown(
            "Job 42 already failed and has no pending retry".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_create_job_error_status_uses_conflict_for_active_destination() {
        let status = map_create_job_error_status(&HarDataError::InvalidConfig(
            "destination '/srv/sync/output.bin' overlaps active job job-1 destination '/srv/sync/output'".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_create_job_error_status_keeps_bad_request_for_invalid_input() {
        let status = map_create_job_error_status(&HarDataError::InvalidConfig(
            "Invalid region 'missing'".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn map_create_job_error_status_uses_conflict_for_duplicate_active_runtime() {
        let status = map_create_job_error_status(&HarDataError::Unknown(
            "Job job-1 is already active with status Pending".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_finalize_error_status_uses_conflict_for_active_destination_overlap() {
        let status = map_finalize_error_status(&HarDataError::InvalidConfig(
            "destination '/srv/sync/output.bin' overlaps active job job-1 destination '/srv/sync/output'".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_finalize_error_status_uses_conflict_for_duplicate_active_final_transfer() {
        let status = map_finalize_error_status(&HarDataError::Unknown(
            "Job job-1 already has active final transfer job-1_final".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_finalize_error_status_uses_conflict_for_duplicate_active_runtime() {
        let status = map_finalize_error_status(&HarDataError::Unknown(
            "Job job-1_final is already active with status Syncing".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn job_responses_serialize_error_message_only_when_present() {
        let with_error =
            serde_json::to_value(crate::adapters::inbound::http::types::JobStatusResponse {
                job_id: "job-1".to_string(),
                status: "failed".to_string(),
                error_message: Some("disk full".to_string()),
                can_cancel: false,
                can_finalize: false,
                progress: 10,
                current_size: 10,
                total_size: 100,
                source: crate::adapters::inbound::http::types::JobPath {
                    path: "/tmp/source.bin".to_string(),
                    client_id: None,
                },
                dest: crate::adapters::inbound::http::types::JobPath {
                    path: "dest.bin".to_string(),
                    client_id: None,
                },
                region: "local".to_string(),
                job_type: "once".to_string(),
                round_id: 0,
                is_last_round: false,
                priority: 100,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:01Z".to_string(),
            })
            .unwrap();
        assert_eq!(with_error.get("error_message").unwrap(), "disk full");

        let without_error =
            serde_json::to_value(crate::adapters::inbound::http::types::JobSummary {
                job_id: "job-2".to_string(),
                status: "completed".to_string(),
                error_message: None,
                can_cancel: false,
                can_finalize: false,
                progress: 100,
                current_size: 100,
                total_size: 100,
                source: crate::adapters::inbound::http::types::JobPath {
                    path: "/tmp/source.bin".to_string(),
                    client_id: None,
                },
                dest: crate::adapters::inbound::http::types::JobPath {
                    path: "dest.bin".to_string(),
                    client_id: None,
                },
                region: "local".to_string(),
                job_type: "once".to_string(),
                round_id: 0,
                is_last_round: false,
                priority: 100,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:01Z".to_string(),
            })
            .unwrap();
        assert!(without_error.get("error_message").is_none());
    }

    #[test]
    fn resolve_public_job_runtime_fields_prefers_original_runtime() {
        let job = Job::new(
            "job-runtime".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        let mut runtime = SyncJob::new(
            job.job_id.clone(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(99);
        runtime.round_id = 4;
        runtime.is_last_round = false;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, Some(&runtime), None);

        assert_eq!(round_id, 4);
        assert!(!is_last_round);
        assert_eq!(priority, job.priority);
    }

    #[test]
    fn resolve_public_job_runtime_fields_falls_back_to_final_runtime() {
        let mut job = Job::new(
            "job-final-fallback".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.priority = 7;

        let mut final_runtime = SyncJob::new(
            "job-final-fallback_final".to_string(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(JobType::Once)
        .with_priority(107);
        final_runtime.round_id = 3;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, None, Some(&final_runtime));

        assert_eq!(round_id, 3);
        assert!(is_last_round);
        assert_eq!(priority, 7);
    }

    #[test]
    fn resolve_public_job_runtime_fields_prefers_final_runtime_over_stale_persisted_metadata() {
        let mut job = Job::new(
            "job-final-stale-metadata".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.priority = 13;
        job.round_id = 1;
        job.is_last_round = false;

        let mut final_runtime = SyncJob::new(
            "job-final-stale-metadata_final".to_string(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(JobType::Once)
        .with_priority(113);
        final_runtime.round_id = 2;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, None, Some(&final_runtime));

        assert_eq!(round_id, 2);
        assert!(is_last_round);
        assert_eq!(priority, 13);
    }
