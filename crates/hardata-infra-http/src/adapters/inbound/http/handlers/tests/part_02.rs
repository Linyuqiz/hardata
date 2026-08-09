    #[test]
    fn resolve_public_job_runtime_fields_falls_back_to_persisted_job_metadata() {
        let mut job = Job::new(
            "job-persisted-round".to_string(),
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
        job.priority = 11;
        job.round_id = 5;
        job.is_last_round = true;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, None, None);

        assert_eq!(round_id, 5);
        assert!(is_last_round);
        assert_eq!(priority, 11);
    }

    #[test]
    fn resolve_public_job_status_view_projects_active_final_runtime() {
        let mut job = Job::new(
            "job-final-status".to_string(),
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
        job.status = JobStatus::Cancelled;
        job.progress = 5;
        job.current_size = 5;
        job.total_size = 100;
        job.error_message = Some("stale cancelled".to_string());

        let now = chrono::Utc::now();
        let original_runtime = JobRuntimeStatus {
            job_id: job.job_id.clone(),
            status: JobStatus::Cancelled,
            progress: 5,
            current_size: 5,
            total_size: 100,
            region: job.region.clone(),
            error_message: Some("stale cancelled".to_string()),
            created_at: now,
            updated_at: now,
        };
        let final_runtime = JobRuntimeStatus {
            job_id: internal_final_job_id(&job.job_id),
            status: JobStatus::Syncing,
            progress: 63,
            current_size: 630,
            total_size: 1000,
            region: job.region.clone(),
            error_message: None,
            created_at: now,
            updated_at: now + chrono::Duration::seconds(5),
        };

        let view =
            resolve_public_job_status_view(&job, Some(&original_runtime), Some(&final_runtime));

        assert_eq!(view.status, JobStatus::Syncing);
        assert_eq!(view.progress, 63);
        assert_eq!(view.current_size, 630);
        assert_eq!(view.total_size, 1000);
        assert_eq!(view.error_message, None);
        assert_eq!(view.updated_at, final_runtime.updated_at);
    }

    #[test]
    fn resolve_public_job_status_view_uses_original_runtime_for_non_sync_jobs() {
        let job = Job::new(
            "job-once-status".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        );
        let now = chrono::Utc::now();
        let runtime = JobRuntimeStatus {
            job_id: job.job_id.clone(),
            status: JobStatus::Syncing,
            progress: 40,
            current_size: 40,
            total_size: 100,
            region: job.region.clone(),
            error_message: None,
            created_at: now,
            updated_at: now,
        };
        let final_runtime = JobRuntimeStatus {
            job_id: internal_final_job_id(&job.job_id),
            status: JobStatus::Failed,
            progress: 90,
            current_size: 90,
            total_size: 100,
            region: job.region.clone(),
            error_message: Some("ignored".to_string()),
            created_at: now,
            updated_at: now + chrono::Duration::seconds(5),
        };

        let view = resolve_public_job_status_view(&job, Some(&runtime), Some(&final_runtime));

        assert_eq!(view.status, JobStatus::Syncing);
        assert_eq!(view.progress, 40);
        assert_eq!(view.current_size, 40);
        assert_eq!(view.total_size, 100);
    }

    #[test]
    fn project_public_job_snapshot_overlays_resolved_status_fields() {
        let mut job = Job::new(
            "job-project-status".to_string(),
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
        job.status = JobStatus::Cancelled;
        let updated_at = chrono::Utc::now() + chrono::Duration::seconds(7);
        let view = super::PublicJobStatusView {
            status: JobStatus::Syncing,
            progress: 88,
            current_size: 880,
            total_size: 1000,
            error_message: Some("network retry".to_string()),
            updated_at,
        };

        let projected = project_public_job_snapshot(&job, &view);

        assert_eq!(projected.status, JobStatus::Syncing);
        assert_eq!(projected.progress, 88);
        assert_eq!(projected.current_size, 880);
        assert_eq!(projected.total_size, 1000);
        assert_eq!(projected.error_message.as_deref(), Some("network retry"));
        assert_eq!(projected.updated_at, updated_at);
    }

    #[tokio::test]
    async fn finalize_job_returns_public_job_id() {
        let (temp_dir, db, scheduler) = create_scheduler("finalize-response").await;
        let mut job = Job::new(
            "job-finalize-response".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let Json(response) =
            finalize_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
                .await
                .unwrap();

        assert_eq!(response.job_id, public_job_id);
        assert!(scheduler
            .load_job_snapshot("job-finalize-response_final")
            .await
            .unwrap()
            .is_some());

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_reuses_job_id_for_same_idempotency_key() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-header").await;
        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let request = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 0,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-key"),
        );

        let Json(first_response) =
            create_job(State(state.clone()), headers.clone(), Json(request.clone()))
                .await
                .unwrap();
        let Json(second_response) = create_job(State(state), headers, Json(request))
            .await
            .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_rejects_reused_idempotency_key_with_different_payload() {
        let (temp_dir, _db, scheduler) = create_scheduler("create-idempotent-conflict").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-key"),
        );

        let _ = create_job(
            State(state.clone()),
            headers.clone(),
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap();

        let err = create_job(
            State(state),
            headers,
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/other-output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::CONFLICT);
        assert!(err
            .1
            .contains("already used for a different create_job request"));

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_request_id_field_is_idempotent_without_header() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-request-id").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let request = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 0,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: Some("request-idempotent".to_string()),
        };

        let Json(first_response) = create_job(
            State(state.clone()),
            HeaderMap::new(),
            Json(request.clone()),
        )
        .await
        .unwrap();
        let Json(second_response) = create_job(State(state), HeaderMap::new(), Json(request))
            .await
            .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_treats_reordered_filters_as_same_idempotent_request() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-filter-order").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-filter-order"),
        );

        let Json(first_response) = create_job(
            State(state.clone()),
            headers.clone(),
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec!["b".to_string(), "a".to_string()],
                include_regex: vec!["keep-2".to_string(), "keep-1".to_string()],
                request_id: None,
            }),
        )
        .await
        .unwrap();
        let Json(second_response) = create_job(
            State(state),
            headers,
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec!["a".to_string(), "b".to_string(), "a".to_string()],
                include_regex: vec![
                    "keep-1".to_string(),
                    "keep-2".to_string(),
                    "keep-1".to_string(),
                ],
                request_id: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_treats_equivalent_path_syntax_as_same_idempotent_request() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-path-syntax").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-path-syntax"),
        );

        let Json(first_response) = create_job(
            State(state.clone()),
            headers.clone(),
            Json(CreateJobRequest {
                source_path: "/tmp//source.bin".to_string(),
                dest_path: "mirror/./nested//output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap();
        let Json(second_response) = create_job(
            State(state),
            headers,
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/nested/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_final_transfer_is_already_active() {
        let (temp_dir, db, scheduler) = create_scheduler("finalize-idempotent").await;
        let mut job = Job::new(
            "job-finalize-idempotent".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };

        let Json(first_response) = finalize_job(
            State(state.clone()),
            Path(public_job_id.clone()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let Json(second_response) =
            finalize_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
                .await
                .unwrap();

        assert_eq!(first_response.job_id, public_job_id);
        assert_eq!(second_response.job_id, public_job_id);
        assert!(scheduler
            .load_job_snapshot("job-finalize-idempotent_final")
            .await
            .unwrap()
            .is_some());

        fs::remove_dir_all(temp_dir).unwrap();
    }
