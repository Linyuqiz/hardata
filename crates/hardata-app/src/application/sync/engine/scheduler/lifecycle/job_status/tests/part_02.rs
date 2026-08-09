    #[tokio::test]
    async fn can_cancel_status_rejects_terminal_failed_jobs_without_retry() {
        let temp_dir = create_temp_dir("can-cancel-failed-terminal");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        assert!(
            !scheduler
                .can_cancel_status("job-retry-terminal", JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_allows_original_sync_failed_by_active_final_retry() {
        let temp_dir = create_temp_dir("can-cancel-original-final-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-original-final-retry".to_string(),
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
        original.status = JobStatus::Failed;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Failed;
        db.save_job(&final_job).await.unwrap();
        db.save_retry(&final_job_id, "temporary network error")
            .await
            .unwrap();

        assert!(
            scheduler
                .can_cancel_status(&original.job_id, JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_allows_original_sync_failed_by_active_final_transfer() {
        let temp_dir = create_temp_dir("can-cancel-original-active-final");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-original-active-final".to_string(),
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
        original.status = JobStatus::Failed;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(final_job_id, original.source.clone(), original.dest.clone())
            .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        db.save_job(&final_job).await.unwrap();

        assert!(
            scheduler
                .can_cancel_status(&original.job_id, JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_job_from_snapshot_uses_loaded_final_status_index() {
        let temp_dir = create_temp_dir("can-cancel-snapshot-index");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-snapshot-index".to_string(),
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
        original.status = JobStatus::Failed;

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Failed;

        let snapshot_statuses = HashMap::from([
            (original.job_id.clone(), original.status),
            (final_job_id, final_job.status),
        ]);
        let retryable_job_ids = HashSet::from(["job-snapshot-index_final".to_string()]);

        assert!(scheduler.can_cancel_job_from_snapshot(
            &original,
            &snapshot_statuses,
            &retryable_job_ids
        ));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_retryable_job_ids_matches_database_active_retry_state() {
        let temp_dir = create_temp_dir("scheduler-load-retryable");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        db.save_retry("job-retryable", "temporary network error")
            .await
            .unwrap();

        let retryable = scheduler.load_retryable_job_ids().await.unwrap();

        assert!(retryable.contains("job-retryable"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_status_counts_overlays_runtime_without_loading_full_jobs() {
        let temp_dir = create_temp_dir("status-counts-overlay");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut persisted = Job::new(
            "job-status-counts".to_string(),
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
        persisted.status = JobStatus::Failed;
        db.save_job(&persisted).await.unwrap();

        scheduler.job_status_cache.insert(
            persisted.job_id.clone(),
            JobRuntimeStatus {
                job_id: persisted.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 50,
                current_size: 500,
                total_size: 1000,
                region: persisted.region.clone(),
                error_message: None,
                created_at: persisted.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_status_cache.insert(
            "job-runtime-only".to_string(),
            JobRuntimeStatus {
                job_id: "job-runtime-only".to_string(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "local".to_string(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let counts = scheduler.load_job_status_counts().await.unwrap();

        assert_eq!(counts.get(&JobStatus::Syncing), Some(&1));
        assert_eq!(counts.get(&JobStatus::Pending), Some(&1));
        assert_eq!(counts.get(&JobStatus::Failed), None);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_public_job_status_counts_projects_internal_final_runtime() {
        let temp_dir = create_temp_dir("public-status-counts");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut public_job = Job::new(
            "job-public-counts".to_string(),
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
        public_job.status = JobStatus::Failed;
        db.save_job(&public_job).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", public_job.job_id),
            public_job.source.clone(),
            public_job.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            public_job.job_id.clone(),
            JobRuntimeStatus {
                job_id: public_job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 10,
                current_size: 10,
                total_size: 100,
                region: public_job.region.clone(),
                error_message: None,
                created_at: public_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_status_cache.insert(
            final_job.job_id.clone(),
            JobRuntimeStatus {
                job_id: final_job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 50,
                current_size: 50,
                total_size: 100,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let counts = scheduler.load_public_job_status_counts().await.unwrap();

        assert_eq!(counts.get(&JobStatus::Pending), None);
        assert_eq!(counts.get(&JobStatus::Syncing), Some(&1));
        assert_eq!(counts.get(&JobStatus::Failed), None);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_clamps_out_of_range_request_to_tail_page() {
        let temp_dir = create_temp_dir("snapshot-page-clamp-tail");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        for (job_id, minutes_ago) in [
            ("job-tail-first", 0),
            ("job-tail-second", 1),
            ("job-tail-third", 2),
        ] {
            let mut job = Job::new(
                job_id.to_string(),
                JobPath {
                    path: format!("/tmp/{job_id}.bin"),
                    client_id: String::new(),
                },
                JobPath {
                    path: format!("dest/{job_id}.bin"),
                    client_id: String::new(),
                },
            )
            .with_job_type(JobType::Sync);
            job.created_at = now - Duration::minutes(minutes_ago);
            job.updated_at = job.created_at;
            db.save_job(&job).await.unwrap();
        }

        let (total, jobs) = scheduler.load_jobs_snapshot_page(9, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, "job-tail-third");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn resolve_persisted_page_window_limits_reads_to_current_page_neighborhood() {
        assert_eq!(resolve_persisted_page_window(50_000, 100, 3), (49_997, 106));
        assert_eq!(resolve_persisted_page_window(2, 100, 10), (0, 120));
        assert_eq!(resolve_persisted_page_window(0, 100, 0), (0, 100));
    }

    #[tokio::test]
    async fn load_public_jobs_snapshot_page_excludes_internal_final_jobs() {
        let temp_dir = create_temp_dir("public-page-excludes-final");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        let mut original = Job::new(
            "job-public-page".to_string(),
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
        original.status = JobStatus::Pending;
        original.created_at = now - Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut internal_final = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        internal_final.status = JobStatus::Pending;
        internal_final.created_at = now;
        internal_final.updated_at = now;
        db.save_job(&internal_final).await.unwrap();

        scheduler.job_status_cache.insert(
            "job-runtime-public".to_string(),
            JobRuntimeStatus {
                job_id: "job-runtime-public".to_string(),
                status: JobStatus::Syncing,
                progress: 60,
                current_size: 60,
                total_size: 100,
                region: "local".to_string(),
                error_message: None,
                created_at: now + Duration::seconds(1),
                updated_at: now + Duration::seconds(1),
            },
        );
        scheduler.job_status_cache.insert(
            "job-runtime-public_final".to_string(),
            JobRuntimeStatus {
                job_id: "job-runtime-public_final".to_string(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "local".to_string(),
                error_message: None,
                created_at: now + Duration::seconds(2),
                updated_at: now + Duration::seconds(2),
            },
        );

        let (total, jobs) = scheduler
            .load_public_jobs_snapshot_page(0, 10)
            .await
            .unwrap();

        assert_eq!(total, 2);
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|job| !job.job_id.ends_with("_final")));
        assert!(jobs.iter().any(|job| job.job_id == original.job_id));
        assert!(jobs.iter().any(|job| job.job_id == "job-runtime-public"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_correct_when_runtime_job_sorts_in_middle() {
        let temp_dir = create_temp_dir("snapshot-page-runtime-middle");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();

        let mut newest = Job::new(
            "job-p-newest".to_string(),
            JobPath {
                path: "/s/newest".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "d/newest".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        newest.created_at = now - Duration::minutes(1);
        newest.updated_at = newest.created_at;
        db.save_job(&newest).await.unwrap();

        let mut older = Job::new(
            "job-p-older".to_string(),
            JobPath {
                path: "/s/older".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "d/older".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        older.created_at = now - Duration::minutes(10);
        older.updated_at = older.created_at;
        db.save_job(&older).await.unwrap();

        let mut oldest = Job::new(
            "job-p-oldest".to_string(),
            JobPath {
                path: "/s/oldest".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "d/oldest".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        oldest.created_at = now - Duration::minutes(20);
        oldest.updated_at = oldest.created_at;
        db.save_job(&oldest).await.unwrap();

        let runtime_id = "job-runtime-middle".to_string();
        scheduler.job_status_cache.insert(
            runtime_id.clone(),
            JobRuntimeStatus {
                job_id: runtime_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "default".to_string(),
                error_message: None,
                created_at: now - Duration::minutes(5),
                updated_at: now - Duration::minutes(5),
            },
        );
        scheduler.job_cache.insert(
            runtime_id.clone(),
            crate::application::sync::engine::job::SyncJob::new(
                runtime_id.clone(),
                std::path::PathBuf::from("/s/runtime"),
                "d/runtime".to_string(),
                "default".to_string(),
            )
            .with_job_type(JobType::Once),
        );


        let (total, page0) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();
        assert_eq!(total, 4);
        assert_eq!(page0.len(), 2);
        assert_eq!(page0[0].job_id, "job-p-newest");
        assert_eq!(page0[1].job_id, "job-runtime-middle");

        let (total, page1) = scheduler.load_jobs_snapshot_page(1, 2).await.unwrap();
        assert_eq!(total, 4);
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].job_id, "job-p-older");
        assert_eq!(page1[1].job_id, "job-p-oldest");

        let _ = fs::remove_dir_all(temp_dir);
    }
