    #[tokio::test]
    async fn load_jobs_snapshot_preserves_original_created_at_for_runtime_overlay() {
        let temp_dir = create_temp_dir("preserve-created-at");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let original_created_at = Utc::now() - Duration::hours(6);
        let runtime_created_at = Utc::now();
        let mut job = Job::new(
            "job-preserve-created-at".to_string(),
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
        job.status = JobStatus::Pending;
        job.created_at = original_created_at;
        job.updated_at = original_created_at;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 42,
                current_size: 10,
                total_size: 20,
                region: job.region.clone(),
                error_message: None,
                created_at: runtime_created_at,
                updated_at: runtime_created_at,
            },
        );

        scheduler.job_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::job::SyncJob::new(
                job.job_id.clone(),
                std::path::PathBuf::from(&job.source.path),
                job.dest.path.clone(),
                job.region.clone(),
            )
            .with_job_type(job.job_type),
        );

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(snapshot.created_at, original_created_at);
        assert_eq!(snapshot.updated_at, runtime_created_at);
        assert_eq!(snapshot.status, JobStatus::Syncing);
        assert_eq!(snapshot.progress, 42);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_snapshot_overlays_runtime_error_message() {
        let temp_dir = create_temp_dir("snapshot-runtime-error-message");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-snapshot-runtime-error".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = JobStatus::Failed;
        job.error_message = Some("persisted failure".to_string());
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 77,
                current_size: 77,
                total_size: 100,
                region: job.region.clone(),
                error_message: Some("runtime failure".to_string()),
                created_at: job.created_at,
                updated_at: Utc::now(),
            },
        );

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(snapshot.error_message.as_deref(), Some("runtime failure"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_merges_runtime_only_jobs_by_created_at() {
        let temp_dir = create_temp_dir("snapshot-page-runtime-only");
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
        let mut older = Job::new(
            "job-persisted-older".to_string(),
            JobPath {
                path: "/tmp/source-older.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-older.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        older.created_at = now - Duration::minutes(10);
        older.updated_at = older.created_at;
        db.save_job(&older).await.unwrap();

        let mut newer = Job::new(
            "job-persisted-newer".to_string(),
            JobPath {
                path: "/tmp/source-newer.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-newer.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        newer.created_at = now - Duration::minutes(5);
        newer.updated_at = newer.created_at;
        db.save_job(&newer).await.unwrap();

        let runtime_job_id = "job-runtime-only".to_string();
        scheduler.job_status_cache.insert(
            runtime_job_id.clone(),
            JobRuntimeStatus {
                job_id: runtime_job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "default".to_string(),
                error_message: None,
                created_at: now,
                updated_at: now,
            },
        );
        scheduler.job_cache.insert(
            runtime_job_id.clone(),
            crate::application::sync::engine::job::SyncJob::new(
                runtime_job_id.clone(),
                std::path::PathBuf::from("/tmp/runtime-only.bin"),
                "dest-runtime.bin".to_string(),
                "default".to_string(),
            )
            .with_job_type(JobType::Sync),
        );

        let (total, jobs) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, runtime_job_id);
        assert_eq!(jobs[1].job_id, newer.job_id);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_skips_corrupted_rows_beyond_requested_range() {
        let temp_dir = create_temp_dir("snapshot-page-skip-far-corruption");
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
        let mut first = Job::new(
            "job-page-first".to_string(),
            JobPath {
                path: "/tmp/source-first.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-first.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        first.created_at = now;
        first.updated_at = now;
        db.save_job(&first).await.unwrap();

        let mut second = Job::new(
            "job-page-second".to_string(),
            JobPath {
                path: "/tmp/source-second.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-second.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        second.created_at = now - Duration::minutes(1);
        second.updated_at = second.created_at;
        db.save_job(&second).await.unwrap();

        let mut corrupted = Job::new(
            "job-page-corrupted".to_string(),
            JobPath {
                path: "/tmp/source-corrupted.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-corrupted.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        corrupted.created_at = now - Duration::minutes(2);
        corrupted.updated_at = corrupted.created_at;
        db.save_job(&corrupted).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&corrupted.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        let (total, jobs) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, first.job_id);
        assert_eq!(jobs[1].job_id, second.job_id);

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_with_runtime_only_ignores_far_corrupted_rows() {
        let temp_dir = create_temp_dir("snapshot-page-runtime-only-far-corruption");
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
        let mut first = Job::new(
            "job-page-runtime-first".to_string(),
            JobPath {
                path: "/tmp/source-first.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-first.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        first.created_at = now - Duration::minutes(1);
        first.updated_at = first.created_at;
        db.save_job(&first).await.unwrap();

        let mut corrupted = Job::new(
            "job-page-runtime-corrupted".to_string(),
            JobPath {
                path: "/tmp/source-corrupted.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-corrupted.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        corrupted.created_at = now - Duration::minutes(2);
        corrupted.updated_at = corrupted.created_at;
        db.save_job(&corrupted).await.unwrap();

        let runtime_job_id = "job-page-runtime-only".to_string();
        scheduler.job_status_cache.insert(
            runtime_job_id.clone(),
            JobRuntimeStatus {
                job_id: runtime_job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "default".to_string(),
                error_message: None,
                created_at: now,
                updated_at: now,
            },
        );
        scheduler.job_cache.insert(
            runtime_job_id.clone(),
            crate::application::sync::engine::job::SyncJob::new(
                runtime_job_id.clone(),
                std::path::PathBuf::from("/tmp/runtime-only.bin"),
                "dest-runtime.bin".to_string(),
                "default".to_string(),
            )
            .with_job_type(JobType::Sync),
        );

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&corrupted.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        let (total, jobs) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, runtime_job_id);
        assert_eq!(jobs[1].job_id, first.job_id);

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_is_stable_when_created_at_matches() {
        let temp_dir = create_temp_dir("snapshot-page-stable-order");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let created_at = Utc::now();
        for job_id in ["job-page-b", "job-page-a", "job-page-c"] {
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
            job.created_at = created_at;
            job.updated_at = created_at;
            db.save_job(&job).await.unwrap();
        }

        let (_, first_page) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();
        let (_, second_page) = scheduler.load_jobs_snapshot_page(1, 2).await.unwrap();

        assert_eq!(
            first_page
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-page-a", "job-page-b"]
        );
        assert_eq!(
            second_page
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-page-c"]
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_allows_failed_jobs_with_pending_retry() {
        let temp_dir = create_temp_dir("can-cancel-failed-with-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        db.save_retry("job-retry-cancellable", "temporary network error")
            .await
            .unwrap();

        assert!(
            scheduler
                .can_cancel_status("job-retry-cancellable", JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_resolved_job_statuses_prefers_runtime_status() {
        let temp_dir = create_temp_dir("resolved-statuses-runtime");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-resolved-runtime".to_string(),
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
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 42,
                current_size: 42,
                total_size: 100,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: Utc::now(),
            },
        );

        let statuses = scheduler
            .load_resolved_job_statuses(&[job.job_id.clone()])
            .await
            .unwrap();

        assert_eq!(statuses.get(&job.job_id), Some(&JobStatus::Syncing));

        let _ = fs::remove_dir_all(temp_dir);
    }
