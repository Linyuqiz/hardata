    #[tokio::test]
    async fn submit_job_returns_error_when_initial_persistence_fails() {
        let root = temp_dir("submit-job-persist-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DROP TABLE jobs")
            .execute(&raw_pool)
            .await
            .unwrap();

        let job = crate::application::sync::engine::job::SyncJob::new(
            "job-persist-failure".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        );

        let err = scheduler.submit_job(job.clone()).await.unwrap_err();

        assert!(err.to_string().contains("no such table: jobs"));
        assert!(scheduler.job_cache.get(&job.job_id).is_none());
        assert!(scheduler.job_status_cache.get(&job.job_id).is_none());
        assert_eq!(scheduler.job_queue.len().await, 0);

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_allows_failed_sync_with_pending_retry() {
        let root = temp_dir("finalize-failed-sync");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-finalize-failed-sync".to_string(),
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
        job.status = JobStatus::Failed;
        job.priority = 7;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        scheduler.finalize_job(&job.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());

        let final_job_id = format!("{}_final", job.job_id);
        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        assert_eq!(final_snapshot.job_type, JobType::Once);
        assert_eq!(final_snapshot.source.path, job.source.path);
        assert_eq!(final_snapshot.dest.path, job.dest.path);
        let final_runtime = scheduler.job_cache.get(&final_job_id).unwrap();
        assert_eq!(final_runtime.round_id, 1);
        assert!(final_runtime.is_last_round);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_active_final_job_exists() {
        let root = temp_dir("finalize-rejects-active-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-active-final".to_string(),
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
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        db.save_job(&final_job).await.unwrap();

        scheduler.finalize_job(&original.job_id).await.unwrap();
        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        assert_eq!(scheduler.job_queue.len().await, 0);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_checks_destination_conflict_before_cancelling_original_retryable_sync() {
        let root = temp_dir("finalize-prechecks-destination-conflict");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-conflict".to_string(),
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
        original.status = JobStatus::Failed;
        db.save_job(&original).await.unwrap();
        db.save_retry(&original.job_id, "temporary network error")
            .await
            .unwrap();

        let conflicting = SyncJob::new(
            "job-conflicting-active".to_string(),
            PathBuf::from("/tmp/other-source.bin"),
            "mirror".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        scheduler.submit_job(conflicting.clone()).await.unwrap();

        let err = scheduler.finalize_job(&original.job_id).await.unwrap_err();

        assert!(err
            .to_string()
            .contains("overlaps active job job-conflicting-active"));
        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert!(db.get_retry(&original.job_id).await.unwrap().is_some());
        assert!(scheduler
            .load_job_snapshot(&format!("{}_final", original.job_id))
            .await
            .unwrap()
            .is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_allows_retrying_terminal_failed_final_transfer() {
        let root = temp_dir("finalize-retries-terminal-final-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-terminal-final-failure".to_string(),
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
        original.status = JobStatus::Failed;
        original.round_id = 4;
        original.is_last_round = true;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut failed_final = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        failed_final.status = JobStatus::Failed;
        failed_final.round_id = 4;
        failed_final.is_last_round = true;
        failed_final.error_message = Some("disk full".to_string());
        db.save_job(&failed_final).await.unwrap();

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);
        assert_eq!(original_snapshot.round_id, 4);
        assert!(original_snapshot.is_last_round);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        assert_eq!(final_snapshot.job_type, JobType::Once);
        assert_eq!(final_snapshot.round_id, 4);
        assert!(final_snapshot.is_last_round);
        assert_eq!(final_snapshot.error_message, None);

        let final_runtime = scheduler.get_job_info(&final_job_id).unwrap();
        assert_eq!(final_runtime.round_id, 4);
        assert!(final_runtime.is_last_round);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_final_transfer_already_completed() {
        let root = temp_dir("finalize-completed-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-completed-final".to_string(),
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
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        scheduler.notify_job_completed(&final_job_id).await;

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Completed);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Completed);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_preserves_round_metadata_for_final_transfer() {
        let root = temp_dir("finalize-preserves-final-round");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-round-metadata".to_string(),
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
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        let mut original_runtime = SyncJob::new(
            original.job_id.clone(),
            PathBuf::from(&original.source.path),
            original.dest.path.clone(),
            original.region.clone(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(5);
        original_runtime.round_id = 2;
        original_runtime.is_first_round = false;
        scheduler
            .job_cache
            .insert(original.job_id.clone(), original_runtime.clone());
        scheduler.job_status_cache.insert(
            original.job_id.clone(),
            JobRuntimeStatus {
                job_id: original.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: original.region.clone(),
                error_message: None,
                created_at: original.created_at,
                updated_at: original.updated_at,
            },
        );

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let final_runtime = scheduler.job_cache.get(&final_job_id).unwrap();
        assert_eq!(final_runtime.round_id, 3);
        assert!(final_runtime.is_last_round);
        assert_eq!(final_runtime.priority, 105);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_reuses_synced_file_cache_for_final_transfer() {
        let root = temp_dir("finalize-copies-synced-cache");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-synced-cache".to_string(),
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
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        let runtime = SyncJob::new(
            original.job_id.clone(),
            PathBuf::from(&original.source.path),
            original.dest.path.clone(),
            original.region.clone(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(5);
        scheduler.job_cache.insert(original.job_id.clone(), runtime);
        scheduler.job_status_cache.insert(
            original.job_id.clone(),
            JobRuntimeStatus {
                job_id: original.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: original.region.clone(),
                error_message: None,
                created_at: original.created_at,
                updated_at: original.updated_at,
            },
        );

        let file_path = "/tmp/source.bin".to_string();
        let job_cache = DashMap::new();
        job_cache.insert(
            file_path.clone(),
            FileSyncState {
                size: 123,
                mtime: 456,
                change_time: None,
                inode: None,
                dest_mtime: Some(789),
                dest_change_time: None,
                dest_inode: None,
                updated_at: 789,
            },
        );
        scheduler
            .synced_files_cache
            .insert(original.job_id.clone(), job_cache);

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        assert!(scheduler.synced_files_cache.get(&original.job_id).is_none());

        let final_cache = scheduler
            .synced_files_cache
            .get(&final_job_id)
            .expect("final transfer should inherit synced file cache");
        let final_state = final_cache
            .get(&file_path)
            .expect("cached source path should be preserved");
        assert_eq!(final_state.size, 123);
        assert_eq!(final_state.mtime, 456);
        assert_eq!(final_state.dest_mtime, Some(789));
        assert_eq!(final_state.updated_at, 789);

        fs::remove_dir_all(root).unwrap();
    }
