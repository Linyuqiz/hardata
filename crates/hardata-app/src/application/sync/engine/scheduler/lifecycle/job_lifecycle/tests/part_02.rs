    #[tokio::test]
    async fn finalize_job_is_idempotent_when_syncing_active_final_runtime_exists() {
        let root = temp_dir("finalize-waits-active-final-runtime");
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
            "job-finalize-waits-final".to_string(),
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
        original.status = JobStatus::Syncing;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 60,
                current_size: 600,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_cache.insert(
            final_job_id.clone(),
            crate::application::sync::engine::job::SyncJob::new(
                final_job_id.clone(),
                PathBuf::from(&final_job.source.path),
                final_job.dest.path.clone(),
                final_job.region.clone(),
            )
            .with_job_type(final_job.job_type),
        );

        scheduler.finalize_job(&original.job_id).await.unwrap();
        assert!(scheduler.job_cache.get(&final_job_id).is_some());
        assert_eq!(
            scheduler.get_job_status(&final_job_id).unwrap().status,
            JobStatus::Syncing
        );

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Syncing);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Syncing);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_pending_cleans_tmp_artifacts_immediately() {
        let root = temp_dir("cancel-pending-cleans-tmp");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let sync_job = crate::application::sync::engine::job::SyncJob::new(
            "job-cancel-pending".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        scheduler.submit_job(sync_job.clone()).await.unwrap();

        let tmp_path = root.join("pending.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&sync_job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&sync_job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cancel_job(&sync_job.job_id).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db
            .load_tmp_transfer_paths_by_job(&sync_job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(scheduler
            .transfer_manager_pool
            .job_tmp_write_paths(&sync_job.job_id)
            .is_empty());

        let snapshot = scheduler
            .load_job_snapshot(&sync_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_pending_snapshot_without_runtime_succeeds() {
        let root = temp_dir("cancel-pending-snapshot-without-runtime");
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
            "job-cancel-snapshot-pending".to_string(),
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
        job.status = JobStatus::Pending;
        job.progress = 25;
        job.current_size = 128;
        job.total_size = 512;
        db.save_job(&job).await.unwrap();

        let tmp_path = root.join("snapshot-pending.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.cancel_job(&job.job_id).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);
        assert_eq!(snapshot.progress, 25);
        assert_eq!(snapshot.current_size, 128);
        assert_eq!(snapshot.total_size, 512);
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(!tmp_path.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_returns_error_when_persisted_status_update_fails() {
        let root = temp_dir("cancel-job-persist-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let sync_job = crate::application::sync::engine::job::SyncJob::new(
            "job-cancel-persist-failure".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        scheduler.submit_job(sync_job.clone()).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_cancel_update
            BEFORE UPDATE ON jobs
            WHEN NEW.status = 'cancelled'
            BEGIN
                SELECT RAISE(FAIL, 'reject cancelled status');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let err = scheduler.cancel_job(&sync_job.job_id).await.unwrap_err();

        assert!(err.to_string().contains("reject cancelled status"));
        assert_eq!(
            scheduler.get_job_status(&sync_job.job_id).unwrap().status,
            JobStatus::Pending
        );
        assert_eq!(scheduler.job_queue.len().await, 1);

        let snapshot = scheduler
            .load_job_snapshot(&sync_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_duplicate_active_job_id() {
        let root = temp_dir("submit-duplicate-active-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let job = crate::application::sync::engine::job::SyncJob::new(
            "job-duplicate-active".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        scheduler.submit_job(job.clone()).await.unwrap();

        let duplicate = scheduler.submit_job(job.clone()).await;
        assert!(duplicate.is_err());
        assert_eq!(scheduler.job_queue.len().await, 1);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_duplicate_active_destination() {
        let root = temp_dir("submit-duplicate-active-destination");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let first = crate::application::sync::engine::job::SyncJob::new(
            "job-destination-a".to_string(),
            PathBuf::from("/tmp/source-a.bin"),
            "mirror/shared/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let second = crate::application::sync::engine::job::SyncJob::new(
            "job-destination-b".to_string(),
            PathBuf::from("/tmp/source-b.bin"),
            "mirror/shared/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);

        scheduler.submit_job(first.clone()).await.unwrap();

        let err = scheduler.submit_job(second.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("overlaps active job job-destination-a"));
        assert_eq!(scheduler.job_queue.len().await, 1);
        assert!(scheduler
            .load_job_snapshot(&second.job_id)
            .await
            .unwrap()
            .is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_overlapping_active_destination_subpath() {
        let root = temp_dir("submit-overlapping-active-destination");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let first = crate::application::sync::engine::job::SyncJob::new(
            "job-destination-parent".to_string(),
            PathBuf::from("/tmp/source-a"),
            "mirror".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let second = crate::application::sync::engine::job::SyncJob::new(
            "job-destination-child".to_string(),
            PathBuf::from("/tmp/source-b.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);

        scheduler.submit_job(first.clone()).await.unwrap();

        let err = scheduler.submit_job(second.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("overlaps active job job-destination-parent"));
        assert_eq!(scheduler.job_queue.len().await, 1);
        assert!(scheduler
            .load_job_snapshot(&second.job_id)
            .await
            .unwrap()
            .is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_duplicate_paused_job_id() {
        let root = temp_dir("submit-duplicate-paused-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let job = crate::application::sync::engine::job::SyncJob::new(
            "job-duplicate-paused".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        scheduler.job_cache.insert(job.job_id.clone(), job.clone());
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Paused,
                progress: 50,
                current_size: 512,
                total_size: 1024,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let duplicate = scheduler.submit_job(job.clone()).await;
        assert!(duplicate.is_err());
        assert_eq!(scheduler.job_queue.len().await, 0);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_reuse_while_previous_run_is_still_shutting_down() {
        let root = temp_dir("submit-rejects-shutting-down-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let job = crate::application::sync::engine::job::SyncJob::new(
            "job-shutting-down".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        scheduler.running_jobs.insert(job.job_id.clone(), ());
        scheduler.cancelled_jobs.insert(job.job_id.clone(), ());
        scheduler.job_cache.insert(job.job_id.clone(), job.clone());
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Cancelled,
                progress: 40,
                current_size: 400,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let duplicate = scheduler.submit_job(job.clone()).await;
        assert!(duplicate.is_err());
        assert!(scheduler.cancelled_jobs.contains_key(&job.job_id));
        assert!(scheduler.running_jobs.contains_key(&job.job_id));

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_duplicate_active_does_not_overwrite_snapshot() {
        let root = temp_dir("submit-duplicate-active-does-not-overwrite-snapshot");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let original_job = crate::application::sync::engine::job::SyncJob::new(
            "job-duplicate-active-snapshot".to_string(),
            PathBuf::from("/tmp/source-a.bin"),
            "mirror/output-a.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(3);
        scheduler.submit_job(original_job.clone()).await.unwrap();

        let duplicate_job = crate::application::sync::engine::job::SyncJob::new(
            original_job.job_id.clone(),
            PathBuf::from("/tmp/source-b.bin"),
            "mirror/output-b.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(9);

        let duplicate = scheduler.submit_job(duplicate_job).await;
        assert!(duplicate.is_err());

        let snapshot = scheduler
            .load_job_snapshot(&original_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.source.path, "/tmp/source-a.bin");
        assert_eq!(snapshot.dest.path, "mirror/output-a.bin");
        assert_eq!(snapshot.priority, 3);
        assert_eq!(snapshot.status, JobStatus::Pending);

        fs::remove_dir_all(root).unwrap();
    }
