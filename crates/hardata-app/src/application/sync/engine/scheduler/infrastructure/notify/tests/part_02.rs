    #[tokio::test]
    async fn notify_job_failed_retryable_preserves_retry_and_transfer_state() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-retryable-preserve").await;
        let mut job = Job::new(
            "job-notify-failed-retryable".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 10,
                current_size: 128,
                total_size: 1024,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        let retry = db.get_retry(&job.job_id).await.unwrap();
        assert!(retry.is_some());
        let state = db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(state.is_some());
        assert!(scheduler.get_job_status(&job.job_id).is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retry_lookup_failure_clears_runtime_state() {
        let temp_dir = create_temp_dir("failed-retry-read-failure");
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
            "job-notify-failed-retry-read-failure".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 35,
                current_size: 350,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER corrupt_retry_after_insert
            AFTER INSERT ON job_retries
            WHEN NEW.job_id = 'job-notify-failed-retry-read-failure'
            BEGIN
                UPDATE job_retries
                SET last_retry_at = 'broken-timestamp'
                WHERE job_id = NEW.job_id;
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_fatal_clears_stale_retry_and_transfer_state() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-fatal-clear").await;
        let mut job = Job::new(
            "job-notify-failed-fatal".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 55,
                current_size: 512,
                total_size: 1024,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();
        let tmp_path = temp_dir.join("failed-fatal.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "[fatal] permission denied",
                ErrorCategory::Fatal,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        let retry = db.get_retry(&job.job_id).await.unwrap();
        assert!(retry.is_none());
        let state = db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(state.is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(!tmp_path.exists());
        assert!(scheduler
            .transfer_manager_pool
            .job_tmp_write_paths(&job.job_id)
            .is_empty());
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retry_exhausted_clears_retry_and_transfer_state() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-exhausted-clear").await;
        let mut job = Job::new(
            "job-notify-failed-exhausted".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();
        for _ in 0..3 {
            db.update_retry_attempt(&job.job_id, false).await.unwrap();
        }

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 80,
                current_size: 800,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "network failed after max retries",
                ErrorCategory::Retriable,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        let retry = db.get_retry(&job.job_id).await.unwrap();
        assert!(retry.is_none());
        let state = db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(state.is_none());
        assert!(scheduler.get_job_status(&job.job_id).is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_started_updates_original_sync_snapshot_for_final_job() {
        let (temp_dir, db, scheduler) = create_scheduler("final-start-updates-original").await;

        let mut original = Job::new(
            "job-final-parent".to_string(),
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
        original.status = JobStatus::Cancelled;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        final_job.progress = 40;
        final_job.current_size = 400;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Pending,
                progress: 40,
                current_size: 400,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_started(&final_job_id).await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Syncing);
        assert_eq!(original_snapshot.progress, 40);
        assert_eq!(original_snapshot.current_size, 400);
        assert_eq!(original_snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_preserves_original_final_round_metadata_in_snapshot() {
        let (temp_dir, db, scheduler) =
            create_scheduler("final-completed-preserves-round-metadata").await;

        let mut original = Job::new(
            "job-final-parent-completed".to_string(),
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
        original.status = JobStatus::Cancelled;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        final_job.progress = 80;
        final_job.current_size = 800;
        final_job.total_size = 1000;
        final_job.round_id = 4;
        final_job.is_last_round = true;
        db.save_job(&final_job).await.unwrap();

        let mut final_runtime = crate::application::sync::engine::job::SyncJob::new(
            final_job_id.clone(),
            std::path::PathBuf::from(&final_job.source.path),
            final_job.dest.path.clone(),
            final_job.region.clone(),
        )
        .with_job_type(final_job.job_type);
        final_runtime.restore_round_state(4, true);
        scheduler
            .job_cache
            .insert(final_job_id.clone(), final_runtime);
        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 80,
                current_size: 800,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_completed(&final_job_id).await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Completed);
        assert_eq!(original_snapshot.round_id, 4);
        assert!(original_snapshot.is_last_round);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retryable_updates_original_sync_snapshot_for_final_job() {
        let (temp_dir, db, scheduler) = create_scheduler("final-failed-updates-original").await;

        let mut original = Job::new(
            "job-final-parent-failed".to_string(),
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
        original.status = JobStatus::Cancelled;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        final_job.progress = 70;
        final_job.current_size = 700;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 70,
                current_size: 700,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .notify_job_failed(
                &final_job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert_eq!(original_snapshot.progress, 70);
        assert_eq!(original_snapshot.current_size, 700);
        assert_eq!(original_snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }
