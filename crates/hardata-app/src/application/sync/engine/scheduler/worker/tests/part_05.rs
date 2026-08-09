    #[tokio::test]
    async fn idle_round_skips_pending_update_when_status_is_already_clean_pending() {
        let root = temp_dir("worker-idle-skip-clean-pending");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let job = crate::domain::Job::new(
            "job-worker-idle-clean-pending".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        assert!(
            !scheduler
                .should_notify_job_pending_after_round(&job.job_id, false)
                .await
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn idle_round_clears_stale_pending_error_after_successful_idle_round() {
        let root = temp_dir("worker-idle-clears-pending-error");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let mut job = crate::domain::Job::new(
            "job-worker-idle-recovers-failed".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.error_message = Some("previous failure".to_string());
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 20,
                current_size: 20,
                total_size: 100,
                region: job.region.clone(),
                error_message: Some("previous failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        assert!(
            scheduler
                .should_notify_job_pending_after_round(&job.job_id, false)
                .await
        );
        scheduler.notify_job_pending(&job.job_id).await;
        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert!(runtime.error_message.is_none());
        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);
        assert!(snapshot.error_message.is_none());
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn trim_deleted_source_tracking_removes_stale_cache_and_stability_entries() {
        let root = temp_dir("trim-deleted-source-tracking");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(config, db)
            .await
            .unwrap();
        let job_id = "job-trim-tracking";
        let stale_path = "/remote/source/stale.txt".to_string();
        let mtime = unix_timestamp_nanos(SystemTime::now()) + 1_000_000_000;
        let freezer = scheduler.size_freezer_for_job(job_id);
        assert!(!freezer.check_stable_and_update(&stale_path, 4, mtime).await);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(freezer.check_stable_and_update(&stale_path, 4, mtime).await);
        let job_cache = dashmap::DashMap::new();
        job_cache.insert(
            stale_path.clone(),
            FileSyncState {
                size: 4,
                mtime,
                change_time: None,
                inode: None,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
                updated_at: chrono::Utc::now().timestamp(),
            },
        );
        scheduler
            .synced_files_cache
            .insert(job_id.to_string(), job_cache);
        let source_files = vec![ScannedFile {
            path: PathBuf::from("/remote/source/keep.txt"),
            size: 4,
            modified: mtime,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        trim_deleted_source_tracking(
            &scheduler.synced_files_cache,
            &scheduler.size_freezers,
            job_id,
            &source_files,
        )
        .await;
        let trimmed_cache = scheduler
            .synced_files_cache
            .get(job_id)
            .expect("job cache should still exist after trimming");
        assert!(trimmed_cache.get(&stale_path).is_none());
        assert!(
            !freezer.check_stable_and_update(&stale_path, 4, mtime).await,
            "deleted path should not reuse stale stability state"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ensure_job_not_cancelled_uses_cancelled_snapshot_when_runtime_missing() {
        let root = temp_dir("worker-cancelled-snapshot");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::adapters::outbound::persistence::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let mut job = crate::domain::Job::new(
            "job-worker-cancelled".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = crate::domain::JobStatus::Cancelled;
        db.save_job(&job).await.unwrap();
        let err = scheduler
            .ensure_job_not_cancelled(&job.job_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Job cancelled by user"));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ensure_job_not_cancelled_returns_error_when_status_resolution_fails() {
        let root = temp_dir("worker-status-resolution-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::adapters::outbound::persistence::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DROP TABLE jobs")
            .execute(&raw_pool)
            .await
            .unwrap();
        let err = scheduler
            .ensure_job_not_cancelled("job-worker-status-resolution-failure")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such table: jobs"));
        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ensure_job_not_cancelled_returns_error_when_persisted_row_is_missing() {
        let root = temp_dir("worker-missing-persisted-row");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::adapters::outbound::persistence::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let mut job = crate::domain::Job::new(
            "job-worker-missing-persisted-row".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = crate::domain::JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::domain::JobStatus::Syncing,
                progress: 10,
                current_size: 10,
                total_size: 100,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );
        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();
        let err = scheduler
            .ensure_job_not_cancelled(&job.job_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(&job.job_id));
        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn handle_job_execution_error_preserves_cancelled_state() {
        let root = temp_dir("worker-cancelled-error");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::adapters::outbound::persistence::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let mut job = crate::domain::Job::new(
            "job-worker-cancelled-error".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = crate::domain::JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::domain::JobStatus::Cancelled,
                progress: 0,
                current_size: 0,
                total_size: 128,
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
        scheduler
            .handle_job_execution_error(
                &job.job_id,
                &crate::shared::error::HarDataError::Unknown("1 files failed".to_string()),
            )
            .await;
        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::domain::JobStatus::Cancelled);
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn handle_job_execution_error_missing_row_cleans_runtime_without_marking_cancelled() {
        let root = temp_dir("worker-missing-row-error");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::adapters::outbound::persistence::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let mut job = crate::domain::Job::new(
            "job-worker-missing-row-error".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = crate::domain::JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::domain::JobStatus::Syncing,
                progress: 30,
                current_size: 30,
                total_size: 100,
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
        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();
        scheduler
            .handle_job_execution_error(
                &job.job_id,
                &crate::shared::error::HarDataError::NetworkError("network failed".to_string()),
            )
            .await;
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(db.load_job(&job.job_id).await.unwrap().is_none());
        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn handle_job_execution_error_keeps_existing_failed_state() {
        let root = temp_dir("worker-preserve-failed-state");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::adapters::outbound::persistence::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::application::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
                .await
                .unwrap();
        let mut job = crate::domain::Job::new(
            "job-worker-preserve-failed".to_string(),
            crate::domain::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = crate::domain::JobStatus::Failed;
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::domain::JobStatus::Failed,
                progress: 30,
                current_size: 30,
                total_size: 100,
                region: job.region.clone(),
                error_message: Some("existing failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler
            .handle_job_execution_error(
                &job.job_id,
                &crate::shared::error::HarDataError::NetworkError("network failed".to_string()),
            )
            .await;
        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, crate::domain::JobStatus::Failed);
        assert_eq!(runtime.error_message.as_deref(), Some("existing failure"));
        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::domain::JobStatus::Failed);
        std::fs::remove_dir_all(root).unwrap();
    }
