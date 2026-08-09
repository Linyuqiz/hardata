    #[tokio::test]
    async fn cancel_job_clears_all_duplicate_queued_entries() {
        let root = temp_dir("cancel-clears-all-queued-duplicates");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let job = crate::application::sync::engine::job::SyncJob::new(
            "job-cancel-duplicate-queued".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        scheduler.submit_job(job.clone()).await.unwrap();
        scheduler
            .delayed_queue
            .insert(
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                job.clone(),
            )
            .await;
        scheduler
            .delayed_queue
            .insert(
                std::time::Instant::now() + std::time::Duration::from_secs(120),
                job.clone(),
            )
            .await;

        scheduler.cancel_job(&job.job_id).await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_original_sync_job_redirects_to_active_final_job() {
        let root = temp_dir("cancel-original-redirects-final");
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
            "job-cancel-original-final".to_string(),
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
        original.progress = 40;
        original.current_size = 400;
        original.total_size = 1000;
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

        let tmp_path = root.join("final-running.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&final_job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        db.save_transfer_state(
            &final_job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.cancel_job(&original.job_id).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Cancelled);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Cancelled);
        assert!(db
            .load_transfer_state(&final_job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&final_job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(!tmp_path.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_original_sync_job_redirects_to_paused_final_job() {
        let root = temp_dir("cancel-original-redirects-paused-final");
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
            "job-cancel-original-paused-final".to_string(),
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
        final_job.status = JobStatus::Paused;
        final_job.progress = 40;
        final_job.current_size = 400;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.cancel_job(&original.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Cancelled);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recover_pending_jobs_marks_unrecoverable_jobs_failed() {
        let root = temp_dir("recover-invalid-destination");
        let data_dir = root.join("sync-data");
        let outside = root.join("outside");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, data_dir.join("escape")).unwrap();

        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-recover-invalid-dest".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "escape/out.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Pending;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "stale retry").await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();
        db.save_tmp_transfer_path(&job.job_id, outside.join("out.bin.tmp").to_str().unwrap())
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);
        assert!(snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("escapes sync.data_dir")));
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());
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
        assert!(scheduler.job_cache.get(&job.job_id).is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_preserves_persisted_progress() {
        let root = temp_dir("recover-progress");
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
            "job-recover-progress".to_string(),
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
        job.status = JobStatus::Syncing;
        job.progress = 60;
        job.current_size = 600;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert_eq!(runtime.progress, 60);
        assert_eq!(runtime.current_size, 600);
        assert_eq!(runtime.total_size, 1000);
        let recovered_job = scheduler.get_job_info(&job.job_id).unwrap();
        assert_eq!(recovered_job.round_id, 1);
        assert!(!recovered_job.is_first_round);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_prefers_active_final_job_over_original() {
        let root = temp_dir("recover-prefers-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = chrono::Utc::now();
        let mut original = Job::new(
            "job-recover-prefers-final".to_string(),
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
        original.created_at = now - chrono::Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        final_job.created_at = now;
        final_job.updated_at = now;
        db.save_job(&final_job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        assert!(scheduler.get_job_info(&original.job_id).is_none());
        assert!(scheduler.get_job_status(&original.job_id).is_none());

        let final_runtime = scheduler
            .get_job_status(&final_job.job_id)
            .expect("final job should recover");
        assert_eq!(final_runtime.status, JobStatus::Pending);
        assert!(scheduler.get_job_info(&final_job.job_id).is_some());

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_restores_paused_jobs_without_queueing() {
        let root = temp_dir("recover-paused-job");
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
            "job-recover-paused".to_string(),
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
        job.status = JobStatus::Paused;
        job.progress = 61;
        job.current_size = 610;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Paused);
        assert_eq!(runtime.progress, 61);
        assert_eq!(scheduler.job_queue.len().await, 0);
        assert!(scheduler.get_job_info(&job.job_id).is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_updates_original_sync_snapshot_for_paused_final_job() {
        let root = temp_dir("recover-paused-final-updates-original");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = chrono::Utc::now();
        let mut original = Job::new(
            "job-recover-paused-final".to_string(),
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
        original.status = JobStatus::Cancelled;
        original.created_at = now - chrono::Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Paused;
        final_job.progress = 61;
        final_job.current_size = 610;
        final_job.total_size = 1000;
        final_job.created_at = now;
        final_job.updated_at = now;
        db.save_job(&final_job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let final_runtime = scheduler
            .get_job_status(&final_job.job_id)
            .expect("paused final job should recover");
        assert_eq!(final_runtime.status, JobStatus::Paused);
        assert_eq!(final_runtime.progress, 61);
        let final_job_info = scheduler.get_job_info(&final_job.job_id).unwrap();
        assert_eq!(final_job_info.round_id, 1);
        assert!(final_job_info.is_last_round);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Paused);
        assert_eq!(original_snapshot.progress, 61);
        assert_eq!(original_snapshot.current_size, 610);
        assert_eq!(original_snapshot.total_size, 1000);

        fs::remove_dir_all(root).unwrap();
    }
