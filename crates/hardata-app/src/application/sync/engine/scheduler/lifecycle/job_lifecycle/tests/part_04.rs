    #[tokio::test]
    async fn recover_pending_jobs_restores_synced_file_cache_from_transfer_states() {
        let root = temp_dir("recover-restores-synced-file-cache");
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
            "job-recover-cache".to_string(),
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
        db.save_job(&job).await.unwrap();

        let mut completed = FileTransferState::new("remote/finished.bin".to_string(), 4)
            .with_source_version(4096, 1710000000, None, None);
        for chunk in 0..4 {
            completed.mark_chunk_completed(chunk);
        }
        completed.dest_modified = Some(1710000100);
        completed.dest_change_time = Some(1710000101);
        completed.dest_inode = Some(41);
        completed.cache_only = true;
        db.save_transfer_state(&job.job_id, &completed)
            .await
            .unwrap();

        let mut legacy_completed = FileTransferState::new("remote/legacy.bin".to_string(), 2)
            .with_source_version(2048, 1710000004, None, None);
        for chunk in 0..2 {
            legacy_completed.mark_chunk_completed(chunk);
        }
        db.save_transfer_state(&job.job_id, &legacy_completed)
            .await
            .unwrap();

        let zero_byte_checkpoint = FileTransferState::new("remote/empty.bin".to_string(), 0)
            .with_source_version(0, 1710000005, None, None)
            .mark_cache_only();
        db.save_transfer_state(&job.job_id, &zero_byte_checkpoint)
            .await
            .unwrap();

        let incomplete = FileTransferState::new("remote/incomplete.bin".to_string(), 4)
            .with_source_version(2048, 1710000001, None, None);
        db.save_transfer_state(&job.job_id, &incomplete)
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let job_cache = scheduler
            .synced_files_cache
            .get(&job.job_id)
            .expect("recovered job should rebuild synced file cache");
        assert_eq!(job_cache.len(), 3);
        let restored = job_cache
            .get("remote/finished.bin")
            .expect("completed file should be restored");
        assert_eq!(restored.size, 4096);
        assert_eq!(restored.mtime, 1710000000);
        assert_eq!(restored.dest_mtime, Some(1710000100));
        assert_eq!(restored.dest_change_time, Some(1710000101));
        assert_eq!(restored.dest_inode, Some(41));
        assert!(restored.updated_at > 0);

        let legacy_restored = job_cache
            .get("remote/legacy.bin")
            .expect("legacy completed file should still be restored");
        assert_eq!(legacy_restored.size, 2048);
        assert_eq!(legacy_restored.mtime, 1710000004);
        assert_eq!(legacy_restored.dest_mtime, None);

        let zero_byte_restored = job_cache
            .get("remote/empty.bin")
            .expect("zero-byte checkpoint should be restored");
        assert_eq!(zero_byte_restored.size, 0);
        assert_eq!(zero_byte_restored.mtime, 1710000005);
        assert_eq!(zero_byte_restored.dest_mtime, None);
        assert!(job_cache.get("remote/incomplete.bin").is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_restores_paused_job_synced_file_cache() {
        let root = temp_dir("recover-paused-restores-synced-file-cache");
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
            "job-recover-paused-cache".to_string(),
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
        db.save_job(&job).await.unwrap();

        let mut completed = FileTransferState::new("remote/paused-finished.bin".to_string(), 2)
            .with_source_version(1024, 1710000002, None, None);
        for chunk in 0..2 {
            completed.mark_chunk_completed(chunk);
        }
        completed.dest_modified = Some(1710000200);
        completed.dest_change_time = Some(1710000201);
        completed.dest_inode = Some(42);
        completed.cache_only = true;
        db.save_transfer_state(&job.job_id, &completed)
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let job_cache = scheduler
            .synced_files_cache
            .get(&job.job_id)
            .expect("paused job should rebuild synced file cache");
        let restored = job_cache
            .get("remote/paused-finished.bin")
            .expect("completed paused file should be restored");
        assert_eq!(restored.size, 1024);
        assert_eq!(restored.mtime, 1710000002);
        assert_eq!(restored.dest_mtime, Some(1710000200));
        assert_eq!(restored.dest_change_time, Some(1710000201));
        assert_eq!(restored.dest_inode, Some(42));
        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Paused
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recover_pending_jobs_marks_original_failed_when_final_recovery_fails() {
        let root = temp_dir("recover-final-invalid-destination");
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

        let now = chrono::Utc::now();
        let mut original = Job::new(
            "job-recover-final-invalid".to_string(),
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

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert!(original_snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("escapes sync.data_dir")));

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Failed);
        assert!(final_snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("escapes sync.data_dir")));

        assert!(scheduler.get_job_status(&original.job_id).is_none());
        assert!(scheduler.get_job_status(&final_job.job_id).is_none());
        assert!(scheduler.get_job_info(&original.job_id).is_none());
        assert!(scheduler.get_job_info(&final_job.job_id).is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_marks_paused_job_failed_when_destination_conflicts() {
        let root = temp_dir("recover-paused-destination-conflict");
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

        let mut paused = Job::new(
            "job-recover-paused-conflict".to_string(),
            JobPath {
                path: "/tmp/source-paused".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        paused.status = JobStatus::Paused;
        paused.created_at = now;
        paused.updated_at = now;
        db.save_job(&paused).await.unwrap();

        let mut active = Job::new(
            "job-recover-active-conflict".to_string(),
            JobPath {
                path: "/tmp/source-active".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        active.status = JobStatus::Pending;
        active.created_at = now - chrono::Duration::seconds(1);
        active.updated_at = active.created_at;
        db.save_job(&active).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let paused_snapshot = scheduler
            .load_job_snapshot(&paused.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused_snapshot.status, JobStatus::Failed);
        assert!(paused_snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("overlaps active job")));
        assert!(scheduler.get_job_status(&paused.job_id).is_none());
        assert!(scheduler.get_job_info(&paused.job_id).is_none());

        let active_runtime = scheduler.get_job_status(&active.job_id).unwrap();
        assert_eq!(active_runtime.status, JobStatus::Pending);
        assert!(scheduler.get_job_info(&active.job_id).is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_ignores_corrupted_terminal_rows() {
        let root = temp_dir("recover-pending-ignore-terminal-corruption");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut pending = Job::new(
            "job-recover-active".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/active.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        pending.status = JobStatus::Pending;
        db.save_job(&pending).await.unwrap();

        let mut completed = Job::new(
            "job-completed-corrupted".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/completed.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        completed.status = JobStatus::Completed;
        db.save_job(&completed).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&completed.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let recovered = scheduler
            .get_job_status(&pending.job_id)
            .expect("active job should still recover");
        assert_eq!(recovered.status, JobStatus::Pending);
        assert!(scheduler.job_cache.get(&pending.job_id).is_some());

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cleanup_terminal_job_artifacts_removes_stale_cancelled_runtime_state() {
        let root = temp_dir("cleanup-terminal-artifacts");
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
            "job-terminal-cancelled".to_string(),
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
        job.status = JobStatus::Cancelled;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "stale retry").await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        let tmp_path = root.join("cancelled-restart.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cleanup_terminal_job_artifacts().await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
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

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cleanup_terminal_job_artifacts_preserves_retryable_failed_state() {
        let root = temp_dir("cleanup-terminal-retryable-failed");
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
            "job-terminal-failed-retryable".to_string(),
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
        job.status = JobStatus::Failed;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        let tmp_path = root.join("retryable-failed.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cleanup_terminal_job_artifacts().await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(tmp_path.exists());
        assert!(db.get_retry(&job.job_id).await.unwrap().is_some());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());
        assert!(!db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());

        fs::remove_dir_all(root).unwrap();
    }
