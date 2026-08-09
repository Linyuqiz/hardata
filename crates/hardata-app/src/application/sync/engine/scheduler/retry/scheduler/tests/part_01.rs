    #[tokio::test]
    async fn process_pending_retries_skips_jobs_already_pending() {
        let temp_dir = create_temp_dir("skip-duplicate-pending");
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
            "job-retry-pending".to_string(),
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
        job.status = JobStatus::Pending;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 10,
                current_size: 1,
                total_size: 10,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        let retry = db.get_retry(&job.job_id).await.unwrap().unwrap();
        assert_eq!(retry.retry_count, 0);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_persists_pending_status() {
        let temp_dir = create_temp_dir("persist-pending");
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
            "job-retry-persist".to_string(),
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
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);
        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Pending
        );
        assert!(scheduler.get_job_info(&job.job_id).is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_updates_original_sync_snapshot_for_final_job() {
        let temp_dir = create_temp_dir("pending-final-updates-original");
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
            "job-retry-persist-final".to_string(),
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
        original.progress = 37;
        original.current_size = 370;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Failed;
        final_job.progress = 37;
        final_job.current_size = 370;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();
        db.save_retry(&final_job.job_id, "temporary network error")
            .await
            .unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&final_job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        let final_runtime = scheduler.get_job_info(&final_job.job_id).unwrap();
        assert_eq!(final_runtime.round_id, 1);
        assert!(final_runtime.is_last_round);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);
        assert_eq!(original_snapshot.progress, 37);
        assert_eq!(original_snapshot.current_size, 370);
        assert_eq!(original_snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_skips_enqueue_when_pending_status_persist_fails() {
        let temp_dir = create_temp_dir("skip-enqueue-on-persist-failure");
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
            "job-retry-persist-failure".to_string(),
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
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

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
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 75,
                current_size: 750,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("temporary network error".to_string()),
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        sqlx::query("DROP TABLE jobs").execute(&pool).await.unwrap();

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Failed
        );
        assert_eq!(
            db.get_retry(&job.job_id)
                .await
                .unwrap()
                .unwrap()
                .retry_count,
            0
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_cleans_stale_retry_when_pending_status_row_missing() {
        let temp_dir = create_temp_dir("cleanup-stale-retry-on-missing-row");
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
            "job-retry-missing-row".to_string(),
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
        job.progress = 75;
        job.current_size = 750;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

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
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 75,
                current_size: 750,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("temporary network error".to_string()),
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_restores_resumed_round_metadata() {
        let temp_dir = create_temp_dir("restore-resumed-round");
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
            "job-retry-resume-round".to_string(),
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
        job.progress = 75;
        job.current_size = 750;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert_eq!(runtime.progress, 75);
        assert_eq!(runtime.current_size, 750);
        assert_eq!(runtime.total_size, 1000);

        let resumed_job = scheduler.get_job_info(&job.job_id).unwrap();
        assert_eq!(resumed_job.round_id, 1);
        assert!(!resumed_job.is_first_round);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_restores_synced_file_cache_from_transfer_states() {
        let temp_dir = create_temp_dir("restore-retry-synced-file-cache");
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
            "job-retry-restore-cache".to_string(),
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
        job.progress = 75;
        job.current_size = 750;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let mut completed = FileTransferState::new("remote/retry-finished.bin".to_string(), 3)
            .with_source_version(8192, 1710000003, None, None);
        for chunk in 0..3 {
            completed.mark_chunk_completed(chunk);
        }
        completed.dest_modified = Some(1710000300);
        completed.dest_change_time = Some(1710000301);
        completed.dest_inode = Some(43);
        completed.cache_only = true;
        db.save_transfer_state(&job.job_id, &completed)
            .await
            .unwrap();

        let incomplete = FileTransferState::new("remote/retry-incomplete.bin".to_string(), 3)
            .with_source_version(4096, 1710000004, None, None);
        db.save_transfer_state(&job.job_id, &incomplete)
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        let job_cache = scheduler
            .synced_files_cache
            .get(&job.job_id)
            .expect("retry recovery should rebuild synced file cache");
        assert_eq!(job_cache.len(), 1);
        let restored = job_cache
            .get("remote/retry-finished.bin")
            .expect("completed retry file should be restored");
        assert_eq!(restored.size, 8192);
        assert_eq!(restored.mtime, 1710000003);
        assert_eq!(restored.dest_mtime, Some(1710000300));
        assert_eq!(restored.dest_change_time, Some(1710000301));
        assert_eq!(restored.dest_inode, Some(43));
        assert!(job_cache.get("remote/retry-incomplete.bin").is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }
