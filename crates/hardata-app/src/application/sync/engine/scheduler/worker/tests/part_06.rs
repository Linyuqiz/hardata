    #[tokio::test]
    async fn reset_progress_for_new_sync_round_preserves_previous_round_totals() {
        let root = temp_dir("worker-reset-progress");
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
        let mut db_job = crate::domain::Job::new(
            "job-worker-reset-progress".to_string(),
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
        db_job.status = crate::domain::JobStatus::Pending;
        db_job.progress = 100;
        db_job.current_size = 64;
        db_job.total_size = 64;
        db.save_job(&db_job).await.unwrap();
        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::domain::JobStatus::Pending,
                progress: 100,
                current_size: 64,
                total_size: 64,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        let mut cached_job = crate::application::sync::engine::job::SyncJob::new(
            db_job.job_id.clone(),
            std::path::PathBuf::from(&db_job.source.path),
            db_job.dest.path.clone(),
            db_job.region.clone(),
        )
        .with_job_type(JobType::Sync);
        cached_job.round_id = 1;
        scheduler
            .job_cache
            .insert(db_job.job_id.clone(), cached_job);
        let mut next_round_job = crate::application::sync::engine::job::SyncJob::new(
            db_job.job_id.clone(),
            std::path::PathBuf::from(&db_job.source.path),
            db_job.dest.path.clone(),
            db_job.region.clone(),
        )
        .with_job_type(JobType::Sync);
        next_round_job.round_id = 2;
        scheduler
            .reset_progress_for_new_round(&next_round_job, Some(1))
            .await;
        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.progress, 100);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 64);
        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 64);
        assert_eq!(snapshot.total_size, 64);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn reset_progress_for_new_sync_round_preserves_existing_runtime_error_state() {
        let root = temp_dir("worker-reset-progress-preserve-state");
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
        let mut db_job = crate::domain::Job::new(
            "job-worker-reset-progress-preserve-state".to_string(),
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
        db_job.status = crate::domain::JobStatus::Pending;
        db_job.progress = 100;
        db_job.current_size = 64;
        db_job.total_size = 64;
        db.save_job(&db_job).await.unwrap();
        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::domain::JobStatus::Pending,
                progress: 100,
                current_size: 64,
                total_size: 64,
                region: db_job.region.clone(),
                error_message: Some("previous error".to_string()),
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        let mut next_round_job = crate::application::sync::engine::job::SyncJob::new(
            db_job.job_id.clone(),
            std::path::PathBuf::from(&db_job.source.path),
            db_job.dest.path.clone(),
            db_job.region.clone(),
        )
        .with_job_type(JobType::Sync);
        next_round_job.round_id = 2;
        scheduler
            .reset_progress_for_new_round(&next_round_job, Some(1))
            .await;
        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.progress, 100);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 64);
        assert_eq!(runtime.error_message.as_deref(), Some("previous error"));
        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 64);
        assert_eq!(snapshot.total_size, 64);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn handle_progress_update_ignores_completed_runtime_state() {
        let root = temp_dir("worker-ignore-stale-progress");
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
        let mut db_job = crate::domain::Job::new(
            "job-worker-ignore-stale-progress".to_string(),
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
        db_job.status = crate::domain::JobStatus::Completed;
        db_job.progress = 100;
        db_job.current_size = 64;
        db_job.total_size = 64;
        db.save_job(&db_job).await.unwrap();
        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::domain::JobStatus::Completed,
                progress: 100,
                current_size: 64,
                total_size: 64,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler
            .handle_progress_update(&db_job.job_id, 50, 32, 64)
            .await;
        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.status, crate::domain::JobStatus::Completed);
        assert_eq!(runtime.progress, 100);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 64);
        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::domain::JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 64);
        assert_eq!(snapshot.total_size, 64);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn handle_progress_update_keeps_runtime_when_persist_fails() {
        let root = temp_dir("worker-progress-persist-failure");
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
        let mut db_job = crate::domain::Job::new(
            "job-worker-progress-persist-failure".to_string(),
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
        db_job.status = crate::domain::JobStatus::Syncing;
        db_job.progress = 25;
        db_job.current_size = 32;
        db_job.total_size = 128;
        db.save_job(&db_job).await.unwrap();
        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::domain::JobStatus::Syncing,
                progress: 25,
                current_size: 32,
                total_size: 128,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_progress_update
            BEFORE UPDATE ON jobs
            WHEN NEW.job_id = 'job-worker-progress-persist-failure'
              AND NEW.progress != OLD.progress
            BEGIN
                SELECT RAISE(FAIL, 'reject progress update');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();
        scheduler
            .handle_progress_update(&db_job.job_id, 50, 64, 128)
            .await;
        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.status, crate::domain::JobStatus::Syncing);
        assert_eq!(runtime.progress, 25);
        assert_eq!(runtime.current_size, 32);
        assert_eq!(runtime.total_size, 128);
        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::domain::JobStatus::Syncing);
        assert_eq!(snapshot.progress, 25);
        assert_eq!(snapshot.current_size, 32);
        assert_eq!(snapshot.total_size, 128);
        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn handle_progress_update_does_not_overwrite_terminal_snapshot() {
        let root = temp_dir("worker-progress-terminal-snapshot");
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
        let mut db_job = crate::domain::Job::new(
            "job-worker-progress-terminal-snapshot".to_string(),
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
        db_job.status = crate::domain::JobStatus::Completed;
        db_job.progress = 100;
        db_job.current_size = 128;
        db_job.total_size = 128;
        db.save_job(&db_job).await.unwrap();
        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::domain::JobStatus::Syncing,
                progress: 40,
                current_size: 64,
                total_size: 128,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler
            .handle_progress_update(&db_job.job_id, 50, 96, 128)
            .await;
        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.status, crate::domain::JobStatus::Syncing);
        assert_eq!(runtime.progress, 40);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 128);
        let snapshot = db.load_job(&db_job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, crate::domain::JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 128);
        assert_eq!(snapshot.total_size, 128);
        std::fs::remove_dir_all(root).unwrap();
    }
