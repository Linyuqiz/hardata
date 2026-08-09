    #[tokio::test]
    async fn notify_job_started_does_not_revive_cancelled_snapshot_without_runtime() {
        let (temp_dir, db, scheduler) = create_scheduler("started-no-revive").await;
        let job = cancelled_job("job-notify-started-cancelled");
        db.save_job(&job).await.unwrap();

        scheduler.notify_job_started(&job.job_id).await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_does_not_revive_cancelled_snapshot_without_runtime() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-no-revive").await;
        let job = cancelled_job("job-notify-completed-cancelled");
        db.save_job(&job).await.unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_persist_failure_keeps_runtime_state() {
        let temp_dir = create_temp_dir("completed-persist-failure");
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
            "job-notify-completed-persist-failure".to_string(),
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
        job.progress = 90;
        job.current_size = 900;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 90,
                current_size: 900,
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

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_completed_update
            BEFORE UPDATE ON jobs
            WHEN NEW.status = 'completed'
            BEGIN
                SELECT RAISE(FAIL, 'reject completed status');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Completed
        );
        assert!(scheduler.get_job_info(&job.job_id).is_some());
        assert_eq!(
            db.load_job(&job.job_id).await.unwrap().unwrap().status,
            JobStatus::Syncing
        );
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_missing_row_keeps_runtime_state() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-missing-row").await;
        let mut job = Job::new(
            "job-notify-completed-missing-row".to_string(),
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
        job.progress = 88;
        job.current_size = 880;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 88,
                current_size: 880,
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

        let raw_pool =
            open_raw_pool(format!("sqlite://{}", temp_dir.join("state.db").display()).as_str())
                .await;
        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Completed
        );
        assert!(scheduler.get_job_info(&job.job_id).is_some());
        assert!(db.load_job(&job.job_id).await.unwrap().is_none());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_without_runtime_preserves_snapshot_sizes() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-without-runtime").await;
        let mut job = Job::new(
            "job-notify-completed-without-runtime".to_string(),
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
        job.progress = 80;
        job.current_size = 800;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 1000);
        assert_eq!(snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_pending_clears_runtime_error_message() {
        let (temp_dir, db, scheduler) = create_scheduler("pending-clears-runtime-error").await;
        let mut job = Job::new(
            "job-notify-pending-clears-error".to_string(),
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
        job.progress = 45;
        job.current_size = 450;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 45,
                current_size: 450,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("previous failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_pending(&job.job_id).await;

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert!(runtime.error_message.is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_clears_runtime_error_message() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-clears-runtime-error").await;
        let mut job = Job::new(
            "job-notify-completed-clears-error".to_string(),
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
        job.progress = 100;
        job.current_size = 1000;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 99,
                current_size: 990,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("stale failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_completed(&job.job_id).await;

        let runtime = scheduler.get_job_status(&job.job_id);
        assert!(runtime.is_none());

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 1000);
        assert_eq!(snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_cancelled_removes_tmp_artifacts() {
        let (temp_dir, db, scheduler) = create_scheduler("cancelled-cleans-tmp").await;
        let mut job = Job::new(
            "job-notify-cancelled".to_string(),
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
                progress: 33,
                current_size: 128,
                total_size: 512,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let tmp_path = temp_dir.join("cancelled.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.notify_job_cancelled(&job.job_id).await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(scheduler
            .transfer_manager_pool
            .job_tmp_write_paths(&job.job_id)
            .is_empty());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cleanup_job_tmp_artifacts_preserves_registration_when_removal_fails() {
        let (temp_dir, db, scheduler) = create_scheduler("cleanup-tmp-removal-failure").await;
        let job = Job::new(
            "job-tmp-removal-failure".to_string(),
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
        db.save_job(&job).await.unwrap();

        let tmp_dir = temp_dir.join("stuck.tmp");
        fs::create_dir_all(&tmp_dir).unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_dir.to_str().unwrap())
            .await
            .unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&job.job_id, tmp_dir.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cleanup_job_tmp_artifacts(&job.job_id).await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(tmp_dir.exists());
        assert_eq!(
            db.load_tmp_transfer_paths_by_job(&job.job_id)
                .await
                .unwrap(),
            vec![tmp_dir.to_string_lossy().to_string()]
        );
        assert_eq!(
            scheduler
                .transfer_manager_pool
                .job_tmp_write_paths(&job.job_id),
            vec![tmp_dir.to_string_lossy().to_string()]
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_cancelled_snapshot_failure_keeps_persisted_state() {
        let temp_dir = create_temp_dir("cancelled-snapshot-failure");
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
            "job-notify-cancelled-snapshot-failure".to_string(),
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
        job.progress = 40;
        job.current_size = 400;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.notify_job_cancelled(&job.job_id).await;

        let row = sqlx::query(
            "SELECT status, progress, current_size, total_size FROM jobs WHERE job_id = ?1",
        )
        .bind(&job.job_id)
        .fetch_one(&raw_pool)
        .await
        .unwrap();
        assert_eq!(row.try_get::<String, _>("status").unwrap(), "pending");
        assert_eq!(row.try_get::<i64, _>("progress").unwrap(), 40);
        assert_eq!(row.try_get::<i64, _>("current_size").unwrap(), 400);
        assert_eq!(row.try_get::<i64, _>("total_size").unwrap(), 1000);
        assert!(db.get_retry(&job.job_id).await.unwrap().is_some());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());

        raw_pool.close().await;
        scheduler.transfer_manager_pool.shutdown().await;
        let _ = fs::remove_dir_all(temp_dir);
    }
