    #[tokio::test]
    async fn process_pending_retries_removes_inserted_runtime_job_when_retry_attempt_update_fails()
    {
        let temp_dir = create_temp_dir("retry-attempt-update-failure");
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
            "job-retry-attempt-update-failure".to_string(),
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
        original.progress = 42;
        original.current_size = 420;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        let mut job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        job.status = JobStatus::Failed;
        job.progress = 42;
        job.current_size = 420;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let scheduler_pool = SqlitePool::connect(&db_path).await.unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&scheduler_pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE job_retries")
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap_err();

        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert_eq!(scheduler.job_queue.len().await, 0);

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);
        assert_eq!(snapshot.progress, 42);
        assert_eq!(snapshot.current_size, 420);
        assert_eq!(snapshot.total_size, 1000);

        let original_snapshot = db.load_job(&original.job_id).await.unwrap().unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert_eq!(original_snapshot.progress, 42);
        assert_eq!(original_snapshot.current_size, 420);
        assert_eq!(original_snapshot.total_size, 1000);

        scheduler_pool.close().await;
        pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_skips_jobs_already_paused() {
        let temp_dir = create_temp_dir("skip-paused");
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
            "job-retry-paused".to_string(),
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
        job.status = JobStatus::Paused;
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

        assert_eq!(scheduler.job_queue.len().await, 0);
        let retry = db.get_retry(&job.job_id).await.unwrap().unwrap();
        assert_eq!(retry.retry_count, 0);
        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Paused);

        let _ = fs::remove_dir_all(temp_dir);
    }
