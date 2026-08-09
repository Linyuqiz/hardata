    #[tokio::test]
    async fn cleanup_terminal_job_artifacts_ignores_corrupted_active_rows() {
        let root = temp_dir("cleanup-terminal-ignore-active-corruption");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut completed = Job::new(
            "job-terminal-cleanup-ok".to_string(),
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
        completed.status = JobStatus::Cancelled;
        db.save_job(&completed).await.unwrap();
        db.save_retry(&completed.job_id, "stale retry")
            .await
            .unwrap();
        db.save_transfer_state(
            &completed.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        let tmp_path = root.join("cleanup-terminal-ok.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&completed.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        let mut active = Job::new(
            "job-active-corrupted".to_string(),
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
        active.status = JobStatus::Pending;
        db.save_job(&active).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&active.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.cleanup_terminal_job_artifacts().await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db.get_retry(&completed.job_id).await.unwrap().is_none());
        assert!(db
            .load_transfer_state(&completed.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&completed.job_id)
            .await
            .unwrap()
            .is_empty());

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }
