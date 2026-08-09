    #[tokio::test]
    async fn sync_single_file_tmp_mode_cleans_tmp_when_registration_fails() {
        let root = temp_dir("tmp-register-failure-cleans");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        transfer_manager_pool.shutdown().await;

        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-register-fails".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(!tmp.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_preserves_existing_tmp_when_registration_fails() {
        let root = temp_dir("tmp-register-failure-preserves-existing");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        transfer_manager_pool.shutdown().await;

        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"").unwrap();
        std::fs::write(&tmp, b"resume-state").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-register-fails-preserve".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(std::fs::read(&tmp).unwrap(), b"resume-state");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn finalize_file_tmp_mode_replaces_existing_destination() {
        let root = temp_dir("finalize-tmp-replaces-existing");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&dest, b"old").unwrap();
        std::fs::write(&tmp, b"new").unwrap();

        finalize_file(dest.to_str().unwrap(), ReplicateMode::Tmp)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
        assert!(!tmp.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_append_mode_truncates_existing_destination_for_empty_file() {
        let root = temp_dir("append-empty-truncates-dest");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        std::fs::write(&source, b"").unwrap();
        std::fs::write(&dest, b"stale-data").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Append,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-append-empty".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        sync_single_file(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::metadata(&dest).unwrap().len(), 0);

        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_truncates_stale_tmp_for_empty_file() {
        let root = temp_dir("tmp-empty-truncates-stale-tmp");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"").unwrap();
        std::fs::write(&tmp, b"stale-tmp-data").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-empty".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::metadata(&dest).unwrap().len(), 0);
        assert!(!tmp.exists());

        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
