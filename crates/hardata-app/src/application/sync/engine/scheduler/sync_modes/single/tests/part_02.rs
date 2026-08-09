    #[tokio::test]
    async fn sync_single_file_fails_when_transfer_state_load_fails() {
        let root = temp_dir("state-load-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        std::fs::write(&source, b"ABCD").unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Append,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-load-failure".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("DROP TABLE transfer_states")
            .execute(&raw_pool)
            .await
            .unwrap();

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };

        let err = sync_single_file_with_mode(
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
            ReplicateMode::Append,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("no such table: transfer_states"));
        assert!(!dest.exists());

        raw_pool.close().await;
        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_discards_stale_resumable_state_when_source_changes() {
        let root = temp_dir("state-source-version-change");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"AAAA").unwrap();
        std::fs::write(&tmp, b"AAAA").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-source-changed".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let stale_metadata = std::fs::metadata(&source).unwrap();
        let stale_modified = metadata_mtime_nanos(&stale_metadata);
        let mut stale_state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(stale_metadata.len(), stale_modified, None, None);
        stale_state.mark_chunk_completed(0);
        transfer_manager_pool
            .save_state(&job.job_id, &stale_state)
            .await
            .unwrap();

        let (metadata, modified) = {
            let mut changed = None;
            for _ in 0..20 {
                std::thread::sleep(Duration::from_millis(5));
                std::fs::write(&source, b"BBBB").unwrap();
                let metadata = std::fs::metadata(&source).unwrap();
                let modified = metadata_mtime_nanos(&metadata);
                if modified != stale_modified {
                    changed = Some((metadata, modified));
                    break;
                }
            }
            changed.expect("expected rewritten source file to have a distinct mtime")
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified,
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
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

        assert_eq!(std::fs::read(&dest).unwrap(), b"BBBB");
        assert!(!tmp.exists());
        let checkpoint = db
            .load_transfer_state(&job.job_id, source.to_str().unwrap())
            .await
            .unwrap()
            .expect("completed sync should persist a recovery checkpoint");
        assert!(checkpoint.cache_only);
        assert_eq!(checkpoint.source_size, Some(metadata.len()));
        assert_eq!(checkpoint.source_modified, Some(modified));
        assert!(checkpoint.dest_modified.is_some());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sync_single_file_discards_stale_resumable_state_when_source_content_changes_same_size_and_preserved_mtime(
    ) {
        use filetime::{set_file_mtime, FileTime};

        let root = temp_dir("state-source-identity-change");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"AAAA").unwrap();
        std::fs::write(&tmp, b"AAAA").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-source-identity".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let stale_metadata = std::fs::metadata(&source).unwrap();
        let stale_modified = metadata_mtime_nanos(&stale_metadata);
        let stale_change_time = metadata_ctime_nanos(&stale_metadata);
        let stale_inode = metadata_inode(&stale_metadata);
        let mut stale_state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(
                stale_metadata.len(),
                stale_modified,
                stale_change_time,
                stale_inode,
            );
        stale_state.mark_chunk_completed(0);
        transfer_manager_pool
            .save_state(&job.job_id, &stale_state)
            .await
            .unwrap();

        let original_source_mtime =
            FileTime::from_last_modification_time(&std::fs::metadata(&source).unwrap());
        let (metadata, change_time, inode) = {
            let mut changed = None;
            for _ in 0..20 {
                std::thread::sleep(Duration::from_millis(5));
                std::fs::write(&source, b"BBBB").unwrap();
                set_file_mtime(&source, original_source_mtime).unwrap();
                let metadata = std::fs::metadata(&source).unwrap();
                let change_time = metadata_ctime_nanos(&metadata);
                let inode = metadata_inode(&metadata);
                if change_time != stale_change_time || inode != stale_inode {
                    changed = Some((metadata, change_time, inode));
                    break;
                }
            }
            changed.expect("expected rewritten source file to change source identity")
        };
        let modified = metadata_mtime_nanos(&metadata);
        assert_eq!(modified, stale_modified);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified,
            change_time,
            inode,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
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

        assert_eq!(std::fs::read(&dest).unwrap(), b"BBBB");
        assert!(!tmp.exists());
        let checkpoint = db
            .load_transfer_state(&job.job_id, source.to_str().unwrap())
            .await
            .unwrap()
            .expect("completed sync should persist refreshed checkpoint");
        assert_eq!(checkpoint.source_size, Some(metadata.len()));
        assert_eq!(checkpoint.source_modified, Some(modified));
        assert_eq!(checkpoint.source_change_time, change_time);
        assert_eq!(checkpoint.source_inode, inode);

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_discards_stale_resumable_state_when_tmp_file_is_missing() {
        let root = temp_dir("state-tmp-file-missing");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"BBBB").unwrap();
        std::fs::write(&tmp, b"AAAA").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-tmp-missing".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let tmp_version = file_ops::load_regular_file_version(&tmp)
            .await
            .unwrap()
            .unwrap();
        let mut stale_state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(metadata.len(), metadata_mtime_nanos(&metadata), None, None)
            .with_destination_version(
                tmp_version.size,
                tmp_version.modified,
                tmp_version.change_time,
                tmp_version.inode,
            );
        stale_state.mark_chunk_completed(0);
        transfer_manager_pool
            .save_state(&job.job_id, &stale_state)
            .await
            .unwrap();
        std::fs::remove_file(&tmp).unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
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

        assert_eq!(std::fs::read(&dest).unwrap(), b"BBBB");
        assert!(!tmp.exists());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
