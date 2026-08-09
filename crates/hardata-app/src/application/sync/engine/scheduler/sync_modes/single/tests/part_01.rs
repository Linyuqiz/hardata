    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_regular_file_destination_removes_existing_symlink() {
        let root = temp_dir("prepare-regular-file");
        let outside = root.join("outside.txt");
        let dest = root.join("dest/file.txt");
        let write_path = format!("{}.tmp", dest.to_string_lossy());

        std::fs::write(&outside, b"outside").unwrap();
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&outside, &dest).unwrap();

        prepare_regular_file_destination(
            dest.to_str().unwrap(),
            PathBuf::from(&write_path).to_str().unwrap(),
        )
        .await
        .unwrap();

        let dest_metadata = tokio::fs::symlink_metadata(&dest).await;
        assert!(dest_metadata.is_err());
        assert!(dest.parent().unwrap().exists());
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "outside");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn should_cleanup_tmp_after_transfer_error_only_for_cancelled_jobs() {
        assert!(should_cleanup_tmp_after_transfer_error(
            &HarDataError::Unknown("Job cancelled by user".to_string(),)
        ));
        assert!(!should_cleanup_tmp_after_transfer_error(
            &HarDataError::NetworkError("network failed".to_string(),)
        ));
    }

    #[test]
    fn resolve_dedup_source_path_falls_back_to_existing_destination_in_tmp_mode() {
        let root = temp_dir("dedup-source-dest");
        let dest = root.join("dest.bin");
        let write_path = format!("{}.tmp", dest.to_string_lossy());
        std::fs::write(&dest, b"dest").unwrap();

        assert_eq!(
            resolve_dedup_source_path(
                dest.to_str().unwrap(),
                PathBuf::from(&write_path).to_str().unwrap(),
                crate::application::sync::engine::scheduler::ReplicateMode::Tmp,
            ),
            dest.to_str().unwrap()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn load_transfer_state_accepts_legacy_second_precision_source_mtime() {
        let root = temp_dir("legacy-source-mtime");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        std::fs::write(&source, b"AAAA").unwrap();

        let metadata = std::fs::metadata(&source).unwrap();
        let legacy_seconds = metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(metadata.len(), legacy_seconds, None, None);
        transfer_manager_pool
            .save_state("job-legacy-source-mtime", &state)
            .await
            .unwrap();

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

        let restored = load_transfer_state_for_current_source(
            &transfer_manager_pool,
            "job-legacy-source-mtime",
            source.to_str().unwrap(),
            &file,
            dest.to_str().unwrap(),
            1,
        )
        .await
        .unwrap();

        assert_eq!(restored.source_size, Some(metadata.len()));
        assert_eq!(restored.source_modified, Some(legacy_seconds));

        transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_dedup_source_path_prefers_existing_tmp_file_when_resuming() {
        let root = temp_dir("dedup-source-tmp");
        let dest = root.join("dest.bin");
        let write_path = root.join("dest.bin.tmp");
        std::fs::write(&dest, b"dest").unwrap();
        std::fs::write(&write_path, b"tmp").unwrap();

        assert_eq!(
            resolve_dedup_source_path(
                dest.to_str().unwrap(),
                write_path.to_str().unwrap(),
                crate::application::sync::engine::scheduler::ReplicateMode::Tmp,
            ),
            write_path.to_str().unwrap()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_base_dest_path_rejects_prefix_collision() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-prefix".to_string(),
            PathBuf::from("/tmp/source"),
            "syncfoo/out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/syncfoo/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_rebases_parent_traversal_under_data_dir() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-parent-traversal".to_string(),
            PathBuf::from("/tmp/source"),
            "../escape/out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/escape/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_normalizes_embedded_parent_segments() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-embedded-parent".to_string(),
            PathBuf::from("/tmp/source"),
            "sync/nested/../out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_preserves_true_subpath() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-subpath".to_string(),
            PathBuf::from("/tmp/source"),
            "sync/out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_rejects_external_absolute_destination_by_default() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-external".to_string(),
            PathBuf::from("/tmp/source"),
            "/tmp/out.txt".to_string(),
            "local".to_string(),
        );

        let err = resolve_base_dest_path(&config, &job).unwrap_err();
        assert!(err.to_string().contains("outside sync.data_dir"));
    }

    #[test]
    fn calculate_dest_path_allows_external_absolute_destination_when_enabled() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            allow_external_destinations: true,
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-external-allowed".to_string(),
            PathBuf::from("/tmp/source"),
            "/tmp/out".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            calculate_dest_path(&config, &job, "nested/file.txt", 2).unwrap(),
            "/tmp/out/nested/file.txt"
        );
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_does_not_publish_after_cancelled_local_reuse() {
        let root = temp_dir("tmp-cancelled-publish");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"ABCD").unwrap();
        std::fs::write(&dest, b"ABCD").unwrap();

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
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-cancel-before-publish".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = Arc::new(DashMap::new());
        job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 0,
                current_size: 0,
                total_size: 4,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

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

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };
        let progress_cache = job_status_cache.clone();
        let cancelled_jobs = Arc::new(DashMap::new());
        let cancelled_jobs_for_callback = cancelled_jobs.clone();
        let job_id = job.job_id.clone();

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            job_status_cache.as_ref(),
            cancelled_jobs.as_ref(),
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            move |_| {
                if let Some(mut status) = progress_cache.get_mut(&job_id) {
                    status.status = JobStatus::Cancelled;
                }
                cancelled_jobs_for_callback.insert(job_id.clone(), ());
            },
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Job cancelled by user"));
        assert_eq!(std::fs::read(&dest).unwrap(), b"ABCD");
        assert!(!tmp.exists());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_does_not_publish_after_runtime_cancel_cleanup() {
        let root = temp_dir("tmp-cancelled-runtime-cleanup");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"ABCD").unwrap();
        std::fs::write(&dest, b"ABCD").unwrap();

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
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-cancel-after-runtime-cleanup".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = Arc::new(DashMap::new());
        let cancelled_jobs = Arc::new(DashMap::new());
        job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 0,
                current_size: 0,
                total_size: 4,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

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

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };
        let progress_cache = job_status_cache.clone();
        let cancelled_jobs_for_callback = cancelled_jobs.clone();
        let job_id = job.job_id.clone();

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            job_status_cache.as_ref(),
            cancelled_jobs.as_ref(),
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            move |_| {
                progress_cache.remove(&job_id);
                cancelled_jobs_for_callback.insert(job_id.clone(), ());
            },
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Job cancelled by user"));
        assert_eq!(std::fs::read(&dest).unwrap(), b"ABCD");
        assert!(!tmp.exists());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
