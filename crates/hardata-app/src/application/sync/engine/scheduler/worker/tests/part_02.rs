    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_destination_is_missing() {
        let root = temp_dir("worker-resync-missing-destination");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), payload).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(
            ComputeService::new(remote_root.to_string_lossy().as_ref())
                .await
                .unwrap(),
        );
        let server = TcpServer::new(&bind_addr, compute, remote_root.to_string_lossy().as_ref())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::application::config::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            config.clone(),
            db.clone(),
        )
        .await
        .unwrap();
        let job_id = "job-resync-missing-destination".to_string();
        let mut persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.region = "local".to_string();
        db.save_job(&persisted_job).await.unwrap();
        let runtime_job = SyncJob::new(
            job_id.clone(),
            source_dir.clone(),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let mut second_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if second_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            second_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            second_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let dest_file = data_dir.join("target").join("payload.bin");
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        scheduler.notify_job_pending(&job_id).await;
        std::fs::remove_file(&dest_file).unwrap();
        let third_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            third_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_destination_content_changes_same_size_and_preserved_mtime(
    ) {
        use filetime::{set_file_mtime, FileTime};
        let root = temp_dir("worker-resync-same-size-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        let corrupted = vec![b'X'; payload.len()];
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), payload).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(
            ComputeService::new(remote_root.to_string_lossy().as_ref())
                .await
                .unwrap(),
        );
        let server = TcpServer::new(&bind_addr, compute, remote_root.to_string_lossy().as_ref())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::application::config::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            config.clone(),
            db.clone(),
        )
        .await
        .unwrap();
        let job_id = "job-resync-same-size-drift".to_string();
        let mut persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.region = "local".to_string();
        db.save_job(&persisted_job).await.unwrap();
        let runtime_job = SyncJob::new(
            job_id.clone(),
            source_dir.clone(),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let mut result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let source_path = source_dir.join("payload.bin").to_string_lossy().to_string();
        let cached_dest_mtime = {
            let cached = scheduler
                .synced_files_cache
                .get(&job_id)
                .expect("sync cache should exist after first transfer");
            let dest_mtime = cached
                .get(&source_path)
                .expect("source entry should exist in sync cache")
                .dest_mtime;
            dest_mtime
        };
        assert!(cached_dest_mtime.is_some());
        let dest_file = data_dir.join("target").join("payload.bin");
        let original_dest_mtime =
            FileTime::from_last_modification_time(&std::fs::metadata(&dest_file).unwrap());
        scheduler.notify_job_pending(&job_id).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        std::fs::write(&dest_file, &corrupted).unwrap();
        set_file_mtime(&dest_file, original_dest_mtime).unwrap();
        assert_eq!(std::fs::read(&dest_file).unwrap(), corrupted);
        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_source_content_changes_same_size_and_preserved_mtime(
    ) {
        use crate::shared::time::{metadata_ctime_nanos, metadata_inode};
        use filetime::{set_file_mtime, FileTime};
        let root = temp_dir("worker-resync-same-size-source-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        let updated = vec![b'Z'; payload.len()];
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(
            ComputeService::new(remote_root.to_string_lossy().as_ref())
                .await
                .unwrap(),
        );
        let server = TcpServer::new(&bind_addr, compute, remote_root.to_string_lossy().as_ref())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::application::config::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            config.clone(),
            db.clone(),
        )
        .await
        .unwrap();
        let job_id = "job-resync-same-size-source-drift".to_string();
        let mut persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.region = "local".to_string();
        db.save_job(&persisted_job).await.unwrap();
        let runtime_job = SyncJob::new(
            job_id.clone(),
            source_dir.clone(),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let source_metadata = std::fs::metadata(&source_file).unwrap();
        let original_source_mtime = FileTime::from_last_modification_time(&source_metadata);
        let original_source_change_time = metadata_ctime_nanos(&source_metadata);
        let original_source_inode = metadata_inode(&source_metadata);
        scheduler.notify_job_pending(&job_id).await;
        let mut identity_changed = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            std::fs::write(&source_file, &updated).unwrap();
            set_file_mtime(&source_file, original_source_mtime).unwrap();
            let metadata = std::fs::metadata(&source_file).unwrap();
            if metadata_ctime_nanos(&metadata) != original_source_change_time
                || metadata_inode(&metadata) != original_source_inode
            {
                identity_changed = true;
                break;
            }
        }
        assert!(
            identity_changed,
            "expected rewritten source file to change identity"
        );
        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let dest_file = data_dir.join("target").join("payload.bin");
        assert_eq!(std::fs::read(&dest_file).unwrap(), updated);
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_syncs_root_directory_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("worker-syncs-root-directory-permissions");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::set_permissions(&source_dir, std::fs::Permissions::from_mode(0o711)).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();
        let dest_root = data_dir.join("target");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::set_permissions(&dest_root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(
            ComputeService::new(remote_root.to_string_lossy().as_ref())
                .await
                .unwrap(),
        );
        let server = TcpServer::new(&bind_addr, compute, remote_root.to_string_lossy().as_ref())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::application::config::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            config.clone(),
            db.clone(),
        )
        .await
        .unwrap();
        let job_id = "job-syncs-root-directory-permissions".to_string();
        let mut persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.region = "local".to_string();
        db.save_job(&persisted_job).await.unwrap();
        let runtime_job = SyncJob::new(
            job_id.clone(),
            source_dir.clone(),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let mut first_result = scheduler.execute_job(runtime_job).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler
                .execute_job(
                    SyncJob::new(
                        job_id.clone(),
                        source_dir.clone(),
                        "target".to_string(),
                        "local".to_string(),
                    )
                    .with_job_type(JobType::Sync),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let dest_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(dest_mode, 0o711);
        assert_eq!(
            std::fs::read(dest_root.join("payload.bin")).unwrap(),
            payload
        );
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_destination_permissions_change() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("worker-resync-permission-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();
        std::fs::set_permissions(&source_file, std::fs::Permissions::from_mode(0o640)).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(
            ComputeService::new(remote_root.to_string_lossy().as_ref())
                .await
                .unwrap(),
        );
        let server = TcpServer::new(&bind_addr, compute, remote_root.to_string_lossy().as_ref())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::application::config::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            config.clone(),
            db.clone(),
        )
        .await
        .unwrap();
        let job_id = "job-resync-permission-drift".to_string();
        let mut persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.region = "local".to_string();
        db.save_job(&persisted_job).await.unwrap();
        let runtime_job = SyncJob::new(
            job_id.clone(),
            source_dir.clone(),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let dest_file = data_dir.join("target").join("payload.bin");
        let mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(mode, 0o640);
        scheduler.notify_job_pending(&job_id).await;
        std::fs::set_permissions(&dest_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(drifted_mode, 0o600);
        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let restored_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(restored_mode, 0o640);
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
