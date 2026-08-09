    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_root_directory_when_destination_permissions_change() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("worker-resyncs-root-directory-permissions");
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
        let job_id = "job-resyncs-root-directory-permissions".to_string();
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
        let dest_root = data_dir.join("target");
        let initial_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(initial_mode, 0o711);
        scheduler.notify_job_pending(&job_id).await;
        std::fs::set_permissions(&dest_root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(drifted_mode, 0o755);
        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let restored_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(restored_mode, 0o711);
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
    async fn execute_job_syncs_nested_directory_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("worker-syncs-nested-directory-permissions");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let nested_source_dir = source_dir.join("nested");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&nested_source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::set_permissions(&nested_source_dir, std::fs::Permissions::from_mode(0o711))
            .unwrap();
        let source_file = nested_source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();
        let dest_nested_dir = data_dir.join("target").join("nested");
        std::fs::create_dir_all(&dest_nested_dir).unwrap();
        std::fs::set_permissions(&dest_nested_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
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
        let job_id = "job-syncs-nested-directory-permissions".to_string();
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
        let dest_file = dest_nested_dir.join("payload.bin");
        let dir_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(dir_mode, 0o711);
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn execute_job_after_recovery_skips_completed_sync_file_without_retransfer() {
        let root = temp_dir("worker-recovery-skips-retransfer");
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
        let job_id = "job-recovery-skips-retransfer".to_string();
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
        let source_path = source_dir.join("payload.bin").to_string_lossy().to_string();
        let checkpoint = db
            .load_transfer_state(&job_id, &source_path)
            .await
            .unwrap()
            .expect("completed sync round should persist checkpoint");
        assert!(checkpoint.cache_only);
        assert!(checkpoint.dest_modified.is_some());
        assert!(checkpoint.dest_change_time.is_some());
        assert!(checkpoint.dest_inode.is_some());
        let mut paused_job = db.load_job(&job_id).await.unwrap().unwrap();
        paused_job.status = crate::domain::JobStatus::Paused;
        db.save_job(&paused_job).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;
        let recovered_config = SchedulerConfig {
            chunk_cache_path: root
                .join("chunk-cache-recovered")
                .to_string_lossy()
                .to_string(),
            ..config
        };
        let recovered_scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            recovered_config,
            db.clone(),
        )
        .await
        .unwrap();
        recovered_scheduler.recover_pending_jobs().await.unwrap();
        {
            let restored = recovered_scheduler
                .synced_files_cache
                .get(&job_id)
                .expect("recovery should restore completed sync cache");
            let restored_file = restored
                .get(&source_path)
                .expect("completed file checkpoint should be restored");
            assert!(restored_file.dest_mtime.is_some());
            assert!(restored_file.dest_change_time.is_some());
            assert!(restored_file.dest_inode.is_some());
        }
        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::NoTransfer {
                retry_due_to_stability: false
            }
        );
        let dest_file = data_dir.join("target").join("payload.bin");
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_after_recovery_skips_unchanged_symlink_without_retransfer() {
        let root = temp_dir("worker-recovery-skips-symlink-retransfer");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), b"payload-round-trip").unwrap();
        std::os::unix::fs::symlink("payload.bin", source_dir.join("current.bin")).unwrap();
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
        let job_id = "job-recovery-skips-symlink-retransfer".to_string();
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
        let mut paused_job = db.load_job(&job_id).await.unwrap().unwrap();
        paused_job.status = crate::domain::JobStatus::Paused;
        db.save_job(&paused_job).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;
        let recovered_config = SchedulerConfig {
            chunk_cache_path: root
                .join("chunk-cache-recovered")
                .to_string_lossy()
                .to_string(),
            ..config
        };
        let recovered_scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            recovered_config,
            db.clone(),
        )
        .await
        .unwrap();
        recovered_scheduler.recover_pending_jobs().await.unwrap();
        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::NoTransfer {
                retry_due_to_stability: false
            }
        );
        let dest_symlink = data_dir.join("target").join("current.bin");
        assert!(std::fs::symlink_metadata(&dest_symlink)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_link(&dest_symlink).unwrap(),
            std::path::PathBuf::from("payload.bin")
        );
        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
