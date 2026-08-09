    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_after_recovery_resyncs_permission_drift() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("worker-recovery-resyncs-permission-drift");
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
        let job_id = "job-recovery-resyncs-permission-drift".to_string();
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
        let dest_file = data_dir.join("target").join("payload.bin");
        std::fs::set_permissions(&dest_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(drifted_mode, 0o600);
        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let restored_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(restored_mode, 0o640);
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_nested_directory_when_destination_permissions_change() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_dir("worker-resyncs-nested-directory-permissions");
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
        let job_id = "job-resyncs-nested-directory-permissions".to_string();
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
        let dest_nested_dir = data_dir.join("target").join("nested");
        let initial_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(initial_mode, 0o711);
        scheduler.notify_job_pending(&job_id).await;
        std::fs::set_permissions(&dest_nested_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(drifted_mode, 0o755);
        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let restored_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(restored_mode, 0o711);
        assert_eq!(
            std::fs::read(dest_nested_dir.join("payload.bin")).unwrap(),
            payload
        );
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn execute_job_after_recovery_resyncs_same_size_destination_drift() {
        let root = temp_dir("worker-recovery-resyncs-same-size-drift");
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
        let job_id = "job-recovery-resyncs-same-size-drift".to_string();
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
        let dest_file = data_dir.join("target").join("payload.bin");
        tokio::time::sleep(Duration::from_millis(2)).await;
        std::fs::write(&dest_file, &corrupted).unwrap();
        assert_eq!(std::fs::read(&dest_file).unwrap(), corrupted);
        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);
        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn worker_loop_keeps_retry_record_when_final_completion_persist_fails() {
        let root = temp_dir("worker-final-completion-retry-preserved");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
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
        let job_id = "job-worker-final-completion-retry".to_string();
        let persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: "/remote/source".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        db.save_job(&persisted_job).await.unwrap();
        db.save_retry(&job_id, "temporary network error")
            .await
            .unwrap();
        scheduler.job_status_cache.insert(
            job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "local".to_string(),
                error_message: Some("temporary network error".to_string()),
                created_at: persisted_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        let mut runtime_job = SyncJob::new(
            job_id.clone(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_filters(vec!["^/remote/source$".to_string()], Vec::new());
        runtime_job.ensure_final_round_state();
        scheduler
            .job_cache
            .insert(job_id.clone(), runtime_job.clone());
        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_completed_update_from_worker
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
        let scheduler_clone = scheduler.clone();
        let worker = tokio::spawn(async move {
            scheduler_clone.worker_loop(0).await;
        });
        scheduler
            .job_queue
            .enqueue(runtime_job.priority, runtime_job)
            .await;
        scheduler.job_notify.notify_one();
        for _ in 0..50 {
            if scheduler
                .get_job_status(&job_id)
                .map(|status| status.status == JobStatus::Completed)
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let runtime = scheduler.get_job_status(&job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Completed);
        assert!(db.get_retry(&job_id).await.unwrap().is_some());
        assert_eq!(
            db.load_job(&job_id).await.unwrap().unwrap().status,
            JobStatus::Pending
        );
        scheduler
            .shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = scheduler.shutdown_signal.send(true);
        worker.await.unwrap();
        raw_pool.close().await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn execute_job_noop_round_keeps_existing_pending_error_and_skips_started_callback() {
        let root = temp_dir("worker-noop-preserves-failed-status");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(
            config.clone(),
            db.clone(),
        )
        .await
        .unwrap();
        let callback = Arc::new(RecordingStatusCallback::default());
        *scheduler.status_callback.lock().await = Some(callback.clone());
        let job_id = "job-worker-noop-preserve-failed".to_string();
        let mut persisted_job = crate::domain::Job::new(
            job_id.clone(),
            crate::domain::JobPath {
                path: "/remote/source".to_string(),
                client_id: String::new(),
            },
            crate::domain::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.error_message = Some("previous failure".to_string());
        db.save_job(&persisted_job).await.unwrap();
        scheduler.job_status_cache.insert(
            job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job_id.clone(),
                status: JobStatus::Pending,
                progress: 45,
                current_size: 450,
                total_size: 1000,
                region: "local".to_string(),
                error_message: Some("previous failure".to_string()),
                created_at: persisted_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        let runtime_job = SyncJob::new(
            job_id.clone(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_filters(vec!["^/remote/source$".to_string()], Vec::new());
        let result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            result,
            JobExecutionResult::NoTransfer {
                retry_due_to_stability: false
            }
        );
        let runtime = scheduler.get_job_status(&job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert_eq!(runtime.error_message.as_deref(), Some("previous failure"));
        let snapshot = db.load_job(&job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);
        assert_eq!(snapshot.error_message.as_deref(), Some("previous failure"));
        assert!(callback.started_jobs.lock().unwrap().is_empty());
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
