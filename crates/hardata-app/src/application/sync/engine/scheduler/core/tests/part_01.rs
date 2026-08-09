    #[tokio::test]
    async fn start_succeeds_even_when_initial_region_connection_is_unavailable() {
        let _tls_guard = GLOBAL_TLS_TEST_LOCK.lock().await;
        let temp_dir = create_temp_dir("offline-start");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let missing_tcp = free_port();
        let missing_quic = free_port();

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::application::config::RegionConfig {
                name: "offline".to_string(),
                quic_bind: format!("127.0.0.1:{}", missing_quic),
                tcp_bind: format!("127.0.0.1:{}", missing_tcp),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        scheduler.start().await.unwrap();
        let shutdown_started = Instant::now();
        scheduler.shutdown().await.unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(5));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn new_rejects_region_with_invalid_tcp_bind_even_without_quic() {
        let _tls_guard = GLOBAL_TLS_TEST_LOCK.lock().await;
        let temp_dir = create_temp_dir("invalid-tcp-config");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::application::config::RegionConfig {
                name: "broken".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: "".to_string(),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let err = match SyncScheduler::new(config, db).await {
            Ok(_) => panic!("scheduler creation should fail for invalid tcp bind"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("no usable transport client"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn start_times_out_hanging_quic_handshakes_during_initial_connection() {
        let _tls_guard = GLOBAL_TLS_TEST_LOCK.lock().await;
        let temp_dir = create_temp_dir("offline-start-hanging-quic");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let missing_tcp = free_port();
        let blackhole = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let quic_port = blackhole.local_addr().unwrap().port();
        let quic_ca_cert_path = write_quic_ca_cert(&temp_dir);
        let auto_cert_path = std::path::PathBuf::from(".hardata/tls/agent-cert-127.0.0.1.der");
        let quic_ca_cert = fs::read(&quic_ca_cert_path).unwrap();
        let _cert_override = ScopedFileOverride::replace(auto_cert_path, &quic_ca_cert);

        let blackhole_task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                if blackhole.recv_from(&mut buf).await.is_err() {
                    break;
                }
            }
        });

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::application::config::RegionConfig {
                name: "offline".to_string(),
                quic_bind: format!("127.0.0.1:{}", quic_port),
                tcp_bind: format!("127.0.0.1:{}", missing_tcp),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let start_started = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), scheduler.start())
            .await
            .expect("scheduler start should not hang")
            .unwrap();
        assert!(start_started.elapsed() < Duration::from_secs(8));

        let shutdown_started = Instant::now();
        scheduler.shutdown().await.unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(5));

        blackhole_task.abort();
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn new_allows_missing_quic_ca_cert_when_tcp_is_available() {
        let _tls_guard = GLOBAL_TLS_TEST_LOCK.lock().await;
        let auto_cert = std::path::PathBuf::from(".hardata/tls/agent-cert-127.0.0.1.der");
        let _cert_override = ScopedFileOverride::remove(auto_cert);
        let temp_dir = create_temp_dir("missing-quic-cert");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let tcp_port = free_port();

        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            regions: vec![crate::application::config::RegionConfig {
                name: "mixed".to_string(),
                quic_bind: format!("127.0.0.1:{}", free_port()),
                tcp_bind: format!("127.0.0.1:{}", tcp_port),
            }],
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let pool = scheduler.connection_pools.get("mixed").unwrap();
        let pool = pool.lock().await;
        assert!(pool.quic_client.is_none());
        assert!(pool.tcp_client.is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn size_freezer_is_isolated_per_job_and_cleanup_releases_it() {
        let temp_dir = create_temp_dir("size-freezer-scope");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let freezer_a = scheduler.size_freezer_for_job("job-a");
        let freezer_b = scheduler.size_freezer_for_job("job-b");
        assert!(!Arc::ptr_eq(&freezer_a, &freezer_b));

        scheduler.cleanup_runtime_job("job-a");
        let freezer_a_recreated = scheduler.size_freezer_for_job("job-a");
        assert!(!Arc::ptr_eq(&freezer_a, &freezer_a_recreated));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn enqueue_and_schedule_skip_cancelled_jobs() {
        let temp_dir = create_temp_dir("skip-terminal-queue-enqueue");
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
            "job-skip-terminal-queue".to_string(),
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
        job.status = JobStatus::Cancelled;
        db.save_job(&job).await.unwrap();

        let sync_job = crate::application::sync::engine::job::SyncJob::new(
            job.job_id.clone(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(job.job_type);

        scheduler
            .enqueue_job_replacing_queued(sync_job.clone())
            .await;
        scheduler
            .schedule_delayed_job(Instant::now() + Duration::from_secs(60), sync_job)
            .await;

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn enqueue_and_schedule_skip_paused_jobs() {
        let temp_dir = create_temp_dir("skip-paused-queue-enqueue");
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
            "job-skip-paused-queue".to_string(),
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
        job.status = JobStatus::Paused;
        db.save_job(&job).await.unwrap();

        let sync_job = crate::application::sync::engine::job::SyncJob::new(
            job.job_id.clone(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(job.job_type);

        scheduler
            .enqueue_job_replacing_queued(sync_job.clone())
            .await;
        scheduler
            .schedule_delayed_job(Instant::now() + Duration::from_secs(60), sync_job)
            .await;

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn enqueue_and_schedule_skip_when_status_lookup_fails() {
        let temp_dir = create_temp_dir("skip-queue-enqueue-on-status-error");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let sync_job = crate::application::sync::engine::job::SyncJob::new(
            "job-skip-queue-status-error".to_string(),
            std::path::PathBuf::from("/tmp/source.bin"),
            "dest.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("DROP TABLE jobs").execute(&pool).await.unwrap();

        scheduler
            .enqueue_job_replacing_queued(sync_job.clone())
            .await;
        scheduler
            .schedule_delayed_job(Instant::now() + Duration::from_secs(60), sync_job)
            .await;

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cache_cleanup_cycle_also_runs_old_job_retention() {
        let temp_dir = create_temp_dir("cache-cycle-cleans-old-jobs");
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
            "job-old-completed".to_string(),
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
        job.status = JobStatus::Completed;
        db.save_job(&job).await.unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE jobs SET updated_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler
            .run_cache_cleanup_cycle(chrono::Utc::now().timestamp(), 60, 10)
            .await;

        assert!(db.load_job(&job.job_id).await.unwrap().is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cache_cleanup_cycle_removes_empty_job_caches_after_limit_pruning() {
        let temp_dir = create_temp_dir("cache-cycle-removes-empty-job-caches");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = SyncScheduler::new(config, db).await.unwrap();
        let old_cache = DashMap::new();
        old_cache.insert(
            "/remote/old.bin".to_string(),
            FileSyncState {
                size: 1,
                mtime: 1,
                change_time: None,
                inode: None,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
                updated_at: 10,
            },
        );
        scheduler
            .synced_files_cache
            .insert("job-old".to_string(), old_cache);

        let new_cache = DashMap::new();
        new_cache.insert(
            "/remote/new.bin".to_string(),
            FileSyncState {
                size: 1,
                mtime: 1,
                change_time: None,
                inode: None,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
                updated_at: 20,
            },
        );
        scheduler
            .synced_files_cache
            .insert("job-new".to_string(), new_cache);

        scheduler.run_cache_cleanup_cycle(20, 60, 1).await;

        assert!(scheduler.synced_files_cache.get("job-old").is_none());
        let remaining = scheduler
            .synced_files_cache
            .get("job-new")
            .expect("newest cache should be retained");
        assert_eq!(remaining.len(), 1);

        let _ = fs::remove_dir_all(temp_dir);
    }
