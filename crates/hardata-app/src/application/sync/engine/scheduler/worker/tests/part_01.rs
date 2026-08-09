    #[test]
    fn source_file_matches_cached_state_accepts_legacy_second_precision() {
        let file = ScannedFile {
            path: PathBuf::from("/tmp/file.bin"),
            size: 128,
            modified: 1_710_000_000_123_456_789,
            change_time: Some(1_710_000_001_111_111_111),
            inode: Some(42),
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };
        assert!(source_file_matches_cached_state(
            &file,
            128,
            1_710_000_000,
            Some(1_710_000_001),
            Some(42)
        ));
    }
    #[derive(Default)]
    struct RecordingStatusCallback {
        started_jobs: std::sync::Mutex<Vec<String>>,
    }
    impl crate::application::sync::engine::scheduler::JobStatusCallback for RecordingStatusCallback {
        fn on_job_started(&self, job_id: &str) {
            self.started_jobs.lock().unwrap().push(job_id.to_string());
        }
        fn on_job_completed(&self, _job_id: &str) {}
        fn on_job_failed(&self, _job_id: &str, _error: &str) {}
        fn on_job_progress(&self, _job_id: &str, _progress: u8, _current_size: u64) {}
    }
    #[test]
    fn scan_filter_prunes_excluded_directories() {
        let empty: Vec<String> = Vec::new();
        let filter = ScanFilter::new(&["/tmp/cache".to_string()], &empty).unwrap();
        assert!(!filter.should_scan_dir("/tmp/cache"));
        assert!(filter.should_scan_dir("/tmp/data"));
    }
    #[test]
    fn scan_filter_respects_include_patterns_for_files() {
        let empty: Vec<String> = Vec::new();
        let filter = ScanFilter::new(&empty, &["\\.log$".to_string()]).unwrap();
        assert!(filter.should_include_file("/data/app.log"));
        assert!(!filter.should_include_file("/data/app.bin"));
    }
    #[test]
    fn next_sync_schedule_delay_uses_scan_interval_when_no_stability_retry_is_needed() {
        assert_eq!(
            next_sync_schedule_delay(Duration::from_secs(10), Duration::from_secs(1), false),
            Duration::from_secs(10)
        );
    }
    #[test]
    fn next_sync_schedule_delay_uses_shorter_stability_threshold_for_retry() {
        assert_eq!(
            next_sync_schedule_delay(Duration::from_secs(10), Duration::from_secs(1), true),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_sync_schedule_delay(Duration::from_secs(3), Duration::from_secs(20), true),
            Duration::from_secs(3)
        );
    }
    #[test]
    fn next_sync_schedule_delay_clamps_zero_stability_threshold() {
        assert_eq!(
            next_sync_schedule_delay(Duration::from_secs(10), Duration::ZERO, true),
            Duration::from_millis(MIN_STABILITY_RETRY_DELAY_MS)
        );
    }
    #[test]
    fn pending_stability_file_count_detects_partially_stable_scan() {
        assert_eq!(pending_stability_file_count(5, 3), 2);
        assert_eq!(pending_stability_file_count(5, 5), 0);
        assert_eq!(pending_stability_file_count(3, 5), 0);
    }
    #[test]
    fn single_file_root_candidate_matches_only_root_single_entry_file() {
        let files = vec![FileInfo {
            path: "source".to_string(),
            size: 8,
            is_directory: false,
            modified: 0,
            change_time: None,
            inode: None,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        assert!(single_file_root_candidate("/data/source", "/data/source", &files).is_some());
        assert!(single_file_root_candidate("/data/source", "/data", &files).is_none());
    }
    #[test]
    fn parent_lookup_path_uses_dot_for_relative_single_component_source() {
        assert_eq!(parent_lookup_path("source.bin"), Some(".".to_string()));
        assert_eq!(
            parent_lookup_path("/var/data/source.bin"),
            Some("/var/data".to_string())
        );
    }
    #[test]
    fn parent_listing_confirms_single_file_rejects_directory_root() {
        let parent_files = vec![FileInfo {
            path: "source".to_string(),
            size: 0,
            is_directory: true,
            modified: 0,
            change_time: None,
            inode: None,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        assert!(!parent_listing_confirms_single_file(
            "/data/source",
            &parent_files
        ));
    }
    #[test]
    fn parent_listing_confirms_single_file_accepts_file_root() {
        let parent_files = vec![FileInfo {
            path: "source".to_string(),
            size: 8,
            is_directory: false,
            modified: 0,
            change_time: None,
            inode: None,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        assert!(parent_listing_confirms_single_file(
            "/data/source",
            &parent_files
        ));
    }
    #[tokio::test]
    async fn cleanup_deleted_targets_removes_stale_files() {
        let root = temp_dir("cleanup-deleted-targets");
        let dest_root = root.join("target");
        let keep_path = dest_root.join("keep.txt");
        let stale_path = dest_root.join("stale.txt");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::write(&keep_path, b"keep").unwrap();
        std::fs::write(&stale_path, b"stale").unwrap();
        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-cleanup".to_string(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Full);
        let source_files = vec![ScannedFile {
            path: PathBuf::from("/remote/source/keep.txt"),
            size: 4,
            modified: 0,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        cleanup_deleted_targets(&config, &job, &source_files, false, &HashSet::new())
            .await
            .unwrap();
        assert!(Path::new(&keep_path).exists());
        assert!(!Path::new(&stale_path).exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn cleanup_deleted_targets_also_runs_for_once_jobs() {
        let root = temp_dir("cleanup-once-targets");
        let dest_root = root.join("target");
        let keep_path = dest_root.join("keep.txt");
        let stale_path = dest_root.join("stale.txt");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::write(&keep_path, b"keep").unwrap();
        std::fs::write(&stale_path, b"stale").unwrap();
        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-cleanup-once".to_string(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        assert!(should_cleanup_deleted_targets(&job));
        let source_files = vec![ScannedFile {
            path: PathBuf::from("/remote/source/keep.txt"),
            size: 4,
            modified: 0,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        cleanup_deleted_targets(&config, &job, &source_files, false, &HashSet::new())
            .await
            .unwrap();
        assert!(Path::new(&keep_path).exists());
        assert!(!Path::new(&stale_path).exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn cleanup_deleted_targets_preserves_registered_tmp_paths() {
        let root = temp_dir("cleanup-preserves-tmp-paths");
        let dest_root = root.join("target");
        let keep_path = dest_root.join("keep.txt");
        let tmp_path = dest_root.join("keep.txt.tmp");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::write(&keep_path, b"keep").unwrap();
        std::fs::write(&tmp_path, b"partial").unwrap();
        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-cleanup-preserve-tmp".to_string(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let source_files = vec![ScannedFile {
            path: PathBuf::from("/remote/source/keep.txt"),
            size: 4,
            modified: 0,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        let preserved_paths = HashSet::from([tmp_path.clone()]);
        cleanup_deleted_targets(&config, &job, &source_files, false, &preserved_paths)
            .await
            .unwrap();
        assert!(Path::new(&keep_path).exists());
        assert!(Path::new(&tmp_path).exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn cleanup_deleted_targets_is_disabled_when_include_filters_are_active() {
        let job = SyncJob::new(
            "job-cleanup-filtered-include".to_string(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_filters(Vec::new(), vec!["\\.log$".to_string()]);
        assert!(has_active_scan_filters(&job));
        assert!(!should_cleanup_deleted_targets(&job));
    }
    #[test]
    fn cleanup_deleted_targets_is_disabled_when_exclude_filters_are_active() {
        let job = SyncJob::new(
            "job-cleanup-filtered-exclude".to_string(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_filters(vec!["/cache".to_string()], Vec::new());
        assert!(has_active_scan_filters(&job));
        assert!(!should_cleanup_deleted_targets(&job));
    }
    #[tokio::test]
    async fn cleanup_deleted_targets_rejects_external_absolute_root() {
        let root = temp_dir("cleanup-external-root");
        let data_dir = root.join("sync-data");
        let external_root = root.join("external-target");
        let stale_path = external_root.join("stale.txt");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&external_root).unwrap();
        std::fs::write(&stale_path, b"stale").unwrap();
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-cleanup-external".to_string(),
            PathBuf::from("/remote/source"),
            external_root.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Full);
        let source_files = vec![ScannedFile {
            path: PathBuf::from("/remote/source/keep.txt"),
            size: 4,
            modified: 0,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];
        let err = cleanup_deleted_targets(&config, &job, &source_files, false, &HashSet::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("outside sync.data_dir"));
        assert!(Path::new(&stale_path).exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn list_directory_recursive_marks_excluded_root_without_network_access() {
        let root = temp_dir("worker-root-excluded-scan");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::application::sync::engine::scheduler::SyncScheduler::new(config, db)
            .await
            .unwrap();
        let empty: Vec<String> = Vec::new();
        let filter = ScanFilter::new(&["^/remote/source$".to_string()], &empty).unwrap();
        let result = scheduler
            .list_directory_recursive("/remote/source", "local", &filter)
            .await
            .unwrap();
        assert!(result.root_excluded);
        assert!(result.files.is_empty());
        assert!(!result.source_is_single_file);
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn execute_job_skips_target_cleanup_when_root_is_excluded() {
        let root = temp_dir("worker-root-excluded-execute");
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
        let dest_root = PathBuf::from(&config.data_dir).join("target");
        let stale_path = dest_root.join("stale.txt");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::write(&stale_path, b"stale").unwrap();
        let job_id = "job-root-excluded-execute".to_string();
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
        assert!(
            stale_path.exists(),
            "excluded root must not trigger destination cleanup"
        );
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn execute_job_with_filtered_zero_match_does_not_create_destination_root() {
        let root = temp_dir("worker-filtered-zero-match");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("ignore.bin"), b"payload").unwrap();
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
        let job_id = "job-filtered-zero-match".to_string();
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
        .with_job_type(JobType::Sync)
        .with_filters(Vec::new(), vec!["\\.log$".to_string()]);
        let result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            result,
            JobExecutionResult::NoTransfer {
                retry_due_to_stability: false
            }
        );
        assert!(
            !data_dir.join("target").exists(),
            "filtered zero-match rounds must not create an empty destination root"
        );
        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
