    use super::{collect_regular_files, resolve_runtime_destination, CacheBuilder};
    use crate::adapters::outbound::persistence::db::Database;
    use crate::application::sync::engine::scheduler::SchedulerConfig;
    use crate::application::sync::engine::CDCResultCache;
    use crate::domain::{Job, JobPath, JobStatus};
    use dashmap::DashMap;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn collect_regular_files_skips_symlinked_directories() {
        let root = temp_dir("idx-builder-files");
        let local_file = root.join("local.txt");
        let external = temp_dir("idx-builder-external");
        let external_file = external.join("external.txt");
        let linked_dir = root.join("linked");

        std::fs::write(&local_file, b"local").unwrap();
        std::fs::write(&external_file, b"external").unwrap();
        std::os::unix::fs::symlink(&external, &linked_dir).unwrap();

        let files = collect_regular_files(&root).await.unwrap();
        let file_set: HashSet<PathBuf> = files.into_iter().collect();

        assert!(file_set.contains(&local_file));
        assert!(!file_set.contains(&linked_dir.join("external.txt")));

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(external).unwrap();
    }

    #[tokio::test]
    async fn collect_regular_files_includes_single_file_root() {
        let root = temp_dir("idx-builder-single");
        let file = root.join("single.txt");
        std::fs::write(&file, b"single").unwrap();

        let files = collect_regular_files(&file).await.unwrap();
        assert_eq!(files, vec![file.clone()]);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_runtime_destination_rebases_parent_traversal_under_data_dir() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };

        let resolved = resolve_runtime_destination(&config, "../escape/out.txt").unwrap();
        assert_eq!(resolved, PathBuf::from("sync/escape/out.txt"));
    }

    #[test]
    fn resolve_runtime_destination_normalizes_embedded_parent_segments() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };

        let resolved = resolve_runtime_destination(&config, "sync/nested/../out.txt").unwrap();
        assert_eq!(resolved, PathBuf::from("sync/out.txt"));
    }

    #[test]
    fn resolve_runtime_destination_rejects_prefix_collision() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };

        let resolved = resolve_runtime_destination(&config, "syncfoo/out.txt").unwrap();
        assert_eq!(resolved, PathBuf::from("sync/syncfoo/out.txt"));
    }

    #[tokio::test]
    async fn collect_runtime_skip_paths_includes_registered_tmp_write_paths() {
        let root = temp_dir("idx-builder-registered-tmp");
        let data_dir = root.join("sync-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let tmp_file = data_dir.join("result.bin.tmp");
        std::fs::write(&tmp_file, b"in-flight-data").unwrap();

        let db_path = root.join("metadata.sqlite");
        let db = Arc::new(
            Database::new(&format!("sqlite://{}", db_path.to_string_lossy()))
                .await
                .unwrap(),
        );
        db.save_tmp_transfer_path("job-running-tmp", tmp_file.to_str().unwrap())
            .await
            .unwrap();

        let cdc_cache = Arc::new(CDCResultCache::new(&root.join("cdc-cache")).unwrap());
        let config = Arc::new(SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        });
        let builder = CacheBuilder::new(config, None, cdc_cache, db, Arc::new(DashMap::new()));

        let skipped = builder.collect_runtime_skip_paths().await.unwrap();

        assert!(skipped.contains(&tmp_file));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn collect_runtime_skip_paths_does_not_guess_directory_tmp_siblings() {
        let root = temp_dir("idx-builder-no-guess");
        let data_dir = root.join("sync-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let sibling_tmp = data_dir.join("folder.tmp");
        std::fs::write(&sibling_tmp, b"normal-file").unwrap();

        let db_path = root.join("metadata.sqlite");
        let db = Arc::new(
            Database::new(&format!("sqlite://{}", db_path.to_string_lossy()))
                .await
                .unwrap(),
        );
        let mut job = Job::new(
            "job-directory-root".to_string(),
            JobPath {
                path: "/remote/source-dir".to_string(),
                client_id: "agent".to_string(),
            },
            JobPath {
                path: "folder".to_string(),
                client_id: "sync".to_string(),
            },
        );
        job.status = JobStatus::Pending;
        db.save_job(&job).await.unwrap();

        let cdc_cache = Arc::new(CDCResultCache::new(&root.join("cdc-cache")).unwrap());
        let config = Arc::new(SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        });
        let builder = CacheBuilder::new(config, None, cdc_cache, db, Arc::new(DashMap::new()));

        let skipped = builder.collect_runtime_skip_paths().await.unwrap();

        assert!(!skipped.contains(&sibling_tmp));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn collect_runtime_skip_paths_includes_paused_job_destinations() {
        let root = temp_dir("idx-builder-paused-dest");
        let data_dir = root.join("sync-data");
        let paused_dest = data_dir.join("paused.bin");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(&paused_dest, b"partial-paused-data").unwrap();

        let db_path = root.join("metadata.sqlite");
        let db = Arc::new(
            Database::new(&format!("sqlite://{}", db_path.to_string_lossy()))
                .await
                .unwrap(),
        );
        let mut job = Job::new(
            "job-paused-destination".to_string(),
            JobPath {
                path: "/remote/source.bin".to_string(),
                client_id: "agent".to_string(),
            },
            JobPath {
                path: "paused.bin".to_string(),
                client_id: "sync".to_string(),
            },
        );
        job.status = JobStatus::Paused;
        db.save_job(&job).await.unwrap();

        let cdc_cache = Arc::new(CDCResultCache::new(&root.join("cdc-cache")).unwrap());
        let config = Arc::new(SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        });
        let builder = CacheBuilder::new(config, None, cdc_cache, db, Arc::new(DashMap::new()));

        let skipped = builder.collect_runtime_skip_paths().await.unwrap();

        assert!(skipped.contains(&paused_dest));

        let _ = std::fs::remove_dir_all(root);
    }
