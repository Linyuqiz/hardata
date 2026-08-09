    use super::{apply_batch_result, batch_transfer};
    use crate::adapters::outbound::persistence::db::Database;
    use crate::adapters::outbound::transport::gateway::TransportConnection;
    use crate::adapters::outbound::transport::tcp::TcpClient;
    use crate::application::sync::engine::core::FileChunk;
    use crate::application::sync::engine::job::TransferManagerPool;
    use crate::application::sync::engine::scheduler::dedup::{GlobalChunkInfo, LocalChunkInfo};
    use crate::application::sync::engine::scheduler::SchedulerConfig;
    use crate::application::sync::engine::ChunkLocation;
    use crate::application::sync::transfer::batch::BatchTransferResult;
    use crate::domain::chunk::ChunkHash;
    use crate::domain::transfer_state::FileTransferState;
    use crate::shared::time::metadata_mtime_nanos;
    use hardata_infra_agent::agent_server::tcp::TcpServer;
    use hardata_infra_agent::compute::ComputeService;
    use sqlx::SqlitePool;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn apply_batch_result_marks_completed_chunks_on_success() {
        let mut state = FileTransferState::new("source.bin".to_string(), 3);
        let batch_indices = vec![0, 1, 2];
        let result = BatchTransferResult {
            succeeded: 3,
            failed: 0,
            total_bytes: 1024,
            cancelled: false,
            succeeded_indices: vec![0, 1, 2],
            failed_indices: Vec::new(),
        };

        apply_batch_result(&mut state, &batch_indices, &result).unwrap();

        assert!(state.is_chunk_completed(0));
        assert!(state.is_chunk_completed(1));
        assert!(state.is_chunk_completed(2));
        assert!(state.is_completed());
        assert_eq!(state.progress, 100);
    }

    #[test]
    fn apply_batch_result_keeps_partial_progress_and_errors_on_failed_chunks() {
        let mut state = FileTransferState::new("source.bin".to_string(), 3);
        let batch_indices = vec![0, 1, 2];
        let result = BatchTransferResult {
            succeeded: 2,
            failed: 1,
            total_bytes: 768,
            cancelled: false,
            succeeded_indices: vec![0, 2],
            failed_indices: vec![1],
        };

        let error = apply_batch_result(&mut state, &batch_indices, &result).unwrap_err();

        assert!(matches!(
            error,
            crate::shared::error::HarDataError::NetworkError(_)
        ));
        assert!(state.is_chunk_completed(0));
        assert!(state.is_chunk_completed(2));
        assert!(!state.is_chunk_completed(1));
        assert!(!state.is_completed());
        assert_eq!(state.progress, 66);
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-batch-transfer-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn batch_transfer_persists_progress_after_cross_file_copy_only() {
        let dir = temp_dir("persist-copy-only");
        let db_path = format!("sqlite://{}", dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());

        let source_path = dir.join("source.bin");
        let dest_path = dir.join("dest.bin");
        fs::write(&source_path, b"ABCD").unwrap();
        fs::write(&dest_path, b"XXXX").unwrap();

        let source_metadata = fs::metadata(&source_path).unwrap();
        let strong_hash = *blake3::hash(b"ABCD").as_bytes();
        let local_chunk_info = HashMap::from([(
            strong_hash,
            vec![ChunkLocation {
                file_path: source_path.to_string_lossy().to_string(),
                offset: 0,
                size: 4,
                mtime: metadata_mtime_nanos(&source_metadata),
                strong_hash: Some(strong_hash),
            }],
        )]);
        let chunks = vec![FileChunk {
            file_path: "remote/source.bin".to_string(),
            offset: 0,
            length: 4,
            chunk_hash: ChunkHash {
                weak: 1,
                strong: Some(strong_hash),
            },
        }];
        let mut state = FileTransferState::new("remote/source.bin".to_string(), 1);
        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:1".to_string()).unwrap(),
        };

        batch_transfer(
            &SchedulerConfig::default(),
            &transfer_manager_pool,
            "job-copy-only",
            &chunks,
            &mut state,
            &mut connection,
            &HashSet::from([strong_hash]),
            &local_chunk_info,
            &HashMap::new(),
            dest_path.to_str().unwrap(),
            1,
            Arc::new(|| false),
            |_| {},
        )
        .await
        .unwrap();

        assert!(state.is_chunk_completed(0));
        transfer_manager_pool.shutdown().await;

        let loaded = db
            .load_transfer_state("job-copy-only", "remote/source.bin")
            .await
            .unwrap()
            .expect("expected saved transfer state");
        assert!(loaded.is_chunk_completed(0));
        assert_eq!(fs::read(&dest_path).unwrap(), b"ABCD");

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn batch_transfer_persists_partial_progress_when_cancelled_during_local_copy() {
        let dir = temp_dir("cancel-local-copy");
        let db_path = format!("sqlite://{}", dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());

        let source_path = dir.join("source.bin");
        let dest_path = dir.join("dest.bin");
        fs::write(&source_path, b"AAAABBBB").unwrap();
        fs::write(&dest_path, b"XXXXXXXX").unwrap();

        let source_metadata = fs::metadata(&source_path).unwrap();
        let mtime = metadata_mtime_nanos(&source_metadata);
        let strong_a = *blake3::hash(b"AAAA").as_bytes();
        let strong_b = *blake3::hash(b"BBBB").as_bytes();
        let local_chunk_info = HashMap::from([
            (
                strong_a,
                vec![ChunkLocation {
                    file_path: source_path.to_string_lossy().to_string(),
                    offset: 0,
                    size: 4,
                    mtime,
                    strong_hash: Some(strong_a),
                }],
            ),
            (
                strong_b,
                vec![ChunkLocation {
                    file_path: source_path.to_string_lossy().to_string(),
                    offset: 4,
                    size: 4,
                    mtime,
                    strong_hash: Some(strong_b),
                }],
            ),
        ]);
        let chunks = vec![
            FileChunk {
                file_path: "remote/source.bin".to_string(),
                offset: 0,
                length: 4,
                chunk_hash: ChunkHash {
                    weak: 1,
                    strong: Some(strong_a),
                },
            },
            FileChunk {
                file_path: "remote/source.bin".to_string(),
                offset: 4,
                length: 4,
                chunk_hash: ChunkHash {
                    weak: 2,
                    strong: Some(strong_b),
                },
            },
        ];
        let mut state = FileTransferState::new("remote/source.bin".to_string(), 2);
        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:1".to_string()).unwrap(),
        };
        let cancel_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cancel_counter_clone = cancel_counter.clone();
        let cancel: crate::application::sync::transfer::batch::CancelCallback =
            Arc::new(move || {
                cancel_counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 1
            });
        let reported_progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let reported_progress_clone = reported_progress.clone();

        let error = batch_transfer(
            &SchedulerConfig::default(),
            &transfer_manager_pool,
            "job-cancel-local-copy",
            &chunks,
            &mut state,
            &mut connection,
            &HashSet::from([strong_a, strong_b]),
            &local_chunk_info,
            &HashMap::new(),
            dest_path.to_str().unwrap(),
            1,
            cancel,
            move |delta| {
                reported_progress_clone.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Job cancelled by user"));
        assert_eq!(
            reported_progress.load(std::sync::atomic::Ordering::Relaxed),
            4
        );
        transfer_manager_pool.shutdown().await;

        let loaded = db
            .load_transfer_state("job-cancel-local-copy", "remote/source.bin")
            .await
            .unwrap()
            .expect("expected saved transfer state");
        assert!(loaded.is_chunk_completed(0));
        assert!(!loaded.is_chunk_completed(1));
        assert_eq!(fs::read(&dest_path).unwrap(), b"AAAAXXXX");

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn batch_transfer_fails_when_state_persist_after_local_reuse_fails() {
        let dir = temp_dir("local-reuse-persist-failure");
        let db_path = format!("sqlite://{}", dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());

        let source_path = dir.join("source.bin");
        let dest_path = dir.join("dest.bin");
        fs::write(&source_path, b"ABCD").unwrap();
        fs::write(&dest_path, b"XXXX").unwrap();

        let source_metadata = fs::metadata(&source_path).unwrap();
        let mtime = metadata_mtime_nanos(&source_metadata);
        let strong_hash = *blake3::hash(b"ABCD").as_bytes();
        let local_chunk_info = HashMap::from([(
            strong_hash,
            vec![ChunkLocation {
                file_path: source_path.to_string_lossy().to_string(),
                offset: 0,
                size: 4,
                mtime,
                strong_hash: Some(strong_hash),
            }],
        )]);
        let chunks = vec![FileChunk {
            file_path: "remote/source.bin".to_string(),
            offset: 0,
            length: 4,
            chunk_hash: ChunkHash {
                weak: 1,
                strong: Some(strong_hash),
            },
        }];
        let mut state = FileTransferState::new("remote/source.bin".to_string(), 1);
        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:1".to_string()).unwrap(),
        };

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER reject_transfer_state_insert
            BEFORE INSERT ON transfer_states
            WHEN NEW.job_id = 'job-local-reuse-persist-failure'
            BEGIN
                SELECT RAISE(FAIL, 'reject transfer state insert');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let err = batch_transfer(
            &SchedulerConfig::default(),
            &transfer_manager_pool,
            "job-local-reuse-persist-failure",
            &chunks,
            &mut state,
            &mut connection,
            &HashSet::from([strong_hash]),
            &local_chunk_info,
            &HashMap::new(),
            dest_path.to_str().unwrap(),
            1,
            Arc::new(|| false),
            |_| {},
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Failed to persist transfer state after local reuse"));
        assert!(db
            .load_transfer_state("job-local-reuse-persist-failure", "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert_eq!(fs::read(&dest_path).unwrap(), b"ABCD");

        raw_pool.close().await;
        transfer_manager_pool.shutdown().await;
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn batch_transfer_persists_partial_progress_when_cancelled_during_remote_batch() {
        let dir = temp_dir("cancel-remote-batch");
        let db_path = format!("sqlite://{}", dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());

        let remote_root = dir.join("remote");
        fs::create_dir_all(&remote_root).unwrap();
        let source_path = remote_root.join("source.bin");
        let dest_path = dir.join("dest.bin");
        fs::write(&source_path, b"AAAABBBB").unwrap();
        fs::write(&dest_path, b"XXXXXXXX").unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{port}");
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

        let chunks = vec![
            FileChunk {
                file_path: source_path.to_string_lossy().to_string(),
                offset: 0,
                length: 4,
                chunk_hash: ChunkHash {
                    weak: 1,
                    strong: None,
                },
            },
            FileChunk {
                file_path: source_path.to_string_lossy().to_string(),
                offset: 4,
                length: 4,
                chunk_hash: ChunkHash {
                    weak: 2,
                    strong: None,
                },
            },
        ];
        let mut state = FileTransferState::new(source_path.to_string_lossy().to_string(), 2);
        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };
        let existing_strong_hashes = HashSet::new();
        let local_chunk_info: LocalChunkInfo = HashMap::new();
        let global_chunk_info: GlobalChunkInfo = HashMap::new();
        let transferred = Arc::new(AtomicU64::new(0));
        let cancel_progress = transferred.clone();
        let cancel: crate::application::sync::transfer::batch::CancelCallback =
            Arc::new(move || cancel_progress.load(Ordering::Relaxed) >= 4);
        let transferred_for_callback = transferred.clone();

        let error = batch_transfer(
            &SchedulerConfig::default(),
            &transfer_manager_pool,
            "job-cancel-remote-batch",
            &chunks,
            &mut state,
            &mut connection,
            &existing_strong_hashes,
            &local_chunk_info,
            &global_chunk_info,
            dest_path.to_str().unwrap(),
            1,
            cancel,
            move |delta| {
                transferred_for_callback.fetch_add(delta, Ordering::Relaxed);
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Job cancelled by user"));
        assert_eq!(transferred.load(Ordering::Relaxed), 4);
        transfer_manager_pool.shutdown().await;

        let loaded = db
            .load_transfer_state("job-cancel-remote-batch", source_path.to_str().unwrap())
            .await
            .unwrap()
            .expect("expected saved transfer state");
        assert!(loaded.is_chunk_completed(0));
        assert!(!loaded.is_chunk_completed(1));
        assert_eq!(fs::read(&dest_path).unwrap(), b"AAAAXXXX");

        server_handle.abort();
        let _ = server_handle.await;
        let _ = fs::remove_dir_all(dir);
    }
