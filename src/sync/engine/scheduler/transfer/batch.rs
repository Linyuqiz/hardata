use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::core::FileChunk;
use crate::sync::engine::job::TransferManagerPool;
use crate::sync::engine::scheduler::dedup::{GlobalChunkInfo, LocalChunkInfo};
use crate::sync::net::transport::TransportConnection;
use crate::sync::transfer::batch::{BatchTransferItem, CancelCallback};
use crate::util::error::{HarDataError, Result};
use crate::util::file_ops;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

use super::{classify, helpers, local_copy};
use crate::sync::engine::scheduler::infrastructure::config::SchedulerConfig;

fn apply_batch_result(
    state: &mut FileTransferState,
    batch_indices: &[usize],
    result: &crate::sync::transfer::batch::BatchTransferResult,
) -> Result<()> {
    for idx_in_batch in &result.succeeded_indices {
        if let Some(&chunk_idx) = batch_indices.get(*idx_in_batch) {
            state.mark_chunk_completed(chunk_idx);
        }
    }

    if result.failed == 0 && result.failed_indices.is_empty() {
        return Ok(());
    }

    let failed_chunks: Vec<usize> = result
        .failed_indices
        .iter()
        .filter_map(|idx_in_batch| batch_indices.get(*idx_in_batch).copied())
        .collect();
    let failed_count = result.failed.max(failed_chunks.len());
    let detail = if failed_chunks.is_empty() {
        format!("{failed_count} chunks failed")
    } else {
        format!("chunks {:?} failed", failed_chunks)
    };

    Err(crate::util::error::HarDataError::NetworkError(format!(
        "Batch transfer partially failed: {detail}",
    )))
}

async fn save_state_after_local_phase_error(
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    state: &mut FileTransferState,
    dest_path: &str,
) -> Result<()> {
    refresh_destination_version(state, dest_path).await?;
    transfer_manager_pool.save_state(job_id, state).await
}

async fn refresh_destination_version(state: &mut FileTransferState, dest_path: &str) -> Result<()> {
    match file_ops::load_regular_file_version(std::path::Path::new(dest_path)).await {
        Ok(Some(version)) => {
            state.set_destination_version(
                version.size,
                version.modified,
                version.change_time,
                version.inode,
            );
        }
        Ok(_) => state.clear_destination_version(),
        Err(e) => {
            return Err(HarDataError::FileOperation(format!(
                "Failed to inspect transfer destination '{}' for state refresh: {}",
                dest_path, e
            )));
        }
    }

    Ok(())
}

async fn save_state_with_destination_version(
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    state: &mut FileTransferState,
    dest_path: &str,
) -> Result<()> {
    refresh_destination_version(state, dest_path).await?;
    transfer_manager_pool.save_state(job_id, state).await
}

fn completed_delta_bytes(
    chunks: &[FileChunk],
    state: &FileTransferState,
    initial_completed_chunks: &std::collections::HashSet<usize>,
) -> u64 {
    state
        .completed_chunks
        .iter()
        .filter(|chunk_idx| !initial_completed_chunks.contains(chunk_idx))
        .filter_map(|chunk_idx| chunks.get(*chunk_idx))
        .map(|chunk| chunk.length)
        .sum()
}

fn state_persist_error(context: &str, err: &HarDataError) -> HarDataError {
    HarDataError::Unknown(format!("{context}: {}", err))
}

fn combined_transfer_state_error(
    transfer_error: &HarDataError,
    persist_error: &HarDataError,
) -> HarDataError {
    HarDataError::Unknown(format!(
        "{}; failed to persist transfer state: {}",
        transfer_error, persist_error
    ))
}

fn cancelled_transfer_error() -> HarDataError {
    HarDataError::Unknown("Job cancelled by user".to_string())
}

#[allow(clippy::too_many_arguments)]
async fn transfer_chunks_in_batches(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    chunks: &[FileChunk],
    chunk_indices: &[usize],
    dest_offset_map: &HashMap<usize, u64>,
    dest_path: &str,
    state: &mut FileTransferState,
    connection: &mut TransportConnection,
    chunk_progress_callback: super::helpers::ProgressCallback,
    cancel_callback: &CancelCallback,
    realtime_transferred: &Arc<AtomicU64>,
    start_time: Instant,
    max_concurrent_streams: usize,
    file_size: u64,
) -> Result<()> {
    for batch_indices in chunk_indices.chunks(config.batch_size) {
        if cancel_callback() {
            return Err(crate::util::error::HarDataError::Unknown(
                "Job cancelled by user".to_string(),
            ));
        }

        let mut batch_items = Vec::new();

        for &chunk_idx in batch_indices {
            let chunk = &chunks[chunk_idx];
            let dest_offset = dest_offset_map.get(&chunk_idx).copied().unwrap_or(0);

            batch_items.push(BatchTransferItem::new(
                chunk.file_path.clone(),
                dest_path.to_string(),
                chunk.offset,
                dest_offset,
                chunk.length,
                chunk.chunk_hash.weak,
            ));
        }

        match connection
            .read_and_write_batch_with_progress(
                batch_items,
                job_id,
                max_concurrent_streams,
                Some(chunk_progress_callback.clone()),
                Some(cancel_callback.clone()),
            )
            .await
        {
            Ok(result) => {
                let batch_result = apply_batch_result(state, batch_indices, &result);
                let was_cancelled = result.cancelled || cancel_callback();
                let persist_result = save_state_with_destination_version(
                    transfer_manager_pool,
                    job_id,
                    state,
                    dest_path,
                )
                .await;
                if was_cancelled {
                    return match persist_result {
                        Ok(()) => Err(cancelled_transfer_error()),
                        Err(save_err) => Err(combined_transfer_state_error(
                            &cancelled_transfer_error(),
                            &save_err,
                        )),
                    };
                }
                match (batch_result, persist_result) {
                    (Ok(()), Ok(())) => {}
                    (Err(batch_err), Ok(())) => return Err(batch_err),
                    (Ok(()), Err(save_err)) => {
                        return Err(state_persist_error(
                            "Failed to persist transfer state after batch progress",
                            &save_err,
                        ));
                    }
                    (Err(batch_err), Err(save_err)) => {
                        return Err(combined_transfer_state_error(&batch_err, &save_err));
                    }
                }

                let transferred = realtime_transferred.load(Ordering::Relaxed);
                let elapsed = start_time.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    let speed = transferred as f64 / elapsed / 1024.0 / 1024.0;
                    let progress_pct = (transferred as f64 / file_size as f64 * 100.0).min(100.0);
                    info!(
                        "Job {} Batch Progress: {:.1}% ({}/{} bytes) @ {:.2} MB/s",
                        job_id, progress_pct, transferred, file_size, speed
                    );
                }
            }
            Err(e) => {
                error!("Batch transfer failed: {}", e);
                if let Err(save_err) = save_state_with_destination_version(
                    transfer_manager_pool,
                    job_id,
                    state,
                    dest_path,
                )
                .await
                {
                    return Err(combined_transfer_state_error(&e, &save_err));
                }
                return Err(e);
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn batch_transfer<F>(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    chunks: &[FileChunk],
    state: &mut FileTransferState,
    connection: &mut TransportConnection,
    existing_strong_hashes: &HashSet<[u8; 32]>,
    local_chunk_info: &LocalChunkInfo,
    global_chunk_info: &GlobalChunkInfo,
    dest_path: &str,
    max_concurrent_streams: usize,
    cancel_callback: CancelCallback,
    on_batch_progress: F,
) -> Result<()>
where
    F: Fn(u64) + Send + Sync + 'static,
{
    let start_time = Instant::now();
    let initial_completed_chunks = state.completed_chunks.clone();

    let dest_offset_map = helpers::build_offset_map(chunks);

    let mut classification = classify::classify_chunks(
        chunks,
        state,
        &dest_offset_map,
        existing_strong_hashes,
        local_chunk_info,
        global_chunk_info,
        dest_path,
    );

    let relocated_bytes = match local_copy::relocate_local_chunks(
        &classification.chunks_to_relocate,
        dest_path,
        state,
        &cancel_callback,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(e) => {
            let local_delta = completed_delta_bytes(chunks, state, &initial_completed_chunks);
            if local_delta > 0 {
                on_batch_progress(local_delta);
                if let Err(save_err) = save_state_after_local_phase_error(
                    transfer_manager_pool,
                    job_id,
                    state,
                    dest_path,
                )
                .await
                {
                    return Err(combined_transfer_state_error(&e, &save_err));
                }
            }
            return Err(e);
        }
    };

    let (copied_bytes, failed_chunks) = match local_copy::copy_cross_file_chunks(
        chunks,
        &classification.chunks_to_copy,
        dest_path,
        state,
        &cancel_callback,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            let local_delta = completed_delta_bytes(chunks, state, &initial_completed_chunks);
            if local_delta > 0 {
                on_batch_progress(local_delta);
                if let Err(save_err) = save_state_after_local_phase_error(
                    transfer_manager_pool,
                    job_id,
                    state,
                    dest_path,
                )
                .await
                {
                    return Err(combined_transfer_state_error(&e, &save_err));
                }
            }
            return Err(e);
        }
    };

    if !failed_chunks.is_empty() {
        debug!(
            "Adding {} failed cross-file copy chunks to network transfer",
            failed_chunks.len()
        );
        classification.chunks_to_transfer.extend(failed_chunks);
    }

    if classification.skipped_bytes > 0 || relocated_bytes > 0 || copied_bytes > 0 {
        let dedup_delta = classification.skipped_bytes + relocated_bytes + copied_bytes;
        debug!(
            "Reporting dedup progress: skipped={}, relocated={}, copied={}, total={}",
            classification.skipped_bytes, relocated_bytes, copied_bytes, dedup_delta
        );
        on_batch_progress(dedup_delta);
    }

    if classification.dedup_count > 0
        || !classification.chunks_to_relocate.is_empty()
        || copied_bytes > 0
    {
        save_state_with_destination_version(transfer_manager_pool, job_id, state, dest_path)
            .await
            .map_err(|e| {
                state_persist_error("Failed to persist transfer state after local reuse", &e)
            })?;
    }

    debug!(
        "Transferring {} chunks in batches (skipped: {}, relocated: {}, cross-file copied: {})",
        classification.chunks_to_transfer.len(),
        classification.dedup_count,
        classification.chunks_to_relocate.len(),
        classification.chunks_to_copy.len()
    );

    if classification.chunks_to_transfer.is_empty() {
        return Ok(());
    }

    let file_size: u64 = chunks.iter().map(|c| c.length).sum();
    let (realtime_transferred, chunk_progress_callback, progress_callback_arc, last_reported) =
        helpers::create_progress_callback(on_batch_progress, 0);

    transfer_chunks_in_batches(
        config,
        transfer_manager_pool,
        job_id,
        chunks,
        &classification.chunks_to_transfer,
        &dest_offset_map,
        dest_path,
        state,
        connection,
        chunk_progress_callback,
        &cancel_callback,
        &realtime_transferred,
        start_time,
        max_concurrent_streams,
        file_size,
    )
    .await?;

    let final_transferred = realtime_transferred.load(Ordering::Relaxed);
    let last_reported_value = last_reported.load(Ordering::Relaxed);
    if final_transferred > last_reported_value {
        let delta = final_transferred - last_reported_value;
        progress_callback_arc(delta);
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{apply_batch_result, batch_transfer};
    use crate::agent::compute::ComputeService;
    use crate::agent::server::tcp::TcpServer;
    use crate::core::chunk::ChunkHash;
    use crate::core::transfer_state::FileTransferState;
    use crate::sync::engine::core::FileChunk;
    use crate::sync::engine::job::TransferManagerPool;
    use crate::sync::engine::scheduler::dedup::{GlobalChunkInfo, LocalChunkInfo};
    use crate::sync::engine::scheduler::SchedulerConfig;
    use crate::sync::engine::ChunkLocation;
    use crate::sync::net::tcp::TcpClient;
    use crate::sync::net::transport::TransportConnection;
    use crate::sync::storage::db::Database;
    use crate::sync::transfer::batch::BatchTransferResult;
    use crate::util::time::metadata_mtime_nanos;
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
            crate::util::error::HarDataError::NetworkError(_)
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
        let cancel: crate::sync::transfer::batch::CancelCallback = Arc::new(move || {
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
        let cancel: crate::sync::transfer::batch::CancelCallback =
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
}
