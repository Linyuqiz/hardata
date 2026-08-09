use crate::adapters::outbound::transport::gateway::TransportConnection;
use crate::application::sync::engine::core::FileChunk;
use crate::application::sync::engine::job::TransferManagerPool;
use crate::application::sync::engine::scheduler::dedup::{GlobalChunkInfo, LocalChunkInfo};
use crate::application::sync::transfer::batch::{BatchTransferItem, CancelCallback};
use crate::domain::transfer_state::FileTransferState;
use crate::shared::error::{HarDataError, Result};
use crate::shared::file_ops;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

use super::{classify, helpers, local_copy};
use crate::application::sync::engine::scheduler::infrastructure::config::SchedulerConfig;

fn apply_batch_result(
    state: &mut FileTransferState,
    batch_indices: &[usize],
    result: &crate::application::sync::transfer::batch::BatchTransferResult,
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

    Err(crate::shared::error::HarDataError::NetworkError(format!(
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
            return Err(crate::shared::error::HarDataError::Unknown(
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
                        operation = "job.transfer_progress",
                        job_id = %job_id,
                        progress_pct,
                        transferred_bytes = transferred,
                        total_bytes = file_size,
                        throughput_mb_s = speed,
                        "job transfer progress"
                    );
                }
            }
            Err(e) => {
                error!(operation = "job.batch_transfer_failed", job_id = %job_id, error = %e, "job batch transfer failed");
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
