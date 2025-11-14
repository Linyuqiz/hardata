use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::core::FileChunk;
use crate::sync::engine::job::TransferManagerPool;
use crate::sync::engine::scheduler::dedup::{GlobalChunkInfo, LocalChunkInfo};
use crate::sync::net::transport::TransportConnection;
use crate::sync::transfer::batch::BatchTransferItem;
use crate::util::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};

use super::{classify, helpers, local_copy};
use crate::sync::engine::scheduler::infrastructure::config::SchedulerConfig;

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
    realtime_transferred: &Arc<AtomicU64>,
    start_time: Instant,
    max_concurrent_streams: usize,
    file_size: u64,
) -> Result<()> {
    for batch_indices in chunk_indices.chunks(config.batch_size) {
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
            )
            .await
        {
            Ok(result) => {
                for (idx_in_batch, &chunk_idx) in batch_indices.iter().enumerate() {
                    if idx_in_batch < result.succeeded {
                        state.mark_chunk_completed(chunk_idx);
                    }
                }

                if let Err(e) = transfer_manager_pool.save_state(job_id, state).await {
                    warn!("Failed to save transfer state: {}", e);
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
                if let Err(save_err) = transfer_manager_pool.save_state(job_id, state).await {
                    warn!("Failed to save transfer state after error: {}", save_err);
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
    on_batch_progress: F,
) -> Result<()>
where
    F: Fn(u64) + Send + Sync + 'static,
{
    let start_time = Instant::now();

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

    let relocated_bytes =
        local_copy::relocate_local_chunks(&classification.chunks_to_relocate, dest_path, state)
            .await?;

    let (copied_bytes, failed_chunks) = local_copy::copy_cross_file_chunks(
        chunks,
        &classification.chunks_to_copy,
        dest_path,
        state,
    )
    .await?;

    if !failed_chunks.is_empty() {
        info!(
            "Adding {} failed cross-file copy chunks to network transfer",
            failed_chunks.len()
        );
        classification.chunks_to_transfer.extend(failed_chunks);
    }

    if classification.skipped_bytes > 0 || relocated_bytes > 0 || copied_bytes > 0 {
        let dedup_delta = classification.skipped_bytes + relocated_bytes + copied_bytes;
        info!(
            "Reporting dedup progress: skipped={}, relocated={}, copied={}, total={}",
            classification.skipped_bytes, relocated_bytes, copied_bytes, dedup_delta
        );
        on_batch_progress(dedup_delta);
    }

    if classification.dedup_count > 0 || !classification.chunks_to_relocate.is_empty() {
        if let Err(e) = transfer_manager_pool.save_state(job_id, state).await {
            warn!("Failed to save transfer state after dedup: {}", e);
        }
    }

    info!(
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
