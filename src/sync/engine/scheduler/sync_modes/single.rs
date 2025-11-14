use crate::core::job::JobStatus;
use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::net::transport::TransportConnection;
use crate::sync::scanner::ScannedFile;
use crate::util::error::{HarDataError, Result};
use dashmap::DashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::super::dedup;
use super::super::infrastructure::config::{JobRuntimeStatus, ReplicateMode, SchedulerConfig};
use super::super::optimization::PrefetchManager;
use super::super::transfer;

fn get_write_path(dest_path: &str, mode: ReplicateMode) -> String {
    match mode {
        ReplicateMode::Append => dest_path.to_string(),
        ReplicateMode::Tmp => format!("{}.tmp", dest_path),
    }
}

async fn finalize_file(dest_path: &str, mode: ReplicateMode) -> Result<()> {
    if mode == ReplicateMode::Tmp {
        let tmp_path = format!("{}.tmp", dest_path);
        if Path::new(&tmp_path).exists() {
            debug!("Finalizing: {} -> {}", tmp_path, dest_path);
            tokio::fs::rename(&tmp_path, dest_path).await.map_err(|e| {
                HarDataError::FileOperation(format!(
                    "Failed to rename tmp file {} to {}: {}",
                    tmp_path, dest_path, e
                ))
            })?;
        }
    }
    Ok(())
}

async fn cleanup_tmp_file(dest_path: &str, mode: ReplicateMode) {
    if mode == ReplicateMode::Tmp {
        let tmp_path = format!("{}.tmp", dest_path);
        if Path::new(&tmp_path).exists() {
            if let Err(e) = tokio::fs::remove_file(&tmp_path).await {
                warn!("Failed to cleanup tmp file {}: {}", tmp_path, e);
            }
        }
    }
}

async fn should_skip_file(dest_path: &str, remote_file: &ScannedFile) -> bool {
    let dest = Path::new(dest_path);
    if !dest.exists() {
        return false;
    }

    let metadata = match tokio::fs::metadata(dest).await {
        Ok(m) => m,
        Err(_) => return false,
    };

    if metadata.len() != remote_file.size {
        return false;
    }

    let local_mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    if local_mtime >= remote_file.modified && remote_file.modified > 0 {
        info!(
            "Skipping unchanged file: {} (size={}, mtime={})",
            dest_path, remote_file.size, remote_file.modified
        );
        return true;
    }

    false
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_single_file<F>(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    job: &SyncJob,
    file: &ScannedFile,
    source_path: &str,
    dest_path: &str,
    connection: &mut TransportConnection,
    max_concurrent_streams: usize,
    on_batch_progress: F,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
    chunk_index: Option<&Arc<crate::sync::engine::CDCResultCache>>,
    global_index: Option<&Arc<crate::sync::engine::ChunkIndex>>,
) -> Result<()>
where
    F: Fn(u64) + Send + Sync + 'static,
{
    sync_single_file_with_mode(
        config,
        transfer_manager_pool,
        job_status_cache,
        job,
        file,
        source_path,
        dest_path,
        connection,
        max_concurrent_streams,
        on_batch_progress,
        config.replicate_mode,
        prefetch_manager,
        chunk_index,
        global_index,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_single_file_with_mode<F>(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    job: &SyncJob,
    file: &ScannedFile,
    source_path: &str,
    dest_path: &str,
    connection: &mut TransportConnection,
    max_concurrent_streams: usize,
    on_batch_progress: F,
    mode: ReplicateMode,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
    chunk_index: Option<&Arc<crate::sync::engine::CDCResultCache>>,
    global_index: Option<&Arc<crate::sync::engine::ChunkIndex>>,
) -> Result<()>
where
    F: Fn(u64) + Send + Sync + 'static,
{
    if let Some(status) = job_status_cache.get(&job.job_id) {
        if status.status == JobStatus::Cancelled {
            info!("Job {} was cancelled during file transfer", job.job_id);
            return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
        }
    }

    if file.is_dir {
        let dest = Path::new(dest_path);
        if !dest.exists() {
            if let Err(e) = std::fs::create_dir_all(dest) {
                error!("Failed to create directory {}: {}", dest_path, e);
                return Err(HarDataError::FileOperation(format!(
                    "Failed to create directory: {}",
                    e
                )));
            }
            debug!("Created dir: {}", dest_path);
        }
        return Ok(());
    }

    if file.is_symlink {
        if let Some(ref target) = file.symlink_target {
            let dest = Path::new(dest_path);
            if dest.exists() || dest.symlink_metadata().is_ok() {
                if let Err(e) = std::fs::remove_file(dest) {
                    warn!(
                        "Failed to remove existing file before creating symlink: {}",
                        e
                    );
                }
            }
            if let Some(parent) = dest.parent() {
                if !parent.exists() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        error!("Failed to create parent directory for symlink: {}", e);
                        return Err(HarDataError::FileOperation(format!(
                            "Failed to create parent directory: {}",
                            e
                        )));
                    }
                }
            }
            #[cfg(unix)]
            {
                if let Err(e) = std::os::unix::fs::symlink(target, dest) {
                    error!(
                        "Failed to create symlink {} -> {}: {}",
                        dest_path, target, e
                    );
                    return Err(HarDataError::FileOperation(format!(
                        "Failed to create symlink: {}",
                        e
                    )));
                }
                debug!("Created symlink: {} -> {}", dest_path, target);
            }
            #[cfg(not(unix))]
            {
                warn!(
                    "Symlink not supported on this platform, skipping: {}",
                    dest_path
                );
            }
        }
        return Ok(());
    }

    if !job.job_type.is_full() && should_skip_file(dest_path, file).await {
        on_batch_progress(file.size);
        return Ok(());
    }

    let write_path = get_write_path(dest_path, mode);

    if file.size == 0 {
        if let Some(parent) = std::path::Path::new(&write_path).parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    error!("Failed to create parent directory for empty file: {}", e);
                    return Err(HarDataError::FileOperation(format!(
                        "Failed to create parent directory: {}",
                        e
                    )));
                }
            }
        }
        match std::fs::File::create(&write_path) {
            Ok(_) => {
                debug!("Created empty file: {}", write_path);
                finalize_file(dest_path, mode).await?;
            }
            Err(e) => {
                error!("Failed to create empty file {}: {}", write_path, e);
                return Err(HarDataError::FileOperation(format!(
                    "Failed to create empty file: {}",
                    e
                )));
            }
        }
        return Ok(());
    }

    let file_path_str = source_path.to_string();

    let source_path_buf = std::path::Path::new(source_path);
    let mut chunks = transfer::chunk_file(config, source_path_buf, connection).await?;
    debug!("File split into {} chunks (mode={:?})", chunks.len(), mode);

    let (
        existing_strong_hashes,
        _existing_weak_hashes,
        dedup_count,
        local_chunk_info,
        global_chunk_info,
    ) = dedup::check_deduplication(
        config,
        &mut chunks,
        connection,
        file,
        &write_path,
        prefetch_manager,
        chunk_index,
        global_index,
    )
    .await?;

    if dedup_count > 0 {
        info!(
            "{}/{} chunks already exist, will skip network transfer",
            dedup_count,
            chunks.len()
        );
    }

    let mut state = transfer_manager_pool
        .load_state(&job.job_id, &file_path_str)
        .await
        .unwrap_or_else(|| FileTransferState::new(file_path_str.clone(), chunks.len()));

    let transfer_result = transfer::batch_transfer(
        config,
        transfer_manager_pool,
        &job.job_id,
        &chunks,
        &mut state,
        connection,
        &existing_strong_hashes,
        &local_chunk_info,
        &global_chunk_info,
        &write_path,
        max_concurrent_streams,
        on_batch_progress,
    )
    .await;

    if let Err(e) = transfer_result {
        cleanup_tmp_file(dest_path, mode).await;
        return Err(e);
    }

    let write_path_buf = std::path::Path::new(&write_path);
    if write_path_buf.exists() {
        let dest_metadata = tokio::fs::metadata(write_path_buf).await?;
        if dest_metadata.len() != file.size {
            info!(
                "Adjusting file size: {} -> {} bytes",
                dest_metadata.len(),
                file.size
            );
            let dest_file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(write_path_buf)
                .await?;
            dest_file.set_len(file.size).await?;
        }
    }

    finalize_file(dest_path, mode).await?;

    #[cfg(unix)]
    if file.mode != 0 {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(file.mode);
        if let Err(e) = std::fs::set_permissions(dest_path, permissions) {
            warn!("Failed to set file permissions for {}: {}", dest_path, e);
        }
    }

    if let Some(gindex) = global_index {
        if let Err(e) = update_global_index(config, gindex, dest_path).await {
            warn!("Failed to update global chunk index: {}", e);
        }
    }

    if let Err(e) = transfer_manager_pool
        .delete_state(&job.job_id, &file_path_str)
        .await
    {
        warn!("Failed to delete transfer state: {}", e);
    }

    Ok(())
}

async fn update_global_index(
    config: &SchedulerConfig,
    global_index: &Arc<crate::sync::engine::ChunkIndex>,
    file_path: &str,
) -> Result<()> {
    use crate::util::cdc::{StreamingFastCDC, StreamingFastCDCConfig};
    use std::time::UNIX_EPOCH;

    let metadata = tokio::fs::metadata(file_path).await?;
    let mtime = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(|e| crate::util::error::HarDataError::Unknown(format!("Time error: {}", e)))?
        .as_secs() as i64;
    let file_size = metadata.len();

    let cdc_config = StreamingFastCDCConfig {
        min_chunk_size: config.min_chunk_size,
        avg_chunk_size: config.avg_chunk_size,
        max_chunk_size: config.max_chunk_size,
        window_size: 256 * 1024 * 1024,
    };
    let cdc = StreamingFastCDC::new(cdc_config);

    let chunk_entries = cdc.chunk_file(std::path::Path::new(file_path)).await?;

    let chunk_infos: Vec<crate::sync::engine::ChunkInfo> = chunk_entries
        .iter()
        .map(|entry| crate::sync::engine::ChunkInfo {
            offset: entry.offset,
            size: entry.length as u64,
            strong_hash: entry.hash,
            weak_hash: entry.weak_hash,
        })
        .collect();

    let indexed = global_index.batch_insert_chunks(file_path, &chunk_infos, mtime, file_size)?;

    info!(
        "Updated global index for {}: {} chunks indexed",
        file_path, indexed
    );

    Ok(())
}

pub fn calculate_dest_path(
    config: &SchedulerConfig,
    job: &SyncJob,
    relative_path: &str,
    files_len: usize,
) -> String {
    let has_relative_path = !relative_path.is_empty();
    let dest_ends_with_slash = job.dest.ends_with('/');
    let dest_is_dir = dest_ends_with_slash || has_relative_path || files_len > 1;

    let base_dest = if job.dest.starts_with('/') {
        job.dest.clone()
    } else {
        let dest_path = job.dest.trim_start_matches("./");
        let data_dir = config
            .data_dir
            .trim_start_matches("./")
            .trim_end_matches('/');

        if dest_path.starts_with(data_dir) {
            job.dest.clone()
        } else {
            format!(
                "{}/{}",
                config.data_dir.trim_end_matches('/'),
                job.dest.trim_start_matches('/').trim_start_matches("./")
            )
        }
    };

    let result = if files_len == 1 && !dest_is_dir {
        base_dest
    } else {
        format!(
            "{}/{}",
            base_dest.trim_end_matches('/'),
            relative_path.trim_start_matches('/')
        )
    };

    tracing::info!(
        "calculate_dest_path: data_dir={}, job.dest={}, relative_path={}, result={}",
        config.data_dir,
        job.dest,
        relative_path,
        result
    );

    result
}
