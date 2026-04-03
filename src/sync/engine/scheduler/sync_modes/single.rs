use crate::core::job::JobStatus;
use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::net::transport::TransportConnection;
use crate::sync::scanner::ScannedFile;
use crate::util::error::{HarDataError, Result};
use crate::util::file_ops;
use crate::util::time::metadata_mtime_nanos;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::super::dedup;
use super::super::infrastructure::config::{JobRuntimeStatus, ReplicateMode, SchedulerConfig};
use super::super::infrastructure::notify::is_cancelled_error;
use super::super::optimization::PrefetchManager;
use super::super::transfer;

fn get_write_path(dest_path: &str, mode: ReplicateMode) -> String {
    match mode {
        ReplicateMode::Append => dest_path.to_string(),
        ReplicateMode::Tmp => format!("{}.tmp", dest_path),
    }
}

fn resolve_dedup_source_path<'a>(
    dest_path: &'a str,
    write_path: &'a str,
    mode: ReplicateMode,
) -> &'a str {
    if mode == ReplicateMode::Tmp
        && !Path::new(write_path).exists()
        && Path::new(dest_path).exists()
    {
        dest_path
    } else {
        write_path
    }
}

async fn finalize_file(dest_path: &str, mode: ReplicateMode) -> Result<()> {
    if mode == ReplicateMode::Tmp {
        let tmp_path = format!("{}.tmp", dest_path);
        if Path::new(&tmp_path).exists() {
            debug!("Finalizing: {} -> {}", tmp_path, dest_path);
            #[cfg(windows)]
            {
                remove_existing_destination(Path::new(dest_path)).await?;
            }
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

async fn register_tmp_write_path(
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    write_path: &str,
    mode: ReplicateMode,
    existed_before_register: bool,
) -> Result<bool> {
    if mode != ReplicateMode::Tmp {
        return Ok(false);
    }

    tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(write_path)
        .await
        .map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to create tmp write path '{}': {}",
                write_path, e
            ))
        })?;

    if let Err(e) = transfer_manager_pool
        .register_tmp_write_path(job_id, write_path)
        .await
    {
        if !existed_before_register {
            if let Err(cleanup_err) = tokio::fs::remove_file(write_path).await {
                warn!(
                    "Failed to cleanup tmp write path after registration error {} path {}: {}",
                    job_id, write_path, cleanup_err
                );
            }
        }
        return Err(e);
    }
    Ok(true)
}

async fn unregister_tmp_write_path(
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    write_path: &str,
    registered: bool,
) {
    if !registered {
        return;
    }

    if let Err(e) = transfer_manager_pool
        .unregister_tmp_write_path(job_id, write_path)
        .await
    {
        warn!(
            "Failed to unregister tmp write path for job {} path {}: {}",
            job_id, write_path, e
        );
    }
}

fn cancelled_error() -> HarDataError {
    HarDataError::Unknown("Job cancelled by user".to_string())
}

fn is_job_cancelled(
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    cancelled_jobs: &DashMap<String, ()>,
    job_id: &str,
) -> bool {
    cancelled_jobs.contains_key(job_id)
        || job_status_cache
            .get(job_id)
            .map(|status| status.status == JobStatus::Cancelled)
            .unwrap_or(false)
}

async fn abort_cancelled_publish(
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    cancelled_jobs: &DashMap<String, ()>,
    job_id: &str,
    dest_path: &str,
    mode: ReplicateMode,
    stage: &str,
) -> Result<()> {
    if is_job_cancelled(job_status_cache, cancelled_jobs, job_id) {
        info!("Job {} was cancelled before {}", job_id, stage);
        cleanup_tmp_file(dest_path, mode).await;
        return Err(cancelled_error());
    }

    Ok(())
}

fn should_cleanup_tmp_after_transfer_error(error: &HarDataError) -> bool {
    is_cancelled_error(&error.to_string())
}

fn apply_destination_permissions(mode: u32, dest_path: &str) {
    #[cfg(unix)]
    if mode != 0 {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(mode);
        if let Err(e) = std::fs::set_permissions(dest_path, permissions) {
            warn!("Failed to set file permissions for {}: {}", dest_path, e);
        }
    }

    #[cfg(not(unix))]
    let _ = (mode, dest_path);
}

async fn load_regular_destination_version(dest_path: &str) -> Option<file_ops::RegularFileVersion> {
    match file_ops::load_regular_file_version(Path::new(dest_path)).await {
        Ok(version) => version,
        Err(e) => {
            warn!(
                "Failed to read destination metadata for {}: {}",
                dest_path, e
            );
            None
        }
    }
}

async fn save_completed_transfer_checkpoint(
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    state: &FileTransferState,
    dest_path: &str,
) -> Result<()> {
    let checkpoint = match load_regular_destination_version(dest_path).await {
        Some(version) => state.clone().with_destination_version(
            version.size,
            version.modified,
            version.change_time,
            version.inode,
        ),
        None => state.clone(),
    };
    transfer_manager_pool
        .checkpoint_state(job_id, &checkpoint)
        .await
}

fn new_transfer_state_for_current_source(
    file_path: &str,
    file: &ScannedFile,
    total_chunks: usize,
) -> FileTransferState {
    FileTransferState::new(file_path.to_string(), total_chunks).with_source_version(
        file.size,
        file.modified,
        file.change_time,
        file.inode,
    )
}

async fn load_transfer_state_for_current_source(
    transfer_manager_pool: &TransferManagerPool,
    job_id: &str,
    file_path: &str,
    file: &ScannedFile,
    write_path: &str,
    total_chunks: usize,
) -> Result<FileTransferState> {
    let Some(state) = transfer_manager_pool.load_state(job_id, file_path).await? else {
        return Ok(new_transfer_state_for_current_source(
            file_path,
            file,
            total_chunks,
        ));
    };

    let current_destination_version = load_regular_destination_version(write_path).await;

    if state.matches_source_version(
        file.size,
        file.modified,
        file.change_time,
        file.inode,
        total_chunks,
    ) && state.matches_destination_version(
        current_destination_version.map(|version| version.size),
        current_destination_version.map(|version| version.modified),
        current_destination_version.and_then(|version| version.change_time),
        current_destination_version.and_then(|version| version.inode),
    ) {
        return Ok(state);
    }

    debug!(
        "Discarding incompatible transfer state for job {} file {} (saved size {:?}, saved modified {:?}, saved source change {:?}, saved source inode {:?}, saved dest size {:?}, saved dest modified {:?}, saved dest change {:?}, saved dest inode {:?}, saved chunks {}, current size {}, current modified {}, current source change {:?}, current source inode {:?}, current dest size {:?}, current dest modified {:?}, current dest change {:?}, current dest inode {:?}, current chunks {})",
        job_id,
        file_path,
        state.source_size,
        state.source_modified,
        state.source_change_time,
        state.source_inode,
        state.dest_size,
        state.dest_modified,
        state.dest_change_time,
        state.dest_inode,
        state.total_chunks,
        file.size,
        file.modified,
        file.change_time,
        file.inode,
        current_destination_version.map(|version| version.size),
        current_destination_version.map(|version| version.modified),
        current_destination_version.and_then(|version| version.change_time),
        current_destination_version.and_then(|version| version.inode),
        total_chunks
    );

    if let Err(e) = transfer_manager_pool.delete_state(job_id, file_path).await {
        warn!(
            "Failed to delete incompatible transfer state for job {} file {}: {}",
            job_id, file_path, e
        );
    }

    Ok(new_transfer_state_for_current_source(
        file_path,
        file,
        total_chunks,
    ))
}

async fn remove_existing_destination(path: &Path) -> Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(HarDataError::FileOperation(format!(
                "Failed to read destination metadata '{}': {}",
                path.display(),
                e
            )));
        }
    };

    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to remove existing directory '{}': {}",
                path.display(),
                e
            ))
        })?;
    } else {
        tokio::fs::remove_file(path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to remove existing path '{}': {}",
                path.display(),
                e
            ))
        })?;
    }

    Ok(())
}

async fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to create parent directory '{}': {}",
                parent.display(),
                e
            ))
        })?;
    }
    Ok(())
}

async fn prepare_directory_destination(dest_path: &str) -> Result<()> {
    let dest = Path::new(dest_path);
    if let Ok(metadata) = tokio::fs::symlink_metadata(dest).await {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            remove_existing_destination(dest).await?;
        }
    }

    tokio::fs::create_dir_all(dest).await.map_err(|e| {
        HarDataError::FileOperation(format!(
            "Failed to create directory '{}': {}",
            dest.display(),
            e
        ))
    })?;

    Ok(())
}

pub(super) async fn sync_directory_entry(file: &ScannedFile, dest_path: &str) -> Result<()> {
    prepare_directory_destination(dest_path).await?;
    apply_destination_permissions(file.mode, dest_path);
    debug!("Ensured directory exists: {}", dest_path);
    Ok(())
}

async fn prepare_symlink_destination(dest_path: &str) -> Result<()> {
    let dest = Path::new(dest_path);
    remove_existing_destination(dest).await?;
    ensure_parent_dir(dest).await
}

async fn prepare_regular_file_destination(dest_path: &str, write_path: &str) -> Result<()> {
    let dest = Path::new(dest_path);
    if let Ok(metadata) = tokio::fs::symlink_metadata(dest).await {
        if metadata.file_type().is_symlink() || metadata.is_dir() {
            remove_existing_destination(dest).await?;
        }
    }

    let write = Path::new(write_path);
    if write != dest {
        if let Ok(metadata) = tokio::fs::symlink_metadata(write).await {
            if metadata.file_type().is_symlink() || metadata.is_dir() {
                remove_existing_destination(write).await?;
            }
        }
    }

    ensure_parent_dir(write).await
}

async fn prepare_empty_file(write_path: &str) -> Result<()> {
    tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(write_path)
        .await
        .map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to prepare empty file '{}': {}",
                write_path, e
            ))
        })?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_single_file<F>(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    cancelled_jobs: &DashMap<String, ()>,
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
        cancelled_jobs,
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
    cancelled_jobs: &DashMap<String, ()>,
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
    if is_job_cancelled(job_status_cache, cancelled_jobs, &job.job_id) {
        info!("Job {} was cancelled during file transfer", job.job_id);
        return Err(cancelled_error());
    }

    let file_path_str = source_path.to_string();

    if file.is_dir {
        sync_directory_entry(file, dest_path).await?;
        return Ok(());
    }

    if file.is_symlink {
        if let Some(ref target) = file.symlink_target {
            let dest = Path::new(dest_path);
            prepare_symlink_destination(dest_path).await?;
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
                return Err(HarDataError::FileOperation(format!(
                    "Symlink sync not supported on this platform: {}",
                    dest_path
                )));
            }
        }
        return Ok(());
    }

    let write_path = get_write_path(dest_path, mode);
    prepare_regular_file_destination(dest_path, &write_path).await?;
    let write_path_existed = Path::new(&write_path).exists();

    if file.size == 0 {
        let tmp_write_path_registered = register_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            mode,
            write_path_existed,
        )
        .await?;
        prepare_empty_file(&write_path).await?;
        file_ops::sync_file_data(&write_path).await?;
        debug!("Prepared empty file: {}", write_path);
        if let Err(e) = abort_cancelled_publish(
            job_status_cache,
            cancelled_jobs,
            &job.job_id,
            dest_path,
            mode,
            "publishing empty file",
        )
        .await
        {
            unregister_tmp_write_path(
                transfer_manager_pool,
                &job.job_id,
                &write_path,
                tmp_write_path_registered,
            )
            .await;
            return Err(e);
        }
        finalize_file(dest_path, mode).await?;
        unregister_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            tmp_write_path_registered,
        )
        .await;
        apply_destination_permissions(file.mode, dest_path);
        file_ops::sync_parent_directory(dest_path).await?;
        let state = new_transfer_state_for_current_source(&file_path_str, file, 0);
        if let Err(e) = save_completed_transfer_checkpoint(
            transfer_manager_pool,
            &job.job_id,
            &state,
            dest_path,
        )
        .await
        {
            warn!("Failed to save completed transfer checkpoint: {}", e);
        }
        return Ok(());
    }

    let dedup_source_path = resolve_dedup_source_path(dest_path, &write_path, mode);
    let tmp_write_path_registered = register_tmp_write_path(
        transfer_manager_pool,
        &job.job_id,
        &write_path,
        mode,
        write_path_existed,
    )
    .await?;

    let source_path_buf = std::path::Path::new(source_path);
    let mut chunks = transfer::chunk_file(config, source_path_buf, connection).await?;
    debug!("File split into {} chunks (mode={:?})", chunks.len(), mode);

    let (existing_strong_hashes, dedup_count, local_chunk_info, global_chunk_info) =
        if job.job_type.is_full() {
            debug!(
                "Job {} is full mode, skipping deduplication for {}",
                job.job_id, source_path
            );
            (
                HashSet::<[u8; 32]>::new(),
                0usize,
                HashMap::<[u8; 32], Vec<crate::sync::engine::ChunkLocation>>::new(),
                HashMap::<[u8; 32], Vec<crate::sync::engine::ChunkLocation>>::new(),
            )
        } else {
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
                dedup_source_path,
                prefetch_manager,
                chunk_index,
                global_index,
            )
            .await?;
            (
                existing_strong_hashes,
                dedup_count,
                local_chunk_info,
                global_chunk_info,
            )
        };

    if dedup_count > 0 {
        debug!(
            "{}/{} chunks already exist, will skip network transfer",
            dedup_count,
            chunks.len()
        );
    }

    let mut state = load_transfer_state_for_current_source(
        transfer_manager_pool,
        &job.job_id,
        &file_path_str,
        file,
        &write_path,
        chunks.len(),
    )
    .await?;
    let cancel_callback: crate::sync::transfer::batch::CancelCallback = {
        let job_id = job.job_id.clone();
        let job_status_cache = job_status_cache.clone();
        let cancelled_jobs = cancelled_jobs.clone();
        Arc::new(move || is_job_cancelled(&job_status_cache, &cancelled_jobs, &job_id))
    };

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
        cancel_callback,
        on_batch_progress,
    )
    .await;

    if let Err(e) = transfer_result {
        if should_cleanup_tmp_after_transfer_error(&e) {
            cleanup_tmp_file(dest_path, mode).await;
            unregister_tmp_write_path(
                transfer_manager_pool,
                &job.job_id,
                &write_path,
                tmp_write_path_registered,
            )
            .await;
        } else if mode == ReplicateMode::Tmp {
            warn!(
                "Keeping tmp file {}.tmp for resumable retry after transfer error: {}",
                dest_path, e
            );
        }
        return Err(e);
    }

    if let Err(e) = abort_cancelled_publish(
        job_status_cache,
        cancelled_jobs,
        &job.job_id,
        dest_path,
        mode,
        "publishing transferred file",
    )
    .await
    {
        unregister_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            tmp_write_path_registered,
        )
        .await;
        return Err(e);
    }

    let write_path_buf = std::path::Path::new(&write_path);
    if write_path_buf.exists() {
        let dest_metadata = tokio::fs::metadata(write_path_buf).await?;
        if dest_metadata.len() != file.size {
            debug!(
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
        file_ops::sync_file_data(&write_path).await?;
    }

    if let Err(e) = abort_cancelled_publish(
        job_status_cache,
        cancelled_jobs,
        &job.job_id,
        dest_path,
        mode,
        "finalizing tmp file",
    )
    .await
    {
        unregister_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            tmp_write_path_registered,
        )
        .await;
        return Err(e);
    }

    finalize_file(dest_path, mode).await?;
    unregister_tmp_write_path(
        transfer_manager_pool,
        &job.job_id,
        &write_path,
        tmp_write_path_registered,
    )
    .await;

    apply_destination_permissions(file.mode, dest_path);
    file_ops::sync_parent_directory(dest_path).await?;

    if let Some(gindex) = global_index {
        if let Err(e) = update_global_index(config, gindex, dest_path).await {
            warn!("Failed to update global chunk index: {}", e);
        }
    }

    if let Err(e) =
        save_completed_transfer_checkpoint(transfer_manager_pool, &job.job_id, &state, dest_path)
            .await
    {
        warn!("Failed to save completed transfer checkpoint: {}", e);
    }

    Ok(())
}

async fn update_global_index(
    config: &SchedulerConfig,
    global_index: &Arc<crate::sync::engine::ChunkIndex>,
    file_path: &str,
) -> Result<()> {
    use crate::util::cdc::{StreamingFastCDC, StreamingFastCDCConfig};

    let metadata = tokio::fs::metadata(file_path).await?;
    let mtime = metadata_mtime_nanos(&metadata);
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

    debug!(
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
) -> Result<String> {
    let has_relative_path = !relative_path.is_empty();
    let dest_ends_with_slash = job.dest.ends_with('/');
    let dest_is_dir = dest_ends_with_slash || has_relative_path || files_len > 1;

    let base_dest = resolve_base_dest_path(config, job)?;

    let result = if files_len == 1 && !dest_is_dir {
        base_dest
    } else {
        config.resolve_destination_path(
            base_dest
                .join(relative_path.trim_start_matches('/'))
                .to_string_lossy()
                .as_ref(),
        )?
    };

    tracing::debug!(
        "calculate_dest_path: data_dir={}, job.dest={}, relative_path={}, result={}",
        config.data_dir,
        job.dest,
        relative_path,
        result.display()
    );

    Ok(result.to_string_lossy().to_string())
}

pub fn resolve_base_dest_path(config: &SchedulerConfig, job: &SyncJob) -> Result<PathBuf> {
    config.resolve_runtime_destination_path(&job.dest)
}

#[cfg(test)]
mod tests {
    use super::{
        calculate_dest_path, finalize_file, load_transfer_state_for_current_source,
        prepare_regular_file_destination, resolve_base_dest_path, resolve_dedup_source_path,
        should_cleanup_tmp_after_transfer_error, sync_single_file, sync_single_file_with_mode,
    };
    use crate::agent::compute::ComputeService;
    use crate::agent::server::tcp::TcpServer;
    use crate::core::{FileTransferState, JobStatus, JobType};
    use crate::sync::engine::job::{SyncJob, TransferManagerPool};
    use crate::sync::engine::scheduler::{JobRuntimeStatus, ReplicateMode, SchedulerConfig};
    use crate::sync::net::tcp::TcpClient;
    use crate::sync::net::transport::TransportConnection;
    use crate::sync::scanner::ScannedFile;
    use crate::sync::storage::db::Database;
    use crate::util::error::HarDataError;
    use crate::util::file_ops;
    use crate::util::time::{metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos};
    use dashmap::DashMap;
    use sqlx::SqlitePool;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_regular_file_destination_removes_existing_symlink() {
        let root = temp_dir("prepare-regular-file");
        let outside = root.join("outside.txt");
        let dest = root.join("dest/file.txt");
        let write_path = format!("{}.tmp", dest.to_string_lossy());

        std::fs::write(&outside, b"outside").unwrap();
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&outside, &dest).unwrap();

        prepare_regular_file_destination(
            dest.to_str().unwrap(),
            PathBuf::from(&write_path).to_str().unwrap(),
        )
        .await
        .unwrap();

        let dest_metadata = tokio::fs::symlink_metadata(&dest).await;
        assert!(dest_metadata.is_err());
        assert!(dest.parent().unwrap().exists());
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "outside");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn should_cleanup_tmp_after_transfer_error_only_for_cancelled_jobs() {
        assert!(should_cleanup_tmp_after_transfer_error(
            &HarDataError::Unknown("Job cancelled by user".to_string(),)
        ));
        assert!(!should_cleanup_tmp_after_transfer_error(
            &HarDataError::NetworkError("network failed".to_string(),)
        ));
    }

    #[test]
    fn resolve_dedup_source_path_falls_back_to_existing_destination_in_tmp_mode() {
        let root = temp_dir("dedup-source-dest");
        let dest = root.join("dest.bin");
        let write_path = format!("{}.tmp", dest.to_string_lossy());
        std::fs::write(&dest, b"dest").unwrap();

        assert_eq!(
            resolve_dedup_source_path(
                dest.to_str().unwrap(),
                PathBuf::from(&write_path).to_str().unwrap(),
                crate::sync::engine::scheduler::ReplicateMode::Tmp,
            ),
            dest.to_str().unwrap()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn load_transfer_state_accepts_legacy_second_precision_source_mtime() {
        let root = temp_dir("legacy-source-mtime");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        std::fs::write(&source, b"AAAA").unwrap();

        let metadata = std::fs::metadata(&source).unwrap();
        let legacy_seconds = metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(metadata.len(), legacy_seconds, None, None);
        transfer_manager_pool
            .save_state("job-legacy-source-mtime", &state)
            .await
            .unwrap();

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let restored = load_transfer_state_for_current_source(
            &transfer_manager_pool,
            "job-legacy-source-mtime",
            source.to_str().unwrap(),
            &file,
            dest.to_str().unwrap(),
            1,
        )
        .await
        .unwrap();

        assert_eq!(restored.source_size, Some(metadata.len()));
        assert_eq!(restored.source_modified, Some(legacy_seconds));

        transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_dedup_source_path_prefers_existing_tmp_file_when_resuming() {
        let root = temp_dir("dedup-source-tmp");
        let dest = root.join("dest.bin");
        let write_path = root.join("dest.bin.tmp");
        std::fs::write(&dest, b"dest").unwrap();
        std::fs::write(&write_path, b"tmp").unwrap();

        assert_eq!(
            resolve_dedup_source_path(
                dest.to_str().unwrap(),
                write_path.to_str().unwrap(),
                crate::sync::engine::scheduler::ReplicateMode::Tmp,
            ),
            write_path.to_str().unwrap()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_base_dest_path_rejects_prefix_collision() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-prefix".to_string(),
            PathBuf::from("/tmp/source"),
            "syncfoo/out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/syncfoo/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_rebases_parent_traversal_under_data_dir() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-parent-traversal".to_string(),
            PathBuf::from("/tmp/source"),
            "../escape/out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/escape/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_normalizes_embedded_parent_segments() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-embedded-parent".to_string(),
            PathBuf::from("/tmp/source"),
            "sync/nested/../out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_preserves_true_subpath() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-subpath".to_string(),
            PathBuf::from("/tmp/source"),
            "sync/out.txt".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            resolve_base_dest_path(&config, &job)
                .unwrap()
                .to_string_lossy(),
            "sync/out.txt"
        );
    }

    #[test]
    fn resolve_base_dest_path_rejects_external_absolute_destination_by_default() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-external".to_string(),
            PathBuf::from("/tmp/source"),
            "/tmp/out.txt".to_string(),
            "local".to_string(),
        );

        let err = resolve_base_dest_path(&config, &job).unwrap_err();
        assert!(err.to_string().contains("outside sync.data_dir"));
    }

    #[test]
    fn calculate_dest_path_allows_external_absolute_destination_when_enabled() {
        let config = SchedulerConfig {
            data_dir: "sync".to_string(),
            allow_external_destinations: true,
            ..SchedulerConfig::default()
        };
        let job = SyncJob::new(
            "job-external-allowed".to_string(),
            PathBuf::from("/tmp/source"),
            "/tmp/out".to_string(),
            "local".to_string(),
        );

        assert_eq!(
            calculate_dest_path(&config, &job, "nested/file.txt", 2).unwrap(),
            "/tmp/out/nested/file.txt"
        );
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_does_not_publish_after_cancelled_local_reuse() {
        let root = temp_dir("tmp-cancelled-publish");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"ABCD").unwrap();
        std::fs::write(&dest, b"ABCD").unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-cancel-before-publish".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = Arc::new(DashMap::new());
        job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 0,
                current_size: 0,
                total_size: 4,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let metadata = std::fs::metadata(&source).unwrap();
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };
        let progress_cache = job_status_cache.clone();
        let cancelled_jobs = Arc::new(DashMap::new());
        let cancelled_jobs_for_callback = cancelled_jobs.clone();
        let job_id = job.job_id.clone();

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            job_status_cache.as_ref(),
            cancelled_jobs.as_ref(),
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            move |_| {
                if let Some(mut status) = progress_cache.get_mut(&job_id) {
                    status.status = JobStatus::Cancelled;
                }
                cancelled_jobs_for_callback.insert(job_id.clone(), ());
            },
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Job cancelled by user"));
        assert_eq!(std::fs::read(&dest).unwrap(), b"ABCD");
        assert!(!tmp.exists());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_does_not_publish_after_runtime_cancel_cleanup() {
        let root = temp_dir("tmp-cancelled-runtime-cleanup");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"ABCD").unwrap();
        std::fs::write(&dest, b"ABCD").unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-cancel-after-runtime-cleanup".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = Arc::new(DashMap::new());
        let cancelled_jobs = Arc::new(DashMap::new());
        job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 0,
                current_size: 0,
                total_size: 4,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let metadata = std::fs::metadata(&source).unwrap();
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };
        let progress_cache = job_status_cache.clone();
        let cancelled_jobs_for_callback = cancelled_jobs.clone();
        let job_id = job.job_id.clone();

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            job_status_cache.as_ref(),
            cancelled_jobs.as_ref(),
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            move |_| {
                progress_cache.remove(&job_id);
                cancelled_jobs_for_callback.insert(job_id.clone(), ());
            },
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Job cancelled by user"));
        assert_eq!(std::fs::read(&dest).unwrap(), b"ABCD");
        assert!(!tmp.exists());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_fails_when_transfer_state_load_fails() {
        let root = temp_dir("state-load-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        std::fs::write(&source, b"ABCD").unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Append,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-load-failure".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("DROP TABLE transfer_states")
            .execute(&raw_pool)
            .await
            .unwrap();

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };

        let err = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Append,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("no such table: transfer_states"));
        assert!(!dest.exists());

        raw_pool.close().await;
        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_discards_stale_resumable_state_when_source_changes() {
        let root = temp_dir("state-source-version-change");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"AAAA").unwrap();
        std::fs::write(&tmp, b"AAAA").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-source-changed".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let stale_metadata = std::fs::metadata(&source).unwrap();
        let stale_modified = metadata_mtime_nanos(&stale_metadata);
        let mut stale_state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(stale_metadata.len(), stale_modified, None, None);
        stale_state.mark_chunk_completed(0);
        transfer_manager_pool
            .save_state(&job.job_id, &stale_state)
            .await
            .unwrap();

        let (metadata, modified) = {
            let mut changed = None;
            for _ in 0..20 {
                std::thread::sleep(Duration::from_millis(5));
                std::fs::write(&source, b"BBBB").unwrap();
                let metadata = std::fs::metadata(&source).unwrap();
                let modified = metadata_mtime_nanos(&metadata);
                if modified != stale_modified {
                    changed = Some((metadata, modified));
                    break;
                }
            }
            changed.expect("expected rewritten source file to have a distinct mtime")
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified,
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };

        sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"BBBB");
        assert!(!tmp.exists());
        let checkpoint = db
            .load_transfer_state(&job.job_id, source.to_str().unwrap())
            .await
            .unwrap()
            .expect("completed sync should persist a recovery checkpoint");
        assert!(checkpoint.cache_only);
        assert_eq!(checkpoint.source_size, Some(metadata.len()));
        assert_eq!(checkpoint.source_modified, Some(modified));
        assert!(checkpoint.dest_modified.is_some());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sync_single_file_discards_stale_resumable_state_when_source_content_changes_same_size_and_preserved_mtime(
    ) {
        use filetime::{set_file_mtime, FileTime};

        let root = temp_dir("state-source-identity-change");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"AAAA").unwrap();
        std::fs::write(&tmp, b"AAAA").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-source-identity".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let stale_metadata = std::fs::metadata(&source).unwrap();
        let stale_modified = metadata_mtime_nanos(&stale_metadata);
        let stale_change_time = metadata_ctime_nanos(&stale_metadata);
        let stale_inode = metadata_inode(&stale_metadata);
        let mut stale_state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(
                stale_metadata.len(),
                stale_modified,
                stale_change_time,
                stale_inode,
            );
        stale_state.mark_chunk_completed(0);
        transfer_manager_pool
            .save_state(&job.job_id, &stale_state)
            .await
            .unwrap();

        let original_source_mtime =
            FileTime::from_last_modification_time(&std::fs::metadata(&source).unwrap());
        let (metadata, change_time, inode) = {
            let mut changed = None;
            for _ in 0..20 {
                std::thread::sleep(Duration::from_millis(5));
                std::fs::write(&source, b"BBBB").unwrap();
                set_file_mtime(&source, original_source_mtime).unwrap();
                let metadata = std::fs::metadata(&source).unwrap();
                let change_time = metadata_ctime_nanos(&metadata);
                let inode = metadata_inode(&metadata);
                if change_time != stale_change_time || inode != stale_inode {
                    changed = Some((metadata, change_time, inode));
                    break;
                }
            }
            changed.expect("expected rewritten source file to change source identity")
        };
        let modified = metadata_mtime_nanos(&metadata);
        assert_eq!(modified, stale_modified);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified,
            change_time,
            inode,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };

        sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"BBBB");
        assert!(!tmp.exists());
        let checkpoint = db
            .load_transfer_state(&job.job_id, source.to_str().unwrap())
            .await
            .unwrap()
            .expect("completed sync should persist refreshed checkpoint");
        assert_eq!(checkpoint.source_size, Some(metadata.len()));
        assert_eq!(checkpoint.source_modified, Some(modified));
        assert_eq!(checkpoint.source_change_time, change_time);
        assert_eq!(checkpoint.source_inode, inode);

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_discards_stale_resumable_state_when_tmp_file_is_missing() {
        let root = temp_dir("state-tmp-file-missing");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("source.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"BBBB").unwrap();
        std::fs::write(&tmp, b"AAAA").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-state-tmp-missing".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let tmp_version = file_ops::load_regular_file_version(&tmp)
            .await
            .unwrap()
            .unwrap();
        let mut stale_state = FileTransferState::new(source.to_string_lossy().to_string(), 1)
            .with_source_version(metadata.len(), metadata_mtime_nanos(&metadata), None, None)
            .with_destination_version(
                tmp_version.size,
                tmp_version.modified,
                tmp_version.change_time,
                tmp_version.inode,
            );
        stale_state.mark_chunk_completed(0);
        transfer_manager_pool
            .save_state(&job.job_id, &stale_state)
            .await
            .unwrap();
        std::fs::remove_file(&tmp).unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let bind_addr = format!("127.0.0.1:{}", port);
        let compute = Arc::new(ComputeService::new(root.to_str().unwrap()).await.unwrap());
        let server = TcpServer::new(&bind_addr, compute, root.to_str().unwrap())
            .await
            .unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::MetadataExt;
            metadata.mode()
        };
        #[cfg(not(unix))]
        let mode = 0u32;

        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new(bind_addr).unwrap(),
        };

        sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"BBBB");
        assert!(!tmp.exists());

        server_handle.abort();
        let _ = server_handle.await;
        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_cleans_tmp_when_registration_fails() {
        let root = temp_dir("tmp-register-failure-cleans");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        transfer_manager_pool.shutdown().await;

        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-register-fails".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(!tmp.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_preserves_existing_tmp_when_registration_fails() {
        let root = temp_dir("tmp-register-failure-preserves-existing");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        transfer_manager_pool.shutdown().await;

        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"").unwrap();
        std::fs::write(&tmp, b"resume-state").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-register-fails-preserve".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        let result = sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(std::fs::read(&tmp).unwrap(), b"resume-state");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn finalize_file_tmp_mode_replaces_existing_destination() {
        let root = temp_dir("finalize-tmp-replaces-existing");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&dest, b"old").unwrap();
        std::fs::write(&tmp, b"new").unwrap();

        finalize_file(dest.to_str().unwrap(), ReplicateMode::Tmp)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
        assert!(!tmp.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_append_mode_truncates_existing_destination_for_empty_file() {
        let root = temp_dir("append-empty-truncates-dest");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        std::fs::write(&source, b"").unwrap();
        std::fs::write(&dest, b"stale-data").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Append,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-append-empty".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        sync_single_file(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::metadata(&dest).unwrap().len(), 0);

        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn sync_single_file_tmp_mode_truncates_stale_tmp_for_empty_file() {
        let root = temp_dir("tmp-empty-truncates-stale-tmp");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let transfer_manager_pool = TransferManagerPool::new(db.clone());
        let source = root.join("empty.bin");
        let dest = root.join("dest.bin");
        let tmp = root.join("dest.bin.tmp");
        std::fs::write(&source, b"").unwrap();
        std::fs::write(&tmp, b"stale-tmp-data").unwrap();

        let config = SchedulerConfig {
            data_dir: root.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            replicate_mode: ReplicateMode::Tmp,
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.chunk_cache_path).unwrap();

        let job = SyncJob::new(
            "job-tmp-empty".to_string(),
            PathBuf::from(&source),
            dest.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        let job_status_cache = DashMap::new();
        let cancelled_jobs = DashMap::new();

        let metadata = std::fs::metadata(&source).unwrap();
        let file = ScannedFile {
            path: source.clone(),
            size: metadata.len(),
            modified: metadata_mtime_nanos(&metadata),
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:9".to_string()).unwrap(),
        };

        sync_single_file_with_mode(
            &config,
            &transfer_manager_pool,
            &job_status_cache,
            &cancelled_jobs,
            &job,
            &file,
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
            &mut connection,
            1,
            |_| {},
            ReplicateMode::Tmp,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::metadata(&dest).unwrap().len(), 0);
        assert!(!tmp.exists());

        transfer_manager_pool.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
}
