use crate::adapters::outbound::transport::gateway::TransportConnection;
use crate::application::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::application::sync::scanner::ScannedFile;
use crate::domain::job::JobStatus;
use crate::domain::transfer_state::FileTransferState;
use crate::shared::error::{HarDataError, Result};
use crate::shared::file_ops;
use crate::shared::time::metadata_mtime_nanos;
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
            debug!(operation = "job.tmp_file_published", temporary = %tmp_path, destination = %dest_path, "temporary file publish started");
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
                warn!(operation = "job.tmp_file_cleanup_failed", path = %tmp_path, error = %e, "temporary file cleanup failed");
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
                    operation = "job.tmp_path_cleanup_failed",
                    job_id = %job_id,
                    path = %write_path,
                    error = %cleanup_err,
                    "temporary path cleanup failed after registration error"
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
            operation = "job.tmp_path_unregister_failed",
            job_id = %job_id,
            path = %write_path,
            error = %e,
            "temporary path unregister failed"
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
        info!(operation = "job.cancelled_before_publish", job_id = %job_id, stage = %stage, "job cancelled before destination publish");
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
            warn!(operation = "job.destination_permissions_failed", path = %dest_path, error = %e, "destination permissions update failed");
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
                operation = "job.destination_metadata_failed",
                path = %dest_path,
                error = %e,
                "destination metadata read failed"
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
            warn!(operation = "job.transfer_state_delete_failed", job_id = %job_id, path = %file_path, error = %e, "incompatible transfer state deletion failed");
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
    debug!(operation = "job.destination_directory_ready", path = %dest_path, "destination directory ready");
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
