use crate::core::job::JobStatus;
use crate::sync::engine::job::SyncJob;
use crate::sync::scanner::ScannedFile;
use crate::util::error::{HarDataError, Result};
use crate::util::time::{
    metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos, optional_timestamps_match,
    timestamps_match,
};
use dashmap::DashMap;
use regex::Regex;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::core::{FileSyncState, SyncScheduler};
use super::infrastructure::config::normalize_path;
use super::infrastructure::connection;
use super::infrastructure::notify::is_cancelled_error;
use super::sync_modes::files as sync_files;

const MIN_STABILITY_RETRY_DELAY_MS: u64 = 100;

struct RunningJobGuard {
    job_id: String,
    running_jobs: Arc<DashMap<String, ()>>,
    cancelled_jobs: Arc<DashMap<String, ()>>,
}

impl RunningJobGuard {
    fn new(
        job_id: String,
        running_jobs: Arc<DashMap<String, ()>>,
        cancelled_jobs: Arc<DashMap<String, ()>>,
    ) -> Self {
        running_jobs.insert(job_id.clone(), ());
        Self {
            job_id,
            running_jobs,
            cancelled_jobs,
        }
    }
}

fn source_file_matches_cached_state(
    file: &ScannedFile,
    size: u64,
    mtime: i64,
    change_time: Option<i64>,
    inode: Option<u64>,
) -> bool {
    size == file.size
        && timestamps_match(mtime, file.modified)
        && optional_timestamps_match(change_time, file.change_time)
        && inode == file.inode
}

impl Drop for RunningJobGuard {
    fn drop(&mut self) {
        self.running_jobs.remove(&self.job_id);
        self.cancelled_jobs.remove(&self.job_id);
    }
}

struct ScanFilter {
    exclude: Vec<Regex>,
    include: Vec<Regex>,
}

struct RemoteScanResult {
    files: Vec<ScannedFile>,
    source_is_single_file: bool,
    root_excluded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobExecutionResult {
    NoTransfer { retry_due_to_stability: bool },
    Transferred { retry_due_to_stability: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DestinationSyncState {
    requires_sync: bool,
    dest_mtime: Option<i64>,
    dest_change_time: Option<i64>,
    dest_inode: Option<u64>,
}

impl JobExecutionResult {
    fn retry_due_to_stability(self) -> bool {
        match self {
            Self::NoTransfer {
                retry_due_to_stability,
            }
            | Self::Transferred {
                retry_due_to_stability,
            } => retry_due_to_stability,
        }
    }

    fn transferred(self) -> bool {
        matches!(self, Self::Transferred { .. })
    }
}

impl ScanFilter {
    fn new(exclude_patterns: &[String], include_patterns: &[String]) -> Result<Self> {
        Ok(Self {
            exclude: compile_patterns(exclude_patterns, "exclude_regex")?,
            include: compile_patterns(include_patterns, "include_regex")?,
        })
    }

    fn excludes(&self, path: &str) -> bool {
        self.exclude.iter().any(|regex| regex.is_match(path))
    }

    fn include_matches(&self, path: &str) -> bool {
        self.include.is_empty() || self.include.iter().any(|regex| regex.is_match(path))
    }

    fn should_scan_dir(&self, path: &str) -> bool {
        !self.excludes(path)
    }

    fn should_include_dir(&self, path: &str) -> bool {
        self.should_scan_dir(path) && self.include_matches(path)
    }

    fn should_include_file(&self, path: &str) -> bool {
        self.should_scan_dir(path) && self.include_matches(path)
    }
}

impl SyncScheduler {
    async fn list_directory_once(
        &self,
        path: &str,
        region: &str,
    ) -> Result<crate::core::protocol::ListDirectoryResponse> {
        let mut conn = connection::get_connection_with_retry_for_region_with_selector(
            &self.config,
            &self.connection_pools,
            region,
            &self.shutdown,
            Some(&self.protocol_selector),
        )
        .await?;
        conn.list_directory(path).await
    }

    async fn root_path_is_single_file(&self, root_path: &str, region: &str) -> Result<bool> {
        let Some(parent_path) = parent_lookup_path(root_path) else {
            return Ok(false);
        };
        let parent_response = self.list_directory_once(&parent_path, region).await?;
        Ok(parent_listing_confirms_single_file(
            root_path,
            &parent_response.files,
        ))
    }

    async fn load_root_directory_entry(
        &self,
        root_path: &str,
        region: &str,
    ) -> Result<Option<ScannedFile>> {
        let Some(parent_path) = parent_lookup_path(root_path) else {
            return Ok(None);
        };
        let root_name = Path::new(root_path.trim_end_matches('/'))
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if root_name.is_empty() {
            return Ok(None);
        }

        let parent_response = self.list_directory_once(&parent_path, region).await?;
        Ok(parent_response
            .files
            .into_iter()
            .find(|entry| entry.path == root_name && entry.is_directory)
            .map(|entry| ScannedFile {
                path: PathBuf::from(root_path),
                size: 0,
                modified: entry.modified,
                change_time: entry.change_time,
                inode: entry.inode,
                is_dir: true,
                mode: entry.mode,
                is_symlink: false,
                symlink_target: None,
            }))
    }

    async fn load_job_tmp_preserve_paths(&self, job: &SyncJob) -> Result<HashSet<PathBuf>> {
        let root = normalize_path(&super::sync_modes::single::resolve_base_dest_path(
            &self.config,
            job,
        )?);
        let mut paths = HashSet::new();

        for path in self.transfer_manager_pool.job_tmp_write_paths(&job.job_id) {
            let path = normalize_path(Path::new(&path));
            if path.starts_with(&root) {
                paths.insert(path);
            }
        }

        for path in self.db.load_tmp_transfer_paths_by_job(&job.job_id).await? {
            let path = normalize_path(Path::new(&path));
            if path.starts_with(&root) {
                paths.insert(path);
            }
        }

        Ok(paths)
    }

    pub(super) async fn worker_loop(&self, worker_id: usize) {
        info!("Worker {} started", worker_id);

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Worker {} received shutdown signal", worker_id);
                break;
            }

            let job = self.job_queue.dequeue().await;

            if let Some(mut job) = job {
                info!("Worker {} picked up job {}", worker_id, job.job_id);

                let permit = match self.semaphore.acquire().await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Worker {} failed to acquire semaphore: {}", worker_id, e);
                        continue;
                    }
                };
                let _running_guard = RunningJobGuard::new(
                    job.job_id.clone(),
                    self.running_jobs.clone(),
                    self.cancelled_jobs.clone(),
                );

                match self.ensure_job_not_cancelled(&job.job_id).await {
                    Ok(()) => {}
                    Err(e) if is_cancelled_error(&e.to_string()) => {
                        info!("Job {} was cancelled, skipping", job.job_id);
                        self.notify_job_cancelled(&job.job_id).await;
                        drop(permit);
                        continue;
                    }
                    Err(e) => {
                        self.handle_non_executable_job_stop(&job.job_id, &e, "before execution")
                            .await;
                        drop(permit);
                        continue;
                    }
                }

                if job.is_first_round {
                    job.is_first_round = false;
                    job.round_id += 1;
                }

                info!(
                    "Worker {} executing job {} round {} (type={})",
                    worker_id,
                    job.job_id,
                    job.round_id,
                    job.job_type.as_str()
                );

                let mut previous_round_id = None;
                if let Some(mut cached_job) = self.job_cache.get_mut(&job.job_id) {
                    previous_round_id = Some(cached_job.round_id);
                    cached_job.round_id = job.round_id;
                    cached_job.is_last_round = job.is_last_round;
                }
                self.persist_job_round_state(&job.job_id, job.round_id, job.is_last_round)
                    .await;
                self.reset_progress_for_new_round(&job, previous_round_id)
                    .await;

                match self.execute_job(job.clone()).await {
                    Ok(execution_result) => {
                        info!(
                            "Worker {} completed job {} round {}",
                            worker_id, job.job_id, job.round_id
                        );

                        match self.ensure_job_not_cancelled(&job.job_id).await {
                            Ok(()) => {}
                            Err(e) if is_cancelled_error(&e.to_string()) => {
                                info!(
                                    "Job {} was cancelled during execution, skip success handling",
                                    job.job_id
                                );
                                self.notify_job_cancelled(&job.job_id).await;
                                drop(permit);
                                continue;
                            }
                            Err(e) => {
                                self.handle_non_executable_job_stop(
                                    &job.job_id,
                                    &e,
                                    "after execution",
                                )
                                .await;
                                drop(permit);
                                continue;
                            }
                        }

                        if job.is_completed() {
                            info!("Job {} all rounds completed", job.job_id);
                            self.notify_job_completed(&job.job_id).await;
                        } else if job.job_type.is_sync() {
                            self.mark_retry_success(&job.job_id).await;
                            if self
                                .should_notify_job_pending_after_round(
                                    &job.job_id,
                                    execution_result.transferred(),
                                )
                                .await
                            {
                                self.notify_job_pending(&job.job_id).await;
                            }
                            let next_delay = next_sync_schedule_delay(
                                job.scan_interval,
                                self.config.stability_threshold,
                                execution_result.retry_due_to_stability(),
                            );
                            let next_run = std::time::Instant::now() + next_delay;
                            info!(
                                "Job {} (sync) round {} done, scheduling next run in {:?}",
                                job.job_id, job.round_id, next_delay
                            );
                            self.schedule_delayed_job(next_run, job).await;
                        }
                    }
                    Err(e) => {
                        error!("Worker {} failed job {}: {}", worker_id, job.job_id, e);
                        self.handle_job_execution_error(&job.job_id, &e).await;
                    }
                }

                drop(permit);
            } else {
                tokio::select! {
                    _ = self.job_notify.notified() => {}
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                }
            }
        }

        info!("Worker {} stopped", worker_id);
    }

    async fn should_notify_job_pending_after_round(&self, job_id: &str, transferred: bool) -> bool {
        if transferred {
            return true;
        }

        if let Some(entry) = self.job_status_cache.get(job_id) {
            return entry.status != JobStatus::Pending || entry.error_message.is_some();
        }

        match self.load_job_snapshot(job_id).await {
            Ok(Some(snapshot)) => {
                snapshot.status != JobStatus::Pending || snapshot.error_message.is_some()
            }
            Ok(None) => false,
            Err(e) => {
                warn!(
                    "Failed to inspect persisted status before idle pending update for job {}: {}",
                    job_id, e
                );
                true
            }
        }
    }

    async fn execute_job(&self, job: SyncJob) -> Result<JobExecutionResult> {
        info!(
            "Executing job {} (region='{}'): {:?} -> {}",
            job.job_id, job.region, job.source, job.dest
        );

        self.ensure_job_not_cancelled(&job.job_id).await?;

        let source_str = job
            .source
            .to_str()
            .ok_or_else(|| HarDataError::Unknown("Invalid source path".to_string()))?;

        info!(
            "Requesting remote directory listing for: {} (region='{}')",
            source_str, job.region
        );
        let scan_filter = ScanFilter::new(&job.exclude_regex, &job.include_regex)?;

        let scan_result = self
            .list_directory_recursive(source_str, &job.region, &scan_filter)
            .await?;
        let source_is_single_file = scan_result.source_is_single_file;
        let root_excluded = scan_result.root_excluded;
        let mut files = scan_result.files;

        if root_excluded {
            info!(
                "Job {} root path {} is excluded by filters, skip this round without cleanup",
                job.job_id, source_str
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: false,
            });
        }

        self.ensure_job_not_cancelled(&job.job_id).await?;

        info!("Job {} remote scan found {} files", job.job_id, files.len());

        if has_active_scan_filters(&job) && !source_is_single_file && files.is_empty() {
            info!(
                "Job {} filters matched no files, skip destination root creation for this round",
                job.job_id
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: false,
            });
        }

        if !source_is_single_file {
            if !has_active_scan_filters(&job) {
                if let Some(root_entry) = self
                    .load_root_directory_entry(source_str, &job.region)
                    .await?
                {
                    files.insert(0, root_entry);
                }
            }
            ensure_directory_sync_root(&self.config, &job).await?;
        }

        if should_cleanup_deleted_targets(&job) {
            let cleanup_result = match self.load_job_tmp_preserve_paths(&job).await {
                Ok(preserved_tmp_paths) => {
                    cleanup_deleted_targets(
                        &self.config,
                        &job,
                        &files,
                        source_is_single_file,
                        &preserved_tmp_paths,
                    )
                    .await
                }
                Err(e) => {
                    warn!(
                        "Failed to load tmp preserve paths for job {}: {}, skipping deleted target cleanup",
                        job.job_id, e
                    );
                    Ok(())
                }
            };
            cleanup_result?;
            trim_deleted_source_tracking(
                &self.synced_files_cache,
                &self.size_freezers,
                &job.job_id,
                &files,
            )
            .await;
        } else if has_active_scan_filters(&job) {
            info!(
                "Job {} uses include/exclude filters, skip deleted target cleanup to avoid partial-visibility removals",
                job.job_id
            );
        }

        if files.is_empty() {
            info!("Job {} has no files to sync", job.job_id);
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: false,
            });
        }

        let mut has_pending_unstable_files = false;
        let stable_files = if !job.is_final_transfer() {
            let file_sizes: Vec<(String, u64, i64)> = files
                .iter()
                .map(|f| (f.path.to_string_lossy().to_string(), f.size, f.modified))
                .collect();
            let scanned_file_count = file_sizes.len();
            let size_freezer = self.size_freezer_for_job(&job.job_id);
            let stable_names: HashSet<String> = size_freezer
                .get_stable_files(&file_sizes)
                .await
                .into_iter()
                .collect();
            let pending_stability_count =
                pending_stability_file_count(scanned_file_count, stable_names.len());
            has_pending_unstable_files = pending_stability_count > 0;
            let stable_files: Vec<ScannedFile> = files
                .into_iter()
                .filter(|f| stable_names.contains(&f.path.to_string_lossy().to_string()))
                .collect();
            info!(
                "Job {} stability filter: {} stable / {} scanned ({} pending stability)",
                job.job_id,
                stable_files.len(),
                scanned_file_count,
                pending_stability_count
            );
            stable_files
        } else {
            info!("Job {} is final mode, skip stability filter", job.job_id);
            files
        };

        if stable_files.is_empty() {
            let retry_delay =
                next_sync_schedule_delay(job.scan_interval, self.config.stability_threshold, true);
            info!(
                "Job {} has no stable files yet, will re-check stability in {:?}",
                job.job_id, retry_delay
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: has_pending_unstable_files,
            });
        }

        self.synced_files_cache
            .entry(job.job_id.clone())
            .or_default();

        let changed_files: Vec<ScannedFile> = if job.job_type.is_full() {
            info!(
                "Job {} is full mode, forcing all {} files to re-sync",
                job.job_id,
                stable_files.len()
            );
            stable_files
        } else {
            let files_len = stable_files.len();
            let job_cache = self.synced_files_cache.get(&job.job_id);
            let mut changed_files = Vec::new();
            let mut refreshed_dest_mtimes = Vec::new();
            let mut refreshed_cache_states = Vec::new();

            for file in stable_files {
                let file_path = file.path.to_string_lossy().to_string();
                let cached_state = job_cache
                    .as_ref()
                    .and_then(|cache| cache.get(&file_path))
                    .map(|cached| {
                        (
                            cached.size,
                            cached.mtime,
                            cached.change_time,
                            cached.inode,
                            cached.dest_mtime,
                            cached.dest_change_time,
                            cached.dest_inode,
                        )
                    });
                let cached_unchanged = cached_state
                    .map(|(size, mtime, change_time, inode, _, _, _)| {
                        source_file_matches_cached_state(&file, size, mtime, change_time, inode)
                    })
                    .unwrap_or(false);

                if !cached_unchanged {
                    if cached_state.is_none() && (file.is_dir || file.is_symlink) {
                        let destination_state = inspect_destination_sync_state(
                            &self.config,
                            &job,
                            &file,
                            files_len,
                            None,
                            None,
                            None,
                        )
                        .await?;
                        if !destination_state.requires_sync {
                            refreshed_cache_states.push((
                                file_path,
                                FileSyncState {
                                    size: file.size,
                                    mtime: file.modified,
                                    change_time: file.change_time,
                                    inode: file.inode,
                                    dest_mtime: destination_state.dest_mtime,
                                    dest_change_time: destination_state.dest_change_time,
                                    dest_inode: destination_state.dest_inode,
                                    updated_at: 0,
                                },
                            ));
                            continue;
                        }
                    }
                    changed_files.push(file);
                    continue;
                }

                let destination_state = inspect_destination_sync_state(
                    &self.config,
                    &job,
                    &file,
                    files_len,
                    cached_state.and_then(|(_, _, _, _, dest_mtime, _, _)| dest_mtime),
                    cached_state.and_then(|(_, _, _, _, _, dest_change_time, _)| dest_change_time),
                    cached_state.and_then(|(_, _, _, _, _, _, dest_inode)| dest_inode),
                )
                .await?;
                if destination_state.requires_sync {
                    changed_files.push(file);
                    continue;
                }

                if let Some(dest_mtime) = destination_state.dest_mtime {
                    if cached_state
                        .and_then(|(_, _, _, _, dest_mtime, _, _)| dest_mtime)
                        .is_none()
                    {
                        refreshed_dest_mtimes.push((file_path, dest_mtime));
                    }
                }
            }

            if !refreshed_dest_mtimes.is_empty() || !refreshed_cache_states.is_empty() {
                let refreshed_at = chrono::Utc::now().timestamp();
                if let Some(job_cache) = self.synced_files_cache.get(&job.job_id) {
                    for (file_path, mut state) in refreshed_cache_states {
                        state.updated_at = refreshed_at;
                        job_cache.insert(file_path, state);
                    }
                    for (file_path, dest_mtime) in refreshed_dest_mtimes {
                        if let Some(mut cached) = job_cache.get_mut(&file_path) {
                            cached.dest_mtime = Some(dest_mtime);
                            cached.updated_at = refreshed_at;
                        }
                    }
                }
            }

            changed_files
        };

        let cached_count = self
            .synced_files_cache
            .get(&job.job_id)
            .map(|c| c.len())
            .unwrap_or(0);
        info!(
            "Job {} file change filter: {} changed / {} cached",
            job.job_id,
            changed_files.len(),
            cached_count
        );

        if changed_files.is_empty() {
            info!("Job {} has no changed files in this round", job.job_id);
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: has_pending_unstable_files,
            });
        }

        let files_for_cache = changed_files.clone();

        let files = changed_files;

        self.ensure_job_not_cancelled(&job.job_id).await?;
        self.notify_job_started(&job.job_id).await;
        self.ensure_job_not_cancelled(&job.job_id).await?;

        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, u8, u64, u64)>();

        let scheduler = self.clone();
        tokio::spawn(async move {
            while let Some((job_id, progress, current_size, total_size)) = progress_rx.recv().await
            {
                scheduler
                    .handle_progress_update(&job_id, progress, current_size, total_size)
                    .await;
            }
        });

        let notify_progress =
            move |job_id: &str, progress: u8, current_size: u64, total_size: u64| {
                tracing::debug!(
                    "Progress sending: job={}, progress={}%, current={}, total={}",
                    job_id,
                    progress,
                    current_size,
                    total_size
                );
                if let Err(e) =
                    progress_tx.send((job_id.to_string(), progress, current_size, total_size))
                {
                    tracing::warn!("Failed to send progress: {}", e);
                }
            };

        let concurrency_controller =
            self.get_concurrency_controller(&job.region)
                .ok_or_else(|| {
                    crate::util::error::HarDataError::InvalidConfig(format!(
                        "No concurrency controller for region {}",
                        job.region
                    ))
                })?;

        sync_files::sync_files(
            &self.config,
            &self.transfer_manager_pool,
            &self.connection_pools,
            &self.shutdown,
            &self.job_status_cache,
            &self.cancelled_jobs,
            &job,
            files,
            &self.adaptive_controller,
            &concurrency_controller,
            &self.retry_policy,
            &self.protocol_selector,
            &self.prefetch_manager,
            &self.chunk_index,
            notify_progress,
        )
        .await?;

        if let Some(job_cache) = self.synced_files_cache.get(&job.job_id) {
            let now = chrono::Utc::now().timestamp();
            let files_len = files_for_cache.len();
            for file in files_for_cache {
                let path = file.path.to_string_lossy().to_string();
                let dest_mtime =
                    load_destination_cache_state(&self.config, &job, &file, files_len).await;
                job_cache.insert(
                    path,
                    FileSyncState {
                        size: file.size,
                        mtime: file.modified,
                        change_time: file.change_time,
                        inode: file.inode,
                        dest_mtime: dest_mtime.dest_mtime,
                        dest_change_time: dest_mtime.dest_change_time,
                        dest_inode: dest_mtime.dest_inode,
                        updated_at: now,
                    },
                );
            }
        }

        info!("Job {} completed successfully", job.job_id);
        Ok(JobExecutionResult::Transferred {
            retry_due_to_stability: has_pending_unstable_files,
        })
    }

    async fn reset_progress_for_new_round(&self, job: &SyncJob, previous_round_id: Option<i64>) {
        let is_new_sync_round = job.job_type.is_sync()
            && previous_round_id
                .map(|previous_round_id| job.round_id > previous_round_id)
                .unwrap_or(false);

        if !is_new_sync_round {
            return;
        }

        info!(
            "Preserving previous progress snapshot for job {} entering sync round {}",
            job.job_id, job.round_id
        );
    }

    async fn handle_progress_update(
        &self,
        job_id: &str,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) {
        let current_size = current_size.min(total_size);
        tracing::debug!(
            "Progress channel received: job={}, progress={}%, current={}, total={}",
            job_id,
            progress,
            current_size,
            total_size
        );

        let should_persist = if let Some(entry) = self.job_status_cache.get(job_id) {
            if entry.status != JobStatus::Syncing {
                tracing::debug!(
                    "Ignoring stale progress for job {} because runtime status is {:?}",
                    job_id,
                    entry.status
                );
                false
            } else {
                true
            }
        } else {
            tracing::warn!(
                "Job {} not found in job_status_cache when updating progress",
                job_id
            );
            false
        };

        if !should_persist {
            return;
        }

        let updated = match self
            .db
            .update_job_progress(job_id, progress, current_size, total_size)
            .await
        {
            Ok(updated) => updated,
            Err(e) => {
                warn!("Failed to persist progress for job {}: {}", job_id, e);
                return;
            }
        };

        if !updated {
            tracing::debug!(
                "Ignoring stale progress for job {} because persisted status is no longer active",
                job_id
            );
            return;
        }

        let should_notify = if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
            if entry.status != JobStatus::Syncing {
                tracing::debug!(
                    "Skipping runtime progress apply for job {} because status changed to {:?}",
                    job_id,
                    entry.status
                );
                false
            } else {
                entry.progress = progress;
                entry.current_size = current_size;
                entry.total_size = total_size;
                entry.updated_at = chrono::Utc::now();
                tracing::debug!(
                    "Updated job_status_cache: job={}, progress={}%",
                    job_id,
                    progress
                );
                true
            }
        } else {
            tracing::debug!(
                "Job {} removed from job_status_cache after persisting progress",
                job_id
            );
            false
        };

        if !should_notify {
            return;
        }

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_progress(job_id, progress, current_size);
        }
    }

    async fn handle_job_execution_error(&self, job_id: &str, error: &HarDataError) {
        let error_message = error.to_string();

        if is_cancelled_error(&error_message) {
            info!(
                "Job {} was cancelled during execution error handling, preserving cancelled status: {}",
                job_id, error_message
            );
            self.notify_job_cancelled(job_id).await;
            return;
        }

        match self.ensure_job_not_cancelled(job_id).await {
            Ok(()) => {}
            Err(e) if is_cancelled_error(&e.to_string()) => {
                info!(
                    "Job {} was cancelled during execution error handling, preserving cancelled status: {}",
                    job_id, error_message
                );
                self.notify_job_cancelled(job_id).await;
                return;
            }
            Err(e) => {
                self.handle_non_executable_job_stop(job_id, &e, "during execution error handling")
                    .await;
                return;
            }
        }

        let category = self.retry_policy.categorize_error(error);
        self.notify_job_failed(job_id, &error_message, category)
            .await;
    }

    async fn handle_non_executable_job_stop(
        &self,
        job_id: &str,
        error: &HarDataError,
        phase: &str,
    ) {
        match error {
            HarDataError::JobNotFound(_) => {
                warn!(
                    "Dropping job {} {} because the persisted row no longer exists",
                    job_id, phase
                );
                self.cleanup_runtime_job(job_id);
                self.remove_queued_jobs(job_id).await;
            }
            _ => {
                info!(
                    "Skipping job {} {} because it is no longer executable: {}",
                    job_id, phase, error
                );
            }
        }
    }

    async fn ensure_job_not_cancelled(&self, job_id: &str) -> Result<()> {
        if let Some(runtime_status) = self.job_status_cache.get(job_id).map(|entry| entry.status) {
            if runtime_status == JobStatus::Cancelled {
                info!("Job {} was cancelled before side effects", job_id);
                return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
            }

            if !runtime_status.is_active() {
                return Err(HarDataError::Unknown(format!(
                    "Job {} status {:?} is not executable",
                    job_id, runtime_status
                )));
            }
        }

        let persisted_status = self.db.load_job_status(job_id).await?;
        match persisted_status {
            Some(JobStatus::Cancelled) => {
                info!("Job {} was cancelled before side effects", job_id);
                Err(HarDataError::Unknown("Job cancelled by user".to_string()))
            }
            Some(status) if status.is_active() => Ok(()),
            Some(status) => Err(HarDataError::Unknown(format!(
                "Job {} status {:?} is not executable",
                job_id, status
            ))),
            None => Err(HarDataError::JobNotFound(job_id.to_string())),
        }
    }

    async fn list_directory_recursive(
        &self,
        root_path: &str,
        region: &str,
        filter: &ScanFilter,
    ) -> Result<RemoteScanResult> {
        if filter.excludes(root_path) {
            info!("Root path {} excluded by regex, skipping scan", root_path);
            return Ok(RemoteScanResult {
                files: Vec::new(),
                source_is_single_file: false,
                root_excluded: true,
            });
        }

        let mut all_files = Vec::new();
        let mut dirs_to_scan = vec![root_path.to_string()];
        let mut is_single_file = false;

        while let Some(current_dir) = dirs_to_scan.pop() {
            info!(
                "Scanning remote path: {} (region='{}')",
                current_dir, region
            );

            let list_response = self.list_directory_once(&current_dir, region).await?;

            if let Some(file_info) =
                single_file_root_candidate(root_path, &current_dir, &list_response.files)
            {
                if self.root_path_is_single_file(root_path, region).await? {
                    is_single_file = true;
                    if filter.should_include_file(root_path) {
                        all_files.push(ScannedFile {
                            path: PathBuf::from(root_path),
                            size: file_info.size,
                            modified: file_info.modified,
                            change_time: file_info.change_time,
                            inode: file_info.inode,
                            is_dir: false,
                            mode: file_info.mode,
                            is_symlink: file_info.is_symlink,
                            symlink_target: file_info.symlink_target.clone(),
                        });
                    }
                    continue;
                }
            }

            for file_info in list_response.files {
                let full_path = format!("{}/{}", current_dir.trim_end_matches('/'), file_info.path);

                if file_info.is_directory {
                    if filter.should_include_dir(&full_path) {
                        all_files.push(ScannedFile {
                            path: PathBuf::from(&full_path),
                            size: 0,
                            modified: file_info.modified,
                            change_time: file_info.change_time,
                            inode: file_info.inode,
                            is_dir: true,
                            mode: file_info.mode,
                            is_symlink: false,
                            symlink_target: None,
                        });
                    }
                    if filter.should_scan_dir(&full_path) {
                        dirs_to_scan.push(full_path);
                    }
                } else if filter.should_include_file(&full_path) {
                    all_files.push(ScannedFile {
                        path: PathBuf::from(&full_path),
                        size: file_info.size,
                        modified: file_info.modified,
                        change_time: file_info.change_time,
                        inode: file_info.inode,
                        is_dir: false,
                        mode: file_info.mode,
                        is_symlink: file_info.is_symlink,
                        symlink_target: file_info.symlink_target.clone(),
                    });
                }
            }
        }

        info!(
            "Recursive scan complete: {} files found (single_file={})",
            all_files.len(),
            is_single_file
        );
        Ok(RemoteScanResult {
            files: all_files,
            source_is_single_file: is_single_file,
            root_excluded: false,
        })
    }
}

fn single_file_root_candidate<'a>(
    root_path: &str,
    current_dir: &str,
    files: &'a [crate::core::FileInfo],
) -> Option<&'a crate::core::FileInfo> {
    if current_dir != root_path || files.len() != 1 {
        return None;
    }

    let file_info = &files[0];
    if file_info.is_directory {
        return None;
    }

    let root_file_name = std::path::Path::new(root_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    (file_info.path == root_file_name).then_some(file_info)
}

fn parent_lookup_path(root_path: &str) -> Option<String> {
    let normalized_root = root_path.trim_end_matches('/');
    let root = Path::new(normalized_root);
    let parent = root.parent()?;
    if parent.as_os_str().is_empty() {
        Some(".".to_string())
    } else {
        Some(parent.to_string_lossy().to_string())
    }
}

fn parent_listing_confirms_single_file(
    root_path: &str,
    parent_files: &[crate::core::FileInfo],
) -> bool {
    let root_name = Path::new(root_path.trim_end_matches('/'))
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    parent_files
        .iter()
        .any(|entry| entry.path == root_name && !entry.is_directory)
}

fn compile_patterns(patterns: &[String], field: &str) -> Result<Vec<Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            Regex::new(pattern).map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Invalid {} pattern '{}': {}",
                    field, pattern, e
                ))
            })
        })
        .collect()
}

fn has_active_scan_filters(job: &SyncJob) -> bool {
    !job.exclude_regex.is_empty() || !job.include_regex.is_empty()
}

fn should_cleanup_deleted_targets(job: &SyncJob) -> bool {
    !has_active_scan_filters(job)
}

async fn ensure_directory_sync_root(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
) -> Result<()> {
    let root = super::sync_modes::single::resolve_base_dest_path(config, job)?;
    ensure_directory_path(&root).await
}

async fn ensure_directory_path(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => return Ok(()),
        Ok(_) => remove_destination_path(path).await?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(HarDataError::FileOperation(format!(
                "Failed to inspect destination root '{}': {}",
                path.display(),
                e
            )));
        }
    }

    tokio::fs::create_dir_all(path).await.map_err(|e| {
        HarDataError::FileOperation(format!(
            "Failed to create destination root '{}': {}",
            path.display(),
            e
        ))
    })?;

    Ok(())
}

async fn cleanup_deleted_targets(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    source_files: &[ScannedFile],
    source_is_single_file: bool,
    preserved_files: &HashSet<PathBuf>,
) -> Result<()> {
    if source_is_single_file {
        return Ok(());
    }

    let root = super::sync_modes::single::resolve_base_dest_path(config, job)?;
    if tokio::fs::symlink_metadata(&root).await.is_err() {
        return Ok(());
    }

    let (mut expected_files, mut expected_dirs) =
        build_expected_destination_entries(config, job, source_files)?;
    for path in preserved_files {
        let path = normalize_path(path);
        expected_files.insert(path.clone());
        if let Some(parent) = path.parent() {
            add_expected_dirs(parent, &root, &mut expected_dirs);
        }
    }
    prune_destination_tree(&root, &root, &expected_files, &expected_dirs).await
}

fn build_expected_destination_entries(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    source_files: &[ScannedFile],
) -> Result<(HashSet<PathBuf>, HashSet<PathBuf>)> {
    let root = super::sync_modes::single::resolve_base_dest_path(config, job)?;
    let mut expected_files = HashSet::new();
    let mut expected_dirs = HashSet::from([root.clone()]);
    let files_len = source_files.len();

    for file in source_files {
        let source_file_path = file.path.to_string_lossy().to_string();
        let relative_path = relative_source_path(job, &source_file_path);
        let dest_path = PathBuf::from(super::sync_modes::single::calculate_dest_path(
            config,
            job,
            &relative_path,
            files_len,
        )?);

        if file.is_dir {
            add_expected_dirs(&dest_path, &root, &mut expected_dirs);
        } else {
            expected_files.insert(dest_path.clone());
            if let Some(parent) = dest_path.parent() {
                add_expected_dirs(parent, &root, &mut expected_dirs);
            }
        }
    }

    Ok((expected_files, expected_dirs))
}

fn add_expected_dirs(path: &Path, root: &Path, expected_dirs: &mut HashSet<PathBuf>) {
    let root = normalize_path(root);
    let mut current = Some(normalize_path(path));

    while let Some(dir) = current {
        expected_dirs.insert(dir.clone());
        if dir == root {
            break;
        }
        current = dir.parent().map(normalize_path);
    }
}

fn relative_source_path(job: &SyncJob, source_file_path: &str) -> String {
    let source_str = job.source.to_string_lossy();
    source_file_path
        .strip_prefix(source_str.trim_end_matches('/'))
        .unwrap_or(source_file_path)
        .trim_start_matches('/')
        .to_string()
}

async fn inspect_destination_sync_state(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    file: &ScannedFile,
    files_len: usize,
    cached_dest_mtime: Option<i64>,
    cached_dest_change_time: Option<i64>,
    cached_dest_inode: Option<u64>,
) -> Result<DestinationSyncState> {
    let source_file_path = file.path.to_string_lossy().to_string();
    let relative_path = relative_source_path(job, &source_file_path);
    let dest_path = PathBuf::from(super::sync_modes::single::calculate_dest_path(
        config,
        job,
        &relative_path,
        files_len,
    )?);

    let metadata = match tokio::fs::symlink_metadata(&dest_path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(
                "Job {} cached source {} unchanged but destination {} is missing, forcing re-sync",
                job.job_id,
                file.path.display(),
                dest_path.display()
            );
            return Ok(DestinationSyncState {
                requires_sync: true,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            });
        }
        Err(e) => {
            return Err(HarDataError::FileOperation(format!(
                "Failed to inspect destination '{}' for job {}: {}",
                dest_path.display(),
                job.job_id,
                e
            )));
        }
    };

    if file.is_dir {
        let permission_drifted = destination_permissions_drifted(&metadata, file.mode);
        let needs_sync =
            !metadata.is_dir() || metadata.file_type().is_symlink() || permission_drifted;
        if needs_sync {
            if permission_drifted {
                info!(
                    "Job {} directory destination {} permissions drifted, forcing re-sync",
                    job.job_id,
                    dest_path.display()
                );
            } else {
                info!(
                    "Job {} directory destination {} drifted, forcing re-sync",
                    job.job_id,
                    dest_path.display()
                );
            }
        }
        return Ok(DestinationSyncState {
            requires_sync: needs_sync,
            dest_mtime: None,
            dest_change_time: None,
            dest_inode: None,
        });
    }

    if file.is_symlink {
        if !metadata.file_type().is_symlink() {
            info!(
                "Job {} symlink destination {} changed type, forcing re-sync",
                job.job_id,
                dest_path.display()
            );
            return Ok(DestinationSyncState {
                requires_sync: true,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            });
        }

        let current_target = tokio::fs::read_link(&dest_path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to read symlink target '{}' for job {}: {}",
                dest_path.display(),
                job.job_id,
                e
            ))
        })?;
        let current_target = current_target.to_string_lossy().to_string();
        let expected_target = file.symlink_target.as_deref().unwrap_or("");
        let needs_sync = current_target != expected_target;
        if needs_sync {
            info!(
                "Job {} symlink destination {} target drifted, forcing re-sync",
                job.job_id,
                dest_path.display()
            );
        }
        return Ok(DestinationSyncState {
            requires_sync: needs_sync,
            dest_mtime: None,
            dest_change_time: None,
            dest_inode: None,
        });
    }

    let current_dest_mtime = metadata_mtime_nanos(&metadata);
    let current_dest_change_time = metadata_ctime_nanos(&metadata);
    let current_dest_inode = metadata_inode(&metadata);
    let mtime_drifted = cached_dest_mtime
        .map(|dest_mtime| !timestamps_match(dest_mtime, current_dest_mtime))
        .unwrap_or(false);
    let change_time_drifted = current_dest_change_time.is_some()
        && !optional_timestamps_match(cached_dest_change_time, current_dest_change_time);
    let inode_drifted = current_dest_inode.is_some() && cached_dest_inode != current_dest_inode;
    let permission_drifted = destination_permissions_drifted(&metadata, file.mode);
    let needs_sync = !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != file.size
        || mtime_drifted
        || change_time_drifted
        || inode_drifted
        || permission_drifted;
    if needs_sync {
        if mtime_drifted {
            info!(
                "Job {} file destination {} mtime drifted, forcing re-sync",
                job.job_id,
                dest_path.display()
            );
        } else if change_time_drifted || inode_drifted {
            info!(
                "Job {} file destination {} identity drifted, forcing re-sync",
                job.job_id,
                dest_path.display()
            );
        } else if permission_drifted {
            info!(
                "Job {} file destination {} permissions drifted, forcing re-sync",
                job.job_id,
                dest_path.display()
            );
        } else {
            info!(
                "Job {} file destination {} drifted, forcing re-sync",
                job.job_id,
                dest_path.display()
            );
        }
    }
    Ok(DestinationSyncState {
        requires_sync: needs_sync,
        dest_mtime: Some(current_dest_mtime),
        dest_change_time: current_dest_change_time,
        dest_inode: current_dest_inode,
    })
}

#[cfg(unix)]
fn destination_permissions_drifted(metadata: &std::fs::Metadata, expected_mode: u32) -> bool {
    use std::os::unix::fs::PermissionsExt;

    let expected_permissions = expected_mode & 0o7777;
    if expected_permissions == 0 {
        return false;
    }

    let current_permissions = metadata.permissions().mode() & 0o7777;
    current_permissions != expected_permissions
}

#[cfg(not(unix))]
fn destination_permissions_drifted(_metadata: &std::fs::Metadata, _expected_mode: u32) -> bool {
    false
}

async fn load_destination_cache_state(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    file: &ScannedFile,
    files_len: usize,
) -> DestinationSyncState {
    if file.is_dir || file.is_symlink {
        return DestinationSyncState {
            requires_sync: false,
            dest_mtime: None,
            dest_change_time: None,
            dest_inode: None,
        };
    }

    let source_file_path = file.path.to_string_lossy().to_string();
    let relative_path = relative_source_path(job, &source_file_path);
    let dest_path = match super::sync_modes::single::calculate_dest_path(
        config,
        job,
        &relative_path,
        files_len,
    ) {
        Ok(path) => PathBuf::from(path),
        Err(e) => {
            warn!(
                "Failed to resolve destination cache path for job {} source {}: {}",
                job.job_id,
                file.path.display(),
                e
            );
            return DestinationSyncState {
                requires_sync: false,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            };
        }
    };

    match tokio::fs::symlink_metadata(&dest_path).await {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            DestinationSyncState {
                requires_sync: false,
                dest_mtime: Some(metadata_mtime_nanos(&metadata)),
                dest_change_time: metadata_ctime_nanos(&metadata),
                dest_inode: metadata_inode(&metadata),
            }
        }
        Ok(_) => {
            warn!(
                "Skip destination mtime cache for job {} path {} because it is not a regular file",
                job.job_id,
                dest_path.display()
            );
            DestinationSyncState {
                requires_sync: false,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            }
        }
        Err(e) => {
            warn!(
                "Failed to read destination mtime for job {} path {}: {}",
                job.job_id,
                dest_path.display(),
                e
            );
            DestinationSyncState {
                requires_sync: false,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            }
        }
    }
}

async fn trim_deleted_source_tracking(
    synced_files_cache: &std::sync::Arc<
        dashmap::DashMap<String, dashmap::DashMap<String, FileSyncState>>,
    >,
    size_freezers: &std::sync::Arc<dashmap::DashMap<String, std::sync::Arc<super::SizeFreezer>>>,
    job_id: &str,
    source_files: &[ScannedFile],
) {
    let current_paths: HashSet<String> = source_files
        .iter()
        .map(|file| file.path.to_string_lossy().to_string())
        .collect();

    if let Some(job_cache) = synced_files_cache.get(job_id) {
        let stale_paths: Vec<String> = job_cache
            .iter()
            .filter_map(|entry| {
                if current_paths.contains(entry.key()) {
                    None
                } else {
                    Some(entry.key().clone())
                }
            })
            .collect();

        for path in stale_paths {
            job_cache.remove(&path);
        }
    }

    if let Some(size_freezer) = size_freezers.get(job_id) {
        let removed = size_freezer.retain_paths(&current_paths).await;
        if removed > 0 {
            info!(
                "Trimmed {} stale stability entries for job {} after source deletion scan",
                removed, job_id
            );
        }
    }
}

fn next_sync_schedule_delay(
    scan_interval: std::time::Duration,
    stability_threshold: std::time::Duration,
    retry_due_to_stability: bool,
) -> std::time::Duration {
    if !retry_due_to_stability {
        return scan_interval;
    }

    let stability_delay = if stability_threshold.is_zero() {
        std::time::Duration::from_millis(MIN_STABILITY_RETRY_DELAY_MS)
    } else {
        stability_threshold
    };
    scan_interval.min(stability_delay)
}

fn pending_stability_file_count(scanned_file_count: usize, stable_file_count: usize) -> usize {
    scanned_file_count.saturating_sub(stable_file_count)
}

fn prune_destination_tree<'a>(
    current: &'a Path,
    root: &'a Path,
    expected_files: &'a HashSet<PathBuf>,
    expected_dirs: &'a HashSet<PathBuf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let metadata = match tokio::fs::symlink_metadata(current).await {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(HarDataError::FileOperation(format!(
                    "Failed to inspect destination path '{}': {}",
                    current.display(),
                    e
                )));
            }
        };

        let current_path = normalize_path(current);
        let root_path = normalize_path(root);
        let is_symlink = metadata.file_type().is_symlink();

        if is_symlink || metadata.is_file() {
            if !expected_files.contains(&current_path) {
                remove_destination_path(current).await?;
                info!("Removed stale target file {}", current.display());
            }
            return Ok(());
        }

        if current_path != root_path && !expected_dirs.contains(&current_path) {
            remove_destination_path(current).await?;
            info!("Removed stale target directory {}", current.display());
            return Ok(());
        }

        let mut entries = tokio::fs::read_dir(current).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to read destination directory '{}': {}",
                current.display(),
                e
            ))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to read destination entry under '{}': {}",
                current.display(),
                e
            ))
        })? {
            let child_path = entry.path();
            prune_destination_tree(&child_path, root, expected_files, expected_dirs).await?;
        }

        Ok(())
    })
}

async fn remove_destination_path(path: &Path) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|e| {
        HarDataError::FileOperation(format!(
            "Failed to inspect path before removal '{}': {}",
            path.display(),
            e
        ))
    })?;

    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to remove directory '{}': {}",
                path.display(),
                e
            ))
        })?;
    } else {
        tokio::fs::remove_file(path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to remove file '{}': {}",
                path.display(),
                e
            ))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        cleanup_deleted_targets, has_active_scan_filters, next_sync_schedule_delay,
        parent_listing_confirms_single_file, parent_lookup_path, pending_stability_file_count,
        should_cleanup_deleted_targets, single_file_root_candidate,
        source_file_matches_cached_state, trim_deleted_source_tracking, JobExecutionResult,
        ScanFilter, MIN_STABILITY_RETRY_DELAY_MS,
    };
    use crate::agent::compute::ComputeService;
    use crate::agent::server::tcp::TcpServer;
    use crate::core::protocol::FileInfo;
    use crate::core::{JobStatus, JobType};
    use crate::sync::engine::job::SyncJob;
    use crate::sync::engine::scheduler::core::FileSyncState;
    use crate::sync::engine::scheduler::SchedulerConfig;
    use crate::sync::scanner::ScannedFile;
    use crate::sync::storage::db::Database;
    use crate::util::time::unix_timestamp_nanos;
    use sqlx::SqlitePool;
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    async fn open_raw_pool(db_path: &str) -> SqlitePool {
        SqlitePool::connect(db_path).await.unwrap()
    }

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

    impl crate::sync::engine::scheduler::JobStatusCallback for RecordingStatusCallback {
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
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db)
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
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let dest_root = PathBuf::from(&config.data_dir).join("target");
        let stale_path = dest_root.join("stale.txt");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::write(&stale_path, b"stale").unwrap();

        let job_id = "job-root-excluded-execute".to_string();
        let persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: "/remote/source".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-filtered-zero-match".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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

    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_destination_is_missing() {
        let root = temp_dir("worker-resync-missing-destination");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-resync-missing-destination".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut second_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if second_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            second_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            second_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_file = data_dir.join("target").join("payload.bin");
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        scheduler.notify_job_pending(&job_id).await;
        std::fs::remove_file(&dest_file).unwrap();

        let third_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            third_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_destination_content_changes_same_size_and_preserved_mtime(
    ) {
        use filetime::{set_file_mtime, FileTime};

        let root = temp_dir("worker-resync-same-size-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        let corrupted = vec![b'X'; payload.len()];
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-resync-same-size-drift".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let source_path = source_dir.join("payload.bin").to_string_lossy().to_string();
        let cached_dest_mtime = {
            let cached = scheduler
                .synced_files_cache
                .get(&job_id)
                .expect("sync cache should exist after first transfer");
            let dest_mtime = cached
                .get(&source_path)
                .expect("source entry should exist in sync cache")
                .dest_mtime;
            dest_mtime
        };
        assert!(cached_dest_mtime.is_some());

        let dest_file = data_dir.join("target").join("payload.bin");
        let original_dest_mtime =
            FileTime::from_last_modification_time(&std::fs::metadata(&dest_file).unwrap());
        scheduler.notify_job_pending(&job_id).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        std::fs::write(&dest_file, &corrupted).unwrap();
        set_file_mtime(&dest_file, original_dest_mtime).unwrap();
        assert_eq!(std::fs::read(&dest_file).unwrap(), corrupted);

        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_source_content_changes_same_size_and_preserved_mtime(
    ) {
        use crate::util::time::{metadata_ctime_nanos, metadata_inode};
        use filetime::{set_file_mtime, FileTime};

        let root = temp_dir("worker-resync-same-size-source-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        let updated = vec![b'Z'; payload.len()];
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-resync-same-size-source-drift".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let source_metadata = std::fs::metadata(&source_file).unwrap();
        let original_source_mtime = FileTime::from_last_modification_time(&source_metadata);
        let original_source_change_time = metadata_ctime_nanos(&source_metadata);
        let original_source_inode = metadata_inode(&source_metadata);
        scheduler.notify_job_pending(&job_id).await;

        let mut identity_changed = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            std::fs::write(&source_file, &updated).unwrap();
            set_file_mtime(&source_file, original_source_mtime).unwrap();
            let metadata = std::fs::metadata(&source_file).unwrap();
            if metadata_ctime_nanos(&metadata) != original_source_change_time
                || metadata_inode(&metadata) != original_source_inode
            {
                identity_changed = true;
                break;
            }
        }
        assert!(
            identity_changed,
            "expected rewritten source file to change identity"
        );

        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_file = data_dir.join("target").join("payload.bin");
        assert_eq!(std::fs::read(&dest_file).unwrap(), updated);

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_syncs_root_directory_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("worker-syncs-root-directory-permissions");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::set_permissions(&source_dir, std::fs::Permissions::from_mode(0o711)).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();

        let dest_root = data_dir.join("target");
        std::fs::create_dir_all(&dest_root).unwrap();
        std::fs::set_permissions(&dest_root, std::fs::Permissions::from_mode(0o755)).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-syncs-root-directory-permissions".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler
                .execute_job(
                    SyncJob::new(
                        job_id.clone(),
                        source_dir.clone(),
                        "target".to_string(),
                        "local".to_string(),
                    )
                    .with_job_type(JobType::Sync),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(dest_mode, 0o711);
        assert_eq!(
            std::fs::read(dest_root.join("payload.bin")).unwrap(),
            payload
        );

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_cached_file_when_destination_permissions_change() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("worker-resync-permission-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();
        std::fs::set_permissions(&source_file, std::fs::Permissions::from_mode(0o640)).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-resync-permission-drift".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_file = data_dir.join("target").join("payload.bin");
        let mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(mode, 0o640);

        scheduler.notify_job_pending(&job_id).await;
        std::fs::set_permissions(&dest_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(drifted_mode, 0o600);

        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        let restored_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(restored_mode, 0o640);
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_root_directory_when_destination_permissions_change() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("worker-resyncs-root-directory-permissions");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::set_permissions(&source_dir, std::fs::Permissions::from_mode(0o711)).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-resyncs-root-directory-permissions".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_root = data_dir.join("target");
        let initial_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(initial_mode, 0o711);

        scheduler.notify_job_pending(&job_id).await;
        std::fs::set_permissions(&dest_root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(drifted_mode, 0o755);

        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let restored_mode = std::fs::metadata(&dest_root).unwrap().permissions().mode() & 0o7777;
        assert_eq!(restored_mode, 0o711);
        assert_eq!(
            std::fs::read(dest_root.join("payload.bin")).unwrap(),
            payload
        );

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_syncs_nested_directory_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("worker-syncs-nested-directory-permissions");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let nested_source_dir = source_dir.join("nested");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&nested_source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::set_permissions(&nested_source_dir, std::fs::Permissions::from_mode(0o711))
            .unwrap();
        let source_file = nested_source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();

        let dest_nested_dir = data_dir.join("target").join("nested");
        std::fs::create_dir_all(&dest_nested_dir).unwrap();
        std::fs::set_permissions(&dest_nested_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-syncs-nested-directory-permissions".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler
                .execute_job(
                    SyncJob::new(
                        job_id.clone(),
                        source_dir.clone(),
                        "target".to_string(),
                        "local".to_string(),
                    )
                    .with_job_type(JobType::Sync),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_file = dest_nested_dir.join("payload.bin");
        let dir_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(dir_mode, 0o711);
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn execute_job_after_recovery_skips_completed_sync_file_without_retransfer() {
        let root = temp_dir("worker-recovery-skips-retransfer");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-recovery-skips-retransfer".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let source_path = source_dir.join("payload.bin").to_string_lossy().to_string();
        let checkpoint = db
            .load_transfer_state(&job_id, &source_path)
            .await
            .unwrap()
            .expect("completed sync round should persist checkpoint");
        assert!(checkpoint.cache_only);
        assert!(checkpoint.dest_modified.is_some());
        assert!(checkpoint.dest_change_time.is_some());
        assert!(checkpoint.dest_inode.is_some());

        let mut paused_job = db.load_job(&job_id).await.unwrap().unwrap();
        paused_job.status = crate::core::JobStatus::Paused;
        db.save_job(&paused_job).await.unwrap();

        scheduler.transfer_manager_pool.shutdown().await;

        let recovered_config = SchedulerConfig {
            chunk_cache_path: root
                .join("chunk-cache-recovered")
                .to_string_lossy()
                .to_string(),
            ..config
        };
        let recovered_scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(recovered_config, db.clone())
                .await
                .unwrap();
        recovered_scheduler.recover_pending_jobs().await.unwrap();

        {
            let restored = recovered_scheduler
                .synced_files_cache
                .get(&job_id)
                .expect("recovery should restore completed sync cache");
            let restored_file = restored
                .get(&source_path)
                .expect("completed file checkpoint should be restored");
            assert!(restored_file.dest_mtime.is_some());
            assert!(restored_file.dest_change_time.is_some());
            assert!(restored_file.dest_inode.is_some());
        }

        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::NoTransfer {
                retry_due_to_stability: false
            }
        );

        let dest_file = data_dir.join("target").join("payload.bin");
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_after_recovery_skips_unchanged_symlink_without_retransfer() {
        let root = temp_dir("worker-recovery-skips-symlink-retransfer");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), b"payload-round-trip").unwrap();
        std::os::unix::fs::symlink("payload.bin", source_dir.join("current.bin")).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-recovery-skips-symlink-retransfer".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let mut paused_job = db.load_job(&job_id).await.unwrap().unwrap();
        paused_job.status = crate::core::JobStatus::Paused;
        db.save_job(&paused_job).await.unwrap();

        scheduler.transfer_manager_pool.shutdown().await;

        let recovered_config = SchedulerConfig {
            chunk_cache_path: root
                .join("chunk-cache-recovered")
                .to_string_lossy()
                .to_string(),
            ..config
        };
        let recovered_scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(recovered_config, db.clone())
                .await
                .unwrap();
        recovered_scheduler.recover_pending_jobs().await.unwrap();

        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::NoTransfer {
                retry_due_to_stability: false
            }
        );

        let dest_symlink = data_dir.join("target").join("current.bin");
        assert!(std::fs::symlink_metadata(&dest_symlink)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_link(&dest_symlink).unwrap(),
            std::path::PathBuf::from("payload.bin")
        );

        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_after_recovery_resyncs_permission_drift() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("worker-recovery-resyncs-permission-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let source_file = source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();
        std::fs::set_permissions(&source_file, std::fs::Permissions::from_mode(0o640)).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-recovery-resyncs-permission-drift".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let mut paused_job = db.load_job(&job_id).await.unwrap().unwrap();
        paused_job.status = crate::core::JobStatus::Paused;
        db.save_job(&paused_job).await.unwrap();

        scheduler.transfer_manager_pool.shutdown().await;

        let recovered_config = SchedulerConfig {
            chunk_cache_path: root
                .join("chunk-cache-recovered")
                .to_string_lossy()
                .to_string(),
            ..config
        };
        let recovered_scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(recovered_config, db.clone())
                .await
                .unwrap();
        recovered_scheduler.recover_pending_jobs().await.unwrap();

        let dest_file = data_dir.join("target").join("payload.bin");
        std::fs::set_permissions(&dest_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(drifted_mode, 0o600);

        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let restored_mode = std::fs::metadata(&dest_file).unwrap().permissions().mode() & 0o7777;
        assert_eq!(restored_mode, 0o640);
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_job_resyncs_nested_directory_when_destination_permissions_change() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("worker-resyncs-nested-directory-permissions");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let nested_source_dir = source_dir.join("nested");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        std::fs::create_dir_all(&nested_source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::set_permissions(&nested_source_dir, std::fs::Permissions::from_mode(0o711))
            .unwrap();
        let source_file = nested_source_dir.join("payload.bin");
        std::fs::write(&source_file, payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-resyncs-nested-directory-permissions".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let dest_nested_dir = data_dir.join("target").join("nested");
        let initial_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(initial_mode, 0o711);

        scheduler.notify_job_pending(&job_id).await;
        std::fs::set_permissions(&dest_nested_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let drifted_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(drifted_mode, 0o755);

        let resync_result = scheduler.execute_job(runtime_job).await.unwrap();
        assert_eq!(
            resync_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let restored_mode = std::fs::metadata(&dest_nested_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(restored_mode, 0o711);
        assert_eq!(
            std::fs::read(dest_nested_dir.join("payload.bin")).unwrap(),
            payload
        );

        server_handle.abort();
        let _ = server_handle.await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn execute_job_after_recovery_resyncs_same_size_destination_drift() {
        let root = temp_dir("worker-recovery-resyncs-same-size-drift");
        let remote_root = root.join("remote");
        let data_dir = root.join("sync-data");
        let source_dir = remote_root.join("source");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let payload = b"payload-round-trip";
        let corrupted = vec![b'X'; payload.len()];
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(source_dir.join("payload.bin"), payload).unwrap();

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
            stability_threshold: Duration::from_millis(1),
            regions: vec![crate::sync::RegionConfig {
                name: "local".to_string(),
                quic_bind: "127.0.0.1:9443".to_string(),
                tcp_bind: bind_addr,
            }],
            ..SchedulerConfig::default()
        };
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();

        let job_id = "job-recovery-resyncs-same-size-drift".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: source_dir.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
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
        .with_job_type(JobType::Sync);

        let mut first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        if first_result
            == (JobExecutionResult::NoTransfer {
                retry_due_to_stability: true,
            })
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
            first_result = scheduler.execute_job(runtime_job.clone()).await.unwrap();
        }
        assert_eq!(
            first_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );

        let mut paused_job = db.load_job(&job_id).await.unwrap().unwrap();
        paused_job.status = crate::core::JobStatus::Paused;
        db.save_job(&paused_job).await.unwrap();

        scheduler.transfer_manager_pool.shutdown().await;

        let recovered_config = SchedulerConfig {
            chunk_cache_path: root
                .join("chunk-cache-recovered")
                .to_string_lossy()
                .to_string(),
            ..config
        };
        let recovered_scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(recovered_config, db.clone())
                .await
                .unwrap();
        recovered_scheduler.recover_pending_jobs().await.unwrap();

        let dest_file = data_dir.join("target").join("payload.bin");
        tokio::time::sleep(Duration::from_millis(2)).await;
        std::fs::write(&dest_file, &corrupted).unwrap();
        assert_eq!(std::fs::read(&dest_file).unwrap(), corrupted);

        let recovered_job = recovered_scheduler.get_job_info(&job_id).unwrap();
        let recovered_result = recovered_scheduler
            .execute_job(recovered_job)
            .await
            .unwrap();
        assert_eq!(
            recovered_result,
            JobExecutionResult::Transferred {
                retry_due_to_stability: false
            }
        );
        assert_eq!(std::fs::read(&dest_file).unwrap(), payload);

        server_handle.abort();
        let _ = server_handle.await;
        recovered_scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn worker_loop_keeps_retry_record_when_final_completion_persist_fails() {
        let root = temp_dir("worker-final-completion-retry-preserved");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let job_id = "job-worker-final-completion-retry".to_string();
        let persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: "/remote/source".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        db.save_job(&persisted_job).await.unwrap();
        db.save_retry(&job_id, "temporary network error")
            .await
            .unwrap();

        scheduler.job_status_cache.insert(
            job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "local".to_string(),
                error_message: Some("temporary network error".to_string()),
                created_at: persisted_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let mut runtime_job = SyncJob::new(
            job_id.clone(),
            PathBuf::from("/remote/source"),
            "target".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_filters(vec!["^/remote/source$".to_string()], Vec::new());
        runtime_job.ensure_final_round_state();
        scheduler
            .job_cache
            .insert(job_id.clone(), runtime_job.clone());

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_completed_update_from_worker
            BEFORE UPDATE ON jobs
            WHEN NEW.status = 'completed'
            BEGIN
                SELECT RAISE(FAIL, 'reject completed status');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let scheduler_clone = scheduler.clone();
        let worker = tokio::spawn(async move {
            scheduler_clone.worker_loop(0).await;
        });

        scheduler
            .job_queue
            .enqueue(runtime_job.priority, runtime_job)
            .await;
        scheduler.job_notify.notify_one();

        for _ in 0..50 {
            if scheduler
                .get_job_status(&job_id)
                .map(|status| status.status == JobStatus::Completed)
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let runtime = scheduler.get_job_status(&job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Completed);
        assert!(db.get_retry(&job_id).await.unwrap().is_some());
        assert_eq!(
            db.load_job(&job_id).await.unwrap().unwrap().status,
            JobStatus::Pending
        );

        scheduler
            .shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = scheduler.shutdown_signal.send(true);
        worker.await.unwrap();

        raw_pool.close().await;
        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn execute_job_noop_round_keeps_existing_pending_error_and_skips_started_callback() {
        let root = temp_dir("worker-noop-preserves-failed-status");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler =
            crate::sync::engine::scheduler::SyncScheduler::new(config.clone(), db.clone())
                .await
                .unwrap();
        let callback = Arc::new(RecordingStatusCallback::default());
        *scheduler.status_callback.lock().await = Some(callback.clone());

        let job_id = "job-worker-noop-preserve-failed".to_string();
        let mut persisted_job = crate::core::Job::new(
            job_id.clone(),
            crate::core::JobPath {
                path: "/remote/source".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "target".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted_job.error_message = Some("previous failure".to_string());
        db.save_job(&persisted_job).await.unwrap();
        scheduler.job_status_cache.insert(
            job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job_id.clone(),
                status: JobStatus::Pending,
                progress: 45,
                current_size: 450,
                total_size: 1000,
                region: "local".to_string(),
                error_message: Some("previous failure".to_string()),
                created_at: persisted_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

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

        let runtime = scheduler.get_job_status(&job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert_eq!(runtime.error_message.as_deref(), Some("previous failure"));

        let snapshot = db.load_job(&job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);
        assert_eq!(snapshot.error_message.as_deref(), Some("previous failure"));
        assert!(callback.started_jobs.lock().unwrap().is_empty());

        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn idle_round_skips_pending_update_when_status_is_already_clean_pending() {
        let root = temp_dir("worker-idle-skip-clean-pending");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let job = crate::core::Job::new(
            "job-worker-idle-clean-pending".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        assert!(
            !scheduler
                .should_notify_job_pending_after_round(&job.job_id, false)
                .await
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn idle_round_clears_stale_pending_error_after_successful_idle_round() {
        let root = temp_dir("worker-idle-clears-pending-error");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut job = crate::core::Job::new(
            "job-worker-idle-recovers-failed".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.error_message = Some("previous failure".to_string());
        db.save_job(&job).await.unwrap();
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 20,
                current_size: 20,
                total_size: 100,
                region: job.region.clone(),
                error_message: Some("previous failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        assert!(
            scheduler
                .should_notify_job_pending_after_round(&job.job_id, false)
                .await
        );

        scheduler.notify_job_pending(&job.job_id).await;

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert!(runtime.error_message.is_none());

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);
        assert!(snapshot.error_message.is_none());

        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn trim_deleted_source_tracking_removes_stale_cache_and_stability_entries() {
        let root = temp_dir("trim-deleted-source-tracking");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            stability_threshold: Duration::from_millis(1),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db)
            .await
            .unwrap();

        let job_id = "job-trim-tracking";
        let stale_path = "/remote/source/stale.txt".to_string();
        let mtime = unix_timestamp_nanos(SystemTime::now()) + 1_000_000_000;

        let freezer = scheduler.size_freezer_for_job(job_id);
        assert!(!freezer.check_stable_and_update(&stale_path, 4, mtime).await);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(freezer.check_stable_and_update(&stale_path, 4, mtime).await);

        let job_cache = dashmap::DashMap::new();
        job_cache.insert(
            stale_path.clone(),
            FileSyncState {
                size: 4,
                mtime,
                change_time: None,
                inode: None,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
                updated_at: chrono::Utc::now().timestamp(),
            },
        );
        scheduler
            .synced_files_cache
            .insert(job_id.to_string(), job_cache);

        let source_files = vec![ScannedFile {
            path: PathBuf::from("/remote/source/keep.txt"),
            size: 4,
            modified: mtime,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        }];

        trim_deleted_source_tracking(
            &scheduler.synced_files_cache,
            &scheduler.size_freezers,
            job_id,
            &source_files,
        )
        .await;

        let trimmed_cache = scheduler
            .synced_files_cache
            .get(job_id)
            .expect("job cache should still exist after trimming");
        assert!(trimmed_cache.get(&stale_path).is_none());

        assert!(
            !freezer.check_stable_and_update(&stale_path, 4, mtime).await,
            "deleted path should not reuse stale stability state"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn ensure_job_not_cancelled_uses_cancelled_snapshot_when_runtime_missing() {
        let root = temp_dir("worker-cancelled-snapshot");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut job = crate::core::Job::new(
            "job-worker-cancelled".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = crate::core::JobStatus::Cancelled;
        db.save_job(&job).await.unwrap();

        let err = scheduler
            .ensure_job_not_cancelled(&job.job_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Job cancelled by user"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn ensure_job_not_cancelled_returns_error_when_status_resolution_fails() {
        let root = temp_dir("worker-status-resolution-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DROP TABLE jobs")
            .execute(&raw_pool)
            .await
            .unwrap();

        let err = scheduler
            .ensure_job_not_cancelled("job-worker-status-resolution-failure")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such table: jobs"));

        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn ensure_job_not_cancelled_returns_error_when_persisted_row_is_missing() {
        let root = temp_dir("worker-missing-persisted-row");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut job = crate::core::Job::new(
            "job-worker-missing-persisted-row".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = crate::core::JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::core::JobStatus::Syncing,
                progress: 10,
                current_size: 10,
                total_size: 100,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        let err = scheduler
            .ensure_job_not_cancelled(&job.job_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(&job.job_id));

        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn handle_job_execution_error_preserves_cancelled_state() {
        let root = temp_dir("worker-cancelled-error");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut job = crate::core::Job::new(
            "job-worker-cancelled-error".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = crate::core::JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::core::JobStatus::Cancelled,
                progress: 0,
                current_size: 0,
                total_size: 128,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                job.job_id.clone(),
                std::path::PathBuf::from(&job.source.path),
                job.dest.path.clone(),
                job.region.clone(),
            )
            .with_job_type(job.job_type),
        );

        scheduler
            .handle_job_execution_error(
                &job.job_id,
                &crate::util::error::HarDataError::Unknown("1 files failed".to_string()),
            )
            .await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::core::JobStatus::Cancelled);
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());

        scheduler.transfer_manager_pool.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn handle_job_execution_error_missing_row_cleans_runtime_without_marking_cancelled() {
        let root = temp_dir("worker-missing-row-error");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut job = crate::core::Job::new(
            "job-worker-missing-row-error".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = crate::core::JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::core::JobStatus::Syncing,
                progress: 30,
                current_size: 30,
                total_size: 100,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                job.job_id.clone(),
                std::path::PathBuf::from(&job.source.path),
                job.dest.path.clone(),
                job.region.clone(),
            )
            .with_job_type(job.job_type),
        );

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler
            .handle_job_execution_error(
                &job.job_id,
                &crate::util::error::HarDataError::NetworkError("network failed".to_string()),
            )
            .await;

        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(db.load_job(&job.job_id).await.unwrap().is_none());

        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn handle_job_execution_error_keeps_existing_failed_state() {
        let root = temp_dir("worker-preserve-failed-state");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut job = crate::core::Job::new(
            "job-worker-preserve-failed".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = crate::core::JobStatus::Failed;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: crate::core::JobStatus::Failed,
                progress: 30,
                current_size: 30,
                total_size: 100,
                region: job.region.clone(),
                error_message: Some("existing failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .handle_job_execution_error(
                &job.job_id,
                &crate::util::error::HarDataError::NetworkError("network failed".to_string()),
            )
            .await;

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, crate::core::JobStatus::Failed);
        assert_eq!(runtime.error_message.as_deref(), Some("existing failure"));

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::core::JobStatus::Failed);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn reset_progress_for_new_sync_round_preserves_previous_round_totals() {
        let root = temp_dir("worker-reset-progress");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut db_job = crate::core::Job::new(
            "job-worker-reset-progress".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        db_job.status = crate::core::JobStatus::Pending;
        db_job.progress = 100;
        db_job.current_size = 64;
        db_job.total_size = 64;
        db.save_job(&db_job).await.unwrap();

        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::core::JobStatus::Pending,
                progress: 100,
                current_size: 64,
                total_size: 64,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let mut cached_job = crate::sync::engine::job::SyncJob::new(
            db_job.job_id.clone(),
            std::path::PathBuf::from(&db_job.source.path),
            db_job.dest.path.clone(),
            db_job.region.clone(),
        )
        .with_job_type(JobType::Sync);
        cached_job.round_id = 1;
        scheduler
            .job_cache
            .insert(db_job.job_id.clone(), cached_job);

        let mut next_round_job = crate::sync::engine::job::SyncJob::new(
            db_job.job_id.clone(),
            std::path::PathBuf::from(&db_job.source.path),
            db_job.dest.path.clone(),
            db_job.region.clone(),
        )
        .with_job_type(JobType::Sync);
        next_round_job.round_id = 2;

        scheduler
            .reset_progress_for_new_round(&next_round_job, Some(1))
            .await;

        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.progress, 100);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 64);

        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 64);
        assert_eq!(snapshot.total_size, 64);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn reset_progress_for_new_sync_round_preserves_existing_runtime_error_state() {
        let root = temp_dir("worker-reset-progress-preserve-state");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut db_job = crate::core::Job::new(
            "job-worker-reset-progress-preserve-state".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        db_job.status = crate::core::JobStatus::Pending;
        db_job.progress = 100;
        db_job.current_size = 64;
        db_job.total_size = 64;
        db.save_job(&db_job).await.unwrap();

        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::core::JobStatus::Pending,
                progress: 100,
                current_size: 64,
                total_size: 64,
                region: db_job.region.clone(),
                error_message: Some("previous error".to_string()),
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let mut next_round_job = crate::sync::engine::job::SyncJob::new(
            db_job.job_id.clone(),
            std::path::PathBuf::from(&db_job.source.path),
            db_job.dest.path.clone(),
            db_job.region.clone(),
        )
        .with_job_type(JobType::Sync);
        next_round_job.round_id = 2;

        scheduler
            .reset_progress_for_new_round(&next_round_job, Some(1))
            .await;

        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.progress, 100);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 64);
        assert_eq!(runtime.error_message.as_deref(), Some("previous error"));

        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 64);
        assert_eq!(snapshot.total_size, 64);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn handle_progress_update_ignores_completed_runtime_state() {
        let root = temp_dir("worker-ignore-stale-progress");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut db_job = crate::core::Job::new(
            "job-worker-ignore-stale-progress".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        db_job.status = crate::core::JobStatus::Completed;
        db_job.progress = 100;
        db_job.current_size = 64;
        db_job.total_size = 64;
        db.save_job(&db_job).await.unwrap();

        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::core::JobStatus::Completed,
                progress: 100,
                current_size: 64,
                total_size: 64,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .handle_progress_update(&db_job.job_id, 50, 32, 64)
            .await;

        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.status, crate::core::JobStatus::Completed);
        assert_eq!(runtime.progress, 100);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 64);

        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::core::JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 64);
        assert_eq!(snapshot.total_size, 64);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn handle_progress_update_keeps_runtime_when_persist_fails() {
        let root = temp_dir("worker-progress-persist-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut db_job = crate::core::Job::new(
            "job-worker-progress-persist-failure".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        db_job.status = crate::core::JobStatus::Syncing;
        db_job.progress = 25;
        db_job.current_size = 32;
        db_job.total_size = 128;
        db.save_job(&db_job).await.unwrap();

        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::core::JobStatus::Syncing,
                progress: 25,
                current_size: 32,
                total_size: 128,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_progress_update
            BEFORE UPDATE ON jobs
            WHEN NEW.job_id = 'job-worker-progress-persist-failure'
              AND NEW.progress != OLD.progress
            BEGIN
                SELECT RAISE(FAIL, 'reject progress update');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        scheduler
            .handle_progress_update(&db_job.job_id, 50, 64, 128)
            .await;

        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.status, crate::core::JobStatus::Syncing);
        assert_eq!(runtime.progress, 25);
        assert_eq!(runtime.current_size, 32);
        assert_eq!(runtime.total_size, 128);

        let snapshot = scheduler
            .load_job_snapshot(&db_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, crate::core::JobStatus::Syncing);
        assert_eq!(snapshot.progress, 25);
        assert_eq!(snapshot.current_size, 32);
        assert_eq!(snapshot.total_size, 128);

        raw_pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn handle_progress_update_does_not_overwrite_terminal_snapshot() {
        let root = temp_dir("worker-progress-terminal-snapshot");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = std::sync::Arc::new(
            crate::sync::storage::db::Database::new(&db_path)
                .await
                .unwrap(),
        );
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db.clone())
            .await
            .unwrap();

        let mut db_job = crate::core::Job::new(
            "job-worker-progress-terminal-snapshot".to_string(),
            crate::core::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            crate::core::JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        db_job.status = crate::core::JobStatus::Completed;
        db_job.progress = 100;
        db_job.current_size = 128;
        db_job.total_size = 128;
        db.save_job(&db_job).await.unwrap();

        scheduler.job_status_cache.insert(
            db_job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: db_job.job_id.clone(),
                status: crate::core::JobStatus::Syncing,
                progress: 40,
                current_size: 64,
                total_size: 128,
                region: db_job.region.clone(),
                error_message: None,
                created_at: db_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .handle_progress_update(&db_job.job_id, 50, 96, 128)
            .await;

        let runtime = scheduler.get_job_status(&db_job.job_id).unwrap();
        assert_eq!(runtime.status, crate::core::JobStatus::Syncing);
        assert_eq!(runtime.progress, 40);
        assert_eq!(runtime.current_size, 64);
        assert_eq!(runtime.total_size, 128);

        let snapshot = db.load_job(&db_job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, crate::core::JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 128);
        assert_eq!(snapshot.total_size, 128);

        std::fs::remove_dir_all(root).unwrap();
    }
}
