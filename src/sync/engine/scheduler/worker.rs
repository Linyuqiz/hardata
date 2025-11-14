use crate::core::job::JobStatus;
use crate::sync::engine::job::SyncJob;
use crate::sync::scanner::ScannedFile;
use crate::util::error::{HarDataError, Result};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use tracing::{error, info};

use super::core::{FileSyncState, SyncScheduler};
use super::infrastructure::connection;
use super::sync_modes::files as sync_files;

impl SyncScheduler {
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

                if let Some(status) = self.job_status_cache.get(&job.job_id) {
                    if status.status == crate::core::job::JobStatus::Cancelled {
                        info!("Job {} was cancelled, skipping", job.job_id);
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

                if let Some(mut cached_job) = self.job_cache.get_mut(&job.job_id) {
                    cached_job.round_id = job.round_id;
                    cached_job.is_last_round = job.is_last_round;
                }

                match self.execute_job(job.clone()).await {
                    Ok(_) => {
                        info!(
                            "Worker {} completed job {} round {}",
                            worker_id, job.job_id, job.round_id
                        );

                        if job.is_completed() {
                            info!("Job {} all rounds completed", job.job_id);
                            self.notify_job_completed(&job.job_id).await;
                        } else if job.job_type.is_sync() {
                            let next_run = std::time::Instant::now() + job.scan_interval;
                            info!(
                                "Job {} (sync) round {} done, scheduling next run in {:?}",
                                job.job_id, job.round_id, job.scan_interval
                            );
                            self.delayed_queue.insert(next_run, job).await;
                        }
                    }
                    Err(e) => {
                        error!("Worker {} failed job {}: {}", worker_id, job.job_id, e);
                        self.notify_job_failed(&job.job_id, &e.to_string()).await;
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

    async fn execute_job(&self, job: SyncJob) -> Result<()> {
        info!(
            "Executing job {} (region='{}'): {:?} -> {}",
            job.job_id, job.region, job.source, job.dest
        );

        if let Some(status) = self.job_status_cache.get(&job.job_id) {
            if status.status == JobStatus::Cancelled {
                info!("Job {} was cancelled before execution", job.job_id);
                return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
            }
        }

        self.notify_job_started(&job.job_id).await;

        let source_str = job
            .source
            .to_str()
            .ok_or_else(|| HarDataError::Unknown("Invalid source path".to_string()))?;

        info!(
            "Requesting remote directory listing for: {} (region='{}')",
            source_str, job.region
        );

        let files = self
            .list_directory_recursive(source_str, &job.region)
            .await?;

        info!("Job {} remote scan found {} files", job.job_id, files.len());

        if files.is_empty() {
            info!("Job {} has no files to sync", job.job_id);
            return Ok(());
        }

        let stable_files = if !job.is_final_transfer() {
            let file_sizes: Vec<(String, u64, i64)> = files
                .iter()
                .map(|f| (f.path.to_string_lossy().to_string(), f.size, f.modified))
                .collect();
            let stable_names = self.size_freezer.get_stable_files(&file_sizes).await;
            let stable_files: Vec<ScannedFile> = files
                .into_iter()
                .filter(|f| stable_names.contains(&f.path.to_string_lossy().to_string()))
                .collect();
            info!(
                "Job {} stability filter: {} stable / {} total",
                job.job_id,
                stable_files.len(),
                stable_names.len()
            );
            stable_files
        } else {
            info!("Job {} is final mode, skip stability filter", job.job_id);
            files
        };

        if stable_files.is_empty() {
            info!(
                "Job {} has no stable files yet, will retry later",
                job.job_id
            );
            return Ok(());
        }

        self.synced_files_cache
            .entry(job.job_id.clone())
            .or_default();

        let changed_files: Vec<ScannedFile> = {
            let job_cache = self.synced_files_cache.get(&job.job_id);
            stable_files
                .into_iter()
                .filter(|f| {
                    let file_path = f.path.to_string_lossy().to_string();
                    if let Some(ref cache) = job_cache {
                        if let Some(cached) = cache.get(&file_path) {
                            let unchanged = cached.size == f.size && cached.mtime == f.modified;
                            if unchanged {
                                return false;
                            }
                        }
                    }
                    true
                })
                .collect()
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
            return Ok(());
        }

        let files_for_cache: Vec<(String, u64, i64)> = changed_files
            .iter()
            .map(|f| (f.path.to_string_lossy().to_string(), f.size, f.modified))
            .collect();

        let files = changed_files;

        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, u8, u64, u64)>();

        let job_status_cache = self.job_status_cache.clone();
        let db = self.db.clone();
        let status_callback = self.status_callback.clone();
        tokio::spawn(async move {
            while let Some((job_id, progress, current_size, total_size)) = progress_rx.recv().await
            {
                tracing::debug!(
                    "Progress channel received: job={}, progress={}%, current={}, total={}",
                    job_id,
                    progress,
                    current_size,
                    total_size
                );
                if let Some(mut entry) = job_status_cache.get_mut(&job_id) {
                    entry.progress = progress;
                    entry.current_size = current_size;
                    entry.total_size = total_size;
                    entry.updated_at = chrono::Utc::now();
                    tracing::debug!(
                        "Updated job_status_cache: job={}, progress={}%",
                        job_id,
                        progress
                    );
                } else {
                    tracing::warn!(
                        "Job {} not found in job_status_cache when updating progress",
                        job_id
                    );
                }

                let _ = db
                    .update_job_progress(&job_id, progress, current_size, total_size)
                    .await;

                if let Some(ref callback) = *status_callback.lock().await {
                    callback.on_job_progress(&job_id, progress, current_size);
                }
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
            for (path, size, mtime) in files_for_cache {
                job_cache.insert(
                    path,
                    FileSyncState {
                        size,
                        mtime,
                        updated_at: now,
                    },
                );
            }
        }

        info!("Job {} completed successfully", job.job_id);
        Ok(())
    }

    pub(super) async fn list_directory_recursive(
        &self,
        root_path: &str,
        region: &str,
    ) -> Result<Vec<ScannedFile>> {
        let mut all_files = Vec::new();
        let mut dirs_to_scan = vec![root_path.to_string()];
        let mut is_single_file = false;

        while let Some(current_dir) = dirs_to_scan.pop() {
            info!(
                "Scanning remote path: {} (region='{}')",
                current_dir, region
            );

            let mut conn = connection::get_connection_with_retry_for_region_with_selector(
                &self.config,
                &self.connection_pools,
                region,
                &self.shutdown,
                Some(&self.protocol_selector),
            )
            .await?;
            let list_response = conn.list_directory(&current_dir).await?;

            if list_response.files.len() == 1
                && !list_response.files[0].is_directory
                && current_dir == root_path
            {
                let file_info = &list_response.files[0];
                let root_file_name = std::path::Path::new(root_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");

                if file_info.path == root_file_name {
                    is_single_file = true;
                    all_files.push(ScannedFile {
                        path: PathBuf::from(root_path),
                        size: file_info.size,
                        modified: file_info.modified,
                        is_dir: false,
                        mode: file_info.mode,
                        is_symlink: file_info.is_symlink,
                        symlink_target: file_info.symlink_target.clone(),
                    });
                    continue;
                }
            }

            let is_empty_dir = list_response.files.is_empty() && current_dir != root_path;
            if is_empty_dir {
                all_files.push(ScannedFile {
                    path: PathBuf::from(&current_dir),
                    size: 0,
                    modified: 0,
                    is_dir: true,
                    mode: 0o755,
                    is_symlink: false,
                    symlink_target: None,
                });
            }

            for file_info in list_response.files {
                let full_path = format!("{}/{}", current_dir.trim_end_matches('/'), file_info.path);

                if file_info.is_directory {
                    dirs_to_scan.push(full_path);
                } else {
                    all_files.push(ScannedFile {
                        path: PathBuf::from(&full_path),
                        size: file_info.size,
                        modified: file_info.modified,
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
        Ok(all_files)
    }
}
