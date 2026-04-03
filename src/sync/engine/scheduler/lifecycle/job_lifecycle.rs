use crate::core::job::JobStatus;
use crate::core::{Job, JobConfig, JobPath, JobType};
use crate::sync::engine::job::SyncJob;
use crate::sync::engine::scheduler::sync_modes::calculate_progress;
use crate::sync::storage::db::JobRetry;
use crate::util::error::{HarDataError, Result};
use dashmap::DashMap;
use std::path::PathBuf;
use tracing::{info, warn};

use super::super::core::SyncScheduler;
use super::super::infrastructure::config::JobRuntimeStatus;

/// finalize_job 失败时用于恢复原任务状态的快照
struct FinalizeRollbackState {
    snapshot: Option<Job>,
    retry_record: Option<JobRetry>,
    runtime_status: Option<JobRuntimeStatus>,
    sync_job: Option<SyncJob>,
    synced_file_cache: Vec<(String, super::super::core::FileSyncState)>,
}

impl SyncScheduler {
    fn snapshot_synced_file_cache(
        &self,
        job_id: &str,
    ) -> Vec<(String, super::super::core::FileSyncState)> {
        self.synced_files_cache
            .get(job_id)
            .map(|cache| {
                cache
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn restore_synced_file_cache(
        &self,
        job_id: &str,
        snapshot: &[(String, super::super::core::FileSyncState)],
    ) {
        if snapshot.is_empty() {
            self.synced_files_cache.remove(job_id);
            return;
        }

        let job_cache = DashMap::new();
        for (path, state) in snapshot {
            job_cache.insert(path.clone(), state.clone());
        }
        self.synced_files_cache
            .insert(job_id.to_string(), job_cache);
    }

    pub(in crate::sync::engine::scheduler) async fn restore_synced_file_cache_from_transfer_states(
        &self,
        job_id: &str,
    ) -> Result<()> {
        let versions = self
            .db
            .load_completed_transfer_source_versions(job_id)
            .await?;

        if versions.is_empty() {
            self.synced_files_cache.remove(job_id);
            return Ok(());
        }

        let restored_count = versions.len();
        let restored_at = chrono::Utc::now().timestamp();
        let job_cache = DashMap::new();
        for (path, size, mtime, change_time, inode, dest_mtime, dest_change_time, dest_inode) in
            versions
        {
            job_cache.insert(
                path,
                super::super::core::FileSyncState {
                    size,
                    mtime,
                    change_time,
                    inode,
                    dest_mtime,
                    dest_change_time,
                    dest_inode,
                    updated_at: restored_at,
                },
            );
        }
        self.synced_files_cache
            .insert(job_id.to_string(), job_cache);
        info!(
            "Restored {} synced file cache entries for job {} from persisted transfer states",
            restored_count, job_id
        );
        Ok(())
    }

    async fn ensure_destination_available(&self, job_id: &str, dest: &str) -> Result<()> {
        let requested_dest = self.config.resolve_runtime_destination_path(dest)?;

        for (active_job_id, active_dest) in self.db.load_active_job_destinations().await? {
            if active_job_id == job_id {
                continue;
            }
            if is_final_job_pair(job_id, &active_job_id) {
                continue;
            }

            let resolved_active_dest = match self
                .config
                .resolve_runtime_destination_path(&active_dest)
            {
                Ok(path) => path,
                Err(e) => {
                    warn!(
                        "Skip destination conflict check for active job {} path {} because it cannot be resolved: {}",
                        active_job_id, active_dest, e
                    );
                    continue;
                }
            };

            if destinations_overlap(&requested_dest, &resolved_active_dest) {
                return Err(HarDataError::InvalidConfig(format!(
                    "destination '{}' overlaps active job {} destination '{}'",
                    requested_dest.display(),
                    active_job_id,
                    resolved_active_dest.display()
                )));
            }
        }

        Ok(())
    }

    pub async fn submit_job(&self, job: SyncJob) -> Result<()> {
        self.submit_job_internal(job, true).await
    }

    pub(in crate::sync::engine::scheduler) async fn submit_job_internal(
        &self,
        mut job: SyncJob,
        persist: bool,
    ) -> Result<()> {
        info!(
            "Submitting job {} to queue (type={}, persist={}, priority={})",
            job.job_id,
            job.job_type.as_str(),
            persist,
            job.priority
        );

        self.config.resolve_runtime_destination_path(&job.dest)?;

        let recovered_snapshot = if persist {
            None
        } else {
            self.load_job_snapshot(&job.job_id).await?
        };

        if recovered_snapshot.is_some() {
            if job.job_id.ends_with("_final") {
                job.ensure_final_round_state();
            } else {
                job.mark_resumed_round();
            }
            self.restore_synced_file_cache_from_transfer_states(&job.job_id)
                .await?;
        }

        let _queue_guard = self.queue_update_lock.lock().await;
        if self.running_jobs.contains_key(&job.job_id) {
            return Err(HarDataError::Unknown(format!(
                "Job {} is still shutting down",
                job.job_id
            )));
        }

        if let Some(existing) = self.job_status_cache.get(&job.job_id) {
            if existing.status.is_active() {
                return Err(HarDataError::Unknown(format!(
                    "Job {} is already active with status {:?}",
                    job.job_id, existing.status
                )));
            }
        }

        self.ensure_destination_available(&job.job_id, &job.dest)
            .await?;
        self.cancelled_jobs.remove(&job.job_id);

        if persist {
            let db_job = self.sync_job_to_db_job(&job);
            self.db.save_job(&db_job).await.map_err(|e| {
                warn!("Failed to persist job {} to database: {}", job.job_id, e);
                e
            })?;
        }

        self.job_cache.insert(job.job_id.clone(), job.clone());

        let now = chrono::Utc::now();
        let (progress, current_size, total_size, created_at) = recovered_snapshot
            .as_ref()
            .map(|snapshot| {
                let current_size = snapshot.current_size.min(snapshot.total_size);
                (
                    calculate_progress(current_size, snapshot.total_size),
                    current_size,
                    snapshot.total_size,
                    snapshot.created_at,
                )
            })
            .unwrap_or((0, 0, 0, now));

        if let Some(snapshot) = recovered_snapshot.as_ref() {
            info!(
                "Restoring persisted progress for job {}: status={:?}, progress={}%, current_size={}, total_size={}",
                job.job_id,
                snapshot.status,
                progress,
                current_size,
                total_size
            );
        }

        self.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress,
                current_size,
                total_size,
                region: job.region.clone(),
                error_message: None,
                created_at,
                updated_at: now,
            },
        );
        if !persist || job.round_id > 0 || job.is_last_round {
            self.persist_job_round_state(&job.job_id, job.round_id, job.is_last_round)
                .await;
        }
        self.update_original_sync_job_status_from_final(
            &job.job_id,
            JobStatus::Pending,
            progress,
            current_size,
            total_size,
            None,
        )
        .await;

        self.job_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.job_queue.enqueue(job.priority, job).await;
        let queue_len = self.job_queue.len().await;

        self.job_notify.notify_one();
        info!("Job queue length: {}", queue_len);
        Ok(())
    }

    fn sync_job_to_db_job(&self, sync_job: &SyncJob) -> Job {
        let now = chrono::Utc::now();
        Job {
            job_id: sync_job.job_id.clone(),
            region: sync_job.region.clone(),
            source: JobPath {
                path: sync_job.source.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            dest: JobPath {
                path: sync_job.dest.clone(),
                client_id: String::new(),
            },
            status: JobStatus::Pending,
            job_type: sync_job.job_type,
            exclude_regex: sync_job.exclude_regex.clone(),
            include_regex: sync_job.include_regex.clone(),
            priority: sync_job.priority,
            round_id: sync_job.round_id,
            is_last_round: sync_job.is_last_round,
            options: JobConfig::default(),
            progress: 0,
            current_size: 0,
            total_size: 0,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub async fn finalize_job(&self, job_id: &str) -> Result<()> {
        info!("Finalizing job {}", job_id);

        let snapshot = self.load_job_snapshot(job_id).await?;
        let original_job = self
            .job_cache
            .get(job_id)
            .map(|job| job.clone())
            .or_else(|| snapshot.as_ref().map(sync_job_from_snapshot))
            .ok_or_else(|| HarDataError::Unknown(format!("Job {} not found", job_id)))?;
        if !original_job.job_type.is_sync() {
            return Err(HarDataError::Unknown(format!(
                "Job {} is not a sync job, finalize is only supported for sync jobs",
                job_id
            )));
        }
        let final_job_id = format!("{}_final", job_id);

        let has_pending_retry = self
            .db
            .get_retry(job_id)
            .await?
            .map(|retry| retry.retry_count < retry.max_retries)
            .unwrap_or(false);
        let final_snapshot = self.load_job_snapshot(&final_job_id).await?;
        let final_has_pending_retry = if final_snapshot.is_some() {
            self.db
                .get_retry(&final_job_id)
                .await?
                .map(|retry| retry.retry_count < retry.max_retries)
                .unwrap_or(false)
        } else {
            false
        };

        let runtime_status = self
            .job_status_cache
            .get(job_id)
            .map(|status| status.status)
            .or_else(|| snapshot_status(snapshot.clone()));
        let active_final_job_id = final_snapshot.as_ref().and_then(|final_job| {
            let final_is_active = final_job.status.is_active()
                || (final_job.status == JobStatus::Failed && final_has_pending_retry);
            final_is_active.then(|| final_job_id.clone())
        });
        if let Some(active_final_job_id) = active_final_job_id {
            info!(
                "Final transfer {} already active for job {}, treat request as idempotent success",
                active_final_job_id, job_id
            );
            return Ok(());
        }
        if final_snapshot
            .as_ref()
            .map(|final_job| final_job.status == JobStatus::Completed)
            .unwrap_or(false)
        {
            info!(
                "Final transfer {} already completed for job {}, treat request as idempotent success",
                final_job_id, job_id
            );
            return Ok(());
        }
        let restarting_terminal_final_failure = matches!(runtime_status, Some(JobStatus::Failed))
            && final_snapshot
                .as_ref()
                .map(|final_job| final_job.status == JobStatus::Failed && !final_has_pending_retry)
                .unwrap_or(false);
        let needs_wait = matches!(runtime_status, Some(JobStatus::Syncing));

        match runtime_status {
            Some(JobStatus::Pending) | Some(JobStatus::Syncing) => {}
            Some(JobStatus::Failed) if has_pending_retry || restarting_terminal_final_failure => {}
            Some(status) => {
                return Err(HarDataError::Unknown(format!(
                    "Job {} status {:?} cannot be finalized",
                    job_id, status
                )));
            }
            None => {
                return Err(HarDataError::Unknown(format!(
                    "Job {} not found for finalize",
                    job_id
                )));
            }
        }

        self.ensure_destination_available(&final_job_id, &original_job.dest)
            .await?;

        // 在取消原任务前保存回滚快照，用于失败时恢复
        let rollback_state = if restarting_terminal_final_failure {
            None
        } else {
            let retry_record = self.db.get_retry(job_id).await?;
            Some(FinalizeRollbackState {
                snapshot: snapshot.clone(),
                retry_record,
                runtime_status: self.job_status_cache.get(job_id).map(|s| s.clone()),
                sync_job: self.job_cache.get(job_id).map(|j| j.clone()),
                synced_file_cache: self.snapshot_synced_file_cache(job_id),
            })
        };

        if !restarting_terminal_final_failure {
            self.cancel_job(job_id).await?;
        }

        if needs_wait && !restarting_terminal_final_failure {
            for _ in 0..50 {
                if self.job_status_cache.get(job_id).is_none()
                    && self.job_cache.get(job_id).is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            if self.job_status_cache.get(job_id).is_some() || self.job_cache.get(job_id).is_some() {
                if let Some(ref rs) = rollback_state {
                    self.rollback_finalize_original_job(rs).await;
                }
                return Err(HarDataError::Unknown(format!(
                    "Job {} is still shutting down after 5s timeout, cannot finalize safely",
                    job_id
                )));
            }
        }

        let mut final_job = if restarting_terminal_final_failure {
            final_snapshot
                .as_ref()
                .map(sync_job_from_snapshot)
                .ok_or_else(|| {
                    HarDataError::Unknown(format!(
                        "Job {} missing final transfer state for retry",
                        job_id
                    ))
                })?
        } else {
            let mut final_job = original_job.clone();
            final_job.job_id = final_job_id.clone();
            final_job.job_type = JobType::Once;
            final_job.priority = original_job.priority + 100;
            final_job.start_final_round();
            final_job
        };
        final_job.job_id = final_job_id.clone();
        final_job.job_type = JobType::Once;
        final_job.ensure_final_round_state();

        info!(
            "Created final transfer job: {} (source: {:?}, dest: {})",
            final_job.job_id, final_job.source, final_job.dest
        );

        let final_job_cache = rollback_state
            .as_ref()
            .map(|state| state.synced_file_cache.as_slice())
            .unwrap_or(&[]);
        self.restore_synced_file_cache(&final_job.job_id, final_job_cache);

        if let Err(e) = self.submit_job(final_job).await {
            self.synced_files_cache.remove(&final_job_id);
            if let Some(ref rs) = rollback_state {
                self.rollback_finalize_original_job(rs).await;
            }
            return Err(e);
        }
        Ok(())
    }

    /// finalize_job 失败时恢复原任务状态、重试记录和缓存
    async fn rollback_finalize_original_job(&self, rollback: &FinalizeRollbackState) {
        let job_id = rollback
            .sync_job
            .as_ref()
            .map(|j| j.job_id.clone())
            .or_else(|| rollback.snapshot.as_ref().map(|j| j.job_id.clone()));
        let job_id = match job_id {
            Some(id) => id,
            None => return,
        };

        self.cancelled_jobs.remove(&job_id);

        if let Some(ref sync_job) = rollback.sync_job {
            self.job_cache.insert(job_id.clone(), sync_job.clone());
        } else {
            self.job_cache.remove(&job_id);
        }

        if let Some(ref runtime_status) = rollback.runtime_status {
            self.job_status_cache
                .insert(job_id.clone(), runtime_status.clone());
        } else {
            self.job_status_cache.remove(&job_id);
        }

        match rollback.retry_record.as_ref() {
            Some(retry) => {
                let _ = self.db.restore_retry(retry).await;
            }
            None => {
                let _ = self.db.delete_retry(&job_id).await;
            }
        }

        self.restore_synced_file_cache(&job_id, &rollback.synced_file_cache);

        // 若原任务处于 Pending 状态则重新入队
        if rollback
            .runtime_status
            .as_ref()
            .map(|s| s.status == JobStatus::Pending)
            .unwrap_or(false)
        {
            if let Some(ref sync_job) = rollback.sync_job {
                self.enqueue_job_replacing_queued(sync_job.clone()).await;
                self.job_notify.notify_one();
            }
        }

        warn!("Rolled back finalize for job {}", job_id);
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        info!("Cancelling job {}", job_id);

        if let Some(final_job_id) = self.active_final_job_for(job_id).await? {
            info!(
                "Redirecting cancel request from original job {} to active final job {}",
                job_id, final_job_id
            );
            return Box::pin(self.cancel_job(&final_job_id)).await;
        }

        let retry_record = self.db.get_retry(job_id).await?;
        let has_pending_retry = retry_record
            .as_ref()
            .map(|retry| retry.retry_count < retry.max_retries)
            .unwrap_or(false);
        let snapshot = self.load_job_snapshot(job_id).await?;

        if has_pending_retry
            && snapshot
                .as_ref()
                .map(|job| job.status == JobStatus::Completed)
                .unwrap_or(false)
        {
            self.db.delete_retry(job_id).await?;
            info!("Removed stale retry record for completed job {}", job_id);
            return Ok(());
        }

        let cancellation = {
            if let Some(status) = self.job_status_cache.get(job_id) {
                match status.status {
                    JobStatus::Pending => {
                        info!("Job {} cancelled (was pending)", job_id);
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            true,
                        ))
                    }
                    JobStatus::Syncing => {
                        info!("Job {} marked as cancelled (was syncing)", job_id);
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            false,
                        ))
                    }
                    JobStatus::Failed if has_pending_retry => {
                        info!("Job {} cancelled while waiting for retry", job_id);
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            true,
                        ))
                    }
                    JobStatus::Completed | JobStatus::Cancelled => match status.status {
                        JobStatus::Cancelled => {
                            info!(
                                "Job {} already cancelled, treat request as idempotent success",
                                job_id
                            );
                            return Ok(());
                        }
                        _ => {
                            return Err(HarDataError::Unknown(format!(
                                "Job {} already finished with status {:?}",
                                job_id, status.status
                            )));
                        }
                    },
                    JobStatus::Failed => {
                        return Err(HarDataError::Unknown(format!(
                            "Job {} already failed and has no pending retry",
                            job_id
                        )));
                    }
                    _ => {
                        info!(
                            "Job {} cancelled from runtime {:?} state",
                            job_id, status.status
                        );
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            true,
                        ))
                    }
                }
            } else if let Some(job) = snapshot.as_ref() {
                match job.status {
                    JobStatus::Pending | JobStatus::Syncing => {
                        info!(
                            "Job {} cancelled from persisted {:?} snapshot",
                            job_id, job.status
                        );
                        Some((job.progress, job.current_size, job.total_size, true))
                    }
                    JobStatus::Failed if has_pending_retry => {
                        info!("Job {} cancelled from retry queue", job_id);
                        Some((job.progress, job.current_size, job.total_size, true))
                    }
                    JobStatus::Completed | JobStatus::Cancelled => match job.status {
                        JobStatus::Cancelled => {
                            info!(
                                "Job {} already cancelled in snapshot, treat request as idempotent success",
                                job_id
                            );
                            return Ok(());
                        }
                        _ => {
                            return Err(HarDataError::Unknown(format!(
                                "Job {} already finished with status {:?}",
                                job_id, job.status
                            )));
                        }
                    },
                    JobStatus::Failed => {
                        return Err(HarDataError::Unknown(format!(
                            "Job {} already failed and has no pending retry",
                            job_id
                        )));
                    }
                    _ => {
                        info!(
                            "Job {} cancelled from persisted {:?} snapshot",
                            job_id, job.status
                        );
                        Some((job.progress, job.current_size, job.total_size, true))
                    }
                }
            } else {
                None
            }
        };

        if let Some((progress, current_size, total_size, cleanup_now)) = cancellation {
            self.cancelled_jobs.insert(job_id.to_string(), ());
            let updated = self
                .db
                .update_job_status(
                    job_id,
                    JobStatus::Cancelled,
                    progress,
                    current_size,
                    total_size,
                    None,
                )
                .await?;
            if !updated {
                self.cancelled_jobs.remove(job_id);
                return Err(HarDataError::JobNotFound(job_id.to_string()));
            }

            self.remove_queued_jobs(job_id).await;
            if let Err(e) = self.db.delete_retry(job_id).await {
                warn!(
                    "Failed to delete retry record for cancelled job {}: {}",
                    job_id, e
                );
            }

            if let Some(mut status) = self.job_status_cache.get_mut(job_id) {
                status.status = JobStatus::Cancelled;
                status.updated_at = chrono::Utc::now();
                status.error_message = None;
            }
            self.update_original_sync_job_status_from_final(
                job_id,
                JobStatus::Cancelled,
                progress,
                current_size,
                total_size,
                None,
            )
            .await;

            if cleanup_now {
                if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
                    warn!(
                        "Failed to clear transfer states for cancelled job {}: {}",
                        job_id, e
                    );
                }

                self.cleanup_job_tmp_artifacts(job_id).await;
                self.cleanup_runtime_job(job_id);
            }

            if !self.running_jobs.contains_key(job_id) {
                self.cancelled_jobs.remove(job_id);
            }

            Ok(())
        } else {
            Err(HarDataError::Unknown(format!("Job {} not found", job_id)))
        }
    }

    pub(in crate::sync::engine::scheduler) async fn recover_pending_jobs(&self) -> Result<()> {
        info!("Recovering pending jobs from database...");

        let jobs = self.db.load_active_jobs().await?;
        let mut recovered_count = 0;

        for job in jobs {
            if !job.status.is_active() {
                continue;
            }

            info!(
                "Recovering job {}: {} -> {} (status: {:?})",
                job.job_id, job.source.path, job.dest.path, job.status
            );

            let mut sync_job = SyncJob::new(
                job.job_id.clone(),
                PathBuf::from(&job.source.path),
                job.dest.path.clone(),
                job.region.clone(),
            )
            .with_filters(job.exclude_regex.clone(), job.include_regex.clone())
            .with_priority(job.priority)
            .with_job_type(job.job_type);
            sync_job.restore_round_state(job.round_id, job.is_last_round);
            if sync_job.job_id.ends_with("_final") {
                sync_job.ensure_final_round_state();
            }

            if !job.job_id.ends_with("_final") {
                if let Some(final_job_id) = self.active_final_job_for(&job.job_id).await? {
                    info!(
                        "Skipping recovery for original job {} because active final job {} will recover instead",
                        job.job_id, final_job_id
                    );
                    self.remove_queued_jobs(&job.job_id).await;
                    continue;
                }
            }

            if job.status == JobStatus::Paused {
                if let Err(e) = self
                    .ensure_destination_available(&job.job_id, &job.dest.path)
                    .await
                {
                    warn!("Failed to recover paused job {}: {}", job.job_id, e);
                    self.mark_recovery_failure(&job, &e).await;
                    continue;
                }

                self.job_cache.insert(job.job_id.clone(), sync_job.clone());
                if let Err(e) = self
                    .restore_synced_file_cache_from_transfer_states(&job.job_id)
                    .await
                {
                    warn!(
                        "Failed to restore synced file cache for paused job {}: {}",
                        job.job_id, e
                    );
                    self.job_cache.remove(&job.job_id);
                    self.mark_recovery_failure(&job, &e).await;
                    continue;
                }
                self.job_status_cache.insert(
                    job.job_id.clone(),
                    JobRuntimeStatus {
                        job_id: job.job_id.clone(),
                        status: JobStatus::Paused,
                        progress: job.progress,
                        current_size: job.current_size,
                        total_size: job.total_size,
                        region: job.region.clone(),
                        error_message: None,
                        created_at: job.created_at,
                        updated_at: job.updated_at,
                    },
                );
                self.persist_job_round_state(
                    &job.job_id,
                    sync_job.round_id,
                    sync_job.is_last_round,
                )
                .await;
                self.update_original_sync_job_status_from_final(
                    &job.job_id,
                    JobStatus::Paused,
                    job.progress,
                    job.current_size,
                    job.total_size,
                    None,
                )
                .await;
                self.remove_queued_jobs(&job.job_id).await;
                recovered_count += 1;
                continue;
            }

            if let Err(e) = self.submit_job_internal(sync_job, false).await {
                warn!("Failed to recover job {}: {}", job.job_id, e);
                self.mark_recovery_failure(&job, &e).await;
            } else {
                recovered_count += 1;
            }
        }

        info!("Recovered {} pending jobs", recovered_count);
        Ok(())
    }

    pub(in crate::sync::engine::scheduler) async fn active_final_job_for(
        &self,
        job_id: &str,
    ) -> Result<Option<String>> {
        if job_id.ends_with("_final") {
            return Ok(None);
        }

        let Some(snapshot) = self.load_job_snapshot(job_id).await? else {
            return Ok(None);
        };

        if !snapshot.job_type.is_sync() {
            return Ok(None);
        }

        let final_job_id = format!("{}_final", job_id);
        let final_snapshot = self.load_job_snapshot(&final_job_id).await?;
        let Some(final_snapshot) = final_snapshot else {
            return Ok(None);
        };

        let final_has_pending_retry = self
            .db
            .get_retry(&final_job_id)
            .await?
            .map(|retry| retry.retry_count < retry.max_retries)
            .unwrap_or(false);

        let final_is_active = final_snapshot.status.is_active()
            || (final_snapshot.status == JobStatus::Failed && final_has_pending_retry);

        if final_is_active {
            Ok(Some(final_job_id))
        } else {
            Ok(None)
        }
    }

    pub(in crate::sync::engine::scheduler) async fn cleanup_old_jobs(&self, days: i64) {
        match self.db.cleanup_old_jobs(days).await {
            Ok(count) => {
                if count > 0 {
                    info!("Cleaned up {} old jobs (older than {} days)", count, days);
                }
            }
            Err(e) => {
                warn!("Failed to cleanup old jobs: {}", e);
            }
        }
    }

    pub(in crate::sync::engine::scheduler) async fn cleanup_terminal_job_artifacts(&self) {
        let jobs = match self.db.load_terminal_jobs().await {
            Ok(jobs) => jobs,
            Err(e) => {
                warn!("Failed to load jobs for terminal artifact cleanup: {}", e);
                return;
            }
        };
        let retryable_job_ids = match self.db.load_retryable_job_ids().await {
            Ok(job_ids) => job_ids,
            Err(e) => {
                warn!(
                    "Failed to load retryable jobs for terminal artifact cleanup: {}",
                    e
                );
                return;
            }
        };

        for job in jobs {
            let should_cleanup = match job.status {
                JobStatus::Completed | JobStatus::Cancelled => true,
                JobStatus::Failed => !retryable_job_ids.contains(&job.job_id),
                _ => false,
            };

            if !should_cleanup {
                continue;
            }

            if let Err(e) = self.db.delete_retry(&job.job_id).await {
                warn!(
                    "Failed to delete retry record for terminal job {}: {}",
                    job.job_id, e
                );
            }

            if let Err(e) = self
                .transfer_manager_pool
                .clear_job_states(&job.job_id)
                .await
            {
                warn!(
                    "Failed to clear transfer states for terminal job {}: {}",
                    job.job_id, e
                );
            }

            self.cleanup_job_tmp_artifacts(&job.job_id).await;
        }
    }

    async fn mark_recovery_failure(&self, job: &Job, error: &HarDataError) {
        let error_message = error.to_string();
        match self
            .db
            .update_job_status(
                &job.job_id,
                JobStatus::Failed,
                job.progress,
                job.current_size,
                job.total_size,
                Some(&error_message),
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    "Skipped marking unrecoverable job {} as failed because the persisted row no longer exists",
                    job.job_id
                );
            }
            Err(e) => {
                warn!(
                    "Failed to mark unrecoverable job {} as failed: {}",
                    job.job_id, e
                );
            }
        }
        self.update_original_sync_job_status_from_final(
            &job.job_id,
            JobStatus::Failed,
            job.progress,
            job.current_size,
            job.total_size,
            Some(&error_message),
        )
        .await;

        if let Err(e) = self.db.delete_retry(&job.job_id).await {
            warn!(
                "Failed to delete retry record for unrecoverable job {}: {}",
                job.job_id, e
            );
        }

        if let Err(e) = self
            .transfer_manager_pool
            .clear_job_states(&job.job_id)
            .await
        {
            warn!(
                "Failed to clear transfer states for unrecoverable job {}: {}",
                job.job_id, e
            );
        }

        self.cleanup_job_tmp_artifacts(&job.job_id).await;
    }
}

fn snapshot_status(job: Option<Job>) -> Option<JobStatus> {
    job.map(|job| job.status)
}

fn is_final_job_pair(job_id: &str, other_job_id: &str) -> bool {
    if let Some(base_job_id) = job_id.strip_suffix("_final") {
        return other_job_id == base_job_id;
    }

    if let Some(base_job_id) = other_job_id.strip_suffix("_final") {
        return job_id == base_job_id;
    }

    false
}

fn destinations_overlap(left: &std::path::Path, right: &std::path::Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn sync_job_from_snapshot(job: &Job) -> SyncJob {
    let mut sync_job = SyncJob::new(
        job.job_id.clone(),
        PathBuf::from(&job.source.path),
        job.dest.path.clone(),
        job.region.clone(),
    )
    .with_filters(job.exclude_regex.clone(), job.include_regex.clone())
    .with_priority(job.priority)
    .with_job_type(job.job_type);
    sync_job.restore_round_state(job.round_id, job.is_last_round);
    if sync_job.job_id.ends_with("_final") {
        sync_job.ensure_final_round_state();
    }
    sync_job
}

#[cfg(test)]
mod tests {
    use super::super::super::core::SyncScheduler;
    use crate::core::{FileTransferState, Job, JobPath, JobStatus, JobType};
    use crate::sync::engine::job::SyncJob;
    use crate::sync::engine::scheduler::core::FileSyncState;
    use crate::sync::engine::scheduler::{JobRuntimeStatus, SchedulerConfig};
    use crate::sync::storage::db::Database;
    use dashmap::DashMap;
    use sqlx::SqlitePool;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-{name}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    async fn open_raw_pool(db_path: &str) -> SqlitePool {
        SqlitePool::connect(db_path).await.unwrap()
    }

    #[tokio::test]
    async fn submit_job_returns_error_when_initial_persistence_fails() {
        let root = temp_dir("submit-job-persist-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("DROP TABLE jobs")
            .execute(&raw_pool)
            .await
            .unwrap();

        let job = crate::sync::engine::job::SyncJob::new(
            "job-persist-failure".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        );

        let err = scheduler.submit_job(job.clone()).await.unwrap_err();

        assert!(err.to_string().contains("no such table: jobs"));
        assert!(scheduler.job_cache.get(&job.job_id).is_none());
        assert!(scheduler.job_status_cache.get(&job.job_id).is_none());
        assert_eq!(scheduler.job_queue.len().await, 0);

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_allows_failed_sync_with_pending_retry() {
        let root = temp_dir("finalize-failed-sync");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-finalize-failed-sync".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        job.status = JobStatus::Failed;
        job.priority = 7;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        scheduler.finalize_job(&job.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());

        let final_job_id = format!("{}_final", job.job_id);
        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        assert_eq!(final_snapshot.job_type, JobType::Once);
        assert_eq!(final_snapshot.source.path, job.source.path);
        assert_eq!(final_snapshot.dest.path, job.dest.path);
        let final_runtime = scheduler.job_cache.get(&final_job_id).unwrap();
        assert_eq!(final_runtime.round_id, 1);
        assert!(final_runtime.is_last_round);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_active_final_job_exists() {
        let root = temp_dir("finalize-rejects-active-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-active-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        db.save_job(&final_job).await.unwrap();

        scheduler.finalize_job(&original.job_id).await.unwrap();
        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        assert_eq!(scheduler.job_queue.len().await, 0);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_checks_destination_conflict_before_cancelling_original_retryable_sync() {
        let root = temp_dir("finalize-prechecks-destination-conflict");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-conflict".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Failed;
        db.save_job(&original).await.unwrap();
        db.save_retry(&original.job_id, "temporary network error")
            .await
            .unwrap();

        let conflicting = SyncJob::new(
            "job-conflicting-active".to_string(),
            PathBuf::from("/tmp/other-source.bin"),
            "mirror".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);
        scheduler.submit_job(conflicting.clone()).await.unwrap();

        let err = scheduler.finalize_job(&original.job_id).await.unwrap_err();

        assert!(err
            .to_string()
            .contains("overlaps active job job-conflicting-active"));
        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert!(db.get_retry(&original.job_id).await.unwrap().is_some());
        assert!(scheduler
            .load_job_snapshot(&format!("{}_final", original.job_id))
            .await
            .unwrap()
            .is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_allows_retrying_terminal_failed_final_transfer() {
        let root = temp_dir("finalize-retries-terminal-final-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-terminal-final-failure".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Failed;
        original.round_id = 4;
        original.is_last_round = true;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut failed_final = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        failed_final.status = JobStatus::Failed;
        failed_final.round_id = 4;
        failed_final.is_last_round = true;
        failed_final.error_message = Some("disk full".to_string());
        db.save_job(&failed_final).await.unwrap();

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);
        assert_eq!(original_snapshot.round_id, 4);
        assert!(original_snapshot.is_last_round);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        assert_eq!(final_snapshot.job_type, JobType::Once);
        assert_eq!(final_snapshot.round_id, 4);
        assert!(final_snapshot.is_last_round);
        assert_eq!(final_snapshot.error_message, None);

        let final_runtime = scheduler.get_job_info(&final_job_id).unwrap();
        assert_eq!(final_runtime.round_id, 4);
        assert!(final_runtime.is_last_round);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_final_transfer_already_completed() {
        let root = temp_dir("finalize-completed-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-completed-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        scheduler.notify_job_completed(&final_job_id).await;

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Completed);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Completed);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_preserves_round_metadata_for_final_transfer() {
        let root = temp_dir("finalize-preserves-final-round");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-round-metadata".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        let mut original_runtime = SyncJob::new(
            original.job_id.clone(),
            PathBuf::from(&original.source.path),
            original.dest.path.clone(),
            original.region.clone(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(5);
        original_runtime.round_id = 2;
        original_runtime.is_first_round = false;
        scheduler
            .job_cache
            .insert(original.job_id.clone(), original_runtime.clone());
        scheduler.job_status_cache.insert(
            original.job_id.clone(),
            JobRuntimeStatus {
                job_id: original.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: original.region.clone(),
                error_message: None,
                created_at: original.created_at,
                updated_at: original.updated_at,
            },
        );

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let final_runtime = scheduler.job_cache.get(&final_job_id).unwrap();
        assert_eq!(final_runtime.round_id, 3);
        assert!(final_runtime.is_last_round);
        assert_eq!(final_runtime.priority, 105);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_reuses_synced_file_cache_for_final_transfer() {
        let root = temp_dir("finalize-copies-synced-cache");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-synced-cache".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Pending;
        db.save_job(&original).await.unwrap();

        let runtime = SyncJob::new(
            original.job_id.clone(),
            PathBuf::from(&original.source.path),
            original.dest.path.clone(),
            original.region.clone(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(5);
        scheduler.job_cache.insert(original.job_id.clone(), runtime);
        scheduler.job_status_cache.insert(
            original.job_id.clone(),
            JobRuntimeStatus {
                job_id: original.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: original.region.clone(),
                error_message: None,
                created_at: original.created_at,
                updated_at: original.updated_at,
            },
        );

        let file_path = "/tmp/source.bin".to_string();
        let job_cache = DashMap::new();
        job_cache.insert(
            file_path.clone(),
            FileSyncState {
                size: 123,
                mtime: 456,
                change_time: None,
                inode: None,
                dest_mtime: Some(789),
                dest_change_time: None,
                dest_inode: None,
                updated_at: 789,
            },
        );
        scheduler
            .synced_files_cache
            .insert(original.job_id.clone(), job_cache);

        scheduler.finalize_job(&original.job_id).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        assert!(scheduler.synced_files_cache.get(&original.job_id).is_none());

        let final_cache = scheduler
            .synced_files_cache
            .get(&final_job_id)
            .expect("final transfer should inherit synced file cache");
        let final_state = final_cache
            .get(&file_path)
            .expect("cached source path should be preserved");
        assert_eq!(final_state.size, 123);
        assert_eq!(final_state.mtime, 456);
        assert_eq!(final_state.dest_mtime, Some(789));
        assert_eq!(final_state.updated_at, 789);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_syncing_active_final_runtime_exists() {
        let root = temp_dir("finalize-waits-active-final-runtime");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-finalize-waits-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Syncing;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 60,
                current_size: 600,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_cache.insert(
            final_job_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                final_job_id.clone(),
                PathBuf::from(&final_job.source.path),
                final_job.dest.path.clone(),
                final_job.region.clone(),
            )
            .with_job_type(final_job.job_type),
        );

        scheduler.finalize_job(&original.job_id).await.unwrap();
        assert!(scheduler.job_cache.get(&final_job_id).is_some());
        assert_eq!(
            scheduler.get_job_status(&final_job_id).unwrap().status,
            JobStatus::Syncing
        );

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Syncing);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Syncing);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_pending_cleans_tmp_artifacts_immediately() {
        let root = temp_dir("cancel-pending-cleans-tmp");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let sync_job = crate::sync::engine::job::SyncJob::new(
            "job-cancel-pending".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        scheduler.submit_job(sync_job.clone()).await.unwrap();

        let tmp_path = root.join("pending.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&sync_job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&sync_job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cancel_job(&sync_job.job_id).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db
            .load_tmp_transfer_paths_by_job(&sync_job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(scheduler
            .transfer_manager_pool
            .job_tmp_write_paths(&sync_job.job_id)
            .is_empty());

        let snapshot = scheduler
            .load_job_snapshot(&sync_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_pending_snapshot_without_runtime_succeeds() {
        let root = temp_dir("cancel-pending-snapshot-without-runtime");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-cancel-snapshot-pending".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Pending;
        job.progress = 25;
        job.current_size = 128;
        job.total_size = 512;
        db.save_job(&job).await.unwrap();

        let tmp_path = root.join("snapshot-pending.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.cancel_job(&job.job_id).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);
        assert_eq!(snapshot.progress, 25);
        assert_eq!(snapshot.current_size, 128);
        assert_eq!(snapshot.total_size, 512);
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(!tmp_path.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_returns_error_when_persisted_status_update_fails() {
        let root = temp_dir("cancel-job-persist-failure");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let sync_job = crate::sync::engine::job::SyncJob::new(
            "job-cancel-persist-failure".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        scheduler.submit_job(sync_job.clone()).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_cancel_update
            BEFORE UPDATE ON jobs
            WHEN NEW.status = 'cancelled'
            BEGIN
                SELECT RAISE(FAIL, 'reject cancelled status');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let err = scheduler.cancel_job(&sync_job.job_id).await.unwrap_err();

        assert!(err.to_string().contains("reject cancelled status"));
        assert_eq!(
            scheduler.get_job_status(&sync_job.job_id).unwrap().status,
            JobStatus::Pending
        );
        assert_eq!(scheduler.job_queue.len().await, 1);

        let snapshot = scheduler
            .load_job_snapshot(&sync_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_duplicate_active_job_id() {
        let root = temp_dir("submit-duplicate-active-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let job = crate::sync::engine::job::SyncJob::new(
            "job-duplicate-active".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        scheduler.submit_job(job.clone()).await.unwrap();

        let duplicate = scheduler.submit_job(job.clone()).await;
        assert!(duplicate.is_err());
        assert_eq!(scheduler.job_queue.len().await, 1);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_duplicate_active_destination() {
        let root = temp_dir("submit-duplicate-active-destination");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let first = crate::sync::engine::job::SyncJob::new(
            "job-destination-a".to_string(),
            PathBuf::from("/tmp/source-a.bin"),
            "mirror/shared/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let second = crate::sync::engine::job::SyncJob::new(
            "job-destination-b".to_string(),
            PathBuf::from("/tmp/source-b.bin"),
            "mirror/shared/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);

        scheduler.submit_job(first.clone()).await.unwrap();

        let err = scheduler.submit_job(second.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("overlaps active job job-destination-a"));
        assert_eq!(scheduler.job_queue.len().await, 1);
        assert!(scheduler
            .load_job_snapshot(&second.job_id)
            .await
            .unwrap()
            .is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_overlapping_active_destination_subpath() {
        let root = temp_dir("submit-overlapping-active-destination");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let first = crate::sync::engine::job::SyncJob::new(
            "job-destination-parent".to_string(),
            PathBuf::from("/tmp/source-a"),
            "mirror".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        let second = crate::sync::engine::job::SyncJob::new(
            "job-destination-child".to_string(),
            PathBuf::from("/tmp/source-b.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Once);

        scheduler.submit_job(first.clone()).await.unwrap();

        let err = scheduler.submit_job(second.clone()).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("overlaps active job job-destination-parent"));
        assert_eq!(scheduler.job_queue.len().await, 1);
        assert!(scheduler
            .load_job_snapshot(&second.job_id)
            .await
            .unwrap()
            .is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_duplicate_paused_job_id() {
        let root = temp_dir("submit-duplicate-paused-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let job = crate::sync::engine::job::SyncJob::new(
            "job-duplicate-paused".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        scheduler.job_cache.insert(job.job_id.clone(), job.clone());
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Paused,
                progress: 50,
                current_size: 512,
                total_size: 1024,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let duplicate = scheduler.submit_job(job.clone()).await;
        assert!(duplicate.is_err());
        assert_eq!(scheduler.job_queue.len().await, 0);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_rejects_reuse_while_previous_run_is_still_shutting_down() {
        let root = temp_dir("submit-rejects-shutting-down-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        let job = crate::sync::engine::job::SyncJob::new(
            "job-shutting-down".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);

        scheduler.running_jobs.insert(job.job_id.clone(), ());
        scheduler.cancelled_jobs.insert(job.job_id.clone(), ());
        scheduler.job_cache.insert(job.job_id.clone(), job.clone());
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Cancelled,
                progress: 40,
                current_size: 400,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let duplicate = scheduler.submit_job(job.clone()).await;
        assert!(duplicate.is_err());
        assert!(scheduler.cancelled_jobs.contains_key(&job.job_id));
        assert!(scheduler.running_jobs.contains_key(&job.job_id));

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn submit_job_duplicate_active_does_not_overwrite_snapshot() {
        let root = temp_dir("submit-duplicate-active-does-not-overwrite-snapshot");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let original_job = crate::sync::engine::job::SyncJob::new(
            "job-duplicate-active-snapshot".to_string(),
            PathBuf::from("/tmp/source-a.bin"),
            "mirror/output-a.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(3);
        scheduler.submit_job(original_job.clone()).await.unwrap();

        let duplicate_job = crate::sync::engine::job::SyncJob::new(
            original_job.job_id.clone(),
            PathBuf::from("/tmp/source-b.bin"),
            "mirror/output-b.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(9);

        let duplicate = scheduler.submit_job(duplicate_job).await;
        assert!(duplicate.is_err());

        let snapshot = scheduler
            .load_job_snapshot(&original_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.source.path, "/tmp/source-a.bin");
        assert_eq!(snapshot.dest.path, "mirror/output-a.bin");
        assert_eq!(snapshot.priority, 3);
        assert_eq!(snapshot.status, JobStatus::Pending);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_clears_all_duplicate_queued_entries() {
        let root = temp_dir("cancel-clears-all-queued-duplicates");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let job = crate::sync::engine::job::SyncJob::new(
            "job-cancel-duplicate-queued".to_string(),
            PathBuf::from("/tmp/source.bin"),
            "mirror/output.bin".to_string(),
            "local".to_string(),
        )
        .with_job_type(JobType::Sync);
        scheduler.submit_job(job.clone()).await.unwrap();
        scheduler
            .delayed_queue
            .insert(
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                job.clone(),
            )
            .await;
        scheduler
            .delayed_queue
            .insert(
                std::time::Instant::now() + std::time::Duration::from_secs(120),
                job.clone(),
            )
            .await;

        scheduler.cancel_job(&job.job_id).await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(scheduler.delayed_queue.len().await, 0);

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_original_sync_job_redirects_to_active_final_job() {
        let root = temp_dir("cancel-original-redirects-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-cancel-original-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Syncing;
        original.progress = 40;
        original.current_size = 400;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        final_job.progress = 40;
        final_job.current_size = 400;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        let tmp_path = root.join("final-running.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&final_job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        db.save_transfer_state(
            &final_job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.cancel_job(&original.job_id).await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Cancelled);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Cancelled);
        assert!(db
            .load_transfer_state(&final_job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&final_job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(!tmp_path.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancel_original_sync_job_redirects_to_paused_final_job() {
        let root = temp_dir("cancel-original-redirects-paused-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-cancel-original-paused-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Syncing;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Paused;
        final_job.progress = 40;
        final_job.current_size = 400;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.cancel_job(&original.job_id).await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Cancelled);

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recover_pending_jobs_marks_unrecoverable_jobs_failed() {
        let root = temp_dir("recover-invalid-destination");
        let data_dir = root.join("sync-data");
        let outside = root.join("outside");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, data_dir.join("escape")).unwrap();

        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-recover-invalid-dest".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "escape/out.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Pending;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "stale retry").await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();
        db.save_tmp_transfer_path(&job.job_id, outside.join("out.bin.tmp").to_str().unwrap())
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();
        scheduler.transfer_manager_pool.shutdown().await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);
        assert!(snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("escapes sync.data_dir")));
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(scheduler.job_cache.get(&job.job_id).is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_preserves_persisted_progress() {
        let root = temp_dir("recover-progress");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-recover-progress".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Syncing;
        job.progress = 60;
        job.current_size = 600;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert_eq!(runtime.progress, 60);
        assert_eq!(runtime.current_size, 600);
        assert_eq!(runtime.total_size, 1000);
        let recovered_job = scheduler.get_job_info(&job.job_id).unwrap();
        assert_eq!(recovered_job.round_id, 1);
        assert!(!recovered_job.is_first_round);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_prefers_active_final_job_over_original() {
        let root = temp_dir("recover-prefers-final");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = chrono::Utc::now();
        let mut original = Job::new(
            "job-recover-prefers-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Syncing;
        original.created_at = now - chrono::Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        final_job.created_at = now;
        final_job.updated_at = now;
        db.save_job(&final_job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        assert!(scheduler.get_job_info(&original.job_id).is_none());
        assert!(scheduler.get_job_status(&original.job_id).is_none());

        let final_runtime = scheduler
            .get_job_status(&final_job.job_id)
            .expect("final job should recover");
        assert_eq!(final_runtime.status, JobStatus::Pending);
        assert!(scheduler.get_job_info(&final_job.job_id).is_some());

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_restores_paused_jobs_without_queueing() {
        let root = temp_dir("recover-paused-job");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-recover-paused".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Paused;
        job.progress = 61;
        job.current_size = 610;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Paused);
        assert_eq!(runtime.progress, 61);
        assert_eq!(scheduler.job_queue.len().await, 0);
        assert!(scheduler.get_job_info(&job.job_id).is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_updates_original_sync_snapshot_for_paused_final_job() {
        let root = temp_dir("recover-paused-final-updates-original");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = chrono::Utc::now();
        let mut original = Job::new(
            "job-recover-paused-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Cancelled;
        original.created_at = now - chrono::Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Paused;
        final_job.progress = 61;
        final_job.current_size = 610;
        final_job.total_size = 1000;
        final_job.created_at = now;
        final_job.updated_at = now;
        db.save_job(&final_job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let final_runtime = scheduler
            .get_job_status(&final_job.job_id)
            .expect("paused final job should recover");
        assert_eq!(final_runtime.status, JobStatus::Paused);
        assert_eq!(final_runtime.progress, 61);
        let final_job_info = scheduler.get_job_info(&final_job.job_id).unwrap();
        assert_eq!(final_job_info.round_id, 1);
        assert!(final_job_info.is_last_round);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Paused);
        assert_eq!(original_snapshot.progress, 61);
        assert_eq!(original_snapshot.current_size, 610);
        assert_eq!(original_snapshot.total_size, 1000);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_restores_synced_file_cache_from_transfer_states() {
        let root = temp_dir("recover-restores-synced-file-cache");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-recover-cache".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        let mut completed = FileTransferState::new("remote/finished.bin".to_string(), 4)
            .with_source_version(4096, 1710000000, None, None);
        for chunk in 0..4 {
            completed.mark_chunk_completed(chunk);
        }
        completed.dest_modified = Some(1710000100);
        completed.dest_change_time = Some(1710000101);
        completed.dest_inode = Some(41);
        completed.cache_only = true;
        db.save_transfer_state(&job.job_id, &completed)
            .await
            .unwrap();

        let mut legacy_completed = FileTransferState::new("remote/legacy.bin".to_string(), 2)
            .with_source_version(2048, 1710000004, None, None);
        for chunk in 0..2 {
            legacy_completed.mark_chunk_completed(chunk);
        }
        db.save_transfer_state(&job.job_id, &legacy_completed)
            .await
            .unwrap();

        let zero_byte_checkpoint = FileTransferState::new("remote/empty.bin".to_string(), 0)
            .with_source_version(0, 1710000005, None, None)
            .mark_cache_only();
        db.save_transfer_state(&job.job_id, &zero_byte_checkpoint)
            .await
            .unwrap();

        let incomplete = FileTransferState::new("remote/incomplete.bin".to_string(), 4)
            .with_source_version(2048, 1710000001, None, None);
        db.save_transfer_state(&job.job_id, &incomplete)
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let job_cache = scheduler
            .synced_files_cache
            .get(&job.job_id)
            .expect("recovered job should rebuild synced file cache");
        assert_eq!(job_cache.len(), 3);
        let restored = job_cache
            .get("remote/finished.bin")
            .expect("completed file should be restored");
        assert_eq!(restored.size, 4096);
        assert_eq!(restored.mtime, 1710000000);
        assert_eq!(restored.dest_mtime, Some(1710000100));
        assert_eq!(restored.dest_change_time, Some(1710000101));
        assert_eq!(restored.dest_inode, Some(41));
        assert!(restored.updated_at > 0);

        let legacy_restored = job_cache
            .get("remote/legacy.bin")
            .expect("legacy completed file should still be restored");
        assert_eq!(legacy_restored.size, 2048);
        assert_eq!(legacy_restored.mtime, 1710000004);
        assert_eq!(legacy_restored.dest_mtime, None);

        let zero_byte_restored = job_cache
            .get("remote/empty.bin")
            .expect("zero-byte checkpoint should be restored");
        assert_eq!(zero_byte_restored.size, 0);
        assert_eq!(zero_byte_restored.mtime, 1710000005);
        assert_eq!(zero_byte_restored.dest_mtime, None);
        assert!(job_cache.get("remote/incomplete.bin").is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_restores_paused_job_synced_file_cache() {
        let root = temp_dir("recover-paused-restores-synced-file-cache");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-recover-paused-cache".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Paused;
        db.save_job(&job).await.unwrap();

        let mut completed = FileTransferState::new("remote/paused-finished.bin".to_string(), 2)
            .with_source_version(1024, 1710000002, None, None);
        for chunk in 0..2 {
            completed.mark_chunk_completed(chunk);
        }
        completed.dest_modified = Some(1710000200);
        completed.dest_change_time = Some(1710000201);
        completed.dest_inode = Some(42);
        completed.cache_only = true;
        db.save_transfer_state(&job.job_id, &completed)
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let job_cache = scheduler
            .synced_files_cache
            .get(&job.job_id)
            .expect("paused job should rebuild synced file cache");
        let restored = job_cache
            .get("remote/paused-finished.bin")
            .expect("completed paused file should be restored");
        assert_eq!(restored.size, 1024);
        assert_eq!(restored.mtime, 1710000002);
        assert_eq!(restored.dest_mtime, Some(1710000200));
        assert_eq!(restored.dest_change_time, Some(1710000201));
        assert_eq!(restored.dest_inode, Some(42));
        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Paused
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recover_pending_jobs_marks_original_failed_when_final_recovery_fails() {
        let root = temp_dir("recover-final-invalid-destination");
        let data_dir = root.join("sync-data");
        let outside = root.join("outside");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, data_dir.join("escape")).unwrap();

        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = chrono::Utc::now();
        let mut original = Job::new(
            "job-recover-final-invalid".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "escape/out.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Syncing;
        original.created_at = now - chrono::Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        final_job.created_at = now;
        final_job.updated_at = now;
        db.save_job(&final_job).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert!(original_snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("escapes sync.data_dir")));

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Failed);
        assert!(final_snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("escapes sync.data_dir")));

        assert!(scheduler.get_job_status(&original.job_id).is_none());
        assert!(scheduler.get_job_status(&final_job.job_id).is_none());
        assert!(scheduler.get_job_info(&original.job_id).is_none());
        assert!(scheduler.get_job_info(&final_job.job_id).is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_marks_paused_job_failed_when_destination_conflicts() {
        let root = temp_dir("recover-paused-destination-conflict");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = chrono::Utc::now();

        let mut paused = Job::new(
            "job-recover-paused-conflict".to_string(),
            JobPath {
                path: "/tmp/source-paused".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        paused.status = JobStatus::Paused;
        paused.created_at = now;
        paused.updated_at = now;
        db.save_job(&paused).await.unwrap();

        let mut active = Job::new(
            "job-recover-active-conflict".to_string(),
            JobPath {
                path: "/tmp/source-active".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        active.status = JobStatus::Pending;
        active.created_at = now - chrono::Duration::seconds(1);
        active.updated_at = active.created_at;
        db.save_job(&active).await.unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let paused_snapshot = scheduler
            .load_job_snapshot(&paused.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused_snapshot.status, JobStatus::Failed);
        assert!(paused_snapshot
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("overlaps active job")));
        assert!(scheduler.get_job_status(&paused.job_id).is_none());
        assert!(scheduler.get_job_info(&paused.job_id).is_none());

        let active_runtime = scheduler.get_job_status(&active.job_id).unwrap();
        assert_eq!(active_runtime.status, JobStatus::Pending);
        assert!(scheduler.get_job_info(&active.job_id).is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_jobs_ignores_corrupted_terminal_rows() {
        let root = temp_dir("recover-pending-ignore-terminal-corruption");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut pending = Job::new(
            "job-recover-active".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/active.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        pending.status = JobStatus::Pending;
        db.save_job(&pending).await.unwrap();

        let mut completed = Job::new(
            "job-completed-corrupted".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/completed.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        completed.status = JobStatus::Completed;
        db.save_job(&completed).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&completed.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.recover_pending_jobs().await.unwrap();

        let recovered = scheduler
            .get_job_status(&pending.job_id)
            .expect("active job should still recover");
        assert_eq!(recovered.status, JobStatus::Pending);
        assert!(scheduler.job_cache.get(&pending.job_id).is_some());

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cleanup_terminal_job_artifacts_removes_stale_cancelled_runtime_state() {
        let root = temp_dir("cleanup-terminal-artifacts");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-terminal-cancelled".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Cancelled;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "stale retry").await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        let tmp_path = root.join("cancelled-restart.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cleanup_terminal_job_artifacts().await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cleanup_terminal_job_artifacts_preserves_retryable_failed_state() {
        let root = temp_dir("cleanup-terminal-retryable-failed");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-terminal-failed-retryable".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Failed;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        let tmp_path = root.join("retryable-failed.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cleanup_terminal_job_artifacts().await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(tmp_path.exists());
        assert!(db.get_retry(&job.job_id).await.unwrap().is_some());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());
        assert!(!db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cleanup_terminal_job_artifacts_ignores_corrupted_active_rows() {
        let root = temp_dir("cleanup-terminal-ignore-active-corruption");
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut completed = Job::new(
            "job-terminal-cleanup-ok".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        completed.status = JobStatus::Cancelled;
        db.save_job(&completed).await.unwrap();
        db.save_retry(&completed.job_id, "stale retry")
            .await
            .unwrap();
        db.save_transfer_state(
            &completed.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        let tmp_path = root.join("cleanup-terminal-ok.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&completed.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        let mut active = Job::new(
            "job-active-corrupted".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/active.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        active.status = JobStatus::Pending;
        db.save_job(&active).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&active.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.cleanup_terminal_job_artifacts().await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db.get_retry(&completed.job_id).await.unwrap().is_none());
        assert!(db
            .load_transfer_state(&completed.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&completed.job_id)
            .await
            .unwrap()
            .is_empty());

        raw_pool.close().await;
        fs::remove_dir_all(root).unwrap();
    }
}
