use crate::application::sync::engine::scheduler::sync_modes::calculate_progress;
use crate::domain::job::JobStatus;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info, warn};

use super::super::core::SyncScheduler;

impl SyncScheduler {
    pub(in crate::application::sync::engine::scheduler) async fn retry_scheduler_loop(&self) {
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        loop {
            if self.shutdown.load(Ordering::Relaxed) || *shutdown_rx.borrow() {
                break;
            }

            if let Err(e) = self.process_pending_retries().await {
                error!(operation = "retry.scan_failed", error = %e, "retry scan failed");
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn process_pending_retries(&self) -> crate::shared::error::Result<()> {
        let pending_retries = self.db.get_pending_retries().await?;

        if pending_retries.is_empty() {
            return Ok(());
        }

        info!(
            operation = "retry.scan_completed",
            pending_count = pending_retries.len(),
            "retry scan found pending jobs"
        );

        for retry in pending_retries {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            if let Some(status) = self.job_status_cache.get(&retry.job_id) {
                match status.status {
                    JobStatus::Cancelled => {
                        info!(
                            operation = "retry.record_removed",
                            job_id = %retry.job_id,
                            reason = "job_cancelled",
                            "retry record removed"
                        );
                        self.cleanup_stale_retry_record(&retry.job_id, "job already cancelled")
                            .await;
                        continue;
                    }
                    JobStatus::Pending | JobStatus::Syncing | JobStatus::Paused => {
                        info!(
                            operation = "retry.schedule_skipped",
                            job_id = %retry.job_id,
                            status = ?status.status,
                            reason = "job_already_active",
                            "retry scheduling skipped"
                        );
                        continue;
                    }
                    _ => {}
                }
            }

            let mut inserted_runtime_job = false;
            let mut job_info = match self.job_cache.get(&retry.job_id) {
                Some(job) => job.clone(),
                None => {
                    let Some(snapshot) = self.load_job_snapshot(&retry.job_id).await? else {
                        warn!(
                            operation = "retry.schedule_skipped",
                            job_id = %retry.job_id,
                            reason = "job_not_found",
                            "retry scheduling skipped"
                        );
                        self.cleanup_stale_retry_record(
                            &retry.job_id,
                            "job missing from cache and database",
                        )
                        .await;
                        continue;
                    };

                    if snapshot.status == JobStatus::Cancelled
                        || snapshot.status == JobStatus::Completed
                    {
                        info!(
                            operation = "retry.record_removed",
                            job_id = %retry.job_id,
                            status = ?snapshot.status,
                            reason = "job_terminal",
                            "retry record removed"
                        );
                        self.cleanup_stale_retry_record(&retry.job_id, "snapshot already terminal")
                            .await;
                        continue;
                    }

                    if snapshot.status == JobStatus::Paused {
                        info!(operation = "retry.schedule_skipped", job_id = %retry.job_id, reason = "job_paused", "retry scheduling skipped");
                        continue;
                    }

                    let job = {
                        let mut job = crate::application::sync::engine::job::SyncJob::new(
                            snapshot.job_id.clone(),
                            std::path::PathBuf::from(&snapshot.source.path),
                            snapshot.dest.path.clone(),
                            snapshot.region.clone(),
                        )
                        .with_filters(
                            snapshot.exclude_regex.clone(),
                            snapshot.include_regex.clone(),
                        )
                        .with_priority(snapshot.priority)
                        .with_job_type(snapshot.job_type);
                        job.restore_round_state(snapshot.round_id, snapshot.is_last_round);
                        job.mark_resumed_round();
                        if job.job_id.ends_with("_final") {
                            job.ensure_final_round_state();
                        }
                        job
                    };

                    self.job_cache.insert(retry.job_id.clone(), job.clone());
                    if let Err(e) = self
                        .restore_synced_file_cache_from_transfer_states(&retry.job_id)
                        .await
                    {
                        self.job_cache.remove(&retry.job_id);
                        return Err(e);
                    }
                    inserted_runtime_job = true;
                    job
                }
            };
            if job_info.job_id.ends_with("_final") {
                job_info.ensure_final_round_state();
                if let Some(mut cached_job) = self.job_cache.get_mut(&retry.job_id) {
                    cached_job.ensure_final_round_state();
                }
            }
            self.persist_job_round_state(&retry.job_id, job_info.round_id, job_info.is_last_round)
                .await;

            if let Some(status) = self.job_status_cache.get(&retry.job_id) {
                match status.status {
                    JobStatus::Completed | JobStatus::Cancelled => {
                        info!(
                            operation = "retry.terminal_job_cleanup",
                            job_id = %retry.job_id,
                            status = ?status.status,
                            "terminal job retry record cleanup started"
                        );
                        self.cleanup_stale_retry_record(
                            &retry.job_id,
                            "runtime status already terminal",
                        )
                        .await;
                        if inserted_runtime_job {
                            self.job_cache.remove(&retry.job_id);
                        }
                        continue;
                    }
                    JobStatus::Pending | JobStatus::Syncing | JobStatus::Paused => {
                        info!(
                            operation = "retry.schedule_skipped",
                            job_id = %retry.job_id,
                            status = ?status.status,
                            reason = "job_already_active",
                            "retry scheduling skipped"
                        );
                        if inserted_runtime_job {
                            self.job_cache.remove(&retry.job_id);
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            info!(
                operation = "retry.attempt_started",
                job_id = %retry.job_id,
                attempt = retry.retry_count + 1,
                max_attempts = retry.max_retries,
                previous_error = %retry.last_error,
                "retry attempt started"
            );

            let mut pending_runtime = None;
            let (progress, current_size, total_size) = if let Some(status) =
                self.job_status_cache.get(&retry.job_id)
            {
                (status.progress, status.current_size, status.total_size)
            } else {
                let snapshot = self
                    .load_job_snapshot(&retry.job_id)
                    .await?
                    .ok_or_else(|| {
                        crate::shared::error::HarDataError::JobNotFound(retry.job_id.clone())
                    })?;
                let current_size = snapshot.current_size.min(snapshot.total_size);
                let progress = calculate_progress(current_size, snapshot.total_size);
                let now = chrono::Utc::now();
                pending_runtime = Some(
                    crate::application::sync::engine::scheduler::JobRuntimeStatus {
                        job_id: retry.job_id.clone(),
                        status: JobStatus::Pending,
                        progress,
                        current_size,
                        total_size: snapshot.total_size,
                        region: snapshot.region.clone(),
                        error_message: None,
                        created_at: snapshot.created_at,
                        updated_at: now,
                    },
                );
                (progress, current_size, snapshot.total_size)
            };

            let pending_status_updated = match self
                .db
                .update_job_status(
                    &retry.job_id,
                    JobStatus::Pending,
                    progress,
                    current_size,
                    total_size,
                    None,
                )
                .await
            {
                Ok(updated) => updated,
                Err(e) => {
                    warn!(
                        operation = "retry.status_persist_failed",
                        job_id = %retry.job_id,
                        error = %e,
                        "retry pending status persistence failed"
                    );
                    if inserted_runtime_job {
                        self.job_cache.remove(&retry.job_id);
                    }
                    continue;
                }
            };
            if !pending_status_updated {
                warn!(
                    operation = "retry.schedule_skipped",
                    job_id = %retry.job_id,
                    reason = "persisted_row_missing",
                    "retry scheduling skipped"
                );
                self.cleanup_runtime_job(&retry.job_id);
                self.cleanup_stale_retry_record(
                    &retry.job_id,
                    "persisted row missing while transitioning retry to pending",
                )
                .await;
                continue;
            }

            if let Err(e) = self.db.update_retry_attempt(&retry.job_id, false).await {
                warn!(
                    operation = "retry.attempt_record_failed",
                    job_id = %retry.job_id,
                    error = %e,
                    "retry attempt recording failed"
                );
                let rollback_updated = match self
                    .db
                    .update_job_status(
                        &retry.job_id,
                        JobStatus::Failed,
                        progress,
                        current_size,
                        total_size,
                        Some(&retry.last_error),
                    )
                    .await
                {
                    Ok(updated) => updated,
                    Err(revert_error) => {
                        warn!(
                            operation = "retry.rollback_failed",
                            job_id = %retry.job_id,
                            error = %revert_error,
                            "retry status rollback failed"
                        );
                        false
                    }
                };
                if !rollback_updated {
                    warn!(
                        operation = "retry.rollback_skipped",
                        job_id = %retry.job_id,
                        reason = "persisted_row_missing",
                        "retry status rollback skipped"
                    );
                }
                if inserted_runtime_job {
                    self.job_cache.remove(&retry.job_id);
                }
                continue;
            }

            if let Some(mut status) = self.job_status_cache.get_mut(&retry.job_id) {
                status.status = JobStatus::Pending;
                status.error_message = None;
                status.updated_at = chrono::Utc::now();
            } else if let Some(runtime) = pending_runtime {
                self.job_status_cache.insert(retry.job_id.clone(), runtime);
            }

            self.enqueue_job_replacing_queued(job_info.clone()).await;
            self.update_original_sync_job_status_from_final(
                &retry.job_id,
                JobStatus::Pending,
                progress,
                current_size,
                total_size,
                None,
            )
            .await;
            self.job_notify.notify_one();
            info!(
                operation = "retry.scheduled",
                job_id = %retry.job_id,
                attempt = retry.retry_count + 1,
                "retry job enqueued"
            );
        }

        Ok(())
    }

    pub(in crate::application::sync::engine::scheduler) async fn mark_retry_success(
        &self,
        job_id: &str,
    ) {
        match self.db.update_retry_attempt(job_id, true).await {
            Ok(true) => {
                info!(operation = "retry.succeeded", job_id = %job_id, "retry succeeded and record removed");
            }
            Ok(false) => {}
            Err(e) => {
                warn!(operation = "retry.success_record_failed", job_id = %job_id, error = %e, "retry success recording failed");
            }
        }
    }

    async fn cleanup_stale_retry_record(&self, job_id: &str, reason: &str) {
        if let Err(e) = self.db.delete_retry(job_id).await {
            warn!(
                operation = "retry.record_delete_failed",
                job_id = %job_id,
                reason = %reason,
                error = %e,
                "stale retry record deletion failed"
            );
        }
    }
}
