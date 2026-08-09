use crate::domain::job::JobStatus;
use std::collections::HashSet;
use tracing::{info, warn};

use super::super::core::SyncScheduler;
use super::super::retry::ErrorCategory;

pub(in crate::application::sync::engine::scheduler) fn is_cancelled_error(error: &str) -> bool {
    let error_lower = error.to_lowercase();
    error_lower.contains("cancelled by user") || error_lower == "job cancelled"
}

impl SyncScheduler {
    pub(in crate::application::sync::engine::scheduler) async fn notify_job_pending(
        &self,
        job_id: &str,
    ) {
        if self
            .should_skip_runtime_free_status_update(job_id, "pending")
            .await
        {
            return;
        }

        let (progress, current_size, total_size) =
            if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
                entry.status = JobStatus::Pending;
                entry.error_message = None;
                entry.updated_at = chrono::Utc::now();
                (entry.progress, entry.current_size, entry.total_size)
            } else {
                let Some(snapshot) = self
                    .load_inactive_job_snapshot_metrics(job_id, "pending")
                    .await
                else {
                    return;
                };
                snapshot
            };

        if !self
            .update_job_status_in_db(
                job_id,
                JobStatus::Pending,
                progress,
                current_size,
                total_size,
                None,
            )
            .await
        {
            return;
        }
        self.update_original_sync_job_status_from_final(
            job_id,
            JobStatus::Pending,
            progress,
            current_size,
            total_size,
            None,
        )
        .await;
    }

    pub(in crate::application::sync::engine::scheduler) async fn notify_job_started(
        &self,
        job_id: &str,
    ) {
        if self
            .should_skip_runtime_free_status_update(job_id, "started")
            .await
        {
            return;
        }

        let (progress, current_size, total_size) =
            if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
                entry.status = JobStatus::Syncing;
                entry.error_message = None;
                entry.updated_at = chrono::Utc::now();
                (entry.progress, entry.current_size, entry.total_size)
            } else {
                let Some(snapshot) = self
                    .load_inactive_job_snapshot_metrics(job_id, "started")
                    .await
                else {
                    return;
                };
                snapshot
            };

        if !self
            .update_job_status_in_db(
                job_id,
                JobStatus::Syncing,
                progress,
                current_size,
                total_size,
                None,
            )
            .await
        {
            return;
        }
        self.update_original_sync_job_status_from_final(
            job_id,
            JobStatus::Syncing,
            progress,
            current_size,
            total_size,
            None,
        )
        .await;

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_started(job_id);
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn notify_job_cancelled(
        &self,
        job_id: &str,
    ) {
        let (progress, current_size, total_size) = if let Some(mut entry) =
            self.job_status_cache.get_mut(job_id)
        {
            entry.status = JobStatus::Cancelled;
            entry.error_message = None;
            entry.updated_at = chrono::Utc::now();
            (entry.progress, entry.current_size, entry.total_size)
        } else {
            let snapshot = match self.load_job_snapshot(job_id).await {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    warn!(
                        operation = "job.cancel_status_update_skipped",
                        job_id = %job_id,
                        reason = "snapshot_load_failed",
                        error = %e,
                        "cancelled status update skipped"
                    );
                    return;
                }
            };

            if let Some(job) = snapshot {
                (job.progress, job.current_size, job.total_size)
            } else {
                (0, 0, 0)
            }
        };

        if !self
            .update_job_status_in_db(
                job_id,
                JobStatus::Cancelled,
                progress,
                current_size,
                total_size,
                None,
            )
            .await
        {
            return;
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

        if let Err(e) = self.db.delete_retry(job_id).await {
            warn!(
                operation = "job.retry_record_delete_failed",
                job_id = %job_id,
                error = %e,
                "retry record deletion failed"
            );
        }

        if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
            warn!(
                operation = "job.transfer_state_cleanup_failed",
                job_id = %job_id,
                error = %e,
                "transfer state cleanup failed"
            );
        }

        self.cleanup_job_tmp_artifacts(job_id).await;

        self.cleanup_runtime_job(job_id);
    }

    pub(in crate::application::sync::engine::scheduler) async fn notify_job_completed(
        &self,
        job_id: &str,
    ) {
        if self
            .should_skip_runtime_free_status_update(job_id, "completed")
            .await
        {
            return;
        }

        let (current_size, total_size) =
            if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
                entry.status = JobStatus::Completed;
                entry.progress = 100;
                entry.error_message = None;
                entry.updated_at = chrono::Utc::now();
                let total_size = entry.total_size;
                entry.current_size = total_size;
                (total_size, total_size)
            } else {
                let Some(snapshot) = self
                    .load_inactive_job_completion_sizes(job_id, "completed")
                    .await
                else {
                    return;
                };
                snapshot
            };

        if !self
            .update_job_status_in_db(
                job_id,
                JobStatus::Completed,
                100,
                current_size,
                total_size,
                None,
            )
            .await
        {
            return;
        }
        self.update_original_sync_job_status_from_final(
            job_id,
            JobStatus::Completed,
            100,
            current_size,
            total_size,
            None,
        )
        .await;

        if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
            warn!(operation = "job.transfer_state_cleanup_failed", job_id = %job_id, error = %e, "transfer state cleanup failed");
        }

        if let Err(e) = self
            .transfer_manager_pool
            .clear_job_tmp_write_paths(job_id)
            .await
        {
            warn!(operation = "job.tmp_path_cleanup_failed", job_id = %job_id, error = %e, "temporary path cleanup failed");
        }

        self.mark_retry_success(job_id).await;

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_completed(job_id);
        }

        self.cleanup_runtime_job(job_id);
    }

    pub(in crate::application::sync::engine::scheduler) async fn notify_job_failed(
        &self,
        job_id: &str,
        error: &str,
        category: ErrorCategory,
    ) {
        if self
            .should_skip_runtime_free_status_update(job_id, "failed")
            .await
        {
            return;
        }

        let retryable = matches!(
            category,
            ErrorCategory::Transient | ErrorCategory::Retriable
        );
        let mut retry_exhausted = false;
        let (progress, current_size, total_size) =
            if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
                entry.status = JobStatus::Failed;
                entry.error_message = Some(error.to_string());
                entry.updated_at = chrono::Utc::now();
                (entry.progress, entry.current_size, entry.total_size)
            } else {
                let Some(snapshot) = self
                    .load_inactive_job_snapshot_metrics(job_id, "failed")
                    .await
                else {
                    return;
                };
                snapshot
            };

        if !self
            .update_job_status_in_db(
                job_id,
                JobStatus::Failed,
                progress,
                current_size,
                total_size,
                Some(error),
            )
            .await
        {
            return;
        }
        self.update_original_sync_job_status_from_final(
            job_id,
            JobStatus::Failed,
            progress,
            current_size,
            total_size,
            Some(error),
        )
        .await;

        if retryable {
            if let Err(e) = self.db.save_retry(job_id, error).await {
                warn!(operation = "retry.record_save_failed", job_id = %job_id, error = %e, "retry record save failed");
                retry_exhausted = true;
            } else {
                info!(
                    operation = "retry.record_saved",
                    job_id = %job_id,
                    category = ?category,
                    error = %error,
                    "job marked for retry"
                );
                retry_exhausted = match self.db.get_retry(job_id).await {
                    Ok(Some(retry)) => retry.retry_count >= retry.max_retries,
                    Ok(None) => {
                        warn!(
                            operation = "retry.record_missing",
                            job_id = %job_id,
                            "retry record missing after save; failure treated as terminal"
                        );
                        true
                    }
                    Err(e) => {
                        warn!(
                            operation = "retry.record_reload_failed",
                            job_id = %job_id,
                            error = %e,
                            "retry record reload failed; failure treated as terminal"
                        );
                        true
                    }
                };
            }
        } else {
            info!(
                operation = "job.failed_terminal",
                job_id = %job_id,
                category = ?category,
                error = %error,
                "job failed with non-retryable error"
            );
        }

        if !retryable || retry_exhausted {
            if let Err(e) = self.db.delete_retry(job_id).await {
                warn!(
                    operation = "retry.record_delete_failed",
                    job_id = %job_id,
                    error = %e,
                    "terminal retry record deletion failed"
                );
            }

            if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
                warn!(
                    operation = "job.transfer_state_cleanup_failed",
                    job_id = %job_id,
                    error = %e,
                    "terminal transfer state cleanup failed"
                );
            }

            self.cleanup_job_tmp_artifacts(job_id).await;
        }

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_failed(job_id, error);
        }

        if !retryable || retry_exhausted {
            self.cleanup_runtime_job(job_id);
        }
    }

}
