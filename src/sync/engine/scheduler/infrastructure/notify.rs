use crate::core::job::JobStatus;
use tracing::{info, warn};

use super::super::core::SyncScheduler;

fn is_retryable_error(error: &str) -> bool {
    let error_lower = error.to_lowercase();

    let network_keywords = [
        "network",
        "connection",
        "timeout",
        "refused",
        "reset",
        "unreachable",
        "unavailable",
        "disconnect",
        "broken pipe",
        "io error",
        "transport",
        "quic",
        "tcp",
        "failed to connect",
    ];

    for keyword in &network_keywords {
        if error_lower.contains(keyword) {
            return true;
        }
    }

    let permanent_keywords = [
        "not found",
        "permission denied",
        "no such file",
        "invalid path",
        "access denied",
        "cancelled by user",
    ];

    for keyword in &permanent_keywords {
        if error_lower.contains(keyword) {
            return false;
        }
    }

    false
}

impl SyncScheduler {
    pub(in crate::sync::engine::scheduler) async fn notify_job_started(&self, job_id: &str) {
        if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
            entry.status = JobStatus::Syncing;
            entry.updated_at = chrono::Utc::now();
        }

        self.update_job_status_in_db(job_id, JobStatus::Syncing, 0, 0, 0)
            .await;

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_started(job_id);
        }
    }

    pub(in crate::sync::engine::scheduler) async fn notify_job_completed(&self, job_id: &str) {
        let (current_size, total_size) =
            if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
                entry.status = JobStatus::Completed;
                entry.progress = 100;
                entry.updated_at = chrono::Utc::now();
                (entry.current_size, entry.total_size)
            } else {
                (0, 0)
            };

        self.update_job_status_in_db(job_id, JobStatus::Completed, 100, current_size, total_size)
            .await;

        if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
            warn!("Failed to clear transfer states for job {}: {}", job_id, e);
        }

        self.mark_retry_success(job_id).await;

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_completed(job_id);
        }
    }

    pub(in crate::sync::engine::scheduler) async fn notify_job_failed(
        &self,
        job_id: &str,
        error: &str,
    ) {
        let (current_size, total_size) =
            if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
                entry.status = JobStatus::Failed;
                entry.error_message = Some(error.to_string());
                entry.updated_at = chrono::Utc::now();
                (entry.current_size, entry.total_size)
            } else {
                (0, 0)
            };

        self.update_job_status_in_db(job_id, JobStatus::Failed, 0, current_size, total_size)
            .await;

        if is_retryable_error(error) {
            if let Err(e) = self.db.save_retry(job_id, error).await {
                warn!("Failed to save retry record for job {}: {}", job_id, e);
            } else {
                info!(
                    "Job {} marked for retry due to network error: {}",
                    job_id, error
                );
            }
        } else {
            info!("Job {} failed with non-retryable error: {}", job_id, error);
        }

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_failed(job_id, error);
        }
    }

    async fn update_job_status_in_db(
        &self,
        job_id: &str,
        status: JobStatus,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) {
        if let Err(e) = self
            .db
            .update_job_status(job_id, status, progress, current_size, total_size)
            .await
        {
            warn!("Failed to update job {} status in database: {}", job_id, e);
        }
    }
}
