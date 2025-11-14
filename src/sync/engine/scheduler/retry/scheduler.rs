use crate::core::job::JobStatus;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info, warn};

use super::super::core::SyncScheduler;

impl SyncScheduler {
    pub(in crate::sync::engine::scheduler) async fn retry_scheduler_loop(&self) {
        info!("Retry scheduler started");

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Retry scheduler received shutdown signal");
                break;
            }

            if let Err(e) = self.process_pending_retries().await {
                error!("Error processing pending retries: {}", e);
            }

            tokio::time::sleep(Duration::from_secs(30)).await;
        }

        info!("Retry scheduler stopped");
    }

    async fn process_pending_retries(&self) -> crate::util::error::Result<()> {
        let pending_retries = self.db.get_pending_retries().await?;

        if pending_retries.is_empty() {
            return Ok(());
        }

        info!("Found {} jobs pending retry", pending_retries.len());

        for retry in pending_retries {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let job_info = match self.job_cache.get(&retry.job_id) {
                Some(job) => job.clone(),
                None => {
                    warn!("Job {} not found in cache, skipping retry", retry.job_id);
                    let _ = self.db.delete_retry(&retry.job_id).await;
                    continue;
                }
            };

            if let Some(status) = self.job_status_cache.get(&retry.job_id) {
                if status.status == JobStatus::Completed {
                    info!(
                        "Job {} already completed, removing retry record",
                        retry.job_id
                    );
                    let _ = self.db.delete_retry(&retry.job_id).await;
                    continue;
                }
            }

            info!(
                "Retrying job {} (attempt {}/{}): {}",
                retry.job_id,
                retry.retry_count + 1,
                retry.max_retries,
                retry.last_error
            );

            if let Some(mut status) = self.job_status_cache.get_mut(&retry.job_id) {
                status.status = JobStatus::Pending;
                status.error_message = None;
                status.updated_at = chrono::Utc::now();
            }

            self.job_queue
                .enqueue(job_info.priority, job_info.clone())
                .await;
            self.job_notify.notify_one();

            let _ = self.db.update_retry_attempt(&retry.job_id, false).await;
        }

        Ok(())
    }

    pub(in crate::sync::engine::scheduler) async fn mark_retry_success(&self, job_id: &str) {
        if let Err(e) = self.db.update_retry_attempt(job_id, true).await {
            warn!("Failed to mark retry success for job {}: {}", job_id, e);
        } else {
            info!("Job {} retry succeeded, retry record removed", job_id);
        }
    }
}
