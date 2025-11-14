use crate::core::job::JobStatus;
use crate::core::{Job, JobConfig, JobPath, JobType};
use crate::sync::engine::job::SyncJob;
use crate::util::error::{HarDataError, Result};
use std::path::PathBuf;
use tracing::{info, warn};

use super::super::core::SyncScheduler;
use super::super::infrastructure::config::JobRuntimeStatus;

impl SyncScheduler {
    pub async fn submit_job(&self, job: SyncJob) -> Result<()> {
        self.submit_job_internal(job, true).await
    }

    pub(in crate::sync::engine::scheduler) async fn submit_job_internal(
        &self,
        job: SyncJob,
        persist: bool,
    ) -> Result<()> {
        info!(
            "Submitting job {} to queue (type={}, persist={}, priority={})",
            job.job_id,
            job.job_type.as_str(),
            persist,
            job.priority
        );

        if persist {
            let db_job = self.sync_job_to_db_job(&job);
            if let Err(e) = self.db.save_job(&db_job).await {
                warn!("Failed to persist job {} to database: {}", job.job_id, e);
            }
        }

        self.job_cache.insert(job.job_id.clone(), job.clone());

        let now = chrono::Utc::now();
        self.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: job.region.clone(),
                error_message: None,
                created_at: now,
                updated_at: now,
            },
        );

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
            exclude_regex: Vec::new(),
            include_regex: Vec::new(),
            priority: sync_job.priority,
            options: JobConfig::default(),
            progress: 0,
            current_size: 0,
            total_size: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub async fn finalize_job(&self, job_id: &str) -> Result<()> {
        info!("Finalizing job {}", job_id);

        let original_job = self
            .job_cache
            .get(job_id)
            .ok_or_else(|| HarDataError::Unknown(format!("Job {} not found", job_id)))?
            .clone();

        let action = {
            if let Some(mut status) = self.job_status_cache.get_mut(job_id) {
                match status.status {
                    JobStatus::Syncing => {
                        info!(
                            "Job {} is running, marking as cancelled for finalize",
                            job_id
                        );
                        status.status = JobStatus::Cancelled;
                        Some("wait_syncing")
                    }
                    JobStatus::Pending => {
                        info!("Job {} is pending, will remove from queue", job_id);
                        Some("remove_pending")
                    }
                    _ => {
                        info!(
                            "Job {} status: {:?}, proceeding with finalize",
                            job_id, status.status
                        );
                        None
                    }
                }
            } else {
                None
            }
        };

        match action {
            Some("wait_syncing") => {
                for _ in 0..50 {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    if let Some(s) = self.job_status_cache.get(job_id) {
                        if s.status != JobStatus::Syncing {
                            break;
                        }
                    }
                }

                if let Some(status) = self.job_status_cache.get(job_id) {
                    if status.status == JobStatus::Syncing {
                        return Err(HarDataError::Unknown(format!(
                            "Job {} is still syncing after 5s timeout, cannot finalize safely",
                            job_id
                        )));
                    }
                }
            }
            Some("remove_pending") => {
                self.job_queue.retain(|j| j.job_id != job_id).await;
            }
            _ => {}
        }

        let final_job = SyncJob::new(
            format!("{}_final", job_id),
            original_job.source.clone(),
            original_job.dest.clone(),
            original_job.region.clone(),
        )
        .with_job_type(JobType::Once)
        .with_priority(original_job.priority + 100);

        info!(
            "Created final transfer job: {} (source: {:?}, dest: {})",
            final_job.job_id, final_job.source, final_job.dest
        );

        self.submit_job(final_job).await?;
        Ok(())
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        info!("Cancelling job {}", job_id);

        let db_update_info = {
            if let Some(mut status) = self.job_status_cache.get_mut(job_id) {
                match status.status {
                    JobStatus::Pending => {
                        status.status = JobStatus::Cancelled;
                        status.updated_at = chrono::Utc::now();
                        info!("Job {} cancelled (was pending)", job_id);
                    }
                    JobStatus::Syncing => {
                        status.status = JobStatus::Cancelled;
                        status.updated_at = chrono::Utc::now();
                        info!("Job {} marked as cancelled (was syncing)", job_id);
                    }
                    JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
                        return Err(HarDataError::Unknown(format!(
                            "Job {} already finished with status {:?}",
                            job_id, status.status
                        )));
                    }
                    _ => {
                        status.status = JobStatus::Cancelled;
                        status.updated_at = chrono::Utc::now();
                    }
                }
                Some((status.progress, status.current_size, status.total_size))
            } else {
                None
            }
        };

        if let Some((progress, current_size, total_size)) = db_update_info {
            self.job_queue.retain(|j| j.job_id != job_id).await;

            let _ = self
                .db
                .update_job_status(
                    job_id,
                    JobStatus::Cancelled,
                    progress,
                    current_size,
                    total_size,
                )
                .await;

            Ok(())
        } else {
            Err(HarDataError::Unknown(format!("Job {} not found", job_id)))
        }
    }

    pub(in crate::sync::engine::scheduler) async fn recover_pending_jobs(&self) -> Result<()> {
        info!("Recovering pending jobs from database...");

        let jobs = self.db.load_all_jobs().await?;
        let mut recovered_count = 0;

        for job in jobs {
            match job.status {
                JobStatus::Pending | JobStatus::Syncing => {
                    info!(
                        "Recovering job {}: {} -> {} (status: {:?})",
                        job.job_id, job.source.path, job.dest.path, job.status
                    );

                    let sync_job = SyncJob::new(
                        job.job_id.clone(),
                        PathBuf::from(&job.source.path),
                        job.dest.path.clone(),
                        job.region.clone(),
                    )
                    .with_priority(job.priority)
                    .with_job_type(job.job_type);

                    if let Err(e) = self.submit_job_internal(sync_job, false).await {
                        warn!("Failed to recover job {}: {}", job.job_id, e);
                    } else {
                        recovered_count += 1;
                    }
                }
                _ => {}
            }
        }

        info!("Recovered {} pending jobs", recovered_count);
        Ok(())
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
}
