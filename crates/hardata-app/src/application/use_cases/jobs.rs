use crate::application::sync::engine::job::SyncJob;
use crate::application::sync::engine::scheduler::{JobRuntimeStatus, SyncScheduler};
use crate::domain::{Job, JobStatus};
use crate::shared::error::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyReservation {
    pub request_fingerprint: String,
    pub job_id: String,
}

/// Job-management use cases exposed to inbound adapters.
#[async_trait]
pub trait JobUseCases: Send + Sync {
    fn get_job_info(&self, job_id: &str) -> Option<SyncJob>;
    fn get_job_status(&self, job_id: &str) -> Option<JobRuntimeStatus>;
    fn can_cancel_job_from_snapshot(
        &self,
        job: &Job,
        snapshot_statuses: &HashMap<String, JobStatus>,
        retryable_job_ids: &HashSet<String>,
    ) -> bool;

    async fn load_public_job_status_counts(&self) -> Result<HashMap<JobStatus, i64>>;
    async fn load_public_jobs_snapshot_page(
        &self,
        page: usize,
        limit: usize,
    ) -> Result<(usize, Vec<Job>)>;
    async fn load_retryable_job_ids(&self) -> Result<HashSet<String>>;
    async fn load_resolved_job_statuses(
        &self,
        job_ids: &[String],
    ) -> Result<HashMap<String, JobStatus>>;
    async fn load_job_snapshot(&self, job_id: &str) -> Result<Option<Job>>;
    async fn reserve_create_job_idempotency_key(
        &self,
        idempotency_key: &str,
        request_fingerprint: &str,
        job_id: &str,
    ) -> Result<IdempotencyReservation>;

    async fn submit_job(&self, job: SyncJob) -> Result<()>;
    async fn finalize_job(&self, job_id: &str) -> Result<()>;
    async fn cancel_job(&self, job_id: &str) -> Result<()>;
}

#[async_trait]
impl JobUseCases for SyncScheduler {
    fn get_job_info(&self, job_id: &str) -> Option<SyncJob> {
        SyncScheduler::get_job_info(self, job_id)
    }

    fn get_job_status(&self, job_id: &str) -> Option<JobRuntimeStatus> {
        SyncScheduler::get_job_status(self, job_id)
    }

    fn can_cancel_job_from_snapshot(
        &self,
        job: &Job,
        snapshot_statuses: &HashMap<String, JobStatus>,
        retryable_job_ids: &HashSet<String>,
    ) -> bool {
        SyncScheduler::can_cancel_job_from_snapshot(self, job, snapshot_statuses, retryable_job_ids)
    }

    async fn load_public_job_status_counts(&self) -> Result<HashMap<JobStatus, i64>> {
        SyncScheduler::load_public_job_status_counts(self).await
    }

    async fn load_public_jobs_snapshot_page(
        &self,
        page: usize,
        limit: usize,
    ) -> Result<(usize, Vec<Job>)> {
        SyncScheduler::load_public_jobs_snapshot_page(self, page, limit).await
    }

    async fn load_retryable_job_ids(&self) -> Result<HashSet<String>> {
        SyncScheduler::load_retryable_job_ids(self).await
    }

    async fn load_resolved_job_statuses(
        &self,
        job_ids: &[String],
    ) -> Result<HashMap<String, JobStatus>> {
        SyncScheduler::load_resolved_job_statuses(self, job_ids).await
    }

    async fn load_job_snapshot(&self, job_id: &str) -> Result<Option<Job>> {
        SyncScheduler::load_job_snapshot(self, job_id).await
    }

    async fn reserve_create_job_idempotency_key(
        &self,
        idempotency_key: &str,
        request_fingerprint: &str,
        job_id: &str,
    ) -> Result<IdempotencyReservation> {
        let record = SyncScheduler::reserve_create_job_idempotency_key(
            self,
            idempotency_key,
            request_fingerprint,
            job_id,
        )
        .await?;
        Ok(IdempotencyReservation {
            request_fingerprint: record.request_fingerprint,
            job_id: record.job_id,
        })
    }

    async fn submit_job(&self, job: SyncJob) -> Result<()> {
        SyncScheduler::submit_job(self, job).await
    }

    async fn finalize_job(&self, job_id: &str) -> Result<()> {
        SyncScheduler::finalize_job(self, job_id).await
    }

    async fn cancel_job(&self, job_id: &str) -> Result<()> {
        SyncScheduler::cancel_job(self, job_id).await
    }
}
