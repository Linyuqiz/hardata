use crate::core::job::JobStatus;
use crate::sync::engine::job::SyncJob;

use super::super::core::SyncScheduler;
use super::super::infrastructure::config::JobRuntimeStatus;

impl SyncScheduler {
    pub fn get_job_status(&self, job_id: &str) -> Option<JobRuntimeStatus> {
        self.job_status_cache.get(job_id).map(|entry| entry.clone())
    }

    pub fn get_job_info(&self, job_id: &str) -> Option<SyncJob> {
        self.job_cache.get(job_id).map(|entry| entry.clone())
    }

    pub fn update_job_status(
        &self,
        job_id: &str,
        status: JobStatus,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) {
        if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
            entry.status = status;
            entry.progress = progress;
            entry.current_size = current_size;
            entry.total_size = total_size;
            entry.updated_at = chrono::Utc::now();
        }
    }

    pub fn for_each_job_status<F>(&self, mut f: F)
    where
        F: FnMut(&str, &JobRuntimeStatus),
    {
        for entry in self.job_status_cache.iter() {
            f(entry.key(), entry.value());
        }
    }
}
