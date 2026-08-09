use crate::application::sync::engine::job::SyncJob;
use crate::domain::job::JobStatus;
use crate::domain::{Job, JobConfig, JobPath};
use crate::shared::error::Result;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use tracing::warn;

use super::super::core::SyncScheduler;
use super::super::infrastructure::config::JobRuntimeStatus;

impl SyncScheduler {
    pub fn get_job_status(&self, job_id: &str) -> Option<JobRuntimeStatus> {
        self.job_status_cache.get(job_id).map(|entry| entry.clone())
    }

    pub async fn resolve_job_status(&self, job_id: &str) -> Option<JobStatus> {
        match self.try_resolve_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => {
                warn!(operation = "job.status_resolve_failed", job_id = %job_id, error = %e, "job status resolution failed");
                None
            }
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn try_resolve_job_status(
        &self,
        job_id: &str,
    ) -> Result<Option<JobStatus>> {
        if let Some(status) = self.job_status_cache.get(job_id) {
            return Ok(Some(status.status));
        }

        Ok(self.load_job_snapshot(job_id).await?.map(|job| job.status))
    }

    pub async fn can_cancel_status(&self, job_id: &str, status: JobStatus) -> bool {
        match status {
            JobStatus::Pending | JobStatus::Syncing | JobStatus::Paused => true,
            JobStatus::Failed => {
                if self.job_has_pending_retry(job_id).await {
                    return true;
                }

                matches!(self.active_final_job_for(job_id).await, Ok(Some(_)))
            }
            JobStatus::Completed | JobStatus::Cancelled => false,
        }
    }

    pub async fn load_retryable_job_ids(&self) -> Result<HashSet<String>> {
        self.db.load_retryable_job_ids().await
    }

    pub async fn load_resolved_job_statuses(
        &self,
        job_ids: &[String],
    ) -> Result<HashMap<String, JobStatus>> {
        let mut statuses = self.db.load_job_statuses(job_ids).await?;
        for job_id in job_ids {
            if let Some(runtime) = self.job_status_cache.get(job_id) {
                statuses.insert(job_id.clone(), runtime.status);
            }
        }
        Ok(statuses)
    }

    pub fn can_cancel_job_from_snapshot(
        &self,
        job: &Job,
        snapshot_statuses: &HashMap<String, JobStatus>,
        retryable_job_ids: &HashSet<String>,
    ) -> bool {
        match job.status {
            JobStatus::Pending | JobStatus::Syncing | JobStatus::Paused => true,
            JobStatus::Failed => {
                if retryable_job_ids.contains(&job.job_id) {
                    return true;
                }

                if job.job_id.ends_with("_final") || !job.job_type.is_sync() {
                    return false;
                }

                let final_job_id = format!("{}_final", job.job_id);
                let Some(final_status) = snapshot_statuses.get(&final_job_id).copied() else {
                    return false;
                };

                final_status.is_active()
                    || (final_status == JobStatus::Failed
                        && retryable_job_ids.contains(&final_job_id))
            }
            JobStatus::Completed | JobStatus::Cancelled => false,
        }
    }

    async fn job_has_pending_retry(&self, job_id: &str) -> bool {
        match self.db.get_retry(job_id).await {
            Ok(Some(retry)) => retry.retry_count < retry.max_retries,
            Ok(None) => false,
            Err(e) => {
                warn!(operation = "job.retry_state_load_failed", job_id = %job_id, error = %e, "job retry state load failed");
                false
            }
        }
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
        error_message: Option<String>,
    ) {
        if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
            entry.status = status;
            entry.progress = progress;
            entry.current_size = current_size;
            entry.total_size = total_size;
            entry.error_message = error_message;
            entry.updated_at = chrono::Utc::now();
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn persist_job_round_state(
        &self,
        job_id: &str,
        round_id: i64,
        is_last_round: bool,
    ) {
        match self
            .db
            .update_job_round_state(job_id, round_id, is_last_round)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    operation = "job.round_state_persist_skipped",
                    job_id = %job_id,
                    reason = "persisted_row_missing",
                    "job round state persistence skipped"
                );
            }
            Err(e) => {
                warn!(operation = "job.round_state_persist_failed", job_id = %job_id, error = %e, "job round state persistence failed");
            }
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

    pub async fn load_job_status_counts(&self) -> Result<HashMap<JobStatus, i64>> {
        self.load_job_status_counts_filtered(false).await
    }

    pub async fn load_public_job_status_counts(&self) -> Result<HashMap<JobStatus, i64>> {
        self.load_job_status_counts_filtered(true).await
    }

    async fn load_job_status_counts_filtered(
        &self,
        public_only: bool,
    ) -> Result<HashMap<JobStatus, i64>> {
        let mut counts = if public_only {
            self.db.count_public_jobs_by_status().await?
        } else {
            self.db.count_jobs_by_status().await?
        };
        let runtime_statuses = if public_only {
            let mut projected_public_statuses = HashMap::new();
            self.for_each_job_status(|job_id, runtime| {
                if let Some(public_job_id) = job_id.strip_suffix("_final") {
                    projected_public_statuses.insert(public_job_id.to_string(), runtime.status);
                    return;
                }

                projected_public_statuses
                    .entry(job_id.to_string())
                    .or_insert(runtime.status);
            });
            projected_public_statuses.into_iter().collect::<Vec<_>>()
        } else {
            let mut runtime_statuses = Vec::new();
            self.for_each_job_status(|job_id, runtime| {
                runtime_statuses.push((job_id.to_string(), runtime.status));
            });
            runtime_statuses
        };
        let job_ids = runtime_statuses
            .iter()
            .map(|(job_id, _)| job_id.clone())
            .collect::<Vec<_>>();
        let persisted_statuses = self.db.load_job_statuses(&job_ids).await?;

        for (job_id, runtime_status) in runtime_statuses {
            if let Some(persisted_status) = persisted_statuses.get(&job_id).copied() {
                adjust_status_count(&mut counts, persisted_status, -1);
            }
            adjust_status_count(&mut counts, runtime_status, 1);
        }

        Ok(counts)
    }

    pub async fn load_job_snapshot(&self, job_id: &str) -> Result<Option<Job>> {
        let runtime = self.job_status_cache.get(job_id).map(|entry| entry.clone());
        let sync_job = self.job_cache.get(job_id).map(|job| job.clone());
        let persisted = self.db.load_job(job_id).await?;

        match (persisted, runtime) {
            (Some(mut job), Some(runtime)) => {
                overlay_job(&mut job, &runtime, sync_job.as_ref());
                Ok(Some(job))
            }
            (Some(job), None) => Ok(Some(job)),
            (None, Some(runtime)) => Ok(Some(build_runtime_job(&runtime, sync_job.as_ref()))),
            (None, None) => Ok(None),
        }
    }

    pub async fn load_jobs_snapshot(&self) -> Result<Vec<Job>> {
        let mut jobs = self.db.load_all_jobs().await?;
        let mut index_by_id: HashMap<String, usize> = jobs
            .iter()
            .enumerate()
            .map(|(idx, job)| (job.job_id.clone(), idx))
            .collect();

        for entry in self.job_status_cache.iter() {
            let job_id = entry.key().clone();
            let runtime = entry.value().clone();
            let sync_job = self.job_cache.get(&job_id).map(|job| job.clone());

            if let Some(idx) = index_by_id.get(&job_id).copied() {
                overlay_job(&mut jobs[idx], &runtime, sync_job.as_ref());
                continue;
            }

            let job = build_runtime_job(&runtime, sync_job.as_ref());
            index_by_id.insert(job_id, jobs.len());
            jobs.push(job);
        }

        sort_jobs_for_listing(&mut jobs);
        Ok(jobs)
    }

    pub async fn load_jobs_snapshot_page(
        &self,
        page: usize,
        limit: usize,
    ) -> Result<(usize, Vec<Job>)> {
        self.load_jobs_snapshot_page_filtered(page, limit, false)
            .await
    }

    pub async fn load_public_jobs_snapshot_page(
        &self,
        page: usize,
        limit: usize,
    ) -> Result<(usize, Vec<Job>)> {
        self.load_jobs_snapshot_page_filtered(page, limit, true)
            .await
    }

    async fn load_jobs_snapshot_page_filtered(
        &self,
        page: usize,
        limit: usize,
        public_only: bool,
    ) -> Result<(usize, Vec<Job>)> {
        #[derive(Clone)]
        struct RuntimeOnlyEntry {
            job: Job,
        }

        #[derive(Clone)]
        enum PageEntry {
            Persisted(Box<Job>),
            RuntimeOnly(Box<RuntimeOnlyEntry>),
        }

        let mut runtime_overlays = HashMap::new();
        let mut runtime_only_jobs = Vec::new();

        let runtime_entries: Vec<(String, JobRuntimeStatus, Option<SyncJob>)> = self
            .job_status_cache
            .iter()
            .map(|entry| {
                let job_id = entry.key().clone();
                let runtime = entry.value().clone();
                let sync_job = self.job_cache.get(&job_id).map(|job| job.clone());
                (job_id, runtime, sync_job)
            })
            .filter(|(job_id, _, _)| !public_only || is_public_job_id(job_id))
            .collect();

        let runtime_job_ids = runtime_entries
            .iter()
            .map(|(job_id, _, _)| job_id.clone())
            .collect::<Vec<_>>();
        let persisted_runtime_statuses = self.db.load_job_statuses(&runtime_job_ids).await?;

        for (job_id, runtime, sync_job) in runtime_entries {
            if persisted_runtime_statuses.contains_key(&job_id) {
                runtime_overlays.insert(job_id, (runtime, sync_job));
            } else {
                runtime_only_jobs.push(RuntimeOnlyEntry {
                    job: build_runtime_job(&runtime, sync_job.as_ref()),
                });
            }
        }

        runtime_only_jobs.sort_by(|left, right| compare_job_listing(&left.job, &right.job));

        let persisted_total = if public_only {
            self.db.count_public_jobs().await?
        } else {
            self.db.count_jobs().await?
        };
        let total = persisted_total.saturating_add(runtime_only_jobs.len());
        let page_size = limit.max(1);
        let resolved_page = resolve_job_listing_page(total, page, page_size);
        let start = resolved_page.saturating_mul(page_size).min(total);
        let end = start.saturating_add(page_size).min(total);

        if start >= end {
            return Ok((total, Vec::new()));
        }

        let (persisted_offset, persisted_limit) =
            resolve_persisted_page_window(start, page_size, runtime_only_jobs.len());

        let persisted_jobs_raw = if public_only {
            self.db
                .load_public_job_page(persisted_limit, persisted_offset)
                .await?
        } else {
            self.db
                .load_job_page(persisted_limit, persisted_offset)
                .await?
        };
        let mut page_entries = persisted_jobs_raw
            .into_iter()
            .map(|job| PageEntry::Persisted(Box::new(job)))
            .chain(
                runtime_only_jobs
                    .into_iter()
                    .map(|entry| PageEntry::RuntimeOnly(Box::new(entry))),
            )
            .collect::<Vec<_>>();
        page_entries.sort_by(|left, right| match (left, right) {
            (PageEntry::Persisted(left), PageEntry::Persisted(right)) => compare_listing_keys(
                &left.created_at,
                &left.job_id,
                &right.created_at,
                &right.job_id,
            ),
            (PageEntry::Persisted(left), PageEntry::RuntimeOnly(right)) => compare_listing_keys(
                &left.created_at,
                &left.job_id,
                &right.job.created_at,
                &right.job.job_id,
            ),
            (PageEntry::RuntimeOnly(left), PageEntry::Persisted(right)) => compare_listing_keys(
                &left.job.created_at,
                &left.job.job_id,
                &right.created_at,
                &right.job_id,
            ),
            (PageEntry::RuntimeOnly(left), PageEntry::RuntimeOnly(right)) => {
                compare_job_listing(&left.job, &right.job)
            }
        });

        let local_start = {
            let mut persisted_seen = persisted_offset;
            let mut runtime_seen: usize = 0;
            let mut skip = 0;
            for entry in &page_entries {
                if persisted_seen + runtime_seen >= start {
                    break;
                }
                match entry {
                    PageEntry::Persisted(_) => persisted_seen += 1,
                    PageEntry::RuntimeOnly(_) => runtime_seen += 1,
                }
                skip += 1;
            }
            skip
        };

        let jobs = page_entries
            .into_iter()
            .skip(local_start)
            .take(end - start)
            .map(|entry| match entry {
                PageEntry::Persisted(mut job) => {
                    if let Some((runtime, sync_job)) = runtime_overlays.get(&job.job_id) {
                        overlay_job(&mut job, runtime, sync_job.as_ref());
                    }
                    Ok(*job)
                }
                PageEntry::RuntimeOnly(entry) => Ok(entry.job),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((total, jobs))
    }
}

fn is_public_job_id(job_id: &str) -> bool {
    !job_id.ends_with("_final")
}

fn resolve_job_listing_page(total: usize, page: usize, limit: usize) -> usize {
    if total == 0 {
        0
    } else {
        page.min(total.saturating_sub(1) / limit.max(1))
    }
}

fn resolve_persisted_page_window(
    start: usize,
    page_size: usize,
    runtime_only_count: usize,
) -> (usize, usize) {
    if runtime_only_count == 0 {
        return (start, page_size);
    }
    let persisted_offset = start.saturating_sub(runtime_only_count);
    let persisted_limit = page_size.saturating_add(runtime_only_count.saturating_mul(2));
    (persisted_offset, persisted_limit)
}

fn sort_jobs_for_listing(jobs: &mut [Job]) {
    jobs.sort_by(compare_job_listing);
}

fn compare_job_listing(left: &Job, right: &Job) -> Ordering {
    compare_listing_keys(
        &left.created_at,
        &left.job_id,
        &right.created_at,
        &right.job_id,
    )
}

fn compare_listing_keys(
    left_created_at: &chrono::DateTime<chrono::Utc>,
    left_job_id: &str,
    right_created_at: &chrono::DateTime<chrono::Utc>,
    right_job_id: &str,
) -> Ordering {
    right_created_at
        .cmp(left_created_at)
        .then_with(|| left_job_id.cmp(right_job_id))
}

fn overlay_job(job: &mut Job, runtime: &JobRuntimeStatus, sync_job: Option<&SyncJob>) {
    job.status = runtime.status;
    job.progress = runtime.progress;
    job.current_size = runtime.current_size;
    job.total_size = runtime.total_size;
    job.error_message = runtime.error_message.clone();
    job.region = runtime.region.clone();
    job.updated_at = runtime.updated_at;

    if let Some(sync_job) = sync_job {
        job.source.path = sync_job.source.to_string_lossy().to_string();
        job.dest.path = sync_job.dest.clone();
        job.priority = sync_job.priority;
        job.round_id = sync_job.round_id;
        job.is_last_round = sync_job.is_last_round;
        job.job_type = sync_job.job_type;
        job.exclude_regex = sync_job.exclude_regex.clone();
        job.include_regex = sync_job.include_regex.clone();
    }
}

fn build_runtime_job(runtime: &JobRuntimeStatus, sync_job: Option<&SyncJob>) -> Job {
    let source_path = sync_job
        .map(|job| job.source.to_string_lossy().to_string())
        .unwrap_or_default();
    let dest_path = sync_job.map(|job| job.dest.clone()).unwrap_or_default();

    Job {
        job_id: runtime.job_id.clone(),
        region: runtime.region.clone(),
        source: JobPath {
            path: source_path,
            client_id: String::new(),
        },
        dest: JobPath {
            path: dest_path,
            client_id: String::new(),
        },
        status: runtime.status,
        job_type: sync_job.map(|job| job.job_type).unwrap_or_default(),
        exclude_regex: sync_job
            .map(|job| job.exclude_regex.clone())
            .unwrap_or_default(),
        include_regex: sync_job
            .map(|job| job.include_regex.clone())
            .unwrap_or_default(),
        priority: sync_job.map(|job| job.priority).unwrap_or_default(),
        round_id: sync_job.map(|job| job.round_id).unwrap_or_default(),
        is_last_round: sync_job.map(|job| job.is_last_round).unwrap_or_default(),
        options: JobConfig::default(),
        progress: runtime.progress,
        current_size: runtime.current_size,
        total_size: runtime.total_size,
        error_message: runtime.error_message.clone(),
        created_at: runtime.created_at,
        updated_at: runtime.updated_at,
    }
}

fn adjust_status_count(counts: &mut HashMap<JobStatus, i64>, status: JobStatus, delta: i64) {
    let entry = counts.entry(status).or_insert(0);
    *entry += delta;
    if *entry == 0 {
        counts.remove(&status);
    }
}
