use crate::core::job::JobStatus;
use crate::core::{Job, JobConfig, JobPath};
use crate::sync::engine::job::SyncJob;
use crate::util::error::Result;
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
                warn!("Failed to resolve job {} status: {}", job_id, e);
                None
            }
        }
    }

    pub(in crate::sync::engine::scheduler) async fn try_resolve_job_status(
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
                warn!("Failed to load retry state for job {}: {}", job_id, e);
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

    pub(in crate::sync::engine::scheduler) async fn persist_job_round_state(
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
                    "Skipped persisting round state for job {} because the row no longer exists",
                    job_id
                );
            }
            Err(e) => {
                warn!("Failed to persist round state for job {}: {}", job_id, e);
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

        // 动态计算 local_start：追踪每个 item 的全局位置
        // persisted_seen 从 persisted_offset 开始（DB 跳过的行数）
        // 每遇到一个 item，其全局位置 = persisted_seen + runtime_seen
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

#[cfg(test)]
mod tests {
    use super::super::super::core::SyncScheduler;
    use super::resolve_persisted_page_window;
    use crate::core::{Job, JobPath, JobStatus, JobType};
    use crate::sync::engine::scheduler::{JobRuntimeStatus, SchedulerConfig};
    use crate::sync::storage::db::Database;
    use chrono::{Duration, Utc};
    use sqlx::SqlitePool;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-job-status-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    async fn open_raw_pool(db_path: &str) -> SqlitePool {
        SqlitePool::connect(db_path).await.unwrap()
    }

    #[tokio::test]
    async fn load_jobs_snapshot_preserves_original_created_at_for_runtime_overlay() {
        let temp_dir = create_temp_dir("preserve-created-at");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let original_created_at = Utc::now() - Duration::hours(6);
        let runtime_created_at = Utc::now();
        let mut job = Job::new(
            "job-preserve-created-at".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Pending;
        job.created_at = original_created_at;
        job.updated_at = original_created_at;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 42,
                current_size: 10,
                total_size: 20,
                region: job.region.clone(),
                error_message: None,
                created_at: runtime_created_at,
                updated_at: runtime_created_at,
            },
        );

        scheduler.job_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                job.job_id.clone(),
                std::path::PathBuf::from(&job.source.path),
                job.dest.path.clone(),
                job.region.clone(),
            )
            .with_job_type(job.job_type),
        );

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(snapshot.created_at, original_created_at);
        assert_eq!(snapshot.updated_at, runtime_created_at);
        assert_eq!(snapshot.status, JobStatus::Syncing);
        assert_eq!(snapshot.progress, 42);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_snapshot_overlays_runtime_error_message() {
        let temp_dir = create_temp_dir("snapshot-runtime-error-message");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-snapshot-runtime-error".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = JobStatus::Failed;
        job.error_message = Some("persisted failure".to_string());
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 77,
                current_size: 77,
                total_size: 100,
                region: job.region.clone(),
                error_message: Some("runtime failure".to_string()),
                created_at: job.created_at,
                updated_at: Utc::now(),
            },
        );

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(snapshot.error_message.as_deref(), Some("runtime failure"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_merges_runtime_only_jobs_by_created_at() {
        let temp_dir = create_temp_dir("snapshot-page-runtime-only");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        let mut older = Job::new(
            "job-persisted-older".to_string(),
            JobPath {
                path: "/tmp/source-older.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-older.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        older.created_at = now - Duration::minutes(10);
        older.updated_at = older.created_at;
        db.save_job(&older).await.unwrap();

        let mut newer = Job::new(
            "job-persisted-newer".to_string(),
            JobPath {
                path: "/tmp/source-newer.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-newer.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        newer.created_at = now - Duration::minutes(5);
        newer.updated_at = newer.created_at;
        db.save_job(&newer).await.unwrap();

        let runtime_job_id = "job-runtime-only".to_string();
        scheduler.job_status_cache.insert(
            runtime_job_id.clone(),
            JobRuntimeStatus {
                job_id: runtime_job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "default".to_string(),
                error_message: None,
                created_at: now,
                updated_at: now,
            },
        );
        scheduler.job_cache.insert(
            runtime_job_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                runtime_job_id.clone(),
                std::path::PathBuf::from("/tmp/runtime-only.bin"),
                "dest-runtime.bin".to_string(),
                "default".to_string(),
            )
            .with_job_type(JobType::Sync),
        );

        let (total, jobs) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, runtime_job_id);
        assert_eq!(jobs[1].job_id, newer.job_id);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_skips_corrupted_rows_beyond_requested_range() {
        let temp_dir = create_temp_dir("snapshot-page-skip-far-corruption");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        let mut first = Job::new(
            "job-page-first".to_string(),
            JobPath {
                path: "/tmp/source-first.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-first.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        first.created_at = now;
        first.updated_at = now;
        db.save_job(&first).await.unwrap();

        let mut second = Job::new(
            "job-page-second".to_string(),
            JobPath {
                path: "/tmp/source-second.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-second.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        second.created_at = now - Duration::minutes(1);
        second.updated_at = second.created_at;
        db.save_job(&second).await.unwrap();

        let mut corrupted = Job::new(
            "job-page-corrupted".to_string(),
            JobPath {
                path: "/tmp/source-corrupted.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-corrupted.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        corrupted.created_at = now - Duration::minutes(2);
        corrupted.updated_at = corrupted.created_at;
        db.save_job(&corrupted).await.unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&corrupted.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        let (total, jobs) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, first.job_id);
        assert_eq!(jobs[1].job_id, second.job_id);

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_with_runtime_only_ignores_far_corrupted_rows() {
        let temp_dir = create_temp_dir("snapshot-page-runtime-only-far-corruption");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        let mut first = Job::new(
            "job-page-runtime-first".to_string(),
            JobPath {
                path: "/tmp/source-first.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-first.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        first.created_at = now - Duration::minutes(1);
        first.updated_at = first.created_at;
        db.save_job(&first).await.unwrap();

        let mut corrupted = Job::new(
            "job-page-runtime-corrupted".to_string(),
            JobPath {
                path: "/tmp/source-corrupted.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest-corrupted.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        corrupted.created_at = now - Duration::minutes(2);
        corrupted.updated_at = corrupted.created_at;
        db.save_job(&corrupted).await.unwrap();

        let runtime_job_id = "job-page-runtime-only".to_string();
        scheduler.job_status_cache.insert(
            runtime_job_id.clone(),
            JobRuntimeStatus {
                job_id: runtime_job_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "default".to_string(),
                error_message: None,
                created_at: now,
                updated_at: now,
            },
        );
        scheduler.job_cache.insert(
            runtime_job_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                runtime_job_id.clone(),
                std::path::PathBuf::from("/tmp/runtime-only.bin"),
                "dest-runtime.bin".to_string(),
                "default".to_string(),
            )
            .with_job_type(JobType::Sync),
        );

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&corrupted.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        let (total, jobs) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, runtime_job_id);
        assert_eq!(jobs[1].job_id, first.job_id);

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_is_stable_when_created_at_matches() {
        let temp_dir = create_temp_dir("snapshot-page-stable-order");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let created_at = Utc::now();
        for job_id in ["job-page-b", "job-page-a", "job-page-c"] {
            let mut job = Job::new(
                job_id.to_string(),
                JobPath {
                    path: format!("/tmp/{job_id}.bin"),
                    client_id: String::new(),
                },
                JobPath {
                    path: format!("dest/{job_id}.bin"),
                    client_id: String::new(),
                },
            )
            .with_job_type(JobType::Sync);
            job.created_at = created_at;
            job.updated_at = created_at;
            db.save_job(&job).await.unwrap();
        }

        let (_, first_page) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();
        let (_, second_page) = scheduler.load_jobs_snapshot_page(1, 2).await.unwrap();

        assert_eq!(
            first_page
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-page-a", "job-page-b"]
        );
        assert_eq!(
            second_page
                .iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-page-c"]
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_allows_failed_jobs_with_pending_retry() {
        let temp_dir = create_temp_dir("can-cancel-failed-with-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        db.save_retry("job-retry-cancellable", "temporary network error")
            .await
            .unwrap();

        assert!(
            scheduler
                .can_cancel_status("job-retry-cancellable", JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_resolved_job_statuses_prefers_runtime_status() {
        let temp_dir = create_temp_dir("resolved-statuses-runtime");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut job = Job::new(
            "job-resolved-runtime".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Failed;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 42,
                current_size: 42,
                total_size: 100,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: Utc::now(),
            },
        );

        let statuses = scheduler
            .load_resolved_job_statuses(&[job.job_id.clone()])
            .await
            .unwrap();

        assert_eq!(statuses.get(&job.job_id), Some(&JobStatus::Syncing));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_rejects_terminal_failed_jobs_without_retry() {
        let temp_dir = create_temp_dir("can-cancel-failed-terminal");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db).await.unwrap();

        assert!(
            !scheduler
                .can_cancel_status("job-retry-terminal", JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_allows_original_sync_failed_by_active_final_retry() {
        let temp_dir = create_temp_dir("can-cancel-original-final-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-original-final-retry".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Failed;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Failed;
        db.save_job(&final_job).await.unwrap();
        db.save_retry(&final_job_id, "temporary network error")
            .await
            .unwrap();

        assert!(
            scheduler
                .can_cancel_status(&original.job_id, JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_status_allows_original_sync_failed_by_active_final_transfer() {
        let temp_dir = create_temp_dir("can-cancel-original-active-final");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-original-active-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Failed;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(final_job_id, original.source.clone(), original.dest.clone())
            .with_job_type(JobType::Once);
        final_job.status = JobStatus::Pending;
        db.save_job(&final_job).await.unwrap();

        assert!(
            scheduler
                .can_cancel_status(&original.job_id, JobStatus::Failed)
                .await
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn can_cancel_job_from_snapshot_uses_loaded_final_status_index() {
        let temp_dir = create_temp_dir("can-cancel-snapshot-index");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut original = Job::new(
            "job-snapshot-index".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Failed;

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Failed;

        let snapshot_statuses = HashMap::from([
            (original.job_id.clone(), original.status),
            (final_job_id, final_job.status),
        ]);
        let retryable_job_ids = HashSet::from(["job-snapshot-index_final".to_string()]);

        assert!(scheduler.can_cancel_job_from_snapshot(
            &original,
            &snapshot_statuses,
            &retryable_job_ids
        ));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_retryable_job_ids_matches_database_active_retry_state() {
        let temp_dir = create_temp_dir("scheduler-load-retryable");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        db.save_retry("job-retryable", "temporary network error")
            .await
            .unwrap();

        let retryable = scheduler.load_retryable_job_ids().await.unwrap();

        assert!(retryable.contains("job-retryable"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_status_counts_overlays_runtime_without_loading_full_jobs() {
        let temp_dir = create_temp_dir("status-counts-overlay");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut persisted = Job::new(
            "job-status-counts".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        persisted.status = JobStatus::Failed;
        db.save_job(&persisted).await.unwrap();

        scheduler.job_status_cache.insert(
            persisted.job_id.clone(),
            JobRuntimeStatus {
                job_id: persisted.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 50,
                current_size: 500,
                total_size: 1000,
                region: persisted.region.clone(),
                error_message: None,
                created_at: persisted.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_status_cache.insert(
            "job-runtime-only".to_string(),
            JobRuntimeStatus {
                job_id: "job-runtime-only".to_string(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "local".to_string(),
                error_message: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );

        let counts = scheduler.load_job_status_counts().await.unwrap();

        assert_eq!(counts.get(&JobStatus::Syncing), Some(&1));
        assert_eq!(counts.get(&JobStatus::Pending), Some(&1));
        assert_eq!(counts.get(&JobStatus::Failed), None);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_public_job_status_counts_projects_internal_final_runtime() {
        let temp_dir = create_temp_dir("public-status-counts");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let mut public_job = Job::new(
            "job-public-counts".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        public_job.status = JobStatus::Failed;
        db.save_job(&public_job).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", public_job.job_id),
            public_job.source.clone(),
            public_job.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            public_job.job_id.clone(),
            JobRuntimeStatus {
                job_id: public_job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 10,
                current_size: 10,
                total_size: 100,
                region: public_job.region.clone(),
                error_message: None,
                created_at: public_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );
        scheduler.job_status_cache.insert(
            final_job.job_id.clone(),
            JobRuntimeStatus {
                job_id: final_job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 50,
                current_size: 50,
                total_size: 100,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let counts = scheduler.load_public_job_status_counts().await.unwrap();

        assert_eq!(counts.get(&JobStatus::Pending), None);
        assert_eq!(counts.get(&JobStatus::Syncing), Some(&1));
        assert_eq!(counts.get(&JobStatus::Failed), None);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_clamps_out_of_range_request_to_tail_page() {
        let temp_dir = create_temp_dir("snapshot-page-clamp-tail");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        for (job_id, minutes_ago) in [
            ("job-tail-first", 0),
            ("job-tail-second", 1),
            ("job-tail-third", 2),
        ] {
            let mut job = Job::new(
                job_id.to_string(),
                JobPath {
                    path: format!("/tmp/{job_id}.bin"),
                    client_id: String::new(),
                },
                JobPath {
                    path: format!("dest/{job_id}.bin"),
                    client_id: String::new(),
                },
            )
            .with_job_type(JobType::Sync);
            job.created_at = now - Duration::minutes(minutes_ago);
            job.updated_at = job.created_at;
            db.save_job(&job).await.unwrap();
        }

        let (total, jobs) = scheduler.load_jobs_snapshot_page(9, 2).await.unwrap();

        assert_eq!(total, 3);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, "job-tail-third");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn resolve_persisted_page_window_limits_reads_to_current_page_neighborhood() {
        assert_eq!(resolve_persisted_page_window(50_000, 100, 3), (49_997, 106));
        assert_eq!(resolve_persisted_page_window(2, 100, 10), (0, 120));
        assert_eq!(resolve_persisted_page_window(0, 100, 0), (0, 100));
    }

    #[tokio::test]
    async fn load_public_jobs_snapshot_page_excludes_internal_final_jobs() {
        let temp_dir = create_temp_dir("public-page-excludes-final");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();
        let mut original = Job::new(
            "job-public-page".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        original.status = JobStatus::Pending;
        original.created_at = now - Duration::seconds(1);
        original.updated_at = original.created_at;
        db.save_job(&original).await.unwrap();

        let mut internal_final = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        internal_final.status = JobStatus::Pending;
        internal_final.created_at = now;
        internal_final.updated_at = now;
        db.save_job(&internal_final).await.unwrap();

        scheduler.job_status_cache.insert(
            "job-runtime-public".to_string(),
            JobRuntimeStatus {
                job_id: "job-runtime-public".to_string(),
                status: JobStatus::Syncing,
                progress: 60,
                current_size: 60,
                total_size: 100,
                region: "local".to_string(),
                error_message: None,
                created_at: now + Duration::seconds(1),
                updated_at: now + Duration::seconds(1),
            },
        );
        scheduler.job_status_cache.insert(
            "job-runtime-public_final".to_string(),
            JobRuntimeStatus {
                job_id: "job-runtime-public_final".to_string(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "local".to_string(),
                error_message: None,
                created_at: now + Duration::seconds(2),
                updated_at: now + Duration::seconds(2),
            },
        );

        let (total, jobs) = scheduler
            .load_public_jobs_snapshot_page(0, 10)
            .await
            .unwrap();

        assert_eq!(total, 2);
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|job| !job.job_id.ends_with("_final")));
        assert!(jobs.iter().any(|job| job.job_id == original.job_id));
        assert!(jobs.iter().any(|job| job.job_id == "job-runtime-public"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_jobs_snapshot_page_correct_when_runtime_job_sorts_in_middle() {
        let temp_dir = create_temp_dir("snapshot-page-runtime-middle");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();

        let now = Utc::now();

        // 创建 3 个 persisted jobs: newest(-1m), older(-10m), oldest(-20m)
        let mut newest = Job::new(
            "job-p-newest".to_string(),
            JobPath {
                path: "/s/newest".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "d/newest".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        newest.created_at = now - Duration::minutes(1);
        newest.updated_at = newest.created_at;
        db.save_job(&newest).await.unwrap();

        let mut older = Job::new(
            "job-p-older".to_string(),
            JobPath {
                path: "/s/older".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "d/older".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        older.created_at = now - Duration::minutes(10);
        older.updated_at = older.created_at;
        db.save_job(&older).await.unwrap();

        let mut oldest = Job::new(
            "job-p-oldest".to_string(),
            JobPath {
                path: "/s/oldest".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "d/oldest".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        oldest.created_at = now - Duration::minutes(20);
        oldest.updated_at = oldest.created_at;
        db.save_job(&oldest).await.unwrap();

        // runtime_only job 排在中间 (-5m)
        let runtime_id = "job-runtime-middle".to_string();
        scheduler.job_status_cache.insert(
            runtime_id.clone(),
            JobRuntimeStatus {
                job_id: runtime_id.clone(),
                status: JobStatus::Pending,
                progress: 0,
                current_size: 0,
                total_size: 0,
                region: "default".to_string(),
                error_message: None,
                created_at: now - Duration::minutes(5),
                updated_at: now - Duration::minutes(5),
            },
        );
        scheduler.job_cache.insert(
            runtime_id.clone(),
            crate::sync::engine::job::SyncJob::new(
                runtime_id.clone(),
                std::path::PathBuf::from("/s/runtime"),
                "d/runtime".to_string(),
                "default".to_string(),
            )
            .with_job_type(JobType::Once),
        );

        // 全局排序: newest(-1m), runtime(-5m), older(-10m), oldest(-20m)
        // total = 4

        // page=0, limit=2 应返回 [newest, runtime]
        let (total, page0) = scheduler.load_jobs_snapshot_page(0, 2).await.unwrap();
        assert_eq!(total, 4);
        assert_eq!(page0.len(), 2);
        assert_eq!(page0[0].job_id, "job-p-newest");
        assert_eq!(page0[1].job_id, "job-runtime-middle");

        // page=1, limit=2 应返回 [older, oldest]
        let (total, page1) = scheduler.load_jobs_snapshot_page(1, 2).await.unwrap();
        assert_eq!(total, 4);
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].job_id, "job-p-older");
        assert_eq!(page1[1].job_id, "job-p-oldest");

        let _ = fs::remove_dir_all(temp_dir);
    }
}
