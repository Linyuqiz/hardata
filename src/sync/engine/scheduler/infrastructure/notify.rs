use crate::core::job::JobStatus;
use std::collections::HashSet;
use tracing::{info, warn};

use super::super::core::SyncScheduler;
use super::super::retry::ErrorCategory;

pub(in crate::sync::engine::scheduler) fn is_cancelled_error(error: &str) -> bool {
    let error_lower = error.to_lowercase();
    error_lower.contains("cancelled by user") || error_lower == "job cancelled"
}

impl SyncScheduler {
    pub(in crate::sync::engine::scheduler) async fn notify_job_pending(&self, job_id: &str) {
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

    pub(in crate::sync::engine::scheduler) async fn notify_job_started(&self, job_id: &str) {
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

    pub(in crate::sync::engine::scheduler) async fn notify_job_cancelled(&self, job_id: &str) {
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
                        "Skip 'cancelled' status update for inactive job {} because snapshot loading failed: {}",
                        job_id, e
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
                "Failed to delete retry record for cancelled job {}: {}",
                job_id, e
            );
        }

        if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
            warn!(
                "Failed to clear transfer states for cancelled job {}: {}",
                job_id, e
            );
        }

        self.cleanup_job_tmp_artifacts(job_id).await;

        self.cleanup_runtime_job(job_id);
    }

    pub(in crate::sync::engine::scheduler) async fn notify_job_completed(&self, job_id: &str) {
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
            warn!("Failed to clear transfer states for job {}: {}", job_id, e);
        }

        if let Err(e) = self
            .transfer_manager_pool
            .clear_job_tmp_write_paths(job_id)
            .await
        {
            warn!("Failed to clear tmp write paths for job {}: {}", job_id, e);
        }

        self.mark_retry_success(job_id).await;

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_completed(job_id);
        }

        self.cleanup_runtime_job(job_id);
    }

    pub(in crate::sync::engine::scheduler) async fn notify_job_failed(
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
                warn!("Failed to save retry record for job {}: {}", job_id, e);
                retry_exhausted = true;
            } else {
                info!(
                    "Job {} marked for retry ({:?}): {}",
                    job_id, category, error
                );
                retry_exhausted = match self.db.get_retry(job_id).await {
                    Ok(Some(retry)) => retry.retry_count >= retry.max_retries,
                    Ok(None) => {
                        warn!(
                            "Retry record for job {} disappeared after save, treating failure as terminal",
                            job_id
                        );
                        true
                    }
                    Err(e) => {
                        warn!(
                            "Failed to reload retry record for job {} after save: {}, treating failure as terminal",
                            job_id, e
                        );
                        true
                    }
                };
            }
        } else {
            info!(
                "Job {} failed with non-retryable error ({:?}): {}",
                job_id, category, error
            );
        }

        if !retryable || retry_exhausted {
            if let Err(e) = self.db.delete_retry(job_id).await {
                warn!(
                    "Failed to delete retry record for terminally failed job {}: {}",
                    job_id, e
                );
            }

            if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
                warn!(
                    "Failed to clear transfer states for terminally failed job {}: {}",
                    job_id, e
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

    async fn should_skip_runtime_free_status_update(&self, job_id: &str, transition: &str) -> bool {
        if self.job_status_cache.contains_key(job_id) {
            return false;
        }

        let snapshot_status = match self.try_resolve_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => {
                warn!(
                    "Skip '{}' status update for inactive job {} because status resolution failed: {}",
                    transition, job_id, e
                );
                return true;
            }
        };
        let should_skip = matches!(
            snapshot_status,
            Some(JobStatus::Cancelled | JobStatus::Completed | JobStatus::Failed)
        );

        if should_skip {
            info!(
                "Skip '{}' status update for inactive job {} with snapshot status {:?}",
                transition, job_id, snapshot_status
            );
        }

        should_skip
    }

    async fn update_job_status_in_db(
        &self,
        job_id: &str,
        status: JobStatus,
        progress: u8,
        current_size: u64,
        total_size: u64,
        error_message: Option<&str>,
    ) -> bool {
        let updated = match self
            .db
            .update_job_status(
                job_id,
                status,
                progress,
                current_size,
                total_size,
                error_message,
            )
            .await
        {
            Ok(updated) => updated,
            Err(e) => {
                warn!("Failed to update job {} status in database: {}", job_id, e);
                return false;
            }
        };

        if !updated {
            warn!(
                "Skipped updating job {} status in database because the row no longer exists",
                job_id
            );
            return false;
        }

        true
    }

    async fn load_inactive_job_snapshot_metrics(
        &self,
        job_id: &str,
        transition: &str,
    ) -> Option<(u8, u64, u64)> {
        match self.load_job_snapshot(job_id).await {
            Ok(Some(job)) => Some((job.progress, job.current_size, job.total_size)),
            Ok(None) => Some((0, 0, 0)),
            Err(e) => {
                warn!(
                    "Skip '{}' status update for inactive job {} because snapshot loading failed: {}",
                    transition, job_id, e
                );
                None
            }
        }
    }

    async fn load_inactive_job_completion_sizes(
        &self,
        job_id: &str,
        transition: &str,
    ) -> Option<(u64, u64)> {
        match self.load_job_snapshot(job_id).await {
            Ok(Some(job)) => Some((job.total_size, job.total_size)),
            Ok(None) => Some((0, 0)),
            Err(e) => {
                warn!(
                    "Skip '{}' status update for inactive job {} because snapshot loading failed: {}",
                    transition, job_id, e
                );
                None
            }
        }
    }

    pub(in crate::sync::engine::scheduler) fn cleanup_runtime_job(&self, job_id: &str) {
        self.job_cache.remove(job_id);
        self.job_status_cache.remove(job_id);
        self.synced_files_cache.remove(job_id);
        self.size_freezers.remove(job_id);
    }

    pub(in crate::sync::engine::scheduler) async fn cleanup_job_tmp_artifacts(&self, job_id: &str) {
        let mut tmp_paths: HashSet<String> = self
            .transfer_manager_pool
            .job_tmp_write_paths(job_id)
            .into_iter()
            .collect();

        match self.db.load_tmp_transfer_paths_by_job(job_id).await {
            Ok(paths) => {
                tmp_paths.extend(paths);
            }
            Err(e) => {
                warn!(
                    "Failed to load tmp write paths for terminal job {}: {}",
                    job_id, e
                );
            }
        }

        for path in tmp_paths {
            let should_unregister = match tokio::fs::remove_file(&path).await {
                Ok(_) => {
                    info!("Removed tmp file for terminal job {}: {}", job_id, path);
                    true
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                Err(e) => {
                    warn!(
                        "Failed to remove tmp file for terminal job {} path {}: {}",
                        job_id, path, e
                    );
                    false
                }
            };

            if should_unregister {
                if let Err(e) = self
                    .transfer_manager_pool
                    .unregister_tmp_write_path(job_id, &path)
                    .await
                {
                    warn!(
                        "Failed to unregister tmp write path for job {} path {} during cleanup: {}",
                        job_id, path, e
                    );
                }
            }
        }
    }

    pub(in crate::sync::engine::scheduler) async fn update_original_sync_job_status_from_final(
        &self,
        final_job_id: &str,
        status: JobStatus,
        progress: u8,
        current_size: u64,
        total_size: u64,
        error_message: Option<&str>,
    ) {
        let Some(original_job_id) = final_job_id.strip_suffix("_final") else {
            return;
        };

        let job = match self.load_job_snapshot(original_job_id).await {
            Ok(Some(job)) => job,
            Ok(None) => return,
            Err(e) => {
                warn!(
                    "Skip updating original sync job {} from final job {} because snapshot loading failed: {}",
                    original_job_id, final_job_id, e
                );
                return;
            }
        };

        if !job.job_type.is_sync() {
            return;
        }

        let final_round_state = if let Some(final_job) = self.job_cache.get(final_job_id) {
            Some((final_job.round_id, final_job.is_last_round))
        } else {
            match self.load_job_snapshot(final_job_id).await {
                Ok(Some(final_job)) => Some((
                    final_job
                        .round_id
                        .max(i64::from(final_job.job_id.ends_with("_final"))),
                    final_job.is_last_round || final_job.job_id.ends_with("_final"),
                )),
                Ok(None) => None,
                Err(e) => {
                    warn!(
                        "Skip syncing round state from final job {} to original job {} because snapshot loading failed: {}",
                        final_job_id, original_job_id, e
                    );
                    None
                }
            }
        };

        if let Some(mut entry) = self.job_status_cache.get_mut(original_job_id) {
            entry.status = status;
            entry.progress = progress;
            entry.current_size = current_size;
            entry.total_size = total_size;
            entry.error_message = error_message.map(str::to_string);
            entry.updated_at = chrono::Utc::now();
        }
        if let Some((round_id, is_last_round)) = final_round_state {
            if let Some(mut original_job) = self.job_cache.get_mut(original_job_id) {
                original_job.restore_round_state(round_id, is_last_round);
            }
            self.persist_job_round_state(original_job_id, round_id, is_last_round)
                .await;
        }

        self.update_job_status_in_db(
            original_job_id,
            status,
            progress,
            current_size,
            total_size,
            error_message,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::core::SyncScheduler;
    use crate::core::FileTransferState;
    use crate::core::{Job, JobPath, JobStatus, JobType};
    use crate::sync::engine::scheduler::retry::ErrorCategory;
    use crate::sync::engine::scheduler::SchedulerConfig;
    use crate::sync::storage::db::Database;
    use sqlx::{Row, SqlitePool};
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-notify-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    async fn create_scheduler(
        label: &str,
    ) -> (std::path::PathBuf, Arc<Database>, Arc<SyncScheduler>) {
        let temp_dir = create_temp_dir(label);
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();
        (temp_dir, db, scheduler)
    }

    async fn open_raw_pool(db_path: &str) -> SqlitePool {
        SqlitePool::connect(db_path).await.unwrap()
    }

    fn cancelled_job(job_id: &str) -> Job {
        let mut job = Job::new(
            job_id.to_string(),
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
        job.status = JobStatus::Cancelled;
        job
    }

    #[tokio::test]
    async fn notify_job_started_does_not_revive_cancelled_snapshot_without_runtime() {
        let (temp_dir, db, scheduler) = create_scheduler("started-no-revive").await;
        let job = cancelled_job("job-notify-started-cancelled");
        db.save_job(&job).await.unwrap();

        scheduler.notify_job_started(&job.job_id).await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_does_not_revive_cancelled_snapshot_without_runtime() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-no-revive").await;
        let job = cancelled_job("job-notify-completed-cancelled");
        db.save_job(&job).await.unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_persist_failure_keeps_runtime_state() {
        let temp_dir = create_temp_dir("completed-persist-failure");
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
            "job-notify-completed-persist-failure".to_string(),
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
        job.status = JobStatus::Syncing;
        job.progress = 90;
        job.current_size = 900;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 90,
                current_size: 900,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_completed_update
            BEFORE UPDATE ON jobs
            WHEN NEW.status = 'completed'
            BEGIN
                SELECT RAISE(FAIL, 'reject completed status');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Completed
        );
        assert!(scheduler.get_job_info(&job.job_id).is_some());
        assert_eq!(
            db.load_job(&job.job_id).await.unwrap().unwrap().status,
            JobStatus::Syncing
        );
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_missing_row_keeps_runtime_state() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-missing-row").await;
        let mut job = Job::new(
            "job-notify-completed-missing-row".to_string(),
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
        job.status = JobStatus::Syncing;
        job.progress = 88;
        job.current_size = 880;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_transfer_state(
            &job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 88,
                current_size: 880,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let raw_pool =
            open_raw_pool(format!("sqlite://{}", temp_dir.join("state.db").display()).as_str())
                .await;
        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Completed
        );
        assert!(scheduler.get_job_info(&job.job_id).is_some());
        assert!(db.load_job(&job.job_id).await.unwrap().is_none());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_without_runtime_preserves_snapshot_sizes() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-without-runtime").await;
        let mut job = Job::new(
            "job-notify-completed-without-runtime".to_string(),
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
        job.status = JobStatus::Syncing;
        job.progress = 80;
        job.current_size = 800;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.notify_job_completed(&job.job_id).await;

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 1000);
        assert_eq!(snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_pending_clears_runtime_error_message() {
        let (temp_dir, db, scheduler) = create_scheduler("pending-clears-runtime-error").await;
        let mut job = Job::new(
            "job-notify-pending-clears-error".to_string(),
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
        job.progress = 45;
        job.current_size = 450;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 45,
                current_size: 450,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("previous failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_pending(&job.job_id).await;

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert!(runtime.error_message.is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_clears_runtime_error_message() {
        let (temp_dir, db, scheduler) = create_scheduler("completed-clears-runtime-error").await;
        let mut job = Job::new(
            "job-notify-completed-clears-error".to_string(),
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
        job.status = JobStatus::Syncing;
        job.progress = 100;
        job.current_size = 1000;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 99,
                current_size: 990,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("stale failure".to_string()),
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_completed(&job.job_id).await;

        let runtime = scheduler.get_job_status(&job.job_id);
        assert!(runtime.is_none());

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Completed);
        assert_eq!(snapshot.progress, 100);
        assert_eq!(snapshot.current_size, 1000);
        assert_eq!(snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_cancelled_removes_tmp_artifacts() {
        let (temp_dir, db, scheduler) = create_scheduler("cancelled-cleans-tmp").await;
        let mut job = Job::new(
            "job-notify-cancelled".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 33,
                current_size: 128,
                total_size: 512,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let tmp_path = temp_dir.join("cancelled.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler.notify_job_cancelled(&job.job_id).await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(!tmp_path.exists());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(scheduler
            .transfer_manager_pool
            .job_tmp_write_paths(&job.job_id)
            .is_empty());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cleanup_job_tmp_artifacts_preserves_registration_when_removal_fails() {
        let (temp_dir, db, scheduler) = create_scheduler("cleanup-tmp-removal-failure").await;
        let job = Job::new(
            "job-tmp-removal-failure".to_string(),
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
        db.save_job(&job).await.unwrap();

        let tmp_dir = temp_dir.join("stuck.tmp");
        fs::create_dir_all(&tmp_dir).unwrap();
        db.save_tmp_transfer_path(&job.job_id, tmp_dir.to_str().unwrap())
            .await
            .unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&job.job_id, tmp_dir.to_str().unwrap())
            .await
            .unwrap();

        scheduler.cleanup_job_tmp_artifacts(&job.job_id).await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(tmp_dir.exists());
        assert_eq!(
            db.load_tmp_transfer_paths_by_job(&job.job_id)
                .await
                .unwrap(),
            vec![tmp_dir.to_string_lossy().to_string()]
        );
        assert_eq!(
            scheduler
                .transfer_manager_pool
                .job_tmp_write_paths(&job.job_id),
            vec![tmp_dir.to_string_lossy().to_string()]
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_cancelled_snapshot_failure_keeps_persisted_state() {
        let temp_dir = create_temp_dir("cancelled-snapshot-failure");
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
            "job-notify-cancelled-snapshot-failure".to_string(),
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
        job.progress = 40;
        job.current_size = 400;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&raw_pool)
            .await
            .unwrap();

        scheduler.notify_job_cancelled(&job.job_id).await;

        let row = sqlx::query(
            "SELECT status, progress, current_size, total_size FROM jobs WHERE job_id = ?1",
        )
        .bind(&job.job_id)
        .fetch_one(&raw_pool)
        .await
        .unwrap();
        assert_eq!(row.try_get::<String, _>("status").unwrap(), "pending");
        assert_eq!(row.try_get::<i64, _>("progress").unwrap(), 40);
        assert_eq!(row.try_get::<i64, _>("current_size").unwrap(), 400);
        assert_eq!(row.try_get::<i64, _>("total_size").unwrap(), 1000);
        assert!(db.get_retry(&job.job_id).await.unwrap().is_some());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_some());

        raw_pool.close().await;
        scheduler.transfer_manager_pool.shutdown().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retryable_preserves_retry_and_transfer_state() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-retryable-preserve").await;
        let mut job = Job::new(
            "job-notify-failed-retryable".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 10,
                current_size: 128,
                total_size: 1024,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        let retry = db.get_retry(&job.job_id).await.unwrap();
        assert!(retry.is_some());
        let state = db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(state.is_some());
        assert!(scheduler.get_job_status(&job.job_id).is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retry_lookup_failure_clears_runtime_state() {
        let temp_dir = create_temp_dir("failed-retry-read-failure");
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
            "job-notify-failed-retry-read-failure".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 35,
                current_size: 350,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        let raw_pool = open_raw_pool(&db_path).await;
        sqlx::query(
            r#"
            CREATE TRIGGER corrupt_retry_after_insert
            AFTER INSERT ON job_retries
            WHEN NEW.job_id = 'job-notify-failed-retry-read-failure'
            BEGIN
                UPDATE job_retries
                SET last_retry_at = 'broken-timestamp'
                WHERE job_id = NEW.job_id;
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap()
            .is_none());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);

        raw_pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_fatal_clears_stale_retry_and_transfer_state() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-fatal-clear").await;
        let mut job = Job::new(
            "job-notify-failed-fatal".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 55,
                current_size: 512,
                total_size: 1024,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();
        let tmp_path = temp_dir.join("failed-fatal.tmp");
        fs::write(&tmp_path, b"partial").unwrap();
        scheduler
            .transfer_manager_pool
            .register_tmp_write_path(&job.job_id, tmp_path.to_str().unwrap())
            .await
            .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "[fatal] permission denied",
                ErrorCategory::Fatal,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        let retry = db.get_retry(&job.job_id).await.unwrap();
        assert!(retry.is_none());
        let state = db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(state.is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(!tmp_path.exists());
        assert!(scheduler
            .transfer_manager_pool
            .job_tmp_write_paths(&job.job_id)
            .is_empty());
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retry_exhausted_clears_retry_and_transfer_state() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-exhausted-clear").await;
        let mut job = Job::new(
            "job-notify-failed-exhausted".to_string(),
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
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();
        for _ in 0..3 {
            db.update_retry_attempt(&job.job_id, false).await.unwrap();
        }

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 80,
                current_size: 800,
                total_size: 1000,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: chrono::Utc::now(),
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

        let state = FileTransferState::new("remote/source.bin".to_string(), 4);
        scheduler
            .transfer_manager_pool
            .save_state(&job.job_id, &state)
            .await
            .unwrap();

        scheduler
            .notify_job_failed(
                &job.job_id,
                "network failed after max retries",
                ErrorCategory::Retriable,
            )
            .await;
        scheduler.transfer_manager_pool.shutdown().await;

        let retry = db.get_retry(&job.job_id).await.unwrap();
        assert!(retry.is_none());
        let state = db
            .load_transfer_state(&job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(state.is_none());
        assert!(scheduler.get_job_status(&job.job_id).is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_started_updates_original_sync_snapshot_for_final_job() {
        let (temp_dir, db, scheduler) = create_scheduler("final-start-updates-original").await;

        let mut original = Job::new(
            "job-final-parent".to_string(),
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
        original.status = JobStatus::Cancelled;
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

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Pending,
                progress: 40,
                current_size: 400,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_started(&final_job_id).await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Syncing);
        assert_eq!(original_snapshot.progress, 40);
        assert_eq!(original_snapshot.current_size, 400);
        assert_eq!(original_snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_completed_preserves_original_final_round_metadata_in_snapshot() {
        let (temp_dir, db, scheduler) =
            create_scheduler("final-completed-preserves-round-metadata").await;

        let mut original = Job::new(
            "job-final-parent-completed".to_string(),
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
        original.status = JobStatus::Cancelled;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        final_job.progress = 80;
        final_job.current_size = 800;
        final_job.total_size = 1000;
        final_job.round_id = 4;
        final_job.is_last_round = true;
        db.save_job(&final_job).await.unwrap();

        let mut final_runtime = crate::sync::engine::job::SyncJob::new(
            final_job_id.clone(),
            std::path::PathBuf::from(&final_job.source.path),
            final_job.dest.path.clone(),
            final_job.region.clone(),
        )
        .with_job_type(final_job.job_type);
        final_runtime.restore_round_state(4, true);
        scheduler
            .job_cache
            .insert(final_job_id.clone(), final_runtime);
        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 80,
                current_size: 800,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler.notify_job_completed(&final_job_id).await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Completed);
        assert_eq!(original_snapshot.round_id, 4);
        assert!(original_snapshot.is_last_round);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_retryable_updates_original_sync_snapshot_for_final_job() {
        let (temp_dir, db, scheduler) = create_scheduler("final-failed-updates-original").await;

        let mut original = Job::new(
            "job-final-parent-failed".to_string(),
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
        original.status = JobStatus::Cancelled;
        db.save_job(&original).await.unwrap();

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        final_job.progress = 70;
        final_job.current_size = 700;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 70,
                current_size: 700,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .notify_job_failed(
                &final_job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert_eq!(original_snapshot.progress, 70);
        assert_eq!(original_snapshot.current_size, 700);
        assert_eq!(original_snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_updates_original_sync_runtime_error_message_for_final_job() {
        let (temp_dir, db, scheduler) =
            create_scheduler("final-failed-updates-original-runtime-error").await;

        let mut original = Job::new(
            "job-final-parent-runtime-error".to_string(),
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
        original.status = JobStatus::Syncing;
        original.progress = 20;
        original.current_size = 200;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        scheduler.job_status_cache.insert(
            original.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: original.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 20,
                current_size: 200,
                total_size: 1000,
                region: original.region.clone(),
                error_message: Some("stale original error".to_string()),
                created_at: original.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        final_job.progress = 70;
        final_job.current_size = 700;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 70,
                current_size: 700,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .notify_job_failed(
                &final_job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;

        let original_runtime = scheduler.get_job_status(&original.job_id).unwrap();
        assert_eq!(original_runtime.status, JobStatus::Failed);
        assert_eq!(original_runtime.progress, 70);
        assert_eq!(original_runtime.current_size, 700);
        assert_eq!(original_runtime.total_size, 1000);
        assert_eq!(
            original_runtime.error_message.as_deref(),
            Some("temporary network error")
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_persists_error_message_for_inactive_job() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-persists-error-message").await;

        let mut job = Job::new(
            "job-failed-persists-error".to_string(),
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
        job.status = JobStatus::Syncing;
        job.progress = 61;
        job.current_size = 610;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler
            .notify_job_failed(&job.job_id, "remote write failed", ErrorCategory::Fatal)
            .await;

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);
        assert_eq!(
            snapshot.error_message.as_deref(),
            Some("remote write failed")
        );

        let _ = fs::remove_dir_all(temp_dir);
    }
}
