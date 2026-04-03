use crate::core::job::JobStatus;
use crate::sync::engine::scheduler::sync_modes::calculate_progress;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info, warn};

use super::super::core::SyncScheduler;

impl SyncScheduler {
    pub(in crate::sync::engine::scheduler) async fn retry_scheduler_loop(&self) {
        info!("Retry scheduler loop started");
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        loop {
            if self.shutdown.load(Ordering::Relaxed) || *shutdown_rx.borrow() {
                info!("Retry scheduler received shutdown signal");
                break;
            }

            if let Err(e) = self.process_pending_retries().await {
                error!("Error processing pending retries: {}", e);
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        info!("Retry scheduler wake-up received shutdown signal");
                        break;
                    }
                }
            }
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

            if let Some(status) = self.job_status_cache.get(&retry.job_id) {
                match status.status {
                    JobStatus::Cancelled => {
                        info!(
                            "Job {} already cancelled, removing retry record",
                            retry.job_id
                        );
                        self.cleanup_stale_retry_record(&retry.job_id, "job already cancelled")
                            .await;
                        continue;
                    }
                    JobStatus::Pending | JobStatus::Syncing | JobStatus::Paused => {
                        info!(
                            "Job {} is already {:?}, skip duplicate retry scheduling",
                            retry.job_id, status.status
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
                            "Job {} not found in cache or database, skipping retry",
                            retry.job_id
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
                            "Job {} already {:?}, removing retry record",
                            retry.job_id, snapshot.status
                        );
                        self.cleanup_stale_retry_record(&retry.job_id, "snapshot already terminal")
                            .await;
                        continue;
                    }

                    if snapshot.status == JobStatus::Paused {
                        info!("Job {} is paused, skip retry scheduling", retry.job_id);
                        continue;
                    }

                    let job = {
                        let mut job = crate::sync::engine::job::SyncJob::new(
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
                            "Job {} already {:?}, removing retry record",
                            retry.job_id, status.status
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
                            "Job {} is already {:?}, skip duplicate retry scheduling",
                            retry.job_id, status.status
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
                "Retrying job {} (attempt {}/{}): {}",
                retry.job_id,
                retry.retry_count + 1,
                retry.max_retries,
                retry.last_error
            );

            let mut pending_runtime = None;
            let (progress, current_size, total_size) =
                if let Some(status) = self.job_status_cache.get(&retry.job_id) {
                    (status.progress, status.current_size, status.total_size)
                } else {
                    let snapshot =
                        self.load_job_snapshot(&retry.job_id)
                            .await?
                            .ok_or_else(|| {
                                crate::util::error::HarDataError::JobNotFound(retry.job_id.clone())
                            })?;
                    let current_size = snapshot.current_size.min(snapshot.total_size);
                    let progress = calculate_progress(current_size, snapshot.total_size);
                    let now = chrono::Utc::now();
                    pending_runtime = Some(crate::sync::engine::scheduler::JobRuntimeStatus {
                        job_id: retry.job_id.clone(),
                        status: JobStatus::Pending,
                        progress,
                        current_size,
                        total_size: snapshot.total_size,
                        region: snapshot.region.clone(),
                        error_message: None,
                        created_at: snapshot.created_at,
                        updated_at: now,
                    });
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
                        "Failed to persist pending retry status for job {}: {}",
                        retry.job_id, e
                    );
                    if inserted_runtime_job {
                        self.job_cache.remove(&retry.job_id);
                    }
                    continue;
                }
            };
            if !pending_status_updated {
                warn!(
                    "Skipped pending retry scheduling for job {} because the persisted row no longer exists",
                    retry.job_id
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
                    "Failed to record retry attempt for job {}: {}",
                    retry.job_id, e
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
                            "Failed to roll back pending retry status for job {}: {}",
                            retry.job_id, revert_error
                        );
                        false
                    }
                };
                if !rollback_updated {
                    warn!(
                        "Skipped rolling back pending retry status for job {} because the persisted row no longer exists",
                        retry.job_id
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
        }

        Ok(())
    }

    pub(in crate::sync::engine::scheduler) async fn mark_retry_success(&self, job_id: &str) {
        match self.db.update_retry_attempt(job_id, true).await {
            Ok(true) => {
                info!("Job {} retry succeeded, retry record removed", job_id);
            }
            Ok(false) => {}
            Err(e) => {
                warn!("Failed to mark retry success for job {}: {}", job_id, e);
            }
        }
    }

    async fn cleanup_stale_retry_record(&self, job_id: &str, reason: &str) {
        if let Err(e) = self.db.delete_retry(job_id).await {
            warn!(
                "Failed to delete stale retry record for job {} ({}): {}",
                job_id, reason, e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::core::SyncScheduler;
    use crate::core::{FileTransferState, Job, JobPath, JobStatus, JobType};
    use crate::sync::engine::scheduler::SchedulerConfig;
    use crate::sync::storage::db::Database;
    use chrono::{Duration, Utc};
    use sqlx::sqlite::SqlitePool;
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-retry-scheduler-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn process_pending_retries_skips_jobs_already_pending() {
        let temp_dir = create_temp_dir("skip-duplicate-pending");
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
            "job-retry-pending".to_string(),
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
        job.status = JobStatus::Pending;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress: 10,
                current_size: 1,
                total_size: 10,
                region: job.region.clone(),
                error_message: None,
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        let retry = db.get_retry(&job.job_id).await.unwrap().unwrap();
        assert_eq!(retry.retry_count, 0);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_persists_pending_status() {
        let temp_dir = create_temp_dir("persist-pending");
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
            "job-retry-persist".to_string(),
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
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Pending);
        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Pending
        );
        assert!(scheduler.get_job_info(&job.job_id).is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_updates_original_sync_snapshot_for_final_job() {
        let temp_dir = create_temp_dir("pending-final-updates-original");
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
            "job-retry-persist-final".to_string(),
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
        original.progress = 37;
        original.current_size = 370;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        let mut final_job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Failed;
        final_job.progress = 37;
        final_job.current_size = 370;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();
        db.save_retry(&final_job.job_id, "temporary network error")
            .await
            .unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&final_job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let final_snapshot = scheduler
            .load_job_snapshot(&final_job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_snapshot.status, JobStatus::Pending);
        let final_runtime = scheduler.get_job_info(&final_job.job_id).unwrap();
        assert_eq!(final_runtime.round_id, 1);
        assert!(final_runtime.is_last_round);

        let original_snapshot = scheduler
            .load_job_snapshot(&original.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Pending);
        assert_eq!(original_snapshot.progress, 37);
        assert_eq!(original_snapshot.current_size, 370);
        assert_eq!(original_snapshot.total_size, 1000);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_skips_enqueue_when_pending_status_persist_fails() {
        let temp_dir = create_temp_dir("skip-enqueue-on-persist-failure");
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
            "job-retry-persist-failure".to_string(),
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
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

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
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 75,
                current_size: 750,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("temporary network error".to_string()),
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        sqlx::query("DROP TABLE jobs").execute(&pool).await.unwrap();

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert_eq!(
            scheduler.get_job_status(&job.job_id).unwrap().status,
            JobStatus::Failed
        );
        assert_eq!(
            db.get_retry(&job.job_id)
                .await
                .unwrap()
                .unwrap()
                .retry_count,
            0
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_cleans_stale_retry_when_pending_status_row_missing() {
        let temp_dir = create_temp_dir("cleanup-stale-retry-on-missing-row");
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
            "job-retry-missing-row".to_string(),
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
        job.progress = 75;
        job.current_size = 750;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

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
        scheduler.job_status_cache.insert(
            job.job_id.clone(),
            crate::sync::engine::scheduler::JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Failed,
                progress: 75,
                current_size: 750,
                total_size: 1000,
                region: job.region.clone(),
                error_message: Some("temporary network error".to_string()),
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
        );

        sqlx::query("DELETE FROM jobs WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(db.get_retry(&job.job_id).await.unwrap().is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_restores_resumed_round_metadata() {
        let temp_dir = create_temp_dir("restore-resumed-round");
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
            "job-retry-resume-round".to_string(),
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
        job.progress = 75;
        job.current_size = 750;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        assert_eq!(runtime.progress, 75);
        assert_eq!(runtime.current_size, 750);
        assert_eq!(runtime.total_size, 1000);

        let resumed_job = scheduler.get_job_info(&job.job_id).unwrap();
        assert_eq!(resumed_job.round_id, 1);
        assert!(!resumed_job.is_first_round);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_restores_synced_file_cache_from_transfer_states() {
        let temp_dir = create_temp_dir("restore-retry-synced-file-cache");
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
            "job-retry-restore-cache".to_string(),
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
        job.progress = 75;
        job.current_size = 750;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let mut completed = FileTransferState::new("remote/retry-finished.bin".to_string(), 3)
            .with_source_version(8192, 1710000003, None, None);
        for chunk in 0..3 {
            completed.mark_chunk_completed(chunk);
        }
        completed.dest_modified = Some(1710000300);
        completed.dest_change_time = Some(1710000301);
        completed.dest_inode = Some(43);
        completed.cache_only = true;
        db.save_transfer_state(&job.job_id, &completed)
            .await
            .unwrap();

        let incomplete = FileTransferState::new("remote/retry-incomplete.bin".to_string(), 3)
            .with_source_version(4096, 1710000004, None, None);
        db.save_transfer_state(&job.job_id, &incomplete)
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        let runtime = scheduler.get_job_status(&job.job_id).unwrap();
        assert_eq!(runtime.status, JobStatus::Pending);
        let job_cache = scheduler
            .synced_files_cache
            .get(&job.job_id)
            .expect("retry recovery should rebuild synced file cache");
        assert_eq!(job_cache.len(), 1);
        let restored = job_cache
            .get("remote/retry-finished.bin")
            .expect("completed retry file should be restored");
        assert_eq!(restored.size, 8192);
        assert_eq!(restored.mtime, 1710000003);
        assert_eq!(restored.dest_mtime, Some(1710000300));
        assert_eq!(restored.dest_change_time, Some(1710000301));
        assert_eq!(restored.dest_inode, Some(43));
        assert!(job_cache.get("remote/retry-incomplete.bin").is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_removes_inserted_runtime_job_when_retry_attempt_update_fails()
    {
        let temp_dir = create_temp_dir("retry-attempt-update-failure");
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
            "job-retry-attempt-update-failure".to_string(),
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
        original.progress = 42;
        original.current_size = 420;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        let mut job = Job::new(
            format!("{}_final", original.job_id),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        job.status = JobStatus::Failed;
        job.progress = 42;
        job.current_size = 420;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let scheduler_pool = SqlitePool::connect(&db_path).await.unwrap();
        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&scheduler_pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE job_retries")
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap_err();

        assert!(scheduler.get_job_info(&job.job_id).is_none());
        assert!(scheduler.get_job_status(&job.job_id).is_none());
        assert_eq!(scheduler.job_queue.len().await, 0);

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);
        assert_eq!(snapshot.progress, 42);
        assert_eq!(snapshot.current_size, 420);
        assert_eq!(snapshot.total_size, 1000);

        let original_snapshot = db.load_job(&original.job_id).await.unwrap().unwrap();
        assert_eq!(original_snapshot.status, JobStatus::Failed);
        assert_eq!(original_snapshot.progress, 42);
        assert_eq!(original_snapshot.current_size, 420);
        assert_eq!(original_snapshot.total_size, 1000);

        scheduler_pool.close().await;
        pool.close().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn process_pending_retries_skips_jobs_already_paused() {
        let temp_dir = create_temp_dir("skip-paused");
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
            "job-retry-paused".to_string(),
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
        job.status = JobStatus::Paused;
        db.save_job(&job).await.unwrap();
        db.save_retry(&job.job_id, "temporary network error")
            .await
            .unwrap();

        let pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(&job.job_id)
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        scheduler.process_pending_retries().await.unwrap();

        assert_eq!(scheduler.job_queue.len().await, 0);
        let retry = db.get_retry(&job.job_id).await.unwrap().unwrap();
        assert_eq!(retry.retry_count, 0);
        let snapshot = scheduler
            .load_job_snapshot(&job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Paused);

        let _ = fs::remove_dir_all(temp_dir);
    }
}
