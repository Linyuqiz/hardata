impl SyncScheduler {
    async fn reset_progress_for_new_round(&self, job: &SyncJob, previous_round_id: Option<i64>) {
        let is_new_sync_round = job.job_type.is_sync()
            && previous_round_id
                .map(|previous_round_id| job.round_id > previous_round_id)
                .unwrap_or(false);

        if !is_new_sync_round {
            return;
        }

        info!(
            operation = "job.progress_snapshot_preserved",
            job_id = %job.job_id,
            round_id = job.round_id,
            "previous progress snapshot preserved"
        );
    }

    async fn handle_progress_update(
        &self,
        job_id: &str,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) {
        let current_size = current_size.min(total_size);
        tracing::debug!(
            operation = "job.progress_received",
            job_id = %job_id,
            progress,
            current_size,
            total_size,
            "job progress received"
        );

        let should_persist = if let Some(entry) = self.job_status_cache.get(job_id) {
            if entry.status != JobStatus::Syncing {
                tracing::debug!(
                    operation = "job.progress_ignored",
                    job_id = %job_id,
                    status = ?entry.status,
                    reason = "runtime_status_inactive",
                    "stale job progress ignored"
                );
                false
            } else {
                true
            }
        } else {
            tracing::warn!(
                operation = "job.progress_ignored",
                job_id = %job_id,
                reason = "runtime_status_missing",
                "job progress ignored"
            );
            false
        };

        if !should_persist {
            return;
        }

        let updated = match self
            .db
            .update_job_progress(job_id, progress, current_size, total_size)
            .await
        {
            Ok(updated) => updated,
            Err(e) => {
                warn!(operation = "job.progress_persist_failed", job_id = %job_id, error = %e, "job progress persistence failed");
                return;
            }
        };

        if !updated {
            tracing::debug!(
                operation = "job.progress_ignored",
                job_id = %job_id,
                reason = "persisted_status_inactive",
                "stale job progress ignored"
            );
            return;
        }

        let should_notify = if let Some(mut entry) = self.job_status_cache.get_mut(job_id) {
            if entry.status != JobStatus::Syncing {
                tracing::debug!(
                    operation = "job.progress_ignored",
                    job_id = %job_id,
                    status = ?entry.status,
                    reason = "runtime_status_changed",
                    "job progress ignored"
                );
                false
            } else {
                entry.progress = progress;
                entry.current_size = current_size;
                entry.total_size = total_size;
                entry.updated_at = chrono::Utc::now();
                tracing::debug!(
                    operation = "job.progress_applied",
                    job_id = %job_id,
                    progress,
                    "job progress applied"
                );
                true
            }
        } else {
            tracing::debug!(
                operation = "job.progress_ignored",
                job_id = %job_id,
                reason = "runtime_status_removed",
                "job progress ignored"
            );
            false
        };

        if !should_notify {
            return;
        }

        if let Some(ref callback) = *self.status_callback.lock().await {
            callback.on_job_progress(job_id, progress, current_size);
        }
    }

    async fn handle_job_execution_error(&self, job_id: &str, error: &HarDataError) {
        let error_message = error.to_string();

        if is_cancelled_error(&error_message) {
            info!(
                operation = "job.cancelled_during_error_handling",
                job_id = %job_id,
                error = %error_message,
                "cancelled status preserved"
            );
            self.notify_job_cancelled(job_id).await;
            return;
        }

        match self.ensure_job_not_cancelled(job_id).await {
            Ok(()) => {}
            Err(e) if is_cancelled_error(&e.to_string()) => {
                info!(
                    operation = "job.cancelled_during_error_handling",
                    job_id = %job_id,
                    error = %error_message,
                    "cancelled status preserved"
                );
                self.notify_job_cancelled(job_id).await;
                return;
            }
            Err(e) => {
                self.handle_non_executable_job_stop(job_id, &e, "during execution error handling")
                    .await;
                return;
            }
        }

        let category = self.retry_policy.categorize_error(error);
        self.notify_job_failed(job_id, &error_message, category)
            .await;
    }

    async fn handle_non_executable_job_stop(
        &self,
        job_id: &str,
        error: &HarDataError,
        phase: &str,
    ) {
        match error {
            HarDataError::JobNotFound(_) => {
                warn!(
                    operation = "job.execution_skipped",
                    job_id = %job_id,
                    phase = %phase,
                    reason = "persisted_row_missing",
                    "job dropped from execution"
                );
                self.cleanup_runtime_job(job_id);
                self.remove_queued_jobs(job_id).await;
            }
            _ => {
                info!(
                    operation = "job.execution_skipped",
                    job_id = %job_id,
                    phase = %phase,
                    error = %error,
                    "job is no longer executable"
                );
            }
        }
    }

    async fn ensure_job_not_cancelled(&self, job_id: &str) -> Result<()> {
        if let Some(runtime_status) = self.job_status_cache.get(job_id).map(|entry| entry.status) {
            if runtime_status == JobStatus::Cancelled {
                info!(operation = "job.cancelled_before_side_effects", job_id = %job_id, "job cancelled before side effects");
                return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
            }

            if !runtime_status.is_active() {
                return Err(HarDataError::Unknown(format!(
                    "Job {} status {:?} is not executable",
                    job_id, runtime_status
                )));
            }
        }

        let persisted_status = self.db.load_job_status(job_id).await?;
        match persisted_status {
            Some(JobStatus::Cancelled) => {
                info!(operation = "job.cancelled_before_side_effects", job_id = %job_id, "job cancelled before side effects");
                Err(HarDataError::Unknown("Job cancelled by user".to_string()))
            }
            Some(status) if status.is_active() => Ok(()),
            Some(status) => Err(HarDataError::Unknown(format!(
                "Job {} status {:?} is not executable",
                job_id, status
            ))),
            None => Err(HarDataError::JobNotFound(job_id.to_string())),
        }
    }
}
