impl SyncScheduler {
    pub async fn finalize_job(&self, job_id: &str) -> Result<()> {
        info!(operation = "job.finalize_started", job_id = %job_id, "final transfer requested");

        let snapshot = self.load_job_snapshot(job_id).await?;
        let original_job = self
            .job_cache
            .get(job_id)
            .map(|job| job.clone())
            .or_else(|| snapshot.as_ref().map(sync_job_from_snapshot))
            .ok_or_else(|| HarDataError::Unknown(format!("Job {} not found", job_id)))?;
        if !original_job.job_type.is_sync() {
            return Err(HarDataError::Unknown(format!(
                "Job {} is not a sync job, finalize is only supported for sync jobs",
                job_id
            )));
        }
        let final_job_id = format!("{}_final", job_id);

        let has_pending_retry = self
            .db
            .get_retry(job_id)
            .await?
            .map(|retry| retry.retry_count < retry.max_retries)
            .unwrap_or(false);
        let final_snapshot = self.load_job_snapshot(&final_job_id).await?;
        let final_has_pending_retry = if final_snapshot.is_some() {
            self.db
                .get_retry(&final_job_id)
                .await?
                .map(|retry| retry.retry_count < retry.max_retries)
                .unwrap_or(false)
        } else {
            false
        };

        let runtime_status = self
            .job_status_cache
            .get(job_id)
            .map(|status| status.status)
            .or_else(|| snapshot_status(snapshot.clone()));
        let active_final_job_id = final_snapshot.as_ref().and_then(|final_job| {
            let final_is_active = final_job.status.is_active()
                || (final_job.status == JobStatus::Failed && final_has_pending_retry);
            final_is_active.then(|| final_job_id.clone())
        });
        if let Some(active_final_job_id) = active_final_job_id {
            info!(
                operation = "job.finalize_idempotent",
                job_id = %job_id,
                final_job_id = %active_final_job_id,
                reason = "final_transfer_active",
                "final transfer request already satisfied"
            );
            return Ok(());
        }
        if final_snapshot
            .as_ref()
            .map(|final_job| final_job.status == JobStatus::Completed)
            .unwrap_or(false)
        {
            info!(
                operation = "job.finalize_idempotent",
                job_id = %job_id,
                final_job_id = %final_job_id,
                reason = "final_transfer_completed",
                "final transfer request already satisfied"
            );
            return Ok(());
        }
        let restarting_terminal_final_failure = matches!(runtime_status, Some(JobStatus::Failed))
            && final_snapshot
                .as_ref()
                .map(|final_job| final_job.status == JobStatus::Failed && !final_has_pending_retry)
                .unwrap_or(false);
        let needs_wait = matches!(runtime_status, Some(JobStatus::Syncing));

        match runtime_status {
            Some(JobStatus::Pending) | Some(JobStatus::Syncing) => {}
            Some(JobStatus::Failed) if has_pending_retry || restarting_terminal_final_failure => {}
            Some(status) => {
                return Err(HarDataError::Unknown(format!(
                    "Job {} status {:?} cannot be finalized",
                    job_id, status
                )));
            }
            None => {
                return Err(HarDataError::Unknown(format!(
                    "Job {} not found for finalize",
                    job_id
                )));
            }
        }

        self.ensure_destination_available(&final_job_id, &original_job.dest)
            .await?;

        let rollback_state = if restarting_terminal_final_failure {
            None
        } else {
            let retry_record = self.db.get_retry(job_id).await?;
            Some(FinalizeRollbackState {
                snapshot: snapshot.clone(),
                retry_record,
                runtime_status: self.job_status_cache.get(job_id).map(|s| s.clone()),
                sync_job: self.job_cache.get(job_id).map(|j| j.clone()),
                synced_file_cache: self.snapshot_synced_file_cache(job_id),
            })
        };

        if !restarting_terminal_final_failure {
            self.cancel_job(job_id).await?;
        }

        if needs_wait && !restarting_terminal_final_failure {
            for _ in 0..50 {
                if self.job_status_cache.get(job_id).is_none()
                    && self.job_cache.get(job_id).is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            if self.job_status_cache.get(job_id).is_some() || self.job_cache.get(job_id).is_some() {
                if let Some(ref rs) = rollback_state {
                    self.rollback_finalize_original_job(rs).await;
                }
                return Err(HarDataError::Unknown(format!(
                    "Job {} is still shutting down after 5s timeout, cannot finalize safely",
                    job_id
                )));
            }
        }

        let mut final_job = if restarting_terminal_final_failure {
            final_snapshot
                .as_ref()
                .map(sync_job_from_snapshot)
                .ok_or_else(|| {
                    HarDataError::Unknown(format!(
                        "Job {} missing final transfer state for retry",
                        job_id
                    ))
                })?
        } else {
            let mut final_job = original_job.clone();
            final_job.job_id = final_job_id.clone();
            final_job.job_type = JobType::Once;
            final_job.priority = original_job.priority + 100;
            final_job.start_final_round();
            final_job
        };
        final_job.job_id = final_job_id.clone();
        final_job.job_type = JobType::Once;
        final_job.ensure_final_round_state();

        info!(
            operation = "job.final_transfer_created",
            job_id = %job_id,
            final_job_id = %final_job.job_id,
            source = %final_job.source.display(),
            destination = %final_job.dest,
            restarting_terminal_failure = restarting_terminal_final_failure,
            "final transfer job created"
        );

        let final_job_cache = rollback_state
            .as_ref()
            .map(|state| state.synced_file_cache.as_slice())
            .unwrap_or(&[]);
        self.restore_synced_file_cache(&final_job.job_id, final_job_cache);

        if let Err(e) = self.submit_job(final_job).await {
            self.synced_files_cache.remove(&final_job_id);
            if let Some(ref rs) = rollback_state {
                self.rollback_finalize_original_job(rs).await;
            }
            return Err(e);
        }
        Ok(())
    }

    /// Restore the original job after a failed finalize operation.
    async fn rollback_finalize_original_job(&self, rollback: &FinalizeRollbackState) {
        let job_id = rollback
            .sync_job
            .as_ref()
            .map(|j| j.job_id.clone())
            .or_else(|| rollback.snapshot.as_ref().map(|j| j.job_id.clone()));
        let job_id = match job_id {
            Some(id) => id,
            None => return,
        };

        self.cancelled_jobs.remove(&job_id);

        if let Some(ref sync_job) = rollback.sync_job {
            self.job_cache.insert(job_id.clone(), sync_job.clone());
        } else {
            self.job_cache.remove(&job_id);
        }

        if let Some(ref runtime_status) = rollback.runtime_status {
            self.job_status_cache
                .insert(job_id.clone(), runtime_status.clone());
        } else {
            self.job_status_cache.remove(&job_id);
        }

        match rollback.retry_record.as_ref() {
            Some(retry) => {
                let _ = self.db.restore_retry(retry).await;
            }
            None => {
                let _ = self.db.delete_retry(&job_id).await;
            }
        }

        self.restore_synced_file_cache(&job_id, &rollback.synced_file_cache);

        if rollback
            .runtime_status
            .as_ref()
            .map(|s| s.status == JobStatus::Pending)
            .unwrap_or(false)
        {
            if let Some(ref sync_job) = rollback.sync_job {
                self.enqueue_job_replacing_queued(sync_job.clone()).await;
                self.job_notify.notify_one();
            }
        }

        warn!(
            operation = "job.finalize_rolled_back",
            job_id = %job_id,
            "finalize operation rolled back"
        );
    }

}
