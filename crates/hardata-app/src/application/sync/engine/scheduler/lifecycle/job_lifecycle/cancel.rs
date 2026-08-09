impl SyncScheduler {
    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        info!(operation = "job.cancel_started", job_id = %job_id, "job cancellation started");

        if let Some(final_job_id) = self.active_final_job_for(job_id).await? {
            info!(
                operation = "job.cancel_redirected",
                job_id = %job_id,
                final_job_id = %final_job_id,
                "job cancellation redirected to active final transfer"
            );
            return Box::pin(self.cancel_job(&final_job_id)).await;
        }

        let retry_record = self.db.get_retry(job_id).await?;
        let has_pending_retry = retry_record
            .as_ref()
            .map(|retry| retry.retry_count < retry.max_retries)
            .unwrap_or(false);
        let snapshot = self.load_job_snapshot(job_id).await?;

        if has_pending_retry
            && snapshot
                .as_ref()
                .map(|job| job.status == JobStatus::Completed)
                .unwrap_or(false)
        {
            self.db.delete_retry(job_id).await?;
            info!(
                operation = "job.retry_record_removed",
                job_id = %job_id,
                reason = "completed_job",
                "stale retry record removed"
            );
            return Ok(());
        }

        let cancellation = {
            if let Some(status) = self.job_status_cache.get(job_id) {
                match status.status {
                    JobStatus::Pending => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = "pending",
                            cleanup_now = true,
                            "job marked cancelled"
                        );
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            true,
                        ))
                    }
                    JobStatus::Syncing => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = "syncing",
                            cleanup_now = false,
                            "job marked cancelled"
                        );
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            false,
                        ))
                    }
                    JobStatus::Failed if has_pending_retry => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = "failed",
                            reason = "retry_pending",
                            cleanup_now = true,
                            "job marked cancelled"
                        );
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            true,
                        ))
                    }
                    JobStatus::Completed | JobStatus::Cancelled => match status.status {
                        JobStatus::Cancelled => {
                            info!(
                                operation = "job.cancel_idempotent",
                                job_id = %job_id,
                                "job already cancelled"
                            );
                            return Ok(());
                        }
                        _ => {
                            return Err(HarDataError::Unknown(format!(
                                "Job {} already finished with status {:?}",
                                job_id, status.status
                            )));
                        }
                    },
                    JobStatus::Failed => {
                        return Err(HarDataError::Unknown(format!(
                            "Job {} already failed and has no pending retry",
                            job_id
                        )));
                    }
                    _ => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = ?status.status,
                            cleanup_now = true,
                            "job marked cancelled from runtime state"
                        );
                        Some((
                            status.progress,
                            status.current_size,
                            status.total_size,
                            true,
                        ))
                    }
                }
            } else if let Some(job) = snapshot.as_ref() {
                match job.status {
                    JobStatus::Pending | JobStatus::Syncing => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = ?job.status,
                            cleanup_now = true,
                            "job marked cancelled from persisted state"
                        );
                        Some((job.progress, job.current_size, job.total_size, true))
                    }
                    JobStatus::Failed if has_pending_retry => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = "failed",
                            reason = "retry_pending",
                            cleanup_now = true,
                            "job marked cancelled from retry queue"
                        );
                        Some((job.progress, job.current_size, job.total_size, true))
                    }
                    JobStatus::Completed | JobStatus::Cancelled => match job.status {
                        JobStatus::Cancelled => {
                            info!(
                                operation = "job.cancel_idempotent",
                                job_id = %job_id,
                                "job already cancelled in persisted state"
                            );
                            return Ok(());
                        }
                        _ => {
                            return Err(HarDataError::Unknown(format!(
                                "Job {} already finished with status {:?}",
                                job_id, job.status
                            )));
                        }
                    },
                    JobStatus::Failed => {
                        return Err(HarDataError::Unknown(format!(
                            "Job {} already failed and has no pending retry",
                            job_id
                        )));
                    }
                    _ => {
                        info!(
                            operation = "job.cancel_marked",
                            job_id = %job_id,
                            previous_status = ?job.status,
                            cleanup_now = true,
                            "job marked cancelled from persisted state"
                        );
                        Some((job.progress, job.current_size, job.total_size, true))
                    }
                }
            } else {
                None
            }
        };

        if let Some((progress, current_size, total_size, cleanup_now)) = cancellation {
            self.cancelled_jobs.insert(job_id.to_string(), ());
            let updated = self
                .db
                .update_job_status(
                    job_id,
                    JobStatus::Cancelled,
                    progress,
                    current_size,
                    total_size,
                    None,
                )
                .await?;
            if !updated {
                self.cancelled_jobs.remove(job_id);
                return Err(HarDataError::JobNotFound(job_id.to_string()));
            }

            self.remove_queued_jobs(job_id).await;
            if let Err(e) = self.db.delete_retry(job_id).await {
                warn!(
                    operation = "job.retry_record_delete_failed",
                    job_id = %job_id,
                    error = %e,
                    "failed to delete retry record for cancelled job"
                );
            }

            if let Some(mut status) = self.job_status_cache.get_mut(job_id) {
                status.status = JobStatus::Cancelled;
                status.updated_at = chrono::Utc::now();
                status.error_message = None;
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

            if cleanup_now {
                if let Err(e) = self.transfer_manager_pool.clear_job_states(job_id).await {
                    warn!(
                        operation = "job.transfer_state_cleanup_failed",
                        job_id = %job_id,
                        error = %e,
                        "failed to clear transfer states for cancelled job"
                    );
                }

                self.cleanup_job_tmp_artifacts(job_id).await;
                self.cleanup_runtime_job(job_id);
            }

            if !self.running_jobs.contains_key(job_id) {
                self.cancelled_jobs.remove(job_id);
            }

            info!(
                operation = "job.cancelled",
                job_id = %job_id,
                cleanup_now,
                "job cancellation completed"
            );
            Ok(())
        } else {
            Err(HarDataError::Unknown(format!("Job {} not found", job_id)))
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn recover_pending_jobs(
        &self,
    ) -> Result<()> {
        let jobs = self.db.load_active_jobs().await?;
        let mut recovered_count = 0;

        for job in jobs {
            if !job.status.is_active() {
                continue;
            }

            info!(
                operation = "job.recovery_started",
                job_id = %job.job_id,
                region = %job.region,
                persisted_status = ?job.status,
                round_id = job.round_id,
                is_last_round = job.is_last_round,
                "job recovery started"
            );

            let mut sync_job = SyncJob::new(
                job.job_id.clone(),
                PathBuf::from(&job.source.path),
                job.dest.path.clone(),
                job.region.clone(),
            )
            .with_filters(job.exclude_regex.clone(), job.include_regex.clone())
            .with_priority(job.priority)
            .with_job_type(job.job_type);
            sync_job.restore_round_state(job.round_id, job.is_last_round);
            if sync_job.job_id.ends_with("_final") {
                sync_job.ensure_final_round_state();
            }

            if !job.job_id.ends_with("_final") {
                if let Some(final_job_id) = self.active_final_job_for(&job.job_id).await? {
                    info!(
                        operation = "job.recovery_skipped",
                        job_id = %job.job_id,
                        final_job_id = %final_job_id,
                        reason = "active_final_transfer",
                        "original job recovery skipped"
                    );
                    self.remove_queued_jobs(&job.job_id).await;
                    continue;
                }
            }

            if job.status == JobStatus::Paused {
                if let Err(e) = self
                    .ensure_destination_available(&job.job_id, &job.dest.path)
                    .await
                {
                    warn!(
                        operation = "job.recovery_failed",
                        job_id = %job.job_id,
                        error = %e,
                        "paused job recovery failed"
                    );
                    self.mark_recovery_failure(&job, &e).await;
                    continue;
                }

                self.job_cache.insert(job.job_id.clone(), sync_job.clone());
                if let Err(e) = self
                    .restore_synced_file_cache_from_transfer_states(&job.job_id)
                    .await
                {
                    warn!(
                        operation = "job.recovery_cache_restore_failed",
                        job_id = %job.job_id,
                        error = %e,
                        "paused job cache restore failed"
                    );
                    self.job_cache.remove(&job.job_id);
                    self.mark_recovery_failure(&job, &e).await;
                    continue;
                }
                self.job_status_cache.insert(
                    job.job_id.clone(),
                    JobRuntimeStatus {
                        job_id: job.job_id.clone(),
                        status: JobStatus::Paused,
                        progress: job.progress,
                        current_size: job.current_size,
                        total_size: job.total_size,
                        region: job.region.clone(),
                        error_message: None,
                        created_at: job.created_at,
                        updated_at: job.updated_at,
                    },
                );
                self.persist_job_round_state(
                    &job.job_id,
                    sync_job.round_id,
                    sync_job.is_last_round,
                )
                .await;
                self.update_original_sync_job_status_from_final(
                    &job.job_id,
                    JobStatus::Paused,
                    job.progress,
                    job.current_size,
                    job.total_size,
                    None,
                )
                .await;
                self.remove_queued_jobs(&job.job_id).await;
                recovered_count += 1;
                continue;
            }

            if let Err(e) = self.submit_job_internal(sync_job, false).await {
                warn!(
                    operation = "job.recovery_failed",
                    job_id = %job.job_id,
                    error = %e,
                    "job recovery failed"
                );
                self.mark_recovery_failure(&job, &e).await;
            } else {
                recovered_count += 1;
            }
        }

        info!(
            operation = "job.recovery_completed",
            recovered_count,
            "job recovery completed"
        );
        Ok(())
    }

    pub(in crate::application::sync::engine::scheduler) async fn active_final_job_for(
        &self,
        job_id: &str,
    ) -> Result<Option<String>> {
        if job_id.ends_with("_final") {
            return Ok(None);
        }

        let Some(snapshot) = self.load_job_snapshot(job_id).await? else {
            return Ok(None);
        };

        if !snapshot.job_type.is_sync() {
            return Ok(None);
        }

        let final_job_id = format!("{}_final", job_id);
        let final_snapshot = self.load_job_snapshot(&final_job_id).await?;
        let Some(final_snapshot) = final_snapshot else {
            return Ok(None);
        };

        let final_has_pending_retry = self
            .db
            .get_retry(&final_job_id)
            .await?
            .map(|retry| retry.retry_count < retry.max_retries)
            .unwrap_or(false);

        let final_is_active = final_snapshot.status.is_active()
            || (final_snapshot.status == JobStatus::Failed && final_has_pending_retry);

        if final_is_active {
            Ok(Some(final_job_id))
        } else {
            Ok(None)
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn cleanup_old_jobs(
        &self,
        days: i64,
    ) {
        match self.db.cleanup_old_jobs(days).await {
            Ok(count) => {
                if count > 0 {
                    info!(
                        operation = "job.retention_cleanup_completed",
                        deleted_count = count,
                        older_than_days = days,
                        "old job cleanup completed"
                    );
                }
            }
            Err(e) => {
                warn!(
                    operation = "job.retention_cleanup_failed",
                    older_than_days = days,
                    error = %e,
                    "old job cleanup failed"
                );
            }
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn cleanup_terminal_job_artifacts(
        &self,
    ) {
        let jobs = match self.db.load_terminal_jobs().await {
            Ok(jobs) => jobs,
            Err(e) => {
                warn!(
                    operation = "job.terminal_artifact_cleanup_failed",
                    phase = "load_terminal_jobs",
                    error = %e,
                    "terminal artifact cleanup failed"
                );
                return;
            }
        };
        let retryable_job_ids = match self.db.load_retryable_job_ids().await {
            Ok(job_ids) => job_ids,
            Err(e) => {
                warn!(
                    operation = "job.terminal_artifact_cleanup_failed",
                    phase = "load_retryable_jobs",
                    error = %e,
                    "terminal artifact cleanup failed"
                );
                return;
            }
        };

        for job in jobs {
            let should_cleanup = match job.status {
                JobStatus::Completed | JobStatus::Cancelled => true,
                JobStatus::Failed => !retryable_job_ids.contains(&job.job_id),
                _ => false,
            };

            if !should_cleanup {
                continue;
            }

            if let Err(e) = self.db.delete_retry(&job.job_id).await {
                warn!(
                    operation = "job.terminal_retry_delete_failed",
                    job_id = %job.job_id,
                    error = %e,
                    "terminal retry record deletion failed"
                );
            }

            if let Err(e) = self
                .transfer_manager_pool
                .clear_job_states(&job.job_id)
                .await
            {
                warn!(
                    operation = "job.terminal_transfer_state_clear_failed",
                    job_id = %job.job_id,
                    error = %e,
                    "terminal transfer state cleanup failed"
                );
            }

            self.cleanup_job_tmp_artifacts(&job.job_id).await;
        }
    }

}
