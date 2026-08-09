impl SyncScheduler {
    async fn should_skip_runtime_free_status_update(&self, job_id: &str, transition: &str) -> bool {
        if self.job_status_cache.contains_key(job_id) {
            return false;
        }

        let snapshot_status = match self.try_resolve_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => {
                warn!(
                    operation = "job.status_update_skipped",
                    job_id = %job_id,
                    transition = %transition,
                    reason = "status_resolution_failed",
                    error = %e,
                    "job status update skipped"
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
                operation = "job.status_update_skipped",
                job_id = %job_id,
                transition = %transition,
                snapshot_status = ?snapshot_status,
                "job status update skipped"
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
                warn!(operation = "job.status_persist_failed", job_id = %job_id, status = ?status, error = %e, "job status persistence failed");
                return false;
            }
        };

        if !updated {
            warn!(
                operation = "job.status_persist_skipped",
                job_id = %job_id,
                reason = "persisted_row_missing",
                "job status persistence skipped"
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
                    operation = "job.status_update_skipped",
                    job_id = %job_id,
                    transition = %transition,
                    reason = "snapshot_load_failed",
                    error = %e,
                    "job status update skipped"
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
                    operation = "job.status_update_skipped",
                    job_id = %job_id,
                    transition = %transition,
                    reason = "snapshot_load_failed",
                    error = %e,
                    "job status update skipped"
                );
                None
            }
        }
    }

    pub(in crate::application::sync::engine::scheduler) fn cleanup_runtime_job(
        &self,
        job_id: &str,
    ) {
        self.job_cache.remove(job_id);
        self.job_status_cache.remove(job_id);
        self.synced_files_cache.remove(job_id);
        self.size_freezers.remove(job_id);
    }

    pub(in crate::application::sync::engine::scheduler) async fn cleanup_job_tmp_artifacts(
        &self,
        job_id: &str,
    ) {
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
                    operation = "job.tmp_path_load_failed",
                    job_id = %job_id,
                    error = %e,
                    "terminal temporary path load failed"
                );
            }
        }

        for path in tmp_paths {
            let should_unregister = match tokio::fs::remove_file(&path).await {
                Ok(_) => {
                    info!(operation = "job.tmp_file_removed", job_id = %job_id, path = %path, "terminal temporary file removed");
                    true
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                Err(e) => {
                    warn!(
                        operation = "job.tmp_file_remove_failed",
                        job_id = %job_id,
                        path = %path,
                        error = %e,
                        "terminal temporary file removal failed"
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
                        operation = "job.tmp_path_unregister_failed",
                        job_id = %job_id,
                        path = %path,
                        error = %e,
                        "temporary path unregister failed"
                    );
                }
            }
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn update_original_sync_job_status_from_final(
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
                    operation = "job.original_status_update_skipped",
                    job_id = %original_job_id,
                    final_job_id = %final_job_id,
                    reason = "snapshot_load_failed",
                    error = %e,
                    "original sync status update skipped"
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
                        operation = "job.round_state_sync_skipped",
                        job_id = %original_job_id,
                        final_job_id = %final_job_id,
                        reason = "snapshot_load_failed",
                        error = %e,
                        "round state sync skipped"
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
