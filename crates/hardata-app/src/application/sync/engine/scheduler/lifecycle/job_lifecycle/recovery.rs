impl SyncScheduler {
    async fn mark_recovery_failure(&self, job: &Job, error: &HarDataError) {
        let error_message = error.to_string();
        match self
            .db
            .update_job_status(
                &job.job_id,
                JobStatus::Failed,
                job.progress,
                job.current_size,
                job.total_size,
                Some(&error_message),
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    operation = "job.recovery_status_update_skipped",
                    job_id = %job.job_id,
                    reason = "persisted_row_missing",
                    "recovery failure status update skipped"
                );
            }
            Err(e) => {
                warn!(
                    operation = "job.recovery_status_update_failed",
                    job_id = %job.job_id,
                    error = %e,
                    "recovery failure status update failed"
                );
            }
        }
        self.update_original_sync_job_status_from_final(
            &job.job_id,
            JobStatus::Failed,
            job.progress,
            job.current_size,
            job.total_size,
            Some(&error_message),
        )
        .await;

        if let Err(e) = self.db.delete_retry(&job.job_id).await {
            warn!(
                operation = "job.recovery_retry_delete_failed",
                job_id = %job.job_id,
                error = %e,
                "recovery retry record deletion failed"
            );
        }

        if let Err(e) = self
            .transfer_manager_pool
            .clear_job_states(&job.job_id)
            .await
        {
            warn!(
                operation = "job.recovery_transfer_state_clear_failed",
                job_id = %job.job_id,
                error = %e,
                "recovery transfer state cleanup failed"
            );
        }

        self.cleanup_job_tmp_artifacts(&job.job_id).await;
    }
}
