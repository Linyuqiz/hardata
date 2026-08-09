impl SyncScheduler {
    pub async fn submit_job(&self, job: SyncJob) -> Result<()> {
        self.submit_job_internal(job, true).await
    }

    pub(in crate::application::sync::engine::scheduler) async fn submit_job_internal(
        &self,
        mut job: SyncJob,
        persist: bool,
    ) -> Result<()> {
        info!(
            operation = "job.submit_started",
            job_id = %job.job_id,
            region = %job.region,
            job_type = job.job_type.as_str(),
            persist,
            priority = job.priority,
            "job submission started"
        );

        self.config.resolve_runtime_destination_path(&job.dest)?;

        let recovered_snapshot = if persist {
            None
        } else {
            self.load_job_snapshot(&job.job_id).await?
        };

        if recovered_snapshot.is_some() {
            if job.job_id.ends_with("_final") {
                job.ensure_final_round_state();
            } else {
                job.mark_resumed_round();
            }
            self.restore_synced_file_cache_from_transfer_states(&job.job_id)
                .await?;
        }

        let _queue_guard = self.queue_update_lock.lock().await;
        if self.running_jobs.contains_key(&job.job_id) {
            return Err(HarDataError::Unknown(format!(
                "Job {} is still shutting down",
                job.job_id
            )));
        }

        if let Some(existing) = self.job_status_cache.get(&job.job_id) {
            if existing.status.is_active() {
                return Err(HarDataError::Unknown(format!(
                    "Job {} is already active with status {:?}",
                    job.job_id, existing.status
                )));
            }
        }

        self.ensure_destination_available(&job.job_id, &job.dest)
            .await?;
        self.cancelled_jobs.remove(&job.job_id);

        if persist {
            let db_job = self.sync_job_to_db_job(&job);
            self.db.save_job(&db_job).await.map_err(|e| {
                warn!(
                    operation = "job.persist_failed",
                    job_id = %job.job_id,
                    error = %e,
                    "job persistence failed"
                );
                e
            })?;
        }

        self.job_cache.insert(job.job_id.clone(), job.clone());

        let now = chrono::Utc::now();
        let (progress, current_size, total_size, created_at) = recovered_snapshot
            .as_ref()
            .map(|snapshot| {
                let current_size = snapshot.current_size.min(snapshot.total_size);
                (
                    calculate_progress(current_size, snapshot.total_size),
                    current_size,
                    snapshot.total_size,
                    snapshot.created_at,
                )
            })
            .unwrap_or((0, 0, 0, now));

        if let Some(snapshot) = recovered_snapshot.as_ref() {
            info!(
                operation = "job.recovery_state_loaded",
                job_id = %job.job_id,
                persisted_status = ?snapshot.status,
                progress,
                current_size,
                total_size,
                "persisted job state loaded"
            );
        }

        self.job_status_cache.insert(
            job.job_id.clone(),
            JobRuntimeStatus {
                job_id: job.job_id.clone(),
                status: JobStatus::Pending,
                progress,
                current_size,
                total_size,
                region: job.region.clone(),
                error_message: None,
                created_at,
                updated_at: now,
            },
        );
        if !persist || job.round_id > 0 || job.is_last_round {
            self.persist_job_round_state(&job.job_id, job.round_id, job.is_last_round)
                .await;
        }
        self.update_original_sync_job_status_from_final(
            &job.job_id,
            JobStatus::Pending,
            progress,
            current_size,
            total_size,
            None,
        )
        .await;

        self.job_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        let submitted_job_id = job.job_id.clone();
        self.job_queue.enqueue(job.priority, job).await;
        let queue_len = self.job_queue.len().await;

        self.job_notify.notify_one();
        info!(
            operation = "job.submitted",
            job_id = %submitted_job_id,
            queue_length = queue_len,
            "job submitted to queue"
        );
        Ok(())
    }

    fn sync_job_to_db_job(&self, sync_job: &SyncJob) -> Job {
        let now = chrono::Utc::now();
        Job {
            job_id: sync_job.job_id.clone(),
            region: sync_job.region.clone(),
            source: JobPath {
                path: sync_job.source.to_string_lossy().to_string(),
                client_id: String::new(),
            },
            dest: JobPath {
                path: sync_job.dest.clone(),
                client_id: String::new(),
            },
            status: JobStatus::Pending,
            job_type: sync_job.job_type,
            exclude_regex: sync_job.exclude_regex.clone(),
            include_regex: sync_job.include_regex.clone(),
            priority: sync_job.priority,
            round_id: sync_job.round_id,
            is_last_round: sync_job.is_last_round,
            options: JobConfig::default(),
            progress: 0,
            current_size: 0,
            total_size: 0,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

}
