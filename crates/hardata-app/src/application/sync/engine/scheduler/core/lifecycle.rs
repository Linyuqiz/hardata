impl SyncScheduler {
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        if let Err(e) = connection::establish_all_connections(
            &self.config,
            &self.connection_pools,
            &self.shutdown,
        )
        .await
        {
            warn!(
                operation = "scheduler.connection_setup_failed",
                error = %e,
                fallback = "lazy_reconnect",
                "initial region connection setup failed"
            );
        }

        self.cleanup_old_jobs(7).await;
        self.cleanup_terminal_job_artifacts().await;

        self.recover_pending_jobs().await?;

        if self.config.enable_cache_preheat {
            let cache_builder = Arc::new(super::CacheBuilder::new(
                self.config.clone(),
                self.config.global_index.clone(),
                Arc::clone(&self.chunk_index),
                Arc::clone(&self.db),
                self.transfer_manager_pool.tmp_write_paths(),
            ));

            cache_builder.start().await;
            *self.cache_builder.lock().await = Some(cache_builder);
        }

        let worker_count = self.config.max_concurrent_jobs;

        let mut workers = self.workers.lock().await;
        for worker_id in 0..worker_count {
            let scheduler = Arc::clone(self);
            let handle = tokio::spawn(async move {
                scheduler.worker_loop(worker_id).await;
            });
            workers.push(handle);
        }

        let scheduler = Arc::clone(self);
        let retry_handle = tokio::spawn(async move {
            scheduler.retry_scheduler_loop().await;
        });
        *self.retry_scheduler.lock().await = Some(retry_handle);

        let scheduler = Arc::clone(self);
        let delayed_handle = tokio::spawn(async move {
            scheduler.delayed_scheduler_loop().await;
        });
        *self.delayed_scheduler.lock().await = Some(delayed_handle);

        let scheduler = Arc::clone(self);
        let cache_handle = tokio::spawn(async move {
            scheduler.cache_cleaner_loop().await;
        });
        *self.cache_cleaner.lock().await = Some(cache_handle);

        info!(
            operation = "scheduler.started",
            worker_count,
            background_tasks = "retry,delayed,cache_cleaner",
            "scheduler started"
        );
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        info!(operation = "scheduler.shutdown_started", "scheduler shutdown started");

        self.shutdown.store(true, Ordering::Relaxed);
        self.shutdown_signal.send_replace(true);
        self.job_notify.notify_waiters();

        let mut workers = self.workers.lock().await;
        while let Some(worker) = workers.pop() {
            if let Err(e) = worker.await {
                warn!(operation = "scheduler.worker_join_failed", error = %e, "scheduler worker join failed");
            }
        }

        let mut retry_scheduler = self.retry_scheduler.lock().await;
        if let Some(handle) = retry_scheduler.take() {
            if let Err(e) = handle.await {
                warn!(operation = "scheduler.retry_join_failed", error = %e, "retry scheduler join failed");
            }
        }

        let mut delayed_scheduler = self.delayed_scheduler.lock().await;
        if let Some(handle) = delayed_scheduler.take() {
            if let Err(e) = handle.await {
                warn!(operation = "scheduler.delayed_join_failed", error = %e, "delayed scheduler join failed");
            }
        }

        let mut cache_cleaner = self.cache_cleaner.lock().await;
        if let Some(handle) = cache_cleaner.take() {
            if let Err(e) = handle.await {
                warn!(operation = "scheduler.cache_cleaner_join_failed", error = %e, "cache cleaner join failed");
            }
        }

        let mut cache_builder = self.cache_builder.lock().await;
        if let Some(builder) = cache_builder.take() {
            builder.shutdown().await;
        }

        self.transfer_manager_pool.shutdown().await;

        info!(operation = "scheduler.stopped", "scheduler stopped");
        Ok(())
    }

    pub(super) async fn delayed_scheduler_loop(&self) {
        let check_interval = std::time::Duration::from_millis(100);
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        loop {
            if self.shutdown.load(Ordering::Relaxed) || *shutdown_rx.borrow() {
                break;
            }

            let ready_jobs = self.delayed_queue.pop_ready().await;
            for mut job in ready_jobs {
                job.start_new_round();
                info!(
                    operation = "job.delayed_ready",
                    job_id = %job.job_id,
                    round_id = job.round_id,
                    "delayed job ready"
                );
                self.enqueue_job_replacing_queued(job).await;
                self.job_notify.notify_one();
            }

            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }

    pub(in crate::application::sync::engine::scheduler) async fn enqueue_job_replacing_queued(
        &self,
        job: SyncJob,
    ) {
        let _queue_guard = self.queue_update_lock.lock().await;
        if self.should_skip_terminal_queue_enqueue(&job.job_id).await {
            return;
        }
        self.job_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.job_queue.enqueue(job.priority, job).await;
    }

    pub(in crate::application::sync::engine::scheduler) async fn schedule_delayed_job(
        &self,
        run_at: std::time::Instant,
        job: SyncJob,
    ) {
        let _queue_guard = self.queue_update_lock.lock().await;
        if self.should_skip_terminal_queue_enqueue(&job.job_id).await {
            return;
        }
        self.job_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job.job_id)
            .await;
        self.delayed_queue.insert(run_at, job).await;
    }

    pub(in crate::application::sync::engine::scheduler) async fn remove_queued_jobs(
        &self,
        job_id: &str,
    ) {
        let _queue_guard = self.queue_update_lock.lock().await;
        self.job_queue
            .retain(|queued| queued.job_id != job_id)
            .await;
        self.delayed_queue
            .retain(|queued| queued.job_id != job_id)
            .await;
    }

    async fn should_skip_terminal_queue_enqueue(&self, job_id: &str) -> bool {
        match match self.try_resolve_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => {
                warn!(
                    operation = "queue.enqueue_skipped",
                    job_id = %job_id,
                    reason = "status_resolution_failed",
                    error = %e,
                    "queue enqueue skipped"
                );
                return true;
            }
        } {
            Some(JobStatus::Paused) => {
                info!(operation = "queue.enqueue_skipped", job_id = %job_id, reason = "paused", "queue enqueue skipped");
                true
            }
            Some(JobStatus::Cancelled) | Some(JobStatus::Completed) => {
                info!(operation = "queue.enqueue_skipped", job_id = %job_id, reason = "terminal", "queue enqueue skipped");
                true
            }
            Some(JobStatus::Failed) => {
                let has_pending_retry = match self.db.get_retry(job_id).await {
                    Ok(Some(retry)) => retry.retry_count < retry.max_retries,
                    Ok(None) => false,
                    Err(e) => {
                        warn!(
                            operation = "queue.enqueue_skipped",
                            job_id = %job_id,
                            reason = "retry_lookup_failed",
                            error = %e,
                            "queue enqueue skipped"
                        );
                        return true;
                    }
                };
                if has_pending_retry {
                    false
                } else {
                    info!(
                        operation = "queue.enqueue_skipped",
                        job_id = %job_id,
                        reason = "failed_without_retry",
                        "queue enqueue skipped"
                    );
                    true
                }
            }
            _ => false,
        }
    }

    pub(super) async fn cache_cleaner_loop(&self) {
        use crate::shared::constants::{
            FILE_CACHE_CLEANUP_INTERVAL_SECS, FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS,
        };

        let check_interval = std::time::Duration::from_secs(FILE_CACHE_CLEANUP_INTERVAL_SECS);
        let ttl_secs = FILE_CACHE_TTL_SECS as i64;
        let max_entries = FILE_CACHE_MAX_ENTRIES;
        let mut shutdown_rx = self.shutdown_signal.subscribe();

        loop {
            if self.shutdown.load(Ordering::Relaxed) || *shutdown_rx.borrow() {
                break;
            }

            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }

            self.run_cache_cleanup_cycle(chrono::Utc::now().timestamp(), ttl_secs, max_entries)
                .await;
        }
    }

    async fn run_cache_cleanup_cycle(&self, now: i64, ttl_secs: i64, max_entries: usize) {
        let mut total_entries = 0usize;
        let mut expired_count = 0usize;
        let mut jobs_to_remove = Vec::new();

        for job_entry in self.synced_files_cache.iter() {
            let job_id = job_entry.key().clone();
            let job_cache = job_entry.value();
            let mut expired_files = Vec::new();

            for file_entry in job_cache.iter() {
                total_entries += 1;
                let state = file_entry.value();
                if now - state.updated_at > ttl_secs {
                    expired_files.push(file_entry.key().clone());
                }
            }

            for file_path in expired_files {
                job_cache.remove(&file_path);
                expired_count += 1;
            }

            if job_cache.is_empty() {
                jobs_to_remove.push(job_id);
            }
        }

        for job_id in jobs_to_remove {
            self.synced_files_cache.remove(&job_id);
        }

        let mut remaining_entries = total_entries.saturating_sub(expired_count);

        if remaining_entries > max_entries {
            let mut all_entries: Vec<(String, String, i64)> = Vec::new();

            for job_entry in self.synced_files_cache.iter() {
                let job_id = job_entry.key().clone();
                for file_entry in job_entry.value().iter() {
                    all_entries.push((
                        job_id.clone(),
                        file_entry.key().clone(),
                        file_entry.value().updated_at,
                    ));
                }
            }

            all_entries.sort_by_key(|(_, _, updated_at)| *updated_at);

            let to_remove = all_entries.len().saturating_sub(max_entries);
            let mut removed_by_limit = 0usize;

            for (job_id, file_path, _) in all_entries.into_iter().take(to_remove) {
                if let Some(job_cache) = self.synced_files_cache.get(&job_id) {
                    job_cache.remove(&file_path);
                    removed_by_limit += 1;
                }
            }

            if removed_by_limit > 0 {
                info!(
                    operation = "cache.cleanup_limit_completed",
                    removed_entries = removed_by_limit,
                    max_entries,
                    "cache limit cleanup completed"
                );
            }

            let empty_jobs: Vec<String> = self
                .synced_files_cache
                .iter()
                .filter_map(|entry| entry.value().is_empty().then(|| entry.key().clone()))
                .collect();
            for job_id in empty_jobs {
                self.synced_files_cache.remove(&job_id);
            }

            remaining_entries = self
                .synced_files_cache
                .iter()
                .map(|entry| entry.value().len())
                .sum();
        }

        if expired_count > 0 {
            info!(
                operation = "cache.cleanup_expired_completed",
                removed_entries = expired_count,
                ttl_seconds = ttl_secs,
                remaining_entries,
                "expired cache cleanup completed"
            );
        }

        self.cleanup_old_jobs(7).await;
    }
}
