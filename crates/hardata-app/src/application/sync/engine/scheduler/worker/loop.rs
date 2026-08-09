impl SyncScheduler {
    async fn list_directory_once(
        &self,
        path: &str,
        region: &str,
    ) -> Result<crate::protocol::ListDirectoryResponse> {
        let mut conn = connection::get_connection_with_retry_for_region_with_selector(
            &self.config,
            &self.connection_pools,
            region,
            &self.shutdown,
            Some(&self.protocol_selector),
        )
        .await?;
        conn.list_directory(path).await
    }

    async fn root_path_is_single_file(&self, root_path: &str, region: &str) -> Result<bool> {
        let Some(parent_path) = parent_lookup_path(root_path) else {
            return Ok(false);
        };
        let parent_response = self.list_directory_once(&parent_path, region).await?;
        Ok(parent_listing_confirms_single_file(
            root_path,
            &parent_response.files,
        ))
    }

    async fn load_root_directory_entry(
        &self,
        root_path: &str,
        region: &str,
    ) -> Result<Option<ScannedFile>> {
        let Some(parent_path) = parent_lookup_path(root_path) else {
            return Ok(None);
        };
        let root_name = Path::new(root_path.trim_end_matches('/'))
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if root_name.is_empty() {
            return Ok(None);
        }

        let parent_response = self.list_directory_once(&parent_path, region).await?;
        Ok(parent_response
            .files
            .into_iter()
            .find(|entry| entry.path == root_name && entry.is_directory)
            .map(|entry| ScannedFile {
                path: PathBuf::from(root_path),
                size: 0,
                modified: entry.modified,
                change_time: entry.change_time,
                inode: entry.inode,
                is_dir: true,
                mode: entry.mode,
                is_symlink: false,
                symlink_target: None,
            }))
    }

    async fn load_job_tmp_preserve_paths(&self, job: &SyncJob) -> Result<HashSet<PathBuf>> {
        let root = normalize_path(&super::sync_modes::single::resolve_base_dest_path(
            &self.config,
            job,
        )?);
        let mut paths = HashSet::new();

        for path in self.transfer_manager_pool.job_tmp_write_paths(&job.job_id) {
            let path = normalize_path(Path::new(&path));
            if path.starts_with(&root) {
                paths.insert(path);
            }
        }

        for path in self.db.load_tmp_transfer_paths_by_job(&job.job_id).await? {
            let path = normalize_path(Path::new(&path));
            if path.starts_with(&root) {
                paths.insert(path);
            }
        }

        Ok(paths)
    }

    pub(super) async fn worker_loop(&self, worker_id: usize) {
        info!(operation = "worker.started", worker_id, "worker started");

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!(operation = "worker.shutdown_requested", worker_id, "worker shutdown requested");
                break;
            }

            let job = self.job_queue.dequeue().await;

            if let Some(mut job) = job {
                info!(
                    operation = "job.dequeued",
                    worker_id,
                    job_id = %job.job_id,
                    priority = job.priority,
                    "job dequeued"
                );

                let permit = match self.semaphore.acquire().await {
                    Ok(p) => p,
                    Err(e) => {
                        error!(
                            operation = "worker.semaphore_acquire_failed",
                            worker_id,
                            error = %e,
                            "worker semaphore acquisition failed"
                        );
                        continue;
                    }
                };
                let _running_guard = RunningJobGuard::new(
                    job.job_id.clone(),
                    self.running_jobs.clone(),
                    self.cancelled_jobs.clone(),
                );

                match self.ensure_job_not_cancelled(&job.job_id).await {
                    Ok(()) => {}
                    Err(e) if is_cancelled_error(&e.to_string()) => {
                        info!(
                            operation = "job.cancelled_before_execution",
                            job_id = %job.job_id,
                            worker_id,
                            "job cancelled before execution"
                        );
                        self.notify_job_cancelled(&job.job_id).await;
                        drop(permit);
                        continue;
                    }
                    Err(e) => {
                        self.handle_non_executable_job_stop(&job.job_id, &e, "before execution")
                            .await;
                        drop(permit);
                        continue;
                    }
                }

                if job.is_first_round {
                    job.is_first_round = false;
                    job.round_id += 1;
                }

                info!(
                    operation = "job.round_started",
                    worker_id,
                    job_id = %job.job_id,
                    region = %job.region,
                    round_id = job.round_id,
                    job_type = job.job_type.as_str(),
                    is_last_round = job.is_last_round,
                    "job round started"
                );

                let mut previous_round_id = None;
                if let Some(mut cached_job) = self.job_cache.get_mut(&job.job_id) {
                    previous_round_id = Some(cached_job.round_id);
                    cached_job.round_id = job.round_id;
                    cached_job.is_last_round = job.is_last_round;
                }
                self.persist_job_round_state(&job.job_id, job.round_id, job.is_last_round)
                    .await;
                self.reset_progress_for_new_round(&job, previous_round_id)
                    .await;

                match self.execute_job(job.clone()).await {
                    Ok(execution_result) => {
                        info!(
                            operation = "job.round_completed",
                            worker_id,
                            job_id = %job.job_id,
                            round_id = job.round_id,
                            transferred = execution_result.transferred(),
                            retry_due_to_stability = execution_result.retry_due_to_stability(),
                            "job round completed"
                        );

                        match self.ensure_job_not_cancelled(&job.job_id).await {
                            Ok(()) => {}
                            Err(e) if is_cancelled_error(&e.to_string()) => {
                                info!(
                                    operation = "job.cancelled_after_execution",
                                    job_id = %job.job_id,
                                    round_id = job.round_id,
                                    "job cancelled after execution; success handling skipped"
                                );
                                self.notify_job_cancelled(&job.job_id).await;
                                drop(permit);
                                continue;
                            }
                            Err(e) => {
                                self.handle_non_executable_job_stop(
                                    &job.job_id,
                                    &e,
                                    "after execution",
                                )
                                .await;
                                drop(permit);
                                continue;
                            }
                        }

                        if job.is_completed() {
                            info!(
                                operation = "job.completed",
                                job_id = %job.job_id,
                                round_id = job.round_id,
                                "job completed"
                            );
                            self.notify_job_completed(&job.job_id).await;
                        } else if job.job_type.is_sync() {
                            self.mark_retry_success(&job.job_id).await;
                            if self
                                .should_notify_job_pending_after_round(
                                    &job.job_id,
                                    execution_result.transferred(),
                                )
                                .await
                            {
                                self.notify_job_pending(&job.job_id).await;
                            }
                            let next_delay = next_sync_schedule_delay(
                                job.scan_interval,
                                self.config.stability_threshold,
                                execution_result.retry_due_to_stability(),
                            );
                            let next_run = std::time::Instant::now() + next_delay;
                            info!(
                                operation = "job.next_round_scheduled",
                                job_id = %job.job_id,
                                round_id = job.round_id,
                                delay_ms = next_delay.as_millis() as u64,
                                "sync job next round scheduled"
                            );
                            self.schedule_delayed_job(next_run, job).await;
                        }
                    }
                    Err(e) => {
                        error!(
                            operation = "job.failed",
                            worker_id,
                            job_id = %job.job_id,
                            round_id = job.round_id,
                            error = %e,
                            "job execution failed"
                        );
                        self.handle_job_execution_error(&job.job_id, &e).await;
                    }
                }

                drop(permit);
            } else {
                tokio::select! {
                    _ = self.job_notify.notified() => {}
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                }
            }
        }

        info!(operation = "worker.stopped", worker_id, "worker stopped");
    }

    async fn should_notify_job_pending_after_round(&self, job_id: &str, transferred: bool) -> bool {
        if transferred {
            return true;
        }

        if let Some(entry) = self.job_status_cache.get(job_id) {
            return entry.status != JobStatus::Pending || entry.error_message.is_some();
        }

        match self.load_job_snapshot(job_id).await {
            Ok(Some(snapshot)) => {
                snapshot.status != JobStatus::Pending || snapshot.error_message.is_some()
            }
            Ok(None) => false,
            Err(e) => {
                warn!(
                    operation = "job.status_inspection_failed",
                    job_id = %job_id,
                    error = %e,
                    "failed to inspect persisted status before idle pending update"
                );
                true
            }
        }
    }
}
