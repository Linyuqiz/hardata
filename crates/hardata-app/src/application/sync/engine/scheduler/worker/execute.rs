impl SyncScheduler {
    async fn execute_job(&self, job: SyncJob) -> Result<JobExecutionResult> {
        info!(
            operation = "job.execution_started",
            job_id = %job.job_id,
            region = %job.region,
            source = %job.source.display(),
            destination = %job.dest,
            job_type = job.job_type.as_str(),
            round_id = job.round_id,
            "job execution started"
        );

        self.ensure_job_not_cancelled(&job.job_id).await?;

        let source_str = job
            .source
            .to_str()
            .ok_or_else(|| HarDataError::Unknown("Invalid source path".to_string()))?;

        info!(
            operation = "job.remote_scan_started",
            job_id = %job.job_id,
            region = %job.region,
            source = %source_str,
            "remote directory scan started"
        );
        let scan_filter = ScanFilter::new(&job.exclude_regex, &job.include_regex)?;

        let scan_result = self
            .list_directory_recursive(source_str, &job.region, &scan_filter)
            .await?;
        let source_is_single_file = scan_result.source_is_single_file;
        let root_excluded = scan_result.root_excluded;
        let mut files = scan_result.files;

        if root_excluded {
            info!(
                operation = "job.round_skipped",
                job_id = %job.job_id,
                reason = "root_excluded",
                source = %source_str,
                "job round skipped"
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: false,
            });
        }

        self.ensure_job_not_cancelled(&job.job_id).await?;

        info!(
            operation = "job.remote_scan_completed",
            job_id = %job.job_id,
            region = %job.region,
            file_count = files.len(),
            source_is_single_file,
            "remote directory scan completed"
        );

        if has_active_scan_filters(&job) && !source_is_single_file && files.is_empty() {
            info!(
                operation = "job.round_skipped",
                job_id = %job.job_id,
                reason = "filters_matched_no_files",
                "job round skipped"
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: false,
            });
        }

        if !source_is_single_file {
            if !has_active_scan_filters(&job) {
                if let Some(root_entry) = self
                    .load_root_directory_entry(source_str, &job.region)
                    .await?
                {
                    files.insert(0, root_entry);
                }
            }
            ensure_directory_sync_root(&self.config, &job).await?;
        }

        if should_cleanup_deleted_targets(&job) {
            let cleanup_result = match self.load_job_tmp_preserve_paths(&job).await {
                Ok(preserved_tmp_paths) => {
                    cleanup_deleted_targets(
                        &self.config,
                        &job,
                        &files,
                        source_is_single_file,
                        &preserved_tmp_paths,
                    )
                    .await
                }
                Err(e) => {
                    warn!(
                        operation = "job.cleanup_preserve_paths_failed",
                        job_id = %job.job_id,
                        error = %e,
                        "failed to load temporary paths; deleted-target cleanup skipped"
                    );
                    Ok(())
                }
            };
            cleanup_result?;
            trim_deleted_source_tracking(
                &self.synced_files_cache,
                &self.size_freezers,
                &job.job_id,
                &files,
            )
            .await;
        } else if has_active_scan_filters(&job) {
            info!(
                operation = "job.cleanup_skipped",
                job_id = %job.job_id,
                reason = "active_scan_filters",
                "deleted-target cleanup skipped"
            );
        }

        if files.is_empty() {
            info!(
                operation = "job.round_skipped",
                job_id = %job.job_id,
                reason = "no_files",
                "job round skipped"
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: false,
            });
        }

        let mut has_pending_unstable_files = false;
        let stable_files = if !job.is_final_transfer() {
            let file_sizes: Vec<(String, u64, i64)> = files
                .iter()
                .map(|f| (f.path.to_string_lossy().to_string(), f.size, f.modified))
                .collect();
            let scanned_file_count = file_sizes.len();
            let size_freezer = self.size_freezer_for_job(&job.job_id);
            let stable_names: HashSet<String> = size_freezer
                .get_stable_files(&file_sizes)
                .await
                .into_iter()
                .collect();
            let pending_stability_count =
                pending_stability_file_count(scanned_file_count, stable_names.len());
            has_pending_unstable_files = pending_stability_count > 0;
            let stable_files: Vec<ScannedFile> = files
                .into_iter()
                .filter(|f| stable_names.contains(&f.path.to_string_lossy().to_string()))
                .collect();
            info!(
                operation = "job.stability_evaluated",
                job_id = %job.job_id,
                stable_file_count = stable_files.len(),
                scanned_file_count,
                pending_stability_count,
                "job file stability evaluated"
            );
            stable_files
        } else {
            info!(
                operation = "job.stability_skipped",
                job_id = %job.job_id,
                reason = "final_transfer",
                "job stability filter skipped"
            );
            files
        };

        if stable_files.is_empty() {
            let retry_delay =
                next_sync_schedule_delay(job.scan_interval, self.config.stability_threshold, true);
            info!(
                operation = "job.round_skipped",
                job_id = %job.job_id,
                reason = "no_stable_files",
                retry_delay_ms = retry_delay.as_millis() as u64,
                "job round skipped until files stabilize"
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: has_pending_unstable_files,
            });
        }

        self.synced_files_cache
            .entry(job.job_id.clone())
            .or_default();

        let changed_files: Vec<ScannedFile> = if job.job_type.is_full() {
            info!(
                operation = "job.change_filter_completed",
                job_id = %job.job_id,
                mode = "full",
                changed_file_count = stable_files.len(),
                "full mode selected all stable files"
            );
            stable_files
        } else {
            let files_len = stable_files.len();
            let job_cache = self.synced_files_cache.get(&job.job_id);
            let mut changed_files = Vec::new();
            let mut refreshed_dest_mtimes = Vec::new();
            let mut refreshed_cache_states = Vec::new();

            for file in stable_files {
                let file_path = file.path.to_string_lossy().to_string();
                let cached_state = job_cache
                    .as_ref()
                    .and_then(|cache| cache.get(&file_path))
                    .map(|cached| {
                        (
                            cached.size,
                            cached.mtime,
                            cached.change_time,
                            cached.inode,
                            cached.dest_mtime,
                            cached.dest_change_time,
                            cached.dest_inode,
                        )
                    });
                let cached_unchanged = cached_state
                    .map(|(size, mtime, change_time, inode, _, _, _)| {
                        source_file_matches_cached_state(&file, size, mtime, change_time, inode)
                    })
                    .unwrap_or(false);

                if !cached_unchanged {
                    if cached_state.is_none() && (file.is_dir || file.is_symlink) {
                        let destination_state = inspect_destination_sync_state(
                            &self.config,
                            &job,
                            &file,
                            files_len,
                            None,
                            None,
                            None,
                        )
                        .await?;
                        if !destination_state.requires_sync {
                            refreshed_cache_states.push((
                                file_path,
                                FileSyncState {
                                    size: file.size,
                                    mtime: file.modified,
                                    change_time: file.change_time,
                                    inode: file.inode,
                                    dest_mtime: destination_state.dest_mtime,
                                    dest_change_time: destination_state.dest_change_time,
                                    dest_inode: destination_state.dest_inode,
                                    updated_at: 0,
                                },
                            ));
                            continue;
                        }
                    }
                    changed_files.push(file);
                    continue;
                }

                let destination_state = inspect_destination_sync_state(
                    &self.config,
                    &job,
                    &file,
                    files_len,
                    cached_state.and_then(|(_, _, _, _, dest_mtime, _, _)| dest_mtime),
                    cached_state.and_then(|(_, _, _, _, _, dest_change_time, _)| dest_change_time),
                    cached_state.and_then(|(_, _, _, _, _, _, dest_inode)| dest_inode),
                )
                .await?;
                if destination_state.requires_sync {
                    changed_files.push(file);
                    continue;
                }

                if let Some(dest_mtime) = destination_state.dest_mtime {
                    if cached_state
                        .and_then(|(_, _, _, _, dest_mtime, _, _)| dest_mtime)
                        .is_none()
                    {
                        refreshed_dest_mtimes.push((file_path, dest_mtime));
                    }
                }
            }

            if !refreshed_dest_mtimes.is_empty() || !refreshed_cache_states.is_empty() {
                let refreshed_at = chrono::Utc::now().timestamp();
                if let Some(job_cache) = self.synced_files_cache.get(&job.job_id) {
                    for (file_path, mut state) in refreshed_cache_states {
                        state.updated_at = refreshed_at;
                        job_cache.insert(file_path, state);
                    }
                    for (file_path, dest_mtime) in refreshed_dest_mtimes {
                        if let Some(mut cached) = job_cache.get_mut(&file_path) {
                            cached.dest_mtime = Some(dest_mtime);
                            cached.updated_at = refreshed_at;
                        }
                    }
                }
            }

            changed_files
        };

        let cached_count = self
            .synced_files_cache
            .get(&job.job_id)
            .map(|c| c.len())
            .unwrap_or(0);
        info!(
            operation = "job.change_filter_completed",
            job_id = %job.job_id,
            changed_file_count = changed_files.len(),
            cached_file_count = cached_count,
            "job file change filter completed"
        );

        if changed_files.is_empty() {
            info!(
                operation = "job.round_skipped",
                job_id = %job.job_id,
                reason = "no_changed_files",
                "job round skipped"
            );
            return Ok(JobExecutionResult::NoTransfer {
                retry_due_to_stability: has_pending_unstable_files,
            });
        }

        let files_for_cache = changed_files.clone();

        let files = changed_files;

        self.ensure_job_not_cancelled(&job.job_id).await?;
        self.notify_job_started(&job.job_id).await;
        self.ensure_job_not_cancelled(&job.job_id).await?;

        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, u8, u64, u64)>();

        let scheduler = self.clone();
        tokio::spawn(async move {
            while let Some((job_id, progress, current_size, total_size)) = progress_rx.recv().await
            {
                scheduler
                    .handle_progress_update(&job_id, progress, current_size, total_size)
                    .await;
            }
        });

        let notify_progress =
            move |job_id: &str, progress: u8, current_size: u64, total_size: u64| {
                tracing::debug!(
                    operation = "job.progress_emitted",
                    job_id = %job_id,
                    progress,
                    current_size,
                    total_size,
                    "job progress emitted"
                );
                if let Err(e) =
                    progress_tx.send((job_id.to_string(), progress, current_size, total_size))
                {
                    tracing::warn!(
                        operation = "job.progress_delivery_failed",
                        job_id = %job_id,
                        error = %e,
                        "job progress delivery failed"
                    );
                }
            };

        let concurrency_controller =
            self.get_concurrency_controller(&job.region)
                .ok_or_else(|| {
                    crate::shared::error::HarDataError::InvalidConfig(format!(
                        "No concurrency controller for region {}",
                        job.region
                    ))
                })?;

        info!(
            operation = "job.transfer_started",
            job_id = %job.job_id,
            region = %job.region,
            file_count = files.len(),
            "job transfer started"
        );

        sync_files::sync_files(
            &self.config,
            &self.transfer_manager_pool,
            &self.connection_pools,
            &self.shutdown,
            &self.job_status_cache,
            &self.cancelled_jobs,
            &job,
            files,
            &self.adaptive_controller,
            &concurrency_controller,
            &self.retry_policy,
            &self.protocol_selector,
            &self.prefetch_manager,
            &self.chunk_index,
            notify_progress,
        )
        .await?;

        let transferred_file_count = files_for_cache.len();
        if let Some(job_cache) = self.synced_files_cache.get(&job.job_id) {
            let now = chrono::Utc::now().timestamp();
            let files_len = files_for_cache.len();
            for file in files_for_cache {
                let path = file.path.to_string_lossy().to_string();
                let dest_mtime =
                    load_destination_cache_state(&self.config, &job, &file, files_len).await;
                job_cache.insert(
                    path,
                    FileSyncState {
                        size: file.size,
                        mtime: file.modified,
                        change_time: file.change_time,
                        inode: file.inode,
                        dest_mtime: dest_mtime.dest_mtime,
                        dest_change_time: dest_mtime.dest_change_time,
                        dest_inode: dest_mtime.dest_inode,
                        updated_at: now,
                    },
                );
            }
        }

        info!(
            operation = "job.transfer_completed",
            job_id = %job.job_id,
            region = %job.region,
            file_count = transferred_file_count,
            "job transfer completed"
        );
        Ok(JobExecutionResult::Transferred {
            retry_due_to_stability: has_pending_unstable_files,
        })
    }
}
