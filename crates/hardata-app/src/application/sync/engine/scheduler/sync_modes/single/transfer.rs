#[allow(clippy::too_many_arguments)]
pub async fn sync_single_file<F>(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    cancelled_jobs: &DashMap<String, ()>,
    job: &SyncJob,
    file: &ScannedFile,
    source_path: &str,
    dest_path: &str,
    connection: &mut TransportConnection,
    max_concurrent_streams: usize,
    on_batch_progress: F,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
    chunk_index: Option<&Arc<crate::application::sync::engine::CDCResultCache>>,
    global_index: Option<&Arc<crate::application::sync::engine::ChunkIndex>>,
) -> Result<()>
where
    F: Fn(u64) + Send + Sync + 'static,
{
    sync_single_file_with_mode(
        config,
        transfer_manager_pool,
        job_status_cache,
        cancelled_jobs,
        job,
        file,
        source_path,
        dest_path,
        connection,
        max_concurrent_streams,
        on_batch_progress,
        config.replicate_mode,
        prefetch_manager,
        chunk_index,
        global_index,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_single_file_with_mode<F>(
    config: &SchedulerConfig,
    transfer_manager_pool: &TransferManagerPool,
    job_status_cache: &DashMap<String, JobRuntimeStatus>,
    cancelled_jobs: &DashMap<String, ()>,
    job: &SyncJob,
    file: &ScannedFile,
    source_path: &str,
    dest_path: &str,
    connection: &mut TransportConnection,
    max_concurrent_streams: usize,
    on_batch_progress: F,
    mode: ReplicateMode,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
    chunk_index: Option<&Arc<crate::application::sync::engine::CDCResultCache>>,
    global_index: Option<&Arc<crate::application::sync::engine::ChunkIndex>>,
) -> Result<()>
where
    F: Fn(u64) + Send + Sync + 'static,
{
    if is_job_cancelled(job_status_cache, cancelled_jobs, &job.job_id) {
        info!(operation = "job.cancelled_during_transfer", job_id = %job.job_id, path = %source_path, "job cancelled during file transfer");
        return Err(cancelled_error());
    }

    let file_path_str = source_path.to_string();

    if file.is_dir {
        sync_directory_entry(file, dest_path).await?;
        return Ok(());
    }

    if file.is_symlink {
        if let Some(ref target) = file.symlink_target {
            let dest = Path::new(dest_path);
            prepare_symlink_destination(dest_path).await?;
            #[cfg(unix)]
            {
                if let Err(e) = std::os::unix::fs::symlink(target, dest) {
                    error!(
                        operation = "job.symlink_create_failed",
                        job_id = %job.job_id,
                        destination = %dest_path,
                        target = %target,
                        error = %e,
                        "symbolic link creation failed"
                    );
                    return Err(HarDataError::FileOperation(format!(
                        "Failed to create symlink: {}",
                        e
                    )));
                }
                debug!(operation = "job.symlink_created", job_id = %job.job_id, destination = %dest_path, target = %target, "symbolic link created");
            }
            #[cfg(not(unix))]
            {
                return Err(HarDataError::FileOperation(format!(
                    "Symlink sync not supported on this platform: {}",
                    dest_path
                )));
            }
        }
        return Ok(());
    }

    let write_path = get_write_path(dest_path, mode);
    prepare_regular_file_destination(dest_path, &write_path).await?;
    let write_path_existed = Path::new(&write_path).exists();

    if file.size == 0 {
        let tmp_write_path_registered = register_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            mode,
            write_path_existed,
        )
        .await?;
        prepare_empty_file(&write_path).await?;
        file_ops::sync_file_data(&write_path).await?;
        debug!(operation = "job.empty_file_prepared", job_id = %job.job_id, path = %write_path, "empty destination file prepared");
        if let Err(e) = abort_cancelled_publish(
            job_status_cache,
            cancelled_jobs,
            &job.job_id,
            dest_path,
            mode,
            "publishing empty file",
        )
        .await
        {
            unregister_tmp_write_path(
                transfer_manager_pool,
                &job.job_id,
                &write_path,
                tmp_write_path_registered,
            )
            .await;
            return Err(e);
        }
        finalize_file(dest_path, mode).await?;
        unregister_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            tmp_write_path_registered,
        )
        .await;
        apply_destination_permissions(file.mode, dest_path);
        file_ops::sync_parent_directory(dest_path).await?;
        let state = new_transfer_state_for_current_source(&file_path_str, file, 0);
        if let Err(e) = save_completed_transfer_checkpoint(
            transfer_manager_pool,
            &job.job_id,
            &state,
            dest_path,
        )
        .await
        {
            warn!(operation = "job.transfer_checkpoint_save_failed", job_id = %job.job_id, error = %e, "completed transfer checkpoint save failed");
        }
        return Ok(());
    }

    let dedup_source_path = resolve_dedup_source_path(dest_path, &write_path, mode);
    let tmp_write_path_registered = register_tmp_write_path(
        transfer_manager_pool,
        &job.job_id,
        &write_path,
        mode,
        write_path_existed,
    )
    .await?;

    let source_path_buf = std::path::Path::new(source_path);
    let mut chunks = transfer::chunk_file(config, source_path_buf, connection).await?;
    debug!(operation = "job.file_chunked", job_id = %job.job_id, path = %source_path, chunk_count = chunks.len(), mode = ?mode, "source file chunked");

    let (existing_strong_hashes, dedup_count, local_chunk_info, global_chunk_info) =
        if job.job_type.is_full() {
            debug!(
                operation = "job.dedup_skipped",
                job_id = %job.job_id,
                path = %source_path,
                reason = "full_mode",
                "file deduplication skipped"
            );
            (
                HashSet::<[u8; 32]>::new(),
                0usize,
                HashMap::<[u8; 32], Vec<crate::application::sync::engine::ChunkLocation>>::new(),
                HashMap::<[u8; 32], Vec<crate::application::sync::engine::ChunkLocation>>::new(),
            )
        } else {
            let (
                existing_strong_hashes,
                _existing_weak_hashes,
                dedup_count,
                local_chunk_info,
                global_chunk_info,
            ) = dedup::check_deduplication(
                config,
                &mut chunks,
                connection,
                file,
                dedup_source_path,
                prefetch_manager,
                chunk_index,
                global_index,
            )
            .await?;
            (
                existing_strong_hashes,
                dedup_count,
                local_chunk_info,
                global_chunk_info,
            )
        };

    if dedup_count > 0 {
        debug!(
            operation = "job.dedup_completed",
            job_id = %job.job_id,
            reused_chunks = dedup_count,
            chunk_count = chunks.len(),
            "file deduplication completed"
        );
    }

    let mut state = load_transfer_state_for_current_source(
        transfer_manager_pool,
        &job.job_id,
        &file_path_str,
        file,
        &write_path,
        chunks.len(),
    )
    .await?;
    let cancel_callback: crate::application::sync::transfer::batch::CancelCallback = {
        let job_id = job.job_id.clone();
        let job_status_cache = job_status_cache.clone();
        let cancelled_jobs = cancelled_jobs.clone();
        Arc::new(move || is_job_cancelled(&job_status_cache, &cancelled_jobs, &job_id))
    };

    let transfer_result = transfer::batch_transfer(
        config,
        transfer_manager_pool,
        &job.job_id,
        &chunks,
        &mut state,
        connection,
        &existing_strong_hashes,
        &local_chunk_info,
        &global_chunk_info,
        &write_path,
        max_concurrent_streams,
        cancel_callback,
        on_batch_progress,
    )
    .await;

    if let Err(e) = transfer_result {
        if should_cleanup_tmp_after_transfer_error(&e) {
            cleanup_tmp_file(dest_path, mode).await;
            unregister_tmp_write_path(
                transfer_manager_pool,
                &job.job_id,
                &write_path,
                tmp_write_path_registered,
            )
            .await;
        } else if mode == ReplicateMode::Tmp {
            warn!(
                operation = "job.tmp_file_preserved",
                job_id = %job.job_id,
                path = %format!("{}.tmp", dest_path),
                error = %e,
                "temporary file preserved for retry"
            );
        }
        return Err(e);
    }

    if let Err(e) = abort_cancelled_publish(
        job_status_cache,
        cancelled_jobs,
        &job.job_id,
        dest_path,
        mode,
        "publishing transferred file",
    )
    .await
    {
        unregister_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            tmp_write_path_registered,
        )
        .await;
        return Err(e);
    }

    let write_path_buf = std::path::Path::new(&write_path);
    if write_path_buf.exists() {
        let dest_metadata = tokio::fs::metadata(write_path_buf).await?;
        if dest_metadata.len() != file.size {
            debug!(
                operation = "job.destination_size_adjusted",
                job_id = %job.job_id,
                previous_size = dest_metadata.len(),
                target_size = file.size,
                "destination file size adjusted"
            );
            let dest_file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(write_path_buf)
                .await?;
            dest_file.set_len(file.size).await?;
        }
        file_ops::sync_file_data(&write_path).await?;
    }

    if let Err(e) = abort_cancelled_publish(
        job_status_cache,
        cancelled_jobs,
        &job.job_id,
        dest_path,
        mode,
        "finalizing tmp file",
    )
    .await
    {
        unregister_tmp_write_path(
            transfer_manager_pool,
            &job.job_id,
            &write_path,
            tmp_write_path_registered,
        )
        .await;
        return Err(e);
    }

    finalize_file(dest_path, mode).await?;
    unregister_tmp_write_path(
        transfer_manager_pool,
        &job.job_id,
        &write_path,
        tmp_write_path_registered,
    )
    .await;

    apply_destination_permissions(file.mode, dest_path);
    file_ops::sync_parent_directory(dest_path).await?;

    if let Some(gindex) = global_index {
        if let Err(e) = update_global_index(config, gindex, dest_path).await {
            warn!(operation = "job.global_index_update_failed", job_id = %job.job_id, error = %e, "global chunk index update failed");
        }
    }

    if let Err(e) =
        save_completed_transfer_checkpoint(transfer_manager_pool, &job.job_id, &state, dest_path)
            .await
    {
        warn!(operation = "job.transfer_checkpoint_save_failed", job_id = %job.job_id, error = %e, "completed transfer checkpoint save failed");
    }

    Ok(())
}

async fn update_global_index(
    config: &SchedulerConfig,
    global_index: &Arc<crate::application::sync::engine::ChunkIndex>,
    file_path: &str,
) -> Result<()> {
    use crate::shared::cdc::{StreamingFastCDC, StreamingFastCDCConfig};

    let metadata = tokio::fs::metadata(file_path).await?;
    let mtime = metadata_mtime_nanos(&metadata);
    let file_size = metadata.len();

    let cdc_config = StreamingFastCDCConfig {
        min_chunk_size: config.min_chunk_size,
        avg_chunk_size: config.avg_chunk_size,
        max_chunk_size: config.max_chunk_size,
        window_size: 256 * 1024 * 1024,
    };
    let cdc = StreamingFastCDC::new(cdc_config);

    let chunk_entries = cdc.chunk_file(std::path::Path::new(file_path)).await?;

    let chunk_infos: Vec<crate::application::sync::engine::ChunkInfo> = chunk_entries
        .iter()
        .map(|entry| crate::application::sync::engine::ChunkInfo {
            offset: entry.offset,
            size: entry.length as u64,
            strong_hash: entry.hash,
            weak_hash: entry.weak_hash,
        })
        .collect();

    let indexed = global_index.batch_insert_chunks(file_path, &chunk_infos, mtime, file_size)?;

    debug!(
        operation = "job.global_index_updated",
        path = %file_path,
        indexed_chunks = indexed,
        "global chunk index updated"
    );

    Ok(())
}
