fn has_active_scan_filters(job: &SyncJob) -> bool {
    !job.exclude_regex.is_empty() || !job.include_regex.is_empty()
}

fn should_cleanup_deleted_targets(job: &SyncJob) -> bool {
    !has_active_scan_filters(job)
}

async fn ensure_directory_sync_root(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
) -> Result<()> {
    let root = super::sync_modes::single::resolve_base_dest_path(config, job)?;
    ensure_directory_path(&root).await
}

async fn ensure_directory_path(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => return Ok(()),
        Ok(_) => remove_destination_path(path).await?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(HarDataError::FileOperation(format!(
                "Failed to inspect destination root '{}': {}",
                path.display(),
                e
            )));
        }
    }

    tokio::fs::create_dir_all(path).await.map_err(|e| {
        HarDataError::FileOperation(format!(
            "Failed to create destination root '{}': {}",
            path.display(),
            e
        ))
    })?;

    Ok(())
}

async fn cleanup_deleted_targets(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    source_files: &[ScannedFile],
    source_is_single_file: bool,
    preserved_files: &HashSet<PathBuf>,
) -> Result<()> {
    if source_is_single_file {
        return Ok(());
    }

    let root = super::sync_modes::single::resolve_base_dest_path(config, job)?;
    if tokio::fs::symlink_metadata(&root).await.is_err() {
        return Ok(());
    }

    let (mut expected_files, mut expected_dirs) =
        build_expected_destination_entries(config, job, source_files)?;
    for path in preserved_files {
        let path = normalize_path(path);
        expected_files.insert(path.clone());
        if let Some(parent) = path.parent() {
            add_expected_dirs(parent, &root, &mut expected_dirs);
        }
    }
    prune_destination_tree(&root, &root, &expected_files, &expected_dirs).await
}

fn build_expected_destination_entries(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    source_files: &[ScannedFile],
) -> Result<(HashSet<PathBuf>, HashSet<PathBuf>)> {
    let root = super::sync_modes::single::resolve_base_dest_path(config, job)?;
    let mut expected_files = HashSet::new();
    let mut expected_dirs = HashSet::from([root.clone()]);
    let files_len = source_files.len();

    for file in source_files {
        let source_file_path = file.path.to_string_lossy().to_string();
        let relative_path = relative_source_path(job, &source_file_path);
        let dest_path = PathBuf::from(super::sync_modes::single::calculate_dest_path(
            config,
            job,
            &relative_path,
            files_len,
        )?);

        if file.is_dir {
            add_expected_dirs(&dest_path, &root, &mut expected_dirs);
        } else {
            expected_files.insert(dest_path.clone());
            if let Some(parent) = dest_path.parent() {
                add_expected_dirs(parent, &root, &mut expected_dirs);
            }
        }
    }

    Ok((expected_files, expected_dirs))
}

fn add_expected_dirs(path: &Path, root: &Path, expected_dirs: &mut HashSet<PathBuf>) {
    let root = normalize_path(root);
    let mut current = Some(normalize_path(path));

    while let Some(dir) = current {
        expected_dirs.insert(dir.clone());
        if dir == root {
            break;
        }
        current = dir.parent().map(normalize_path);
    }
}

fn relative_source_path(job: &SyncJob, source_file_path: &str) -> String {
    let source_str = job.source.to_string_lossy();
    source_file_path
        .strip_prefix(source_str.trim_end_matches('/'))
        .unwrap_or(source_file_path)
        .trim_start_matches('/')
        .to_string()
}

async fn inspect_destination_sync_state(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    file: &ScannedFile,
    files_len: usize,
    cached_dest_mtime: Option<i64>,
    cached_dest_change_time: Option<i64>,
    cached_dest_inode: Option<u64>,
) -> Result<DestinationSyncState> {
    let source_file_path = file.path.to_string_lossy().to_string();
    let relative_path = relative_source_path(job, &source_file_path);
    let dest_path = PathBuf::from(super::sync_modes::single::calculate_dest_path(
        config,
        job,
        &relative_path,
        files_len,
    )?);

    let metadata = match tokio::fs::symlink_metadata(&dest_path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(
                operation = "job.destination_resync_required",
                job_id = %job.job_id,
                source = %file.path.display(),
                destination = %dest_path.display(),
                reason = "destination_missing",
                "destination resync required"
            );
            return Ok(DestinationSyncState {
                requires_sync: true,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            });
        }
        Err(e) => {
            return Err(HarDataError::FileOperation(format!(
                "Failed to inspect destination '{}' for job {}: {}",
                dest_path.display(),
                job.job_id,
                e
            )));
        }
    };

    if file.is_dir {
        let permission_drifted = destination_permissions_drifted(&metadata, file.mode);
        let needs_sync =
            !metadata.is_dir() || metadata.file_type().is_symlink() || permission_drifted;
        if needs_sync {
            if permission_drifted {
                info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "directory_permissions_drifted", "destination resync required");
            } else {
                info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "directory_drifted", "destination resync required");
            }
        }
        return Ok(DestinationSyncState {
            requires_sync: needs_sync,
            dest_mtime: None,
            dest_change_time: None,
            dest_inode: None,
        });
    }

    if file.is_symlink {
        if !metadata.file_type().is_symlink() {
            info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "symlink_type_changed", "destination resync required");
            return Ok(DestinationSyncState {
                requires_sync: true,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            });
        }

        let current_target = tokio::fs::read_link(&dest_path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to read symlink target '{}' for job {}: {}",
                dest_path.display(),
                job.job_id,
                e
            ))
        })?;
        let current_target = current_target.to_string_lossy().to_string();
        let expected_target = file.symlink_target.as_deref().unwrap_or("");
        let needs_sync = current_target != expected_target;
        if needs_sync {
            info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "symlink_target_drifted", "destination resync required");
        }
        return Ok(DestinationSyncState {
            requires_sync: needs_sync,
            dest_mtime: None,
            dest_change_time: None,
            dest_inode: None,
        });
    }

    let current_dest_mtime = metadata_mtime_nanos(&metadata);
    let current_dest_change_time = metadata_ctime_nanos(&metadata);
    let current_dest_inode = metadata_inode(&metadata);
    let mtime_drifted = cached_dest_mtime
        .map(|dest_mtime| !timestamps_match(dest_mtime, current_dest_mtime))
        .unwrap_or(false);
    let change_time_drifted = current_dest_change_time.is_some()
        && !optional_timestamps_match(cached_dest_change_time, current_dest_change_time);
    let inode_drifted = current_dest_inode.is_some() && cached_dest_inode != current_dest_inode;
    let permission_drifted = destination_permissions_drifted(&metadata, file.mode);
    let needs_sync = !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != file.size
        || mtime_drifted
        || change_time_drifted
        || inode_drifted
        || permission_drifted;
    if needs_sync {
        if mtime_drifted {
            info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "mtime_drifted", "destination resync required");
        } else if change_time_drifted || inode_drifted {
            info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "identity_drifted", "destination resync required");
        } else if permission_drifted {
            info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "permissions_drifted", "destination resync required");
        } else {
            info!(operation = "job.destination_resync_required", job_id = %job.job_id, destination = %dest_path.display(), reason = "file_drifted", "destination resync required");
        }
    }
    Ok(DestinationSyncState {
        requires_sync: needs_sync,
        dest_mtime: Some(current_dest_mtime),
        dest_change_time: current_dest_change_time,
        dest_inode: current_dest_inode,
    })
}

#[cfg(unix)]
fn destination_permissions_drifted(metadata: &std::fs::Metadata, expected_mode: u32) -> bool {
    use std::os::unix::fs::PermissionsExt;

    let expected_permissions = expected_mode & 0o7777;
    if expected_permissions == 0 {
        return false;
    }

    let current_permissions = metadata.permissions().mode() & 0o7777;
    current_permissions != expected_permissions
}

#[cfg(not(unix))]
fn destination_permissions_drifted(_metadata: &std::fs::Metadata, _expected_mode: u32) -> bool {
    false
}

async fn load_destination_cache_state(
    config: &super::infrastructure::config::SchedulerConfig,
    job: &SyncJob,
    file: &ScannedFile,
    files_len: usize,
) -> DestinationSyncState {
    if file.is_dir || file.is_symlink {
        return DestinationSyncState {
            requires_sync: false,
            dest_mtime: None,
            dest_change_time: None,
            dest_inode: None,
        };
    }

    let source_file_path = file.path.to_string_lossy().to_string();
    let relative_path = relative_source_path(job, &source_file_path);
    let dest_path = match super::sync_modes::single::calculate_dest_path(
        config,
        job,
        &relative_path,
        files_len,
    ) {
        Ok(path) => PathBuf::from(path),
        Err(e) => {
            warn!(
                operation = "job.destination_cache_path_failed",
                job_id = %job.job_id,
                source = %file.path.display(),
                error = %e,
                "destination cache path resolution failed"
            );
            return DestinationSyncState {
                requires_sync: false,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            };
        }
    };

    match tokio::fs::symlink_metadata(&dest_path).await {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            DestinationSyncState {
                requires_sync: false,
                dest_mtime: Some(metadata_mtime_nanos(&metadata)),
                dest_change_time: metadata_ctime_nanos(&metadata),
                dest_inode: metadata_inode(&metadata),
            }
        }
        Ok(_) => {
            warn!(
                operation = "job.destination_cache_skipped",
                job_id = %job.job_id,
                path = %dest_path.display(),
                reason = "not_regular_file",
                "destination metadata cache skipped"
            );
            DestinationSyncState {
                requires_sync: false,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            }
        }
        Err(e) => {
            warn!(
                operation = "job.destination_metadata_read_failed",
                job_id = %job.job_id,
                path = %dest_path.display(),
                error = %e,
                "destination metadata read failed"
            );
            DestinationSyncState {
                requires_sync: false,
                dest_mtime: None,
                dest_change_time: None,
                dest_inode: None,
            }
        }
    }
}

async fn trim_deleted_source_tracking(
    synced_files_cache: &std::sync::Arc<
        dashmap::DashMap<String, dashmap::DashMap<String, FileSyncState>>,
    >,
    size_freezers: &std::sync::Arc<dashmap::DashMap<String, std::sync::Arc<super::SizeFreezer>>>,
    job_id: &str,
    source_files: &[ScannedFile],
) {
    let current_paths: HashSet<String> = source_files
        .iter()
        .map(|file| file.path.to_string_lossy().to_string())
        .collect();

    if let Some(job_cache) = synced_files_cache.get(job_id) {
        let stale_paths: Vec<String> = job_cache
            .iter()
            .filter_map(|entry| {
                if current_paths.contains(entry.key()) {
                    None
                } else {
                    Some(entry.key().clone())
                }
            })
            .collect();

        for path in stale_paths {
            job_cache.remove(&path);
        }
    }

    if let Some(size_freezer) = size_freezers.get(job_id) {
        let removed = size_freezer.retain_paths(&current_paths).await;
        if removed > 0 {
            info!(operation = "job.stability_entries_pruned", job_id = %job_id, removed_entries = removed, reason = "source_deleted", "stale stability entries pruned");
        }
    }
}

fn next_sync_schedule_delay(
    scan_interval: std::time::Duration,
    stability_threshold: std::time::Duration,
    retry_due_to_stability: bool,
) -> std::time::Duration {
    if !retry_due_to_stability {
        return scan_interval;
    }

    let stability_delay = if stability_threshold.is_zero() {
        std::time::Duration::from_millis(MIN_STABILITY_RETRY_DELAY_MS)
    } else {
        stability_threshold
    };
    scan_interval.min(stability_delay)
}

fn pending_stability_file_count(scanned_file_count: usize, stable_file_count: usize) -> usize {
    scanned_file_count.saturating_sub(stable_file_count)
}

fn prune_destination_tree<'a>(
    current: &'a Path,
    root: &'a Path,
    expected_files: &'a HashSet<PathBuf>,
    expected_dirs: &'a HashSet<PathBuf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let metadata = match tokio::fs::symlink_metadata(current).await {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(HarDataError::FileOperation(format!(
                    "Failed to inspect destination path '{}': {}",
                    current.display(),
                    e
                )));
            }
        };

        let current_path = normalize_path(current);
        let root_path = normalize_path(root);
        let is_symlink = metadata.file_type().is_symlink();

        if is_symlink || metadata.is_file() {
            if !expected_files.contains(&current_path) {
                remove_destination_path(current).await?;
                info!(
                    operation = "job.stale_file_removed",
                    path = %current.display(),
                    "stale target file removed"
                );
            }
            return Ok(());
        }

        if current_path != root_path && !expected_dirs.contains(&current_path) {
            remove_destination_path(current).await?;
            info!(
                operation = "job.stale_directory_removed",
                path = %current.display(),
                "stale target directory removed"
            );
            return Ok(());
        }

        let mut entries = tokio::fs::read_dir(current).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to read destination directory '{}': {}",
                current.display(),
                e
            ))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to read destination entry under '{}': {}",
                current.display(),
                e
            ))
        })? {
            let child_path = entry.path();
            prune_destination_tree(&child_path, root, expected_files, expected_dirs).await?;
        }

        Ok(())
    })
}

async fn remove_destination_path(path: &Path) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|e| {
        HarDataError::FileOperation(format!(
            "Failed to inspect path before removal '{}': {}",
            path.display(),
            e
        ))
    })?;

    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to remove directory '{}': {}",
                path.display(),
                e
            ))
        })?;
    } else {
        tokio::fs::remove_file(path).await.map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to remove file '{}': {}",
                path.display(),
                e
            ))
        })?;
    }

    Ok(())
}
