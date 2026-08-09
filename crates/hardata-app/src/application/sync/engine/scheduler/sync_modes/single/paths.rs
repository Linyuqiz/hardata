pub fn calculate_dest_path(
    config: &SchedulerConfig,
    job: &SyncJob,
    relative_path: &str,
    files_len: usize,
) -> Result<String> {
    let has_relative_path = !relative_path.is_empty();
    let dest_ends_with_slash = job.dest.ends_with('/');
    let dest_is_dir = dest_ends_with_slash || has_relative_path || files_len > 1;

    let base_dest = resolve_base_dest_path(config, job)?;

    let result = if files_len == 1 && !dest_is_dir {
        base_dest
    } else {
        config.resolve_destination_path(
            base_dest
                .join(relative_path.trim_start_matches('/'))
                .to_string_lossy()
                .as_ref(),
        )?
    };

    tracing::debug!(
        operation = "job.destination_path_resolved",
        data_dir = %config.data_dir,
        job_destination = %job.dest,
        relative_path = %relative_path,
        result = %result.display(),
        "destination path resolved"
    );

    Ok(result.to_string_lossy().to_string())
}

pub fn resolve_base_dest_path(config: &SchedulerConfig, job: &SyncJob) -> Result<PathBuf> {
    config.resolve_runtime_destination_path(&job.dest)
}
