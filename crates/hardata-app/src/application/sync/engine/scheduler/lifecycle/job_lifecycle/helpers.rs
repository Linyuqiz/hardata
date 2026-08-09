fn snapshot_status(job: Option<Job>) -> Option<JobStatus> {
    job.map(|job| job.status)
}

fn is_final_job_pair(job_id: &str, other_job_id: &str) -> bool {
    if let Some(base_job_id) = job_id.strip_suffix("_final") {
        return other_job_id == base_job_id;
    }

    if let Some(base_job_id) = other_job_id.strip_suffix("_final") {
        return job_id == base_job_id;
    }

    false
}

fn destinations_overlap(left: &std::path::Path, right: &std::path::Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn sync_job_from_snapshot(job: &Job) -> SyncJob {
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
    sync_job
}
