#[cfg(test)]
fn pagination_bounds(total: usize, page: usize, limit: usize) -> (usize, usize) {
    let start = page.saturating_mul(limit);
    let end = start.saturating_add(limit).min(total);
    (start.min(total), end)
}

#[cfg(test)]
fn summarize_statuses(
    statuses: impl IntoIterator<Item = hardata_app::domain::job::JobStatus>,
) -> StatsResponse {
    let mut stats = StatsResponse {
        total: 0,
        pending: 0,
        running: 0,
        paused: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
    };

    for status in statuses {
        adjust_stats_count(&mut stats, status, 1);
    }

    stats
}

fn stats_from_counts(counts: &HashMap<hardata_app::domain::job::JobStatus, i64>) -> StatsResponse {
    let mut stats = StatsResponse {
        total: 0,
        pending: 0,
        running: 0,
        paused: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
    };

    for (status, count) in counts {
        adjust_stats_count(&mut stats, *status, *count);
    }

    stats
}

fn adjust_stats_count(
    stats: &mut StatsResponse,
    status: hardata_app::domain::job::JobStatus,
    delta: i64,
) {
    use hardata_app::domain::job::JobStatus;

    stats.total += delta;
    match status {
        JobStatus::Pending => stats.pending += delta,
        JobStatus::Syncing => stats.running += delta,
        JobStatus::Paused => stats.paused += delta,
        JobStatus::Completed => stats.completed += delta,
        JobStatus::Failed => stats.failed += delta,
        JobStatus::Cancelled => stats.cancelled += delta,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicJobStatusView {
    status: hardata_app::domain::job::JobStatus,
    progress: u8,
    current_size: u64,
    total_size: u64,
    error_message: Option<String>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

fn resolve_public_job_status_view(
    job: &hardata_app::domain::Job,
    runtime_status: Option<&hardata_app::application::sync::engine::scheduler::JobRuntimeStatus>,
    final_runtime_status: Option<
        &hardata_app::application::sync::engine::scheduler::JobRuntimeStatus,
    >,
) -> PublicJobStatusView {
    let projected_runtime = if job.job_type.is_sync() {
        final_runtime_status.or(runtime_status)
    } else {
        runtime_status
    };

    if let Some(runtime) = projected_runtime {
        return PublicJobStatusView {
            status: runtime.status,
            progress: runtime.progress,
            current_size: runtime.current_size,
            total_size: runtime.total_size,
            error_message: runtime.error_message.clone(),
            updated_at: runtime.updated_at,
        };
    }

    PublicJobStatusView {
        status: job.status,
        progress: job.progress,
        current_size: job.current_size,
        total_size: job.total_size,
        error_message: job.error_message.clone(),
        updated_at: job.updated_at,
    }
}

fn project_public_job_snapshot(
    job: &hardata_app::domain::Job,
    status_view: &PublicJobStatusView,
) -> hardata_app::domain::Job {
    let mut projected = job.clone();
    projected.status = status_view.status;
    projected.progress = status_view.progress;
    projected.current_size = status_view.current_size;
    projected.total_size = status_view.total_size;
    projected.error_message = status_view.error_message.clone();
    projected.updated_at = status_view.updated_at;
    projected
}

fn resolve_public_job_runtime_fields(
    job: &hardata_app::domain::Job,
    runtime: Option<&hardata_app::application::sync::engine::job::SyncJob>,
    final_runtime: Option<&hardata_app::application::sync::engine::job::SyncJob>,
) -> (i64, bool, i32) {
    if let Some(runtime) = runtime {
        return (runtime.round_id, runtime.is_last_round, job.priority);
    }

    if job.job_type.is_sync() {
        if let Some(final_runtime) = final_runtime {
            return (final_runtime.round_id.max(1), true, job.priority);
        }
    }

    if job.round_id > 0 || job.is_last_round {
        return (
            job.round_id.max(i64::from(job.is_last_round)),
            job.is_last_round,
            job.priority,
        );
    }

    (0, false, job.priority)
}

fn is_internal_final_job_id(job_id: &str) -> bool {
    job_id.ends_with("_final")
}

fn internal_final_job_id(job_id: &str) -> String {
    format!("{job_id}_final")
}

fn resolve_public_job_id(job_id: &str) -> Result<&str, (StatusCode, String)> {
    if is_internal_final_job_id(job_id) {
        return Err((StatusCode::NOT_FOUND, format!("Job {} not found", job_id)));
    }

    Ok(job_id)
}

fn can_finalize_job_from_snapshot(
    job: &hardata_app::domain::Job,
    snapshot_statuses: &HashMap<String, hardata_app::domain::job::JobStatus>,
    retryable_job_ids: &HashSet<String>,
) -> bool {
    use hardata_app::domain::job::JobStatus;

    if !job.job_type.is_sync() || is_internal_final_job_id(&job.job_id) {
        return false;
    }

    let final_job_id = internal_final_job_id(&job.job_id);
    let final_status = snapshot_statuses.get(&final_job_id).copied();
    if final_status
        .map(|status| status.is_active())
        .unwrap_or(false)
        || retryable_job_ids.contains(&final_job_id)
    {
        return false;
    }

    match job.status {
        JobStatus::Pending | JobStatus::Syncing => true,
        JobStatus::Failed => {
            retryable_job_ids.contains(&job.job_id) || final_status == Some(JobStatus::Failed)
        }
        JobStatus::Paused | JobStatus::Completed | JobStatus::Cancelled => false,
    }
}

pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "hardata-sync"
    }))
}

pub async fn get_stats(
    State(state): State<SyncApiState>,
    headers: HeaderMap,
) -> Result<Json<StatsResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;

    let counts = state
        .scheduler
        .load_public_job_status_counts()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load job stats: {}", e),
            )
        })?;

    Ok(Json(stats_from_counts(&counts)))
}

pub async fn list_jobs(
    State(state): State<SyncApiState>,
    Query(query): Query<ListJobsQuery>,
    headers: HeaderMap,
) -> Result<Json<ListJobsResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let query = query.normalized();
    let (total, snapshots) = state
        .scheduler
        .load_public_jobs_snapshot_page(query.page, query.limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load jobs: {}", e),
            )
        })?;
    let resolved_page = resolve_list_jobs_page(total, query.page, query.limit);
    if snapshots.is_empty() {
        return Ok(Json(ListJobsResponse {
            total,
            page: resolved_page,
            limit: query.limit,
            jobs: vec![],
        }));
    }

    let retryable_job_ids = state
        .scheduler
        .load_retryable_job_ids()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load retryable jobs: {}", e),
            )
        })?;
    let mut snapshot_statuses: HashMap<String, hardata_app::domain::job::JobStatus> = snapshots
        .iter()
        .map(|job| (job.job_id.clone(), job.status))
        .collect();
    let final_job_ids = snapshots
        .iter()
        .filter(|job| job.job_type.is_sync())
        .map(|job| internal_final_job_id(&job.job_id))
        .collect::<Vec<_>>();
    let related_statuses = state
        .scheduler
        .load_resolved_job_statuses(&final_job_ids)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load related job statuses: {}", e),
            )
        })?;
    snapshot_statuses.extend(related_statuses);

    let mut jobs = Vec::with_capacity(snapshots.len());
    for job in snapshots {
        let final_job_id = job
            .job_type
            .is_sync()
            .then(|| internal_final_job_id(&job.job_id));
        let runtime = state.scheduler.get_job_info(&job.job_id);
        let runtime_status = state.scheduler.get_job_status(&job.job_id);
        let final_runtime = final_job_id
            .as_deref()
            .and_then(|job_id| state.scheduler.get_job_info(job_id));
        let final_runtime_status = final_job_id
            .as_deref()
            .and_then(|job_id| state.scheduler.get_job_status(job_id));
        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, runtime.as_ref(), final_runtime.as_ref());
        let status_view = resolve_public_job_status_view(
            &job,
            runtime_status.as_ref(),
            final_runtime_status.as_ref(),
        );
        let projected_job = project_public_job_snapshot(&job, &status_view);
        let can_cancel = state.scheduler.can_cancel_job_from_snapshot(
            &projected_job,
            &snapshot_statuses,
            &retryable_job_ids,
        );
        let can_finalize =
            can_finalize_job_from_snapshot(&job, &snapshot_statuses, &retryable_job_ids);
        jobs.push(JobSummary {
            can_cancel,
            can_finalize,
            current_size: status_view.current_size,
            total_size: status_view.total_size,
            error_message: status_view.error_message,
            source: JobPath {
                path: job.source.path,
                client_id: if job.source.client_id.is_empty() {
                    None
                } else {
                    Some(job.source.client_id)
                },
            },
            dest: JobPath {
                path: job.dest.path,
                client_id: if job.dest.client_id.is_empty() {
                    None
                } else {
                    Some(job.dest.client_id)
                },
            },
            round_id,
            is_last_round,
            priority,
            created_at: job.created_at.to_rfc3339(),
            updated_at: status_view.updated_at.to_rfc3339(),
            job_id: job.job_id,
            status: status_view.status.as_str().to_string(),
            progress: status_view.progress,
            region: job.region,
            job_type: job.job_type.as_str().to_string(),
        });
    }

    Ok(Json(ListJobsResponse {
        total,
        page: resolved_page,
        limit: query.limit,
        jobs,
    }))
}
