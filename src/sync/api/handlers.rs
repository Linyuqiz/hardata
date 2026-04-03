use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    http::{header, HeaderMap},
    response::{IntoResponse, Json},
};
use std::collections::{HashMap, HashSet};
use tracing::{error, info};

use super::types::{
    CreateJobRequest, CreateJobResponse, JobPath, JobStatusResponse, JobSummary, ListJobsQuery,
    ListJobsResponse, StatsResponse, SyncApiState,
};
use crate::core::JobType;
use crate::sync::engine::scheduler::{normalize_path, SchedulerConfig};
use crate::util::error::HarDataError;

const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";
const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;

fn extract_bearer_token(value: &str) -> Option<&str> {
    let mut parts = value.split_whitespace();
    let scheme = parts.next()?;
    let token = parts.next()?;

    if !scheme.eq_ignore_ascii_case("bearer") || parts.next().is_some() {
        return None;
    }

    Some(token)
}

fn authorize(headers: &HeaderMap, state: &SyncApiState) -> Result<(), (StatusCode, String)> {
    let Some(expected) = &state.api_token else {
        return Ok(());
    };

    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "missing Authorization header".to_string(),
            )
        })?;

    let token = extract_bearer_token(value).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            "invalid Authorization header".to_string(),
        )
    })?;
    if token != expected {
        return Err((StatusCode::UNAUTHORIZED, "invalid api token".to_string()));
    }

    Ok(())
}

fn normalize_idempotency_key(value: &str, source: &str) -> Result<String, (StatusCode, String)> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{source} must not be empty"),
        ));
    }

    if normalized.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{source} exceeds max length {}", MAX_IDEMPOTENCY_KEY_LEN),
        ));
    }

    Ok(normalized.to_string())
}

fn extract_create_job_idempotency_key(
    headers: &HeaderMap,
    request_id: Option<&str>,
) -> Result<Option<String>, (StatusCode, String)> {
    let header_key = match headers.get(IDEMPOTENCY_KEY_HEADER) {
        Some(value) => Some(normalize_idempotency_key(
            value.to_str().map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "Idempotency-Key header must be valid ASCII".to_string(),
                )
            })?,
            "Idempotency-Key header",
        )?),
        None => None,
    };
    let body_key = match request_id {
        Some(value) => Some(normalize_idempotency_key(value, "request_id")?),
        None => None,
    };

    match (header_key, body_key) {
        (Some(header_key), Some(body_key)) if header_key != body_key => Err((
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header must match request_id".to_string(),
        )),
        (Some(header_key), _) => Ok(Some(header_key)),
        (None, Some(body_key)) => Ok(Some(body_key)),
        (None, None) => Ok(None),
    }
}

fn create_job_request_fingerprint(
    req: &CreateJobRequest,
    job_type: JobType,
) -> Result<String, (StatusCode, String)> {
    fn canonicalize_job_path(path: &str) -> String {
        let normalized = normalize_path(std::path::Path::new(path));
        if normalized.as_os_str().is_empty() {
            ".".to_string()
        } else {
            normalized.to_string_lossy().to_string()
        }
    }

    fn canonicalize_patterns(patterns: &[String]) -> Vec<String> {
        let mut normalized = patterns.to_vec();
        normalized.sort();
        normalized.dedup();
        normalized
    }

    #[derive(serde::Serialize)]
    struct CreateJobFingerprint<'a> {
        source_path: &'a str,
        dest_path: &'a str,
        region: &'a str,
        priority: i32,
        job_type: &'a str,
        exclude_regex: &'a [String],
        include_regex: &'a [String],
    }

    let source_path = canonicalize_job_path(&req.source_path);
    let dest_path = canonicalize_job_path(&req.dest_path);
    let exclude_regex = canonicalize_patterns(&req.exclude_regex);
    let include_regex = canonicalize_patterns(&req.include_regex);
    let bytes = serde_json::to_vec(&CreateJobFingerprint {
        source_path: &source_path,
        dest_path: &dest_path,
        region: &req.region,
        priority: req.priority,
        job_type: job_type.as_str(),
        exclude_regex: &exclude_regex,
        include_regex: &include_regex,
    })
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to fingerprint create job request: {}", e),
        )
    })?;

    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn validate_job_path(path: &str, field: &str) -> Result<(), (StatusCode, String)> {
    if path.contains('\0') {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{field} contains invalid null byte"),
        ));
    }

    let invalid_component = std::path::Path::new(path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir));
    if invalid_component {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{field} must not contain '..'"),
        ));
    }

    Ok(())
}

fn validate_regex_patterns(patterns: &[String], field: &str) -> Result<(), (StatusCode, String)> {
    for pattern in patterns {
        regex::Regex::new(pattern).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("{field} contains invalid regex '{}': {}", pattern, e),
            )
        })?;
    }

    Ok(())
}

fn validate_destination_scope(
    dest_path: &str,
    data_dir: &str,
    allow_external_destinations: bool,
) -> Result<(), (StatusCode, String)> {
    let config = SchedulerConfig {
        data_dir: data_dir.to_string(),
        allow_external_destinations,
        ..SchedulerConfig::default()
    };

    config
        .resolve_destination_path(dest_path)
        .map(|_| ())
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("dest_path is not allowed: {}", e),
            )
        })
}

fn resolve_list_jobs_page(total: usize, page: usize, limit: usize) -> usize {
    if total == 0 {
        0
    } else {
        page.min(total.saturating_sub(1) / limit.max(1))
    }
}

#[cfg(test)]
fn pagination_bounds(total: usize, page: usize, limit: usize) -> (usize, usize) {
    let start = page.saturating_mul(limit);
    let end = start.saturating_add(limit).min(total);
    (start.min(total), end)
}

#[cfg(test)]
fn summarize_statuses(
    statuses: impl IntoIterator<Item = crate::core::job::JobStatus>,
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

fn stats_from_counts(counts: &HashMap<crate::core::job::JobStatus, i64>) -> StatsResponse {
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

fn adjust_stats_count(stats: &mut StatsResponse, status: crate::core::job::JobStatus, delta: i64) {
    use crate::core::job::JobStatus;

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
    status: crate::core::job::JobStatus,
    progress: u8,
    current_size: u64,
    total_size: u64,
    error_message: Option<String>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

fn resolve_public_job_status_view(
    job: &crate::core::Job,
    runtime_status: Option<&crate::sync::engine::scheduler::JobRuntimeStatus>,
    final_runtime_status: Option<&crate::sync::engine::scheduler::JobRuntimeStatus>,
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
    job: &crate::core::Job,
    status_view: &PublicJobStatusView,
) -> crate::core::Job {
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
    job: &crate::core::Job,
    runtime: Option<&crate::sync::engine::job::SyncJob>,
    final_runtime: Option<&crate::sync::engine::job::SyncJob>,
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
    job: &crate::core::Job,
    snapshot_statuses: &HashMap<String, crate::core::job::JobStatus>,
    retryable_job_ids: &HashSet<String>,
) -> bool {
    use crate::core::job::JobStatus;

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
    let mut snapshot_statuses: HashMap<String, crate::core::job::JobStatus> = snapshots
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

pub async fn create_job(
    State(state): State<SyncApiState>,
    headers: HeaderMap,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let idempotency_key = extract_create_job_idempotency_key(&headers, req.request_id.as_deref())?;
    if req.source_path.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "source_path is required".to_string(),
        ));
    }
    if req.dest_path.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "dest_path is required".to_string()));
    }
    if req.region.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "region is required".to_string()));
    }
    validate_job_path(&req.source_path, "source_path")?;
    validate_job_path(&req.dest_path, "dest_path")?;
    validate_destination_scope(
        &req.dest_path,
        &state.data_dir,
        state.allow_external_destinations,
    )?;
    validate_regex_patterns(&req.exclude_regex, "exclude_regex")?;
    validate_regex_patterns(&req.include_regex, "include_regex")?;

    if !state.regions.iter().any(|r| r.name == req.region) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Invalid region '{}'", req.region),
        ));
    }

    let job_type_lower = req.job_type.to_lowercase();
    if job_type_lower != "once" && job_type_lower != "sync" && job_type_lower != "full" {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid job_type '{}', must be 'once', 'full' or 'sync'",
                req.job_type
            ),
        ));
    }

    let job_type = JobType::parse(&req.job_type);
    let request_fingerprint = match idempotency_key.as_deref() {
        Some(_) => Some(create_job_request_fingerprint(&req, job_type)?),
        None => None,
    };
    let mut reused_existing_job = false;
    let job_id = if let (Some(idempotency_key), Some(request_fingerprint)) =
        (idempotency_key.as_deref(), request_fingerprint.as_deref())
    {
        let candidate_job_id = uuid::Uuid::new_v4().to_string();
        let record = state
            .scheduler
            .reserve_create_job_idempotency_key(
                idempotency_key,
                request_fingerprint,
                &candidate_job_id,
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to reserve idempotency key: {}", e),
                )
            })?;
        if record.request_fingerprint != request_fingerprint {
            return Err((
                StatusCode::CONFLICT,
                format!(
                    "Idempotency key '{}' is already used for a different create_job request",
                    idempotency_key
                ),
            ));
        }

        reused_existing_job = state
            .scheduler
            .load_job_snapshot(&record.job_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to load idempotent job: {}", e),
                )
            })?
            .is_some();
        record.job_id
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    info!(
        "Received job creation request: source={}, dest={}, region={}, type={}, job_id={}",
        req.source_path,
        req.dest_path,
        req.region,
        job_type.as_str(),
        job_id
    );

    if reused_existing_job {
        info!("Reused existing idempotent create job {}", job_id);
        return Ok(Json(CreateJobResponse { job_id }));
    }

    let sync_job = crate::sync::engine::job::SyncJob::new(
        job_id.clone(),
        std::path::PathBuf::from(&req.source_path),
        req.dest_path.clone(),
        req.region,
    )
    .with_priority(req.priority)
    .with_filters(req.exclude_regex, req.include_regex)
    .with_job_type(job_type);

    match state.scheduler.submit_job(sync_job).await {
        Ok(_) => {
            info!("Job {} submitted successfully", job_id);
            Ok(Json(CreateJobResponse { job_id }))
        }
        Err(e) => {
            if idempotency_key.is_some()
                && state
                    .scheduler
                    .load_job_snapshot(&job_id)
                    .await
                    .map_err(|load_error| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load idempotent job after submit: {}", load_error),
                        )
                    })?
                    .is_some()
            {
                info!("Reused concurrently created idempotent job {}", job_id);
                return Ok(Json(CreateJobResponse { job_id }));
            }
            let status = map_create_job_error_status(&e);
            error!("Failed to submit job {}: {}", job_id, e);
            Err((status, format!("Failed to submit job: {}", e)))
        }
    }
}

pub async fn get_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<JobStatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let public_job_id = resolve_public_job_id(&job_id)?;
    let snapshot = state
        .scheduler
        .load_job_snapshot(public_job_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load job: {}", e),
            )
        })?;
    let runtime = state.scheduler.get_job_info(public_job_id);

    match snapshot {
        Some(job) => {
            let final_job_id = job
                .job_type
                .is_sync()
                .then(|| internal_final_job_id(&job.job_id));
            let runtime_status = state.scheduler.get_job_status(public_job_id);
            let final_runtime = final_job_id
                .as_deref()
                .and_then(|job_id| state.scheduler.get_job_info(job_id));
            let final_runtime_status = final_job_id
                .as_deref()
                .and_then(|job_id| state.scheduler.get_job_status(job_id));
            let mut snapshot_statuses = HashMap::from([(job.job_id.clone(), job.status)]);
            let retryable_job_ids = if job.job_type.is_sync() {
                state
                    .scheduler
                    .load_retryable_job_ids()
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load retryable jobs: {}", e),
                        )
                    })?
            } else {
                HashSet::new()
            };
            if let Some(final_job_id) = final_job_id.as_deref() {
                let related_statuses = state
                    .scheduler
                    .load_resolved_job_statuses(&[final_job_id.to_string()])
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load related job statuses: {}", e),
                        )
                    })?;
                snapshot_statuses.extend(related_statuses);
            }
            let (round_id, is_last_round, priority) =
                resolve_public_job_runtime_fields(&job, runtime.as_ref(), final_runtime.as_ref());
            let status_view = resolve_public_job_status_view(
                &job,
                runtime_status.as_ref(),
                final_runtime_status.as_ref(),
            );
            let projected_job = project_public_job_snapshot(&job, &status_view);
            let can_finalize =
                can_finalize_job_from_snapshot(&job, &snapshot_statuses, &retryable_job_ids);
            Ok(Json(JobStatusResponse {
                job_id: job.job_id,
                status: status_view.status.as_str().to_string(),
                error_message: status_view.error_message,
                can_cancel: state.scheduler.can_cancel_job_from_snapshot(
                    &projected_job,
                    &snapshot_statuses,
                    &retryable_job_ids,
                ),
                can_finalize,
                progress: status_view.progress,
                current_size: status_view.current_size,
                total_size: status_view.total_size,
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
                region: job.region,
                job_type: job.job_type.as_str().to_string(),
                round_id,
                is_last_round,
                priority,
                created_at: job.created_at.to_rfc3339(),
                updated_at: status_view.updated_at.to_rfc3339(),
            }))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            format!("Job {} not found", public_job_id),
        )),
    }
}

pub async fn finalize_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<CreateJobResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let job_id = resolve_public_job_id(&job_id)?.to_string();
    info!("Received finalize request for job: {}", job_id);

    match state.scheduler.finalize_job(&job_id).await {
        Ok(_) => {
            let final_job_id = internal_final_job_id(&job_id);
            info!("Final transfer job {} is ready", final_job_id);
            Ok(Json(CreateJobResponse { job_id }))
        }
        Err(e) => {
            error!("Failed to finalize job {}: {}", job_id, e);
            Err((
                map_finalize_error_status(&e),
                format!("Failed to finalize job: {}", e),
            ))
        }
    }
}

pub async fn cancel_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Result<(), (StatusCode, String)> {
    authorize(&headers, &state)?;
    let public_job_id = resolve_public_job_id(&job_id)?.to_string();
    info!(
        "Received cancel request for job: {} (public id: {})",
        job_id, public_job_id
    );

    match state.scheduler.cancel_job(&public_job_id).await {
        Ok(_) => {
            info!("Job {} cancelled successfully", public_job_id);
            Ok(())
        }
        Err(e) => {
            error!("Failed to cancel job {}: {}", public_job_id, e);
            Err((
                map_cancel_error_status(&e),
                format!("Failed to cancel job: {}", e),
            ))
        }
    }
}

fn map_finalize_error_status(error: &HarDataError) -> StatusCode {
    match error {
        HarDataError::InvalidConfig(message) if message.contains("overlaps active job") => {
            StatusCode::CONFLICT
        }
        HarDataError::JobNotFound(_) => StatusCode::NOT_FOUND,
        HarDataError::InvalidConfig(_)
        | HarDataError::InvalidProtocol(_)
        | HarDataError::FileOperation(_) => StatusCode::BAD_REQUEST,
        HarDataError::Unknown(message) if message.contains("not found") => StatusCode::NOT_FOUND,
        HarDataError::Unknown(message) if message.contains("still shutting down") => {
            StatusCode::CONFLICT
        }
        HarDataError::Unknown(message)
            if message.contains("already has active final transfer")
                || message.contains("already active with status") =>
        {
            StatusCode::CONFLICT
        }
        HarDataError::Unknown(message)
            if message.contains("not a sync job") || message.contains("cannot be finalized") =>
        {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn map_create_job_error_status(error: &HarDataError) -> StatusCode {
    match error {
        HarDataError::InvalidConfig(message)
            if message.contains("already used by active job")
                || message.contains("overlaps active job") =>
        {
            StatusCode::CONFLICT
        }
        HarDataError::InvalidConfig(_) | HarDataError::FileOperation(_) => StatusCode::BAD_REQUEST,
        HarDataError::Unknown(message)
            if message.contains("still shutting down")
                || message.contains("already active with status") =>
        {
            StatusCode::CONFLICT
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn map_cancel_error_status(error: &HarDataError) -> StatusCode {
    match error {
        HarDataError::JobNotFound(_) => StatusCode::NOT_FOUND,
        HarDataError::InvalidConfig(_) | HarDataError::FileOperation(_) => StatusCode::BAD_REQUEST,
        HarDataError::Unknown(message) if message.contains("not found") => StatusCode::NOT_FOUND,
        HarDataError::Unknown(message)
            if message.contains("already finished") || message.contains("already failed") =>
        {
            StatusCode::CONFLICT
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        can_finalize_job_from_snapshot, cancel_job, create_job, create_job_request_fingerprint,
        extract_bearer_token, extract_create_job_idempotency_key, finalize_job,
        internal_final_job_id, is_internal_final_job_id, map_cancel_error_status,
        map_create_job_error_status, map_finalize_error_status, pagination_bounds,
        project_public_job_snapshot, resolve_list_jobs_page, resolve_public_job_id,
        resolve_public_job_runtime_fields, resolve_public_job_status_view, stats_from_counts,
        summarize_statuses, validate_destination_scope, IDEMPOTENCY_KEY_HEADER,
    };
    use crate::core::job::JobStatus;
    use crate::core::{Job, JobPath, JobType};
    use crate::sync::api::types::CreateJobRequest;
    use crate::sync::api::types::SyncApiState;
    use crate::sync::engine::job::SyncJob;
    use crate::sync::engine::scheduler::SyncScheduler;
    use crate::sync::engine::scheduler::{JobRuntimeStatus, SchedulerConfig};
    use crate::sync::storage::db::Database;
    use crate::sync::RegionConfig;
    use crate::util::error::HarDataError;
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use axum::Json;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-api-handlers-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    async fn create_scheduler(
        label: &str,
    ) -> (std::path::PathBuf, Arc<Database>, Arc<SyncScheduler>) {
        let temp_dir = create_temp_dir(label);
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        fs::create_dir_all(&config.data_dir).unwrap();
        let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();
        (temp_dir, db, scheduler)
    }

    fn api_test_regions() -> Vec<RegionConfig> {
        vec![RegionConfig {
            name: "local".to_string(),
            quic_bind: "127.0.0.1:9443".to_string(),
            tcp_bind: "127.0.0.1:9444".to_string(),
        }]
    }

    #[test]
    fn pagination_bounds_handles_regular_page() {
        assert_eq!(pagination_bounds(10, 1, 3), (3, 6));
    }

    #[test]
    fn pagination_bounds_saturates_on_overflowing_page_product() {
        assert_eq!(pagination_bounds(10, usize::MAX, 2), (10, 10));
    }

    #[test]
    fn resolve_list_jobs_page_clamps_out_of_range_request_to_tail() {
        assert_eq!(resolve_list_jobs_page(0, 9, 100), 0);
        assert_eq!(resolve_list_jobs_page(201, 9, 100), 2);
        assert_eq!(resolve_list_jobs_page(201, 1, 100), 1);
    }

    #[test]
    fn validate_destination_scope_rejects_absolute_path_outside_data_dir() {
        let err = validate_destination_scope("/tmp/outside", "/srv/sync", false).unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
        assert!(err.1.contains("outside sync.data_dir"));
    }

    #[test]
    fn validate_destination_scope_allows_relative_path_inside_data_dir() {
        validate_destination_scope("jobs/output.bin", "/srv/sync", false).unwrap();
    }

    #[test]
    fn validate_destination_scope_allows_external_path_when_enabled() {
        validate_destination_scope("/tmp/outside", "/srv/sync", true).unwrap();
    }

    #[test]
    fn summarize_statuses_counts_cancelled_separately_from_failed() {
        let stats = summarize_statuses([
            JobStatus::Pending,
            JobStatus::Failed,
            JobStatus::Cancelled,
            JobStatus::Completed,
            JobStatus::Cancelled,
        ]);

        assert_eq!(stats.total, 5);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.cancelled, 2);
    }

    #[test]
    fn stats_from_counts_preserves_sparse_status_map() {
        let counts = HashMap::from([(JobStatus::Paused, 2), (JobStatus::Completed, 1)]);
        let stats = stats_from_counts(&counts);

        assert_eq!(stats.total, 3);
        assert_eq!(stats.paused, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.pending, 0);
    }

    #[test]
    fn extract_bearer_token_accepts_case_insensitive_scheme() {
        assert_eq!(extract_bearer_token("Bearer secret"), Some("secret"));
        assert_eq!(extract_bearer_token("bearer secret"), Some("secret"));
        assert_eq!(extract_bearer_token("BEARER secret"), Some("secret"));
    }

    #[test]
    fn extract_bearer_token_rejects_malformed_values() {
        assert_eq!(extract_bearer_token("Token secret"), None);
        assert_eq!(extract_bearer_token("Bearer"), None);
        assert_eq!(extract_bearer_token("Bearer secret extra"), None);
    }

    #[test]
    fn extract_create_job_idempotency_key_rejects_mismatched_header_and_body() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("header-key"),
        );

        let err = extract_create_job_idempotency_key(&headers, Some("body-key")).unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("must match request_id"));
    }

    #[test]
    fn create_job_request_fingerprint_ignores_request_id_field() {
        let request = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec!["ignored".to_string()],
            include_regex: vec!["included".to_string()],
            request_id: Some("request-a".to_string()),
        };
        let mut same_request = CreateJobRequest {
            request_id: Some("request-b".to_string()),
            ..request
        };

        let first = create_job_request_fingerprint(&same_request, JobType::Sync).unwrap();
        same_request.request_id = None;
        let second = create_job_request_fingerprint(&same_request, JobType::Sync).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn create_job_request_fingerprint_normalizes_filter_order_and_duplicates() {
        let first = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec!["b".to_string(), "a".to_string(), "a".to_string()],
            include_regex: vec!["keep-2".to_string(), "keep-1".to_string()],
            request_id: None,
        };
        let second = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec!["a".to_string(), "b".to_string()],
            include_regex: vec![
                "keep-1".to_string(),
                "keep-2".to_string(),
                "keep-1".to_string(),
            ],
            request_id: None,
        };

        let first_fingerprint = create_job_request_fingerprint(&first, JobType::Sync).unwrap();
        let second_fingerprint = create_job_request_fingerprint(&second, JobType::Sync).unwrap();

        assert_eq!(first_fingerprint, second_fingerprint);
    }

    #[test]
    fn create_job_request_fingerprint_normalizes_equivalent_path_syntax() {
        let first = CreateJobRequest {
            source_path: "/tmp//source.bin".to_string(),
            dest_path: "mirror/./nested//output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: None,
        };
        let second = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/nested/output.bin".to_string(),
            region: "local".to_string(),
            priority: 3,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: None,
        };

        let first_fingerprint = create_job_request_fingerprint(&first, JobType::Sync).unwrap();
        let second_fingerprint = create_job_request_fingerprint(&second, JobType::Sync).unwrap();

        assert_eq!(first_fingerprint, second_fingerprint);
    }

    #[test]
    fn resolve_public_job_id_rejects_internal_final_suffix() {
        let error = resolve_public_job_id("job-1_final").unwrap_err();
        assert_eq!(error.0, axum::http::StatusCode::NOT_FOUND);
        assert_eq!(error.1, "Job job-1_final not found");
    }

    #[test]
    fn resolve_public_job_id_keeps_regular_public_id() {
        assert_eq!(resolve_public_job_id("job-1").unwrap(), "job-1");
    }

    #[test]
    fn is_internal_final_job_id_only_matches_internal_suffix() {
        assert!(is_internal_final_job_id("job-1_final"));
        assert!(!is_internal_final_job_id("job-1"));
    }

    #[test]
    fn can_finalize_job_from_snapshot_allows_pending_sync_without_active_final() {
        let job = Job::new(
            "job-finalize-ready".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);

        assert!(can_finalize_job_from_snapshot(
            &job,
            &HashMap::new(),
            &HashSet::new()
        ));
    }

    #[test]
    fn can_finalize_job_from_snapshot_rejects_active_final_transfer() {
        let mut job = Job::new(
            "job-finalize-blocked".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Failed;

        let statuses = HashMap::from([(internal_final_job_id(&job.job_id), JobStatus::Pending)]);

        assert!(!can_finalize_job_from_snapshot(
            &job,
            &statuses,
            &HashSet::new()
        ));
    }

    #[test]
    fn can_finalize_job_from_snapshot_allows_retrying_terminal_failed_final_transfer() {
        let mut job = Job::new(
            "job-finalize-retry-final".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Failed;

        let statuses = HashMap::from([(internal_final_job_id(&job.job_id), JobStatus::Failed)]);

        assert!(can_finalize_job_from_snapshot(
            &job,
            &statuses,
            &HashSet::new()
        ));
    }

    #[test]
    fn internal_final_job_id_appends_internal_suffix() {
        assert_eq!(internal_final_job_id("job-1"), "job-1_final");
    }

    #[test]
    fn map_cancel_error_status_uses_internal_error_for_unexpected_failures() {
        let status = map_cancel_error_status(&HarDataError::Database(sqlx::Error::PoolClosed));
        assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn map_cancel_error_status_keeps_conflict_for_terminal_jobs() {
        let status = map_cancel_error_status(&HarDataError::Unknown(
            "Job 42 already failed and has no pending retry".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_create_job_error_status_uses_conflict_for_active_destination() {
        let status = map_create_job_error_status(&HarDataError::InvalidConfig(
            "destination '/srv/sync/output.bin' overlaps active job job-1 destination '/srv/sync/output'".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_create_job_error_status_keeps_bad_request_for_invalid_input() {
        let status = map_create_job_error_status(&HarDataError::InvalidConfig(
            "Invalid region 'missing'".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn map_create_job_error_status_uses_conflict_for_duplicate_active_runtime() {
        let status = map_create_job_error_status(&HarDataError::Unknown(
            "Job job-1 is already active with status Pending".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_finalize_error_status_uses_conflict_for_active_destination_overlap() {
        let status = map_finalize_error_status(&HarDataError::InvalidConfig(
            "destination '/srv/sync/output.bin' overlaps active job job-1 destination '/srv/sync/output'".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_finalize_error_status_uses_conflict_for_duplicate_active_final_transfer() {
        let status = map_finalize_error_status(&HarDataError::Unknown(
            "Job job-1 already has active final transfer job-1_final".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn map_finalize_error_status_uses_conflict_for_duplicate_active_runtime() {
        let status = map_finalize_error_status(&HarDataError::Unknown(
            "Job job-1_final is already active with status Syncing".to_string(),
        ));
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn job_responses_serialize_error_message_only_when_present() {
        let with_error = serde_json::to_value(crate::sync::api::types::JobStatusResponse {
            job_id: "job-1".to_string(),
            status: "failed".to_string(),
            error_message: Some("disk full".to_string()),
            can_cancel: false,
            can_finalize: false,
            progress: 10,
            current_size: 10,
            total_size: 100,
            source: crate::sync::api::types::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: None,
            },
            dest: crate::sync::api::types::JobPath {
                path: "dest.bin".to_string(),
                client_id: None,
            },
            region: "local".to_string(),
            job_type: "once".to_string(),
            round_id: 0,
            is_last_round: false,
            priority: 100,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:01Z".to_string(),
        })
        .unwrap();
        assert_eq!(with_error.get("error_message").unwrap(), "disk full");

        let without_error = serde_json::to_value(crate::sync::api::types::JobSummary {
            job_id: "job-2".to_string(),
            status: "completed".to_string(),
            error_message: None,
            can_cancel: false,
            can_finalize: false,
            progress: 100,
            current_size: 100,
            total_size: 100,
            source: crate::sync::api::types::JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: None,
            },
            dest: crate::sync::api::types::JobPath {
                path: "dest.bin".to_string(),
                client_id: None,
            },
            region: "local".to_string(),
            job_type: "once".to_string(),
            round_id: 0,
            is_last_round: false,
            priority: 100,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:01Z".to_string(),
        })
        .unwrap();
        assert!(without_error.get("error_message").is_none());
    }

    #[test]
    fn resolve_public_job_runtime_fields_prefers_original_runtime() {
        let job = Job::new(
            "job-runtime".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        let mut runtime = SyncJob::new(
            job.job_id.clone(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(JobType::Sync)
        .with_priority(99);
        runtime.round_id = 4;
        runtime.is_last_round = false;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, Some(&runtime), None);

        assert_eq!(round_id, 4);
        assert!(!is_last_round);
        assert_eq!(priority, job.priority);
    }

    #[test]
    fn resolve_public_job_runtime_fields_falls_back_to_final_runtime() {
        let mut job = Job::new(
            "job-final-fallback".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.priority = 7;

        let mut final_runtime = SyncJob::new(
            "job-final-fallback_final".to_string(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(JobType::Once)
        .with_priority(107);
        final_runtime.round_id = 3;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, None, Some(&final_runtime));

        assert_eq!(round_id, 3);
        assert!(is_last_round);
        assert_eq!(priority, 7);
    }

    #[test]
    fn resolve_public_job_runtime_fields_prefers_final_runtime_over_stale_persisted_metadata() {
        let mut job = Job::new(
            "job-final-stale-metadata".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.priority = 13;
        job.round_id = 1;
        job.is_last_round = false;

        let mut final_runtime = SyncJob::new(
            "job-final-stale-metadata_final".to_string(),
            std::path::PathBuf::from(&job.source.path),
            job.dest.path.clone(),
            job.region.clone(),
        )
        .with_job_type(JobType::Once)
        .with_priority(113);
        final_runtime.round_id = 2;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, None, Some(&final_runtime));

        assert_eq!(round_id, 2);
        assert!(is_last_round);
        assert_eq!(priority, 13);
    }

    #[test]
    fn resolve_public_job_runtime_fields_falls_back_to_persisted_job_metadata() {
        let mut job = Job::new(
            "job-persisted-round".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.priority = 11;
        job.round_id = 5;
        job.is_last_round = true;

        let (round_id, is_last_round, priority) =
            resolve_public_job_runtime_fields(&job, None, None);

        assert_eq!(round_id, 5);
        assert!(is_last_round);
        assert_eq!(priority, 11);
    }

    #[test]
    fn resolve_public_job_status_view_projects_active_final_runtime() {
        let mut job = Job::new(
            "job-final-status".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Cancelled;
        job.progress = 5;
        job.current_size = 5;
        job.total_size = 100;
        job.error_message = Some("stale cancelled".to_string());

        let now = chrono::Utc::now();
        let original_runtime = JobRuntimeStatus {
            job_id: job.job_id.clone(),
            status: JobStatus::Cancelled,
            progress: 5,
            current_size: 5,
            total_size: 100,
            region: job.region.clone(),
            error_message: Some("stale cancelled".to_string()),
            created_at: now,
            updated_at: now,
        };
        let final_runtime = JobRuntimeStatus {
            job_id: internal_final_job_id(&job.job_id),
            status: JobStatus::Syncing,
            progress: 63,
            current_size: 630,
            total_size: 1000,
            region: job.region.clone(),
            error_message: None,
            created_at: now,
            updated_at: now + chrono::Duration::seconds(5),
        };

        let view =
            resolve_public_job_status_view(&job, Some(&original_runtime), Some(&final_runtime));

        assert_eq!(view.status, JobStatus::Syncing);
        assert_eq!(view.progress, 63);
        assert_eq!(view.current_size, 630);
        assert_eq!(view.total_size, 1000);
        assert_eq!(view.error_message, None);
        assert_eq!(view.updated_at, final_runtime.updated_at);
    }

    #[test]
    fn resolve_public_job_status_view_uses_original_runtime_for_non_sync_jobs() {
        let job = Job::new(
            "job-once-status".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        );
        let now = chrono::Utc::now();
        let runtime = JobRuntimeStatus {
            job_id: job.job_id.clone(),
            status: JobStatus::Syncing,
            progress: 40,
            current_size: 40,
            total_size: 100,
            region: job.region.clone(),
            error_message: None,
            created_at: now,
            updated_at: now,
        };
        let final_runtime = JobRuntimeStatus {
            job_id: internal_final_job_id(&job.job_id),
            status: JobStatus::Failed,
            progress: 90,
            current_size: 90,
            total_size: 100,
            region: job.region.clone(),
            error_message: Some("ignored".to_string()),
            created_at: now,
            updated_at: now + chrono::Duration::seconds(5),
        };

        let view = resolve_public_job_status_view(&job, Some(&runtime), Some(&final_runtime));

        assert_eq!(view.status, JobStatus::Syncing);
        assert_eq!(view.progress, 40);
        assert_eq!(view.current_size, 40);
        assert_eq!(view.total_size, 100);
    }

    #[test]
    fn project_public_job_snapshot_overlays_resolved_status_fields() {
        let mut job = Job::new(
            "job-project-status".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = JobStatus::Cancelled;
        let updated_at = chrono::Utc::now() + chrono::Duration::seconds(7);
        let view = super::PublicJobStatusView {
            status: JobStatus::Syncing,
            progress: 88,
            current_size: 880,
            total_size: 1000,
            error_message: Some("network retry".to_string()),
            updated_at,
        };

        let projected = project_public_job_snapshot(&job, &view);

        assert_eq!(projected.status, JobStatus::Syncing);
        assert_eq!(projected.progress, 88);
        assert_eq!(projected.current_size, 880);
        assert_eq!(projected.total_size, 1000);
        assert_eq!(projected.error_message.as_deref(), Some("network retry"));
        assert_eq!(projected.updated_at, updated_at);
    }

    #[tokio::test]
    async fn finalize_job_returns_public_job_id() {
        let (temp_dir, db, scheduler) = create_scheduler("finalize-response").await;
        let mut job = Job::new(
            "job-finalize-response".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let Json(response) =
            finalize_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
                .await
                .unwrap();

        assert_eq!(response.job_id, public_job_id);
        assert!(scheduler
            .load_job_snapshot("job-finalize-response_final")
            .await
            .unwrap()
            .is_some());

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_reuses_job_id_for_same_idempotency_key() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-header").await;
        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let request = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 0,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-key"),
        );

        let Json(first_response) =
            create_job(State(state.clone()), headers.clone(), Json(request.clone()))
                .await
                .unwrap();
        let Json(second_response) = create_job(State(state), headers, Json(request))
            .await
            .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_rejects_reused_idempotency_key_with_different_payload() {
        let (temp_dir, _db, scheduler) = create_scheduler("create-idempotent-conflict").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-key"),
        );

        let _ = create_job(
            State(state.clone()),
            headers.clone(),
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap();

        let err = create_job(
            State(state),
            headers,
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/other-output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::CONFLICT);
        assert!(err
            .1
            .contains("already used for a different create_job request"));

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_request_id_field_is_idempotent_without_header() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-request-id").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let request = CreateJobRequest {
            source_path: "/tmp/source.bin".to_string(),
            dest_path: "mirror/output.bin".to_string(),
            region: "local".to_string(),
            priority: 0,
            job_type: "sync".to_string(),
            exclude_regex: vec![],
            include_regex: vec![],
            request_id: Some("request-idempotent".to_string()),
        };

        let Json(first_response) = create_job(
            State(state.clone()),
            HeaderMap::new(),
            Json(request.clone()),
        )
        .await
        .unwrap();
        let Json(second_response) = create_job(State(state), HeaderMap::new(), Json(request))
            .await
            .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_treats_reordered_filters_as_same_idempotent_request() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-filter-order").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-filter-order"),
        );

        let Json(first_response) = create_job(
            State(state.clone()),
            headers.clone(),
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec!["b".to_string(), "a".to_string()],
                include_regex: vec!["keep-2".to_string(), "keep-1".to_string()],
                request_id: None,
            }),
        )
        .await
        .unwrap();
        let Json(second_response) = create_job(
            State(state),
            headers,
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec!["a".to_string(), "b".to_string(), "a".to_string()],
                include_regex: vec![
                    "keep-1".to_string(),
                    "keep-2".to_string(),
                    "keep-1".to_string(),
                ],
                request_id: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn create_job_treats_equivalent_path_syntax_as_same_idempotent_request() {
        let (temp_dir, db, scheduler) = create_scheduler("create-idempotent-path-syntax").await;
        let state = SyncApiState {
            scheduler,
            regions: api_test_regions(),
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_static("create-idem-path-syntax"),
        );

        let Json(first_response) = create_job(
            State(state.clone()),
            headers.clone(),
            Json(CreateJobRequest {
                source_path: "/tmp//source.bin".to_string(),
                dest_path: "mirror/./nested//output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap();
        let Json(second_response) = create_job(
            State(state),
            headers,
            Json(CreateJobRequest {
                source_path: "/tmp/source.bin".to_string(),
                dest_path: "mirror/nested/output.bin".to_string(),
                region: "local".to_string(),
                priority: 0,
                job_type: "sync".to_string(),
                exclude_regex: vec![],
                include_regex: vec![],
                request_id: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(first_response.job_id, second_response.job_id);
        assert_eq!(db.count_public_jobs().await.unwrap(), 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_when_final_transfer_is_already_active() {
        let (temp_dir, db, scheduler) = create_scheduler("finalize-idempotent").await;
        let mut job = Job::new(
            "job-finalize-idempotent".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };

        let Json(first_response) = finalize_job(
            State(state.clone()),
            Path(public_job_id.clone()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let Json(second_response) =
            finalize_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
                .await
                .unwrap();

        assert_eq!(first_response.job_id, public_job_id);
        assert_eq!(second_response.job_id, public_job_id);
        assert!(scheduler
            .load_job_snapshot("job-finalize-idempotent_final")
            .await
            .unwrap()
            .is_some());

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn finalize_job_is_idempotent_after_final_transfer_completed() {
        let (temp_dir, db, scheduler) = create_scheduler("finalize-idempotent-completed").await;
        let mut job = Job::new(
            "job-finalize-idempotent-completed".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };

        let Json(first_response) = finalize_job(
            State(state.clone()),
            Path(public_job_id.clone()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert!(db
            .update_job_status(&public_job_id, JobStatus::Completed, 100, 1, 1, None)
            .await
            .unwrap());
        assert!(db
            .update_job_status(
                &internal_final_job_id(&public_job_id),
                JobStatus::Completed,
                100,
                1,
                1,
                None,
            )
            .await
            .unwrap());
        let Json(second_response) =
            finalize_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
                .await
                .unwrap();

        assert_eq!(first_response.job_id, public_job_id);
        assert_eq!(second_response.job_id, public_job_id);
        assert_eq!(
            scheduler
                .load_job_snapshot(&public_job_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
        assert_eq!(
            db.load_job(&internal_final_job_id(&public_job_id))
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_is_idempotent_when_job_is_already_cancelled() {
        let (temp_dir, db, scheduler) = create_scheduler("cancel-idempotent").await;
        let mut job = Job::new(
            "job-cancel-idempotent".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };

        cancel_job(
            State(state.clone()),
            Path(public_job_id.clone()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        cancel_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
            .await
            .unwrap();

        let snapshot = scheduler
            .load_job_snapshot(&public_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(temp_dir).unwrap();
    }
}
