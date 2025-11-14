use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use tracing::{error, info};

use super::types::{
    CreateJobRequest, CreateJobResponse, JobPath, JobStatusResponse, JobSummary, ListJobsQuery,
    ListJobsResponse, StatsResponse, SyncApiState,
};
use crate::core::JobType;

pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "hardata-sync"
    }))
}

pub async fn get_stats(State(state): State<SyncApiState>) -> impl IntoResponse {
    use crate::core::job::JobStatus;

    let mut total = 0i64;
    let mut pending = 0i64;
    let mut running = 0i64;
    let mut paused = 0i64;
    let mut completed = 0i64;
    let mut failed = 0i64;

    state.scheduler.for_each_job_status(|_job_id, status| {
        total += 1;
        match status.status {
            JobStatus::Pending => pending += 1,
            JobStatus::Syncing => running += 1,
            JobStatus::Paused => paused += 1,
            JobStatus::Completed => completed += 1,
            JobStatus::Failed => failed += 1,
            JobStatus::Cancelled => failed += 1,
        }
    });

    Json(StatsResponse {
        total,
        pending,
        running,
        paused,
        completed,
        failed,
    })
}

pub async fn list_jobs(
    State(state): State<SyncApiState>,
    Query(query): Query<ListJobsQuery>,
) -> impl IntoResponse {
    let mut jobs: Vec<JobSummary> = Vec::new();

    state.scheduler.for_each_job_status(|job_id, status| {
        let (job_type, round_id) = state
            .scheduler
            .get_job_info(job_id)
            .map(|j| (j.job_type.as_str().to_string(), j.round_id))
            .unwrap_or(("once".to_string(), 0));

        jobs.push(JobSummary {
            job_id: status.job_id.clone(),
            status: status.status.as_str().to_string(),
            progress: status.progress,
            region: status.region.clone(),
            job_type,
            round_id,
            created_at: status.created_at.to_rfc3339(),
        });
    });

    jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let total = jobs.len();
    let start = query.page * query.limit;
    let end = std::cmp::min(start + query.limit, total);
    let page_jobs = if start < total {
        jobs[start..end].to_vec()
    } else {
        vec![]
    };

    Json(ListJobsResponse {
        total,
        page: query.page,
        limit: query.limit,
        jobs: page_jobs,
    })
}

pub async fn create_job(
    State(state): State<SyncApiState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>, (StatusCode, String)> {
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

    let job_id = uuid::Uuid::new_v4().to_string();

    let job_type = JobType::parse(&req.job_type);
    info!(
        "Received job creation request: source={}, dest={}, region={}, type={}, job_id={}",
        req.source_path,
        req.dest_path,
        req.region,
        job_type.as_str(),
        job_id
    );

    let sync_job = crate::sync::engine::job::SyncJob::new(
        job_id.clone(),
        std::path::PathBuf::from(&req.source_path),
        req.dest_path.clone(),
        req.region,
    )
    .with_priority(req.priority)
    .with_job_type(job_type);

    match state.scheduler.submit_job(sync_job).await {
        Ok(_) => {
            info!("Job {} submitted successfully", job_id);
            Ok(Json(CreateJobResponse { job_id }))
        }
        Err(e) => {
            error!("Failed to submit job {}: {}", job_id, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to submit job: {}", e),
            ))
        }
    }
}

pub async fn get_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobStatusResponse>, (StatusCode, String)> {
    let status = state.scheduler.get_job_status(&job_id);
    let job_info = state.scheduler.get_job_info(&job_id);

    match (status, job_info) {
        (Some(status), Some(info)) => Ok(Json(JobStatusResponse {
            job_id: status.job_id,
            status: status.status.as_str().to_string(),
            progress: status.progress,
            current_size: status.current_size,
            total_size: status.total_size,
            source: JobPath {
                path: info.source.to_string_lossy().to_string(),
                client_id: None,
            },
            dest: JobPath {
                path: info.dest.clone(),
                client_id: None,
            },
            region: status.region.clone(),
            job_type: info.job_type.as_str().to_string(),
            round_id: info.round_id,
            is_last_round: info.is_last_round,
            priority: info.priority,
            created_at: status.created_at.to_rfc3339(),
            updated_at: status.updated_at.to_rfc3339(),
        })),
        (Some(status), None) => Ok(Json(JobStatusResponse {
            job_id: status.job_id.clone(),
            status: status.status.as_str().to_string(),
            progress: status.progress,
            current_size: status.current_size,
            total_size: status.total_size,
            source: JobPath {
                path: "unknown".to_string(),
                client_id: None,
            },
            dest: JobPath {
                path: "unknown".to_string(),
                client_id: None,
            },
            region: status.region.clone(),
            job_type: "once".to_string(),
            round_id: 0,
            is_last_round: false,
            priority: 0,
            created_at: status.created_at.to_rfc3339(),
            updated_at: status.updated_at.to_rfc3339(),
        })),
        _ => Err((StatusCode::NOT_FOUND, format!("Job {} not found", job_id))),
    }
}

pub async fn finalize_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
) -> Result<Json<CreateJobResponse>, (StatusCode, String)> {
    info!("Received finalize request for job: {}", job_id);

    match state.scheduler.finalize_job(&job_id).await {
        Ok(_) => {
            let final_job_id = format!("{}_final", job_id);
            info!("Final transfer job {} created successfully", final_job_id);
            Ok(Json(CreateJobResponse {
                job_id: final_job_id,
            }))
        }
        Err(e) => {
            error!("Failed to finalize job {}: {}", job_id, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to finalize job: {}", e),
            ))
        }
    }
}

pub async fn cancel_job(
    State(state): State<SyncApiState>,
    Path(job_id): Path<String>,
) -> Result<(), (StatusCode, String)> {
    info!("Received cancel request for job: {}", job_id);

    match state.scheduler.cancel_job(&job_id).await {
        Ok(_) => {
            info!("Job {} cancelled successfully", job_id);
            Ok(())
        }
        Err(e) => {
            error!("Failed to cancel job {}: {}", job_id, e);
            Err((
                StatusCode::BAD_REQUEST,
                format!("Failed to cancel job: {}", e),
            ))
        }
    }
}
