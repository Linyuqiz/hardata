use crate::sync::engine::scheduler::SyncScheduler;
use crate::sync::RegionConfig;
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use super::handlers::{
    cancel_job, create_job, finalize_job, get_job, get_stats, health_check, list_jobs,
};
use super::static_files::{serve_index, serve_static};
use super::types::SyncApiState;

pub fn create_sync_router(scheduler: Arc<SyncScheduler>, regions: Vec<RegionConfig>) -> Router {
    let state = SyncApiState { scheduler, regions };

    Router::new()
        .route("/", get(serve_index))
        .route("/assets/{*path}", get(serve_static))
        .route("/api/v1/stats", get(get_stats))
        .route("/api/v1/jobs", get(list_jobs).post(create_job))
        .route("/api/v1/jobs/{job_id}", get(get_job).delete(cancel_job))
        .route("/api/v1/jobs/{job_id}/final", post(finalize_job))
        .route("/healthz", get(health_check))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
