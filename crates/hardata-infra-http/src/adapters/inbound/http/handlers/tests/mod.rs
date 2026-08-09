use super::{
    can_finalize_job_from_snapshot, cancel_job, create_job, create_job_request_fingerprint,
    extract_bearer_token, extract_create_job_idempotency_key, finalize_job, internal_final_job_id,
    is_internal_final_job_id, map_cancel_error_status, map_create_job_error_status,
    map_finalize_error_status, pagination_bounds, project_public_job_snapshot,
    resolve_list_jobs_page, resolve_public_job_id, resolve_public_job_runtime_fields,
    resolve_public_job_status_view, stats_from_counts, summarize_statuses,
    validate_destination_scope, IDEMPOTENCY_KEY_HEADER,
};
use crate::adapters::inbound::http::types::CreateJobRequest;
use crate::adapters::inbound::http::types::SyncApiState;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::Json;
use hardata_app::application::config::RegionConfig;
use hardata_app::application::sync::engine::job::SyncJob;
use hardata_app::application::sync::engine::scheduler::SyncScheduler;
use hardata_app::application::sync::engine::scheduler::{JobRuntimeStatus, SchedulerConfig};
use hardata_app::domain::job::JobStatus;
use hardata_app::domain::{Job, JobPath, JobType};
use hardata_app::shared::error::HarDataError;
use hardata_infra_persistence::Database;
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

async fn create_scheduler(label: &str) -> (std::path::PathBuf, Arc<Database>, Arc<SyncScheduler>) {
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

include!("part_01.rs");
include!("part_02.rs");
include!("part_03.rs");
