use crate::sync::engine::scheduler::SyncScheduler;
use crate::sync::RegionConfig;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(RustEmbed)]
#[folder = "web/dist"]
pub struct WebAssets;

#[derive(Clone)]
pub struct SyncApiState {
    pub scheduler: Arc<SyncScheduler>,
    pub regions: Vec<RegionConfig>,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub total: i64,
    pub pending: i64,
    pub running: i64,
    pub paused: i64,
    pub completed: i64,
    pub failed: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    pub job_id: String,
    pub status: String,
    pub progress: u8,
    pub region: String,
    pub job_type: String,
    pub round_id: i64,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ListJobsResponse {
    pub total: usize,
    pub page: usize,
    pub limit: usize,
    pub jobs: Vec<JobSummary>,
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub page: usize,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    pub source_path: String,
    pub dest_path: String,
    pub region: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "default_job_type")]
    pub job_type: String,
}

fn default_job_type() -> String {
    "once".to_string()
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub job_id: String,
}

#[derive(Debug, Serialize)]
pub struct JobPath {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub status: String,
    pub progress: u8,
    pub current_size: u64,
    pub total_size: u64,
    pub source: JobPath,
    pub dest: JobPath,
    pub region: String,
    pub job_type: String,
    pub round_id: i64,
    pub is_last_round: bool,
    pub priority: i32,
    pub created_at: String,
    pub updated_at: String,
}
