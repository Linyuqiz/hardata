use crate::sync::engine::scheduler::SyncScheduler;
use crate::sync::RegionConfig;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const DEFAULT_LIST_JOBS_LIMIT: usize = 100;
pub const MAX_LIST_JOBS_LIMIT: usize = 500;

#[derive(RustEmbed)]
#[folder = "$OUT_DIR/hardata-web-dist"]
pub struct WebAssets;

#[derive(Clone)]
pub struct SyncApiState {
    pub scheduler: Arc<SyncScheduler>,
    pub regions: Vec<RegionConfig>,
    pub data_dir: String,
    pub allow_external_destinations: bool,
    pub web_ui: bool,
    pub api_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub total: i64,
    pub pending: i64,
    pub running: i64,
    pub paused: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    pub job_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub can_cancel: bool,
    pub can_finalize: bool,
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
    DEFAULT_LIST_JOBS_LIMIT
}

impl ListJobsQuery {
    pub fn normalized(self) -> Self {
        Self {
            limit: self.limit.clamp(1, MAX_LIST_JOBS_LIMIT),
            page: self.page,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateJobRequest {
    pub source_path: String,
    pub dest_path: String,
    pub region: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "default_job_type")]
    pub job_type: String,
    #[serde(default)]
    pub exclude_regex: Vec<String>,
    #[serde(default)]
    pub include_regex: Vec<String>,
    #[serde(default)]
    pub request_id: Option<String>,
}

fn default_job_type() -> String {
    "once".to_string()
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobPath {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub can_cancel: bool,
    pub can_finalize: bool,
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

#[cfg(test)]
mod tests {
    use super::{ListJobsQuery, DEFAULT_LIST_JOBS_LIMIT, MAX_LIST_JOBS_LIMIT};

    #[test]
    fn list_jobs_query_defaults_to_expected_limit() {
        let query = ListJobsQuery {
            limit: DEFAULT_LIST_JOBS_LIMIT,
            page: 0,
        }
        .normalized();

        assert_eq!(query.limit, DEFAULT_LIST_JOBS_LIMIT);
    }

    #[test]
    fn list_jobs_query_normalized_clamps_zero_limit() {
        let query = ListJobsQuery { limit: 0, page: 3 }.normalized();

        assert_eq!(query.limit, 1);
        assert_eq!(query.page, 3);
    }

    #[test]
    fn list_jobs_query_normalized_clamps_large_limit() {
        let query = ListJobsQuery {
            limit: MAX_LIST_JOBS_LIMIT + 1,
            page: 1,
        }
        .normalized();

        assert_eq!(query.limit, MAX_LIST_JOBS_LIMIT);
    }
}
