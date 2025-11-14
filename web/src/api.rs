use serde::{Deserialize, Serialize};
use gloo_net::http::Request;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stats {
    pub total: i64,
    pub pending: i64,
    pub running: i64,
    pub paused: i64,
    pub completed: i64,
    pub failed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummary {
    pub job_id: String,
    pub status: String,
    pub progress: u8,
    pub job_type: String,
    pub round_id: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobDetail {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobPath {
    pub path: String,
    #[serde(default)]
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListJobsResponse {
    pub total: usize,
    pub page: usize,
    pub limit: usize,
    pub jobs: Vec<JobSummary>,
}

pub async fn fetch_stats() -> Result<Stats, gloo_net::Error> {
    let response = Request::get("/api/v1/stats")
        .send()
        .await?;
    
    response.json::<Stats>().await
}

pub async fn fetch_jobs(limit: usize) -> Result<ListJobsResponse, gloo_net::Error> {
    let url = format!("/api/v1/jobs?limit={}", limit);
    let response = Request::get(&url)
        .send()
        .await?;
    
    response.json::<ListJobsResponse>().await
}

pub async fn fetch_job_detail(job_id: &str) -> Result<JobDetail, gloo_net::Error> {
    let url = format!("/api/v1/jobs/{}", job_id);
    let response = Request::get(&url)
        .send()
        .await?;
    
    response.json::<JobDetail>().await
}

pub async fn cancel_job(job_id: &str) -> Result<(), gloo_net::Error> {
    let url = format!("/api/v1/jobs/{}", job_id);
    Request::delete(&url)
        .send()
        .await?;
    Ok(())
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }

    let k = 1024f64;
    let sizes = ["B", "KB", "MB", "GB", "TB"];
    let i = (bytes as f64).log(k).floor() as usize;
    let value = bytes as f64 / k.powi(i as i32);

    format!("{:.2} {}", value, sizes[i])
}
