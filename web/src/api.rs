use gloo_net::http::{Request, RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    fmt::{Display, Formatter},
};

const TOKEN_STORAGE_KEY: &str = "hardata_api_token";

thread_local! {
    static API_TOKEN_CACHE: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stats {
    pub total: i64,
    pub pending: i64,
    pub running: i64,
    pub paused: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobDetail {
    pub job_id: String,
    pub status: String,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub can_cancel: bool,
    #[serde(default)]
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
    pub jobs: Vec<JobDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    status_code: Option<u16>,
    message: String,
}

impl ApiError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            status_code: None,
            message: message.into(),
        }
    }

    pub(crate) fn http(status_code: u16, message: impl Into<String>) -> Self {
        Self {
            status_code: Some(status_code),
            message: message.into(),
        }
    }

    pub fn is_unauthorized(&self) -> bool {
        self.status_code == Some(401)
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<gloo_net::Error> for ApiError {
    fn from(value: gloo_net::Error) -> Self {
        Self::new(value.to_string())
    }
}

#[cfg(target_arch = "wasm32")]
fn browser_storage() -> Option<web_sys::Storage> {
    web_sys::window().and_then(|window| window.local_storage().ok().flatten())
}

#[cfg(not(target_arch = "wasm32"))]
fn browser_storage() -> Option<web_sys::Storage> {
    None
}

fn cached_api_token() -> Option<String> {
    API_TOKEN_CACHE.with(|cache| cache.borrow().clone())
}

fn set_cached_api_token(token: Option<String>) {
    API_TOKEN_CACHE.with(|cache| {
        *cache.borrow_mut() = token;
    });
}

fn apply_auth(request: RequestBuilder) -> RequestBuilder {
    let token = load_api_token();
    if token.trim().is_empty() {
        request
    } else {
        request.header("Authorization", &format!("Bearer {}", token))
    }
}

pub fn try_load_api_token() -> Result<String, String> {
    let Some(storage) = browser_storage() else {
        return Err("Browser storage is unavailable".to_string());
    };

    let token = storage
        .get_item(TOKEN_STORAGE_KEY)
        .map(|token| token.unwrap_or_default())
        .map_err(|err| format!("{:?}", err))?;
    set_cached_api_token(Some(token.clone()));
    Ok(token)
}

pub fn load_api_token() -> String {
    cached_api_token().unwrap_or_else(|| try_load_api_token().unwrap_or_default())
}

pub fn store_api_token(token: &str) -> Result<(), String> {
    let Some(storage) = browser_storage() else {
        return Err("Browser storage is unavailable".to_string());
    };

    let normalized_token = token.trim().to_string();
    if normalized_token.is_empty() {
        storage
            .remove_item(TOKEN_STORAGE_KEY)
            .map_err(|err| format!("{:?}", err))?;
    } else {
        storage
            .set_item(TOKEN_STORAGE_KEY, &normalized_token)
            .map_err(|err| format!("{:?}", err))?;
    }

    set_cached_api_token(Some(normalized_token));
    Ok(())
}

async fn require_success(response: Response) -> Result<Response, ApiError> {
    if response.ok() {
        return Ok(response);
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let message = if body.trim().is_empty() {
        format!("HTTP {}", status)
    } else {
        format!("HTTP {}: {}", status, body)
    };
    Err(ApiError::http(status, message))
}

pub async fn fetch_stats() -> Result<Stats, ApiError> {
    let response = apply_auth(Request::get("/api/v1/stats"))
        .send()
        .await
        .map_err(ApiError::from)?;
    let response = require_success(response).await?;
    response.json::<Stats>().await.map_err(ApiError::from)
}

pub async fn fetch_jobs(page: usize, limit: usize) -> Result<ListJobsResponse, ApiError> {
    let url = format!("/api/v1/jobs?page={}&limit={}", page, limit);
    let response = apply_auth(Request::get(&url))
        .send()
        .await
        .map_err(ApiError::from)?;
    let response = require_success(response).await?;
    response
        .json::<ListJobsResponse>()
        .await
        .map_err(ApiError::from)
}

pub async fn cancel_job(job_id: &str) -> Result<(), ApiError> {
    let url = format!("/api/v1/jobs/{}", job_id);
    let response = apply_auth(Request::delete(&url))
        .send()
        .await
        .map_err(ApiError::from)?;
    require_success(response).await?;
    Ok(())
}

pub async fn finalize_job(job_id: &str) -> Result<(), ApiError> {
    let url = format!("/api/v1/jobs/{}/final", job_id);
    let response = apply_auth(Request::post(&url))
        .send()
        .await
        .map_err(ApiError::from)?;
    require_success(response).await?;
    Ok(())
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }

    let k = 1024f64;
    let sizes = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let i = ((bytes as f64).log(k).floor() as usize).min(sizes.len() - 1);
    let value = bytes as f64 / k.powi(i as i32);

    format!("{:.2} {}", value, sizes[i])
}

#[cfg(test)]
mod tests {
    use super::{
        cached_api_token, format_bytes, load_api_token, set_cached_api_token, ApiError, JobDetail,
    };

    #[test]
    fn format_bytes_supports_petabyte_scale_without_overflow() {
        assert_eq!(format_bytes(1024_u64.pow(5)), "1.00 PB");
    }

    #[test]
    fn format_bytes_supports_exabyte_scale_without_overflow() {
        assert_eq!(format_bytes(1024_u64.pow(6)), "1.00 EB");
    }

    #[test]
    fn api_error_marks_unauthorized_status() {
        assert!(ApiError::http(401, "HTTP 401").is_unauthorized());
        assert!(!ApiError::http(500, "HTTP 500").is_unauthorized());
    }

    #[test]
    fn load_api_token_prefers_cached_value_when_storage_is_unavailable() {
        set_cached_api_token(Some("cached-token".to_string()));

        assert_eq!(load_api_token(), "cached-token");
        assert_eq!(cached_api_token().as_deref(), Some("cached-token"));

        set_cached_api_token(None);
    }

    #[test]
    fn load_api_token_defaults_to_empty_without_cache_or_storage() {
        set_cached_api_token(None);

        assert_eq!(load_api_token(), "");
        assert_eq!(cached_api_token(), None);
    }

    #[test]
    fn job_detail_deserializes_optional_error_message() {
        let detail: JobDetail = serde_json::from_value(serde_json::json!({
            "job_id": "job-1",
            "status": "failed",
            "error_message": "disk full",
            "can_cancel": false,
            "progress": 12,
            "current_size": 12,
            "total_size": 100,
            "source": { "path": "/tmp/source.bin", "client_id": "" },
            "dest": { "path": "dest.bin", "client_id": "" },
            "region": "local",
            "job_type": "once",
            "round_id": 0,
            "is_last_round": false,
            "priority": 100,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:01Z"
        }))
        .unwrap();

        assert_eq!(detail.error_message.as_deref(), Some("disk full"));
    }
}
