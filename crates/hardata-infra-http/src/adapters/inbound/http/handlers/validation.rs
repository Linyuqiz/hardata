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
use hardata_app::application::sync::engine::scheduler::{normalize_path, SchedulerConfig};
use hardata_app::domain::JobType;
use hardata_app::shared::error::HarDataError;

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

fn token_matches(provided: &str, expected: &str) -> bool {
    let provided_hash = blake3::hash(provided.as_bytes());
    let expected_hash = blake3::hash(expected.as_bytes());
    provided_hash
        .as_bytes()
        .iter()
        .zip(expected_hash.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
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
    if !token_matches(token, expected) {
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
