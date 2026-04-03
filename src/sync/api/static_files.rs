use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use std::path::{Component, Path as FsPath, PathBuf};

use super::types::WebAssets;

fn web_dist_override_dir() -> Option<std::path::PathBuf> {
    std::env::var("HARDATA_WEB_DIST").ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(std::path::PathBuf::from(trimmed))
        }
    })
}

fn resolve_asset_path(relative_path: &str) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in FsPath::new(relative_path).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    Some(normalized)
}

fn build_missing_asset_response(_relative_path: &str, unavailable: bool) -> Response {
    let message = if unavailable {
        "Web UI assets are missing or incomplete. Build the web app before enabling web_ui."
    } else {
        "Not Found"
    };

    Response::builder()
        .status(if unavailable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::NOT_FOUND
        })
        .body(axum::body::Body::from(message))
        .unwrap()
}

fn build_unreadable_asset_response(relative_path: &str) -> Response {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(axum::body::Body::from(format!(
            "Failed to read web UI asset '{}'",
            relative_path
        )))
        .unwrap()
}

fn asset_missing_is_unavailable(base_dir: &FsPath, relative_path: &FsPath) -> bool {
    if relative_path == FsPath::new("index.html") {
        return true;
    }

    relative_path.starts_with("assets") && !base_dir.join("assets").exists()
}

async fn read_asset_from(base_dir: &FsPath, relative_path: &str) -> Result<Vec<u8>, Response> {
    if !base_dir.exists() {
        return Err(build_missing_asset_response(relative_path, true));
    }

    let Some(relative_path_buf) = resolve_asset_path(relative_path) else {
        return Err(build_missing_asset_response(relative_path, false));
    };
    let path = base_dir.join(&relative_path_buf);

    tokio::fs::read(&path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            let unavailable = asset_missing_is_unavailable(base_dir, &relative_path_buf);
            return build_missing_asset_response(relative_path, unavailable);
        }

        build_unreadable_asset_response(relative_path)
    })
}

fn read_embedded_asset(relative_path: &str) -> Result<Vec<u8>, Response> {
    let Some(normalized) = resolve_asset_path(relative_path) else {
        return Err(build_missing_asset_response(relative_path, false));
    };
    // rust-embed 使用正斜杠作为 key，PathBuf 在 Windows 上会产生反斜杠
    let normalized_str: String = normalized
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    WebAssets::get(&normalized_str)
        .map(|content| content.data.into_owned())
        .ok_or_else(|| build_missing_asset_response(relative_path, relative_path == "index.html"))
}

async fn read_asset(relative_path: &str) -> Result<Vec<u8>, Response> {
    if let Some(base_dir) = web_dist_override_dir() {
        read_asset_from(&base_dir, relative_path).await
    } else {
        read_embedded_asset(relative_path)
    }
}

fn ensure_root_base_href(body: Vec<u8>) -> Vec<u8> {
    let html = String::from_utf8_lossy(&body);
    if html.contains("<base ") {
        return body;
    }

    let Some(head_end) = html.find("</head>") else {
        return body;
    };

    let mut updated = String::with_capacity(html.len() + 20);
    updated.push_str(&html[..head_end]);
    updated.push_str("<base href=\"/\">\n");
    updated.push_str(&html[head_end..]);
    updated.into_bytes()
}

pub fn get_mime_type(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html"
    } else if path.ends_with(".css") {
        "text/css"
    } else if path.ends_with(".js") {
        "application/javascript"
    } else if path.ends_with(".wasm") {
        "application/wasm"
    } else if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else {
        "application/octet-stream"
    }
}

pub async fn serve_index() -> impl IntoResponse {
    match read_asset("index.html").await {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(axum::body::Body::from(ensure_root_base_href(body)))
            .unwrap(),
        Err(response) => response,
    }
}

fn should_serve_index_fallback(path: &str) -> bool {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }

    if trimmed
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return false;
    }

    let first_segment = trimmed.split('/').next().unwrap_or_default();
    if matches!(first_segment, "api" | "assets" | "healthz") {
        return false;
    }

    !trimmed.rsplit('/').next().unwrap_or_default().contains('.')
}

pub async fn serve_static(Path(path): Path<String>) -> impl IntoResponse {
    let file_path = format!("assets/{}", path);
    match read_asset(&file_path).await {
        Ok(body) => {
            let mime = get_mime_type(&file_path);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .body(axum::body::Body::from(body))
                .unwrap()
        }
        Err(response) => response,
    }
}

pub async fn serve_spa_fallback(Path(path): Path<String>) -> impl IntoResponse {
    if !should_serve_index_fallback(&path) {
        return build_missing_asset_response(&path, false);
    }

    serve_index().await.into_response()
}

#[cfg(test)]
mod tests {
    use super::{ensure_root_base_href, read_asset_from, should_serve_index_fallback};
    use axum::http::StatusCode;
    use std::path::PathBuf;

    fn temp_dir(prefix: &str) -> PathBuf {
        let unique = format!(
            "hardata-static-files-{}-{}",
            prefix,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    #[test]
    fn spa_fallback_only_handles_non_api_app_paths() {
        assert!(should_serve_index_fallback("jobs/detail"));
        assert!(should_serve_index_fallback("dashboard"));
        assert!(!should_serve_index_fallback("api/v1/jobs"));
        assert!(!should_serve_index_fallback("assets/app.js"));
        assert!(!should_serve_index_fallback("favicon.ico"));
        assert!(!should_serve_index_fallback("../escape"));
    }

    #[tokio::test]
    async fn read_asset_from_marks_missing_index_as_service_unavailable() {
        let dir = temp_dir("missing-index");
        std::fs::create_dir_all(&dir).unwrap();

        let response = read_asset_from(&dir, "index.html").await.unwrap_err();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn read_asset_from_keeps_missing_hashed_asset_as_not_found() {
        let dir = temp_dir("missing-asset");
        std::fs::create_dir_all(dir.join("assets")).unwrap();

        let response = read_asset_from(&dir, "assets/app.js").await.unwrap_err();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ensure_root_base_href_injects_base_tag_once() {
        let html = b"<html><head><link rel=\"stylesheet\" href=\"assets/style.css\"></head></html>"
            .to_vec();

        let updated = String::from_utf8(ensure_root_base_href(html)).unwrap();
        assert!(updated.contains("<base href=\"/\">"));
        assert_eq!(updated.matches("<base href=\"/\">").count(), 1);
    }

    #[test]
    fn ensure_root_base_href_preserves_existing_base_tag() {
        let html = b"<html><head><base href=\"/\"></head></html>".to_vec();

        let updated = String::from_utf8(ensure_root_base_href(html)).unwrap();
        assert_eq!(updated.matches("<base href=\"/\">").count(), 1);
    }
}
