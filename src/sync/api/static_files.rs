use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

use super::types::WebAssets;

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
    match WebAssets::get("index.html") {
        Some(content) => {
            let body = content.data.into_owned();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                .body(axum::body::Body::from(body))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Not Found"))
            .unwrap(),
    }
}

pub async fn serve_static(Path(path): Path<String>) -> impl IntoResponse {
    let file_path = format!("assets/{}", path);
    match WebAssets::get(&file_path) {
        Some(content) => {
            let body = content.data.into_owned();
            let mime = get_mime_type(&file_path);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .body(axum::body::Body::from(body))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Not Found"))
            .unwrap(),
    }
}
