use crate::sync::engine::scheduler::SyncScheduler;
use crate::sync::RegionConfig;
use axum::{
    http::{header, HeaderName, Method},
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use super::handlers::{
    cancel_job, create_job, finalize_job, get_job, get_stats, health_check, list_jobs,
};
use super::static_files::{serve_index, serve_spa_fallback, serve_static};
use super::types::SyncApiState;

fn build_cors_layer(api_token: &Option<String>) -> CorsLayer {
    if api_token.is_some() {
        CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                HeaderName::from_static("idempotency-key"),
            ])
            .allow_origin(tower_http::cors::Any)
    } else {
        CorsLayer::permissive()
    }
}

pub fn create_sync_router(
    scheduler: Arc<SyncScheduler>,
    regions: Vec<RegionConfig>,
    data_dir: String,
    allow_external_destinations: bool,
    web_ui: bool,
    api_token: Option<String>,
) -> Router {
    let cors = build_cors_layer(&api_token);
    let state = SyncApiState {
        scheduler,
        regions,
        data_dir,
        allow_external_destinations,
        web_ui,
        api_token,
    };

    let mut router = Router::new()
        .route("/api/v1/stats", get(get_stats))
        .route("/api/v1/jobs", get(list_jobs).post(create_job))
        .route("/api/v1/jobs/{job_id}", get(get_job).delete(cancel_job))
        .route("/api/v1/jobs/{job_id}/final", post(finalize_job))
        .route("/healthz", get(health_check))
        .layer(cors)
        .with_state(state);

    if web_ui {
        router = router
            .route("/", get(serve_index))
            .route("/assets/{*path}", get(serve_static))
            .route("/{*path}", get(serve_spa_fallback));
    }

    router
}

#[cfg(test)]
mod tests {
    use super::create_sync_router;
    use crate::sync::engine::scheduler::SchedulerConfig;
    use crate::sync::storage::db::Database;
    use axum::body::Body;
    use axum::http::{header, Method, Request, StatusCode};
    use std::sync::Arc;
    use tower::util::ServiceExt;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-router-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    async fn build_app(root: &std::path::Path, api_token: Option<String>) -> axum::Router {
        let db_path = format!("sqlite://{}", root.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let config = SchedulerConfig {
            data_dir: root.join("sync-data").to_string_lossy().to_string(),
            chunk_cache_path: root.join("chunk-cache").to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();

        let scheduler = crate::sync::engine::scheduler::SyncScheduler::new(config, db)
            .await
            .unwrap();
        create_sync_router(
            scheduler,
            Vec::new(),
            root.join("sync-data").to_string_lossy().to_string(),
            false,
            false,
            api_token,
        )
    }

    #[tokio::test]
    async fn router_keeps_permissive_cors_without_token() {
        let root = temp_dir("cors-no-token");
        let app = build_app(&root, None).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/v1/jobs")
                    .header(header::ORIGIN, "http://127.0.0.1:8080")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "*"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn router_restricts_cors_headers_with_token() {
        let root = temp_dir("cors-with-token");
        let app = build_app(&root, Some("secret".to_string())).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/v1/jobs")
                    .header(header::ORIGIN, "http://evil.example.com")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let allowed_headers = response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .expect("should have allow-headers")
            .to_str()
            .unwrap()
            .to_lowercase();
        assert!(allowed_headers.contains("content-type"));
        assert!(allowed_headers.contains("authorization"));
        // permissive 模式会返回 wildcard "*"，受限模式只返回显式列出的 header
        assert!(!allowed_headers.contains("*"));

        std::fs::remove_dir_all(root).unwrap();
    }
}
