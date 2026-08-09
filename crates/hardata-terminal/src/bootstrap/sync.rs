use hardata_app::application::sync::engine;
use hardata_app::application::{RegionConfig, SyncConfig};
use hardata_infra_http::adapters::inbound::http as api;
use hardata_infra_persistence as storage;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use tracing::{debug, warn};

#[path = "index_scan.rs"]
mod index_scan;
#[cfg(test)]
use index_scan::collect_files_recursive;
use index_scan::scan_and_index_local_files;

#[derive(Debug, Deserialize, Serialize)]
pub struct HarDataConfig {
    pub sync: SyncConfig,
}

fn bind_is_loopback(bind: &str) -> bool {
    if let Ok(addr) = bind.parse::<SocketAddr>() {
        return match addr.ip() {
            IpAddr::V4(ip) => ip.is_loopback(),
            IpAddr::V6(ip) => ip.is_loopback(),
        };
    }

    let host = bind
        .split(':')
        .next()
        .unwrap_or_default()
        .trim_matches('[')
        .trim_matches(']');
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

fn normalize_api_token(token: Option<String>) -> Option<String> {
    token.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn validate_regions(regions: &[RegionConfig]) -> hardata_app::shared::error::Result<()> {
    let mut names = std::collections::HashSet::with_capacity(regions.len());
    for region in regions {
        let name = region.name.trim();
        if name.is_empty() {
            return Err(hardata_app::shared::error::HarDataError::InvalidConfig(
                "sync.regions contains an empty region name".to_string(),
            ));
        }
        if !names.insert(name) {
            return Err(hardata_app::shared::error::HarDataError::InvalidConfig(
                format!("sync.regions contains duplicate region name '{}'", name),
            ));
        }
    }
    Ok(())
}

pub async fn run_sync(config_path: String) -> hardata_app::shared::error::Result<()> {
    use std::sync::Arc;
    use tracing::{error, info};

    let config_content = tokio::fs::read_to_string(&config_path).await.map_err(|e| {
        error!(operation = "sync.config_read_failed", config_path = %config_path, error = %e, "sync configuration read failed");
        hardata_app::shared::error::HarDataError::Io(e)
    })?;

    let hardata_config: HarDataConfig = serde_yaml::from_str(&config_content).map_err(|e| {
        error!(operation = "sync.config_parse_failed", config_path = %config_path, error = %e, "sync configuration parse failed");
        hardata_app::shared::error::HarDataError::InvalidConfig(format!("Invalid YAML: {}", e))
    })?;

    let mut config = hardata_config.sync;
    config.api_token = normalize_api_token(config.api_token.take());

    if config.replicate_mode == engine::scheduler::ReplicateMode::Append {
        warn!(
            operation = "sync.replicate_mode_warning",
            replicate_mode = "append",
            reason = "partial_destination_visibility",
            "append mode exposes partially written destination files"
        );
    }

    info!(
        operation = "sync.starting",
        http_bind = %config.http_bind,
        data_dir = %config.data_dir,
        region_count = config.regions.len(),
        "sync service starting"
    );

    if config.api_token.is_none() && !bind_is_loopback(&config.http_bind) {
        return Err(hardata_app::shared::error::HarDataError::InvalidConfig(
            "sync.api_token must be a non-empty token when http_bind is not loopback".to_string(),
        ));
    }

    if !std::path::Path::new(&config.data_dir).exists() {
        std::fs::create_dir_all(&config.data_dir)
            .map_err(hardata_app::shared::error::HarDataError::Io)?;
    }

    if !std::path::Path::new(&config.metadata).exists() {
        std::fs::create_dir_all(&config.metadata)
            .map_err(hardata_app::shared::error::HarDataError::Io)?;
    }

    let db_path = config.db_path();
    let sync_db_url = if db_path.starts_with('/') {
        format!("sqlite://{}", db_path)
    } else {
        format!("sqlite:{}", db_path)
    };
    let sync_db = storage::db::Database::new(&sync_db_url).await?;
    let sync_db_arc = Arc::new(sync_db);

    let regions = if config.regions.is_empty() {
        vec![RegionConfig {
            name: "default".to_string(),
            quic_bind: hardata_app::shared::constants::DEFAULT_QUIC_BIND_ADDR.to_string(),
            tcp_bind: hardata_app::shared::constants::DEFAULT_TCP_BIND_ADDR.to_string(),
        }]
    } else {
        config.regions.clone()
    };
    validate_regions(&regions)?;

    for region in &regions {
        debug!(
            operation = "sync.region_configured",
            region = %region.name,
            quic_bind = %region.quic_bind,
            tcp_bind = %region.tcp_bind,
            "sync region configured"
        );
    }

    let regions_for_api = regions.clone();

    let global_index_path = std::path::Path::new(&config.metadata).join(".index");

    if !global_index_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&global_index_path) {
            warn!(
                operation = "sync.global_index_directory_create_failed",
                path = %global_index_path.display(),
                error = %e,
                "global index directory creation failed"
            );
        }
    }

    let global_index = match engine::ChunkIndex::new(&global_index_path) {
        Ok(index) => {
            info!(
                operation = "sync.global_index_ready",
                path = %global_index_path.display(),
                "global chunk index ready"
            );

            match index.cleanup_stale_entries() {
                Ok(removed) if removed > 0 => {
                    info!(
                        operation = "sync.global_index_cleanup_completed",
                        removed_entries = removed,
                        "global index cleanup completed"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        operation = "sync.global_index_cleanup_failed",
                        error = %e,
                        "global index cleanup failed"
                    );
                }
            }

            let index_arc = std::sync::Arc::new(index);

            let index_clone = Arc::clone(&index_arc);
            let data_dir_clone = config.data_dir.clone();
            let min_chunk_size = hardata_app::shared::cdc::DEFAULT_MIN_CHUNK_SIZE;
            let avg_chunk_size = hardata_app::shared::cdc::DEFAULT_AVG_CHUNK_SIZE;
            let max_chunk_size = hardata_app::shared::cdc::DEFAULT_MAX_CHUNK_SIZE;

            tokio::spawn(async move {
                info!(
                    operation = "sync.global_index_scan_started",
                    "global index scan started"
                );
                match scan_and_index_local_files(
                    &data_dir_clone,
                    &index_clone,
                    min_chunk_size,
                    avg_chunk_size,
                    max_chunk_size,
                )
                .await
                {
                    Ok((scanned, indexed)) => {
                        info!(
                            operation = "sync.global_index_scan_completed",
                            scanned_files = scanned,
                            indexed_chunks = indexed,
                            "global index scan completed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            operation = "sync.global_index_scan_failed",
                            error = %e,
                            "global index scan failed"
                        );
                    }
                }
            });

            Some(index_arc)
        }
        Err(e) => {
            warn!(
                operation = "sync.global_index_open_failed",
                error = %e,
                "global chunk index disabled"
            );
            None
        }
    };

    use hardata_app::shared::constants::{
        FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS, SCHEDULER_BATCH_SIZE,
        SCHEDULER_MAX_CONCURRENCY, SCHEDULER_MAX_CONCURRENT_FILES, SCHEDULER_MAX_CONCURRENT_JOBS,
        SCHEDULER_MIN_CONCURRENCY, SCHEDULER_POOL_SIZE,
    };

    debug!(
        operation = "sync.scheduler_configured",
        pool_size = SCHEDULER_POOL_SIZE,
        min_concurrency = SCHEDULER_MIN_CONCURRENCY,
        max_concurrency = SCHEDULER_MAX_CONCURRENCY,
        file_cache_max_entries = FILE_CACHE_MAX_ENTRIES,
        file_cache_ttl_seconds = FILE_CACHE_TTL_SECS,
        "scheduler constants configured"
    );

    let scheduler_config = engine::scheduler::SchedulerConfig {
        regions,
        data_dir: config.data_dir.clone(),
        chunk_cache_path: config.chunk_cache_path(),
        enable_cache_preheat: true,
        max_concurrent_jobs: SCHEDULER_MAX_CONCURRENT_JOBS,
        min_chunk_size: hardata_app::shared::cdc::DEFAULT_MIN_CHUNK_SIZE,
        avg_chunk_size: hardata_app::shared::cdc::DEFAULT_AVG_CHUNK_SIZE,
        max_chunk_size: hardata_app::shared::cdc::DEFAULT_MAX_CHUNK_SIZE,
        retry_config: hardata_app::shared::retry::RetryConfig::default(),
        compression_strategy: hardata_app::shared::compression::CompressionStrategy::default(),
        batch_size: SCHEDULER_BATCH_SIZE,
        max_concurrent_files: SCHEDULER_MAX_CONCURRENT_FILES,
        stability_threshold: std::time::Duration::from_secs(config.stability_threshold_secs),
        replicate_mode: config.replicate_mode,
        allow_external_destinations: config.allow_external_destinations,
        global_index,
    };

    let scheduler =
        engine::scheduler::SyncScheduler::new(scheduler_config, sync_db_arc.clone()).await?;

    scheduler.start().await?;

    let app = api::create_sync_router(
        scheduler.clone(),
        regions_for_api,
        config.data_dir.clone(),
        config.allow_external_destinations,
        config.web_ui,
        config.api_token.clone(),
    );

    let http_bind = config.http_bind.clone();
    let listener = tokio::net::TcpListener::bind(&http_bind)
        .await
        .map_err(|e| {
            error!(operation = "sync.http_bind_failed", http_bind = %http_bind, error = %e, "HTTP API bind failed");
            hardata_app::shared::error::HarDataError::Io(e)
        })?;

    info!(operation = "sync.http_ready", http_bind = %http_bind, "HTTP API listening");

    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let shutdown_notify_http = shutdown_notify.clone();

    let api_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown_notify_http.notified().await;
                info!(
                    operation = "sync.http_shutdown",
                    "HTTP API graceful shutdown"
                );
            })
            .await
        {
            error!(operation = "sync.http_failed", error = %e, "HTTP API server failed");
        }
    });

    info!(
        operation = "sync.ready",
        http_bind = %http_bind,
        web_ui_enabled = config.web_ui,
        "sync service ready"
    );

    let signal = hardata_app::shared::signal::shutdown_signal().await;
    info!(operation = "sync.shutdown_requested", signal = %signal, "sync shutdown requested");

    shutdown_notify.notify_one();
    let _ = api_handle.await;

    scheduler.shutdown().await?;
    info!(operation = "sync.stopped", "sync shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        bind_is_loopback, collect_files_recursive, normalize_api_token, validate_regions,
        HarDataConfig, RegionConfig,
    };
    use hardata_app::application::sync::engine::scheduler::ReplicateMode;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn normalize_api_token_trims_and_drops_blank_values() {
        assert_eq!(
            normalize_api_token(Some("  secret  ".to_string())),
            Some("secret".to_string())
        );
        assert_eq!(normalize_api_token(Some("   ".to_string())), None);
        assert_eq!(normalize_api_token(None), None);
    }

    #[test]
    fn bind_is_loopback_accepts_common_loopback_hosts() {
        assert!(bind_is_loopback("127.0.0.1:8080"));
        assert!(bind_is_loopback("localhost:8080"));
        assert!(!bind_is_loopback("0.0.0.0:8080"));
    }

    #[test]
    fn validate_regions_rejects_empty_and_duplicate_names() {
        let region = |name: &str| RegionConfig {
            name: name.to_string(),
            quic_bind: "127.0.0.1:9443".to_string(),
            tcp_bind: "127.0.0.1:9444".to_string(),
        };

        assert!(validate_regions(&[region("local")]).is_ok());
        assert!(validate_regions(&[region(" ")]).is_err());
        assert!(validate_regions(&[region("local"), region("local")]).is_err());
    }

    #[tokio::test]
    async fn collect_files_recursive_skips_symlinked_directories() {
        let root = temp_dir("collect-files");
        let local_file = root.join("local.txt");
        let outside_dir = root.join("outside");
        let outside_file = outside_dir.join("outside.txt");
        let linked_dir = root.join("linked");

        std::fs::write(&local_file, b"local").unwrap();
        std::fs::create_dir_all(&outside_dir).unwrap();
        std::fs::write(&outside_file, b"outside").unwrap();
        std::os::unix::fs::symlink(&outside_dir, &linked_dir).unwrap();

        let mut files = Vec::new();
        collect_files_recursive(&root, &mut files).await.unwrap();

        let file_set: std::collections::HashSet<PathBuf> = files.into_iter().collect();
        assert!(file_set.contains(&local_file));
        assert!(file_set.contains(&outside_file));
        assert!(!file_set.contains(&linked_dir.join("outside.txt")));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sync_config_defaults_to_tmp_replicate_mode() {
        let config: HarDataConfig = serde_yaml::from_str(
            r#"
            sync:
              http_bind: "127.0.0.1:9080"
            "#,
        )
        .unwrap();

        assert_eq!(config.sync.replicate_mode, ReplicateMode::Tmp);
        assert_eq!(config.sync.stability_threshold_secs, 20);
    }

    #[test]
    fn sync_config_allows_custom_stability_threshold() {
        let config: HarDataConfig = serde_yaml::from_str(
            r#"
            sync:
              http_bind: "127.0.0.1:9080"
              stability_threshold_secs: 3
            "#,
        )
        .unwrap();

        assert_eq!(config.sync.stability_threshold_secs, 3);
    }
}
