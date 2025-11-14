pub mod api;
pub mod engine;
pub mod net;
pub mod scanner;
pub mod storage;
pub mod transfer;

use crate::util::error::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize, Serialize)]
pub struct SyncConfig {
    pub http_bind: String,
    #[serde(default = "default_sync_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_metadata_dir")]
    pub metadata: String,
    #[serde(default)]
    pub web_ui: bool,
    #[serde(default)]
    pub regions: Vec<RegionConfig>,
}

impl SyncConfig {
    pub fn db_path(&self) -> String {
        format!("{}/data.db", self.metadata)
    }

    pub fn chunk_cache_path(&self) -> String {
        format!("{}/.cache", self.metadata)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegionConfig {
    pub name: String,
    pub quic_bind: String,
    pub tcp_bind: String,
}

fn default_sync_data_dir() -> String {
    ".hardata/sync".to_string()
}

fn default_metadata_dir() -> String {
    ".hardata".to_string()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HarDataConfig {
    pub sync: SyncConfig,
}

pub async fn run_sync(config_path: String) -> crate::util::error::Result<()> {
    use std::sync::Arc;
    use tracing::{error, info};

    let config_content = tokio::fs::read_to_string(&config_path).await.map_err(|e| {
        error!("Failed to read config: {}", e);
        crate::util::error::HarDataError::Io(e)
    })?;

    let hardata_config: HarDataConfig = serde_yaml::from_str(&config_content).map_err(|e| {
        error!("Failed to parse config: {}", e);
        crate::util::error::HarDataError::InvalidConfig(format!("Invalid YAML: {}", e))
    })?;

    let config = hardata_config.sync;

    info!(
        "Sync starting: http={}, data_dir={}, regions={}",
        config.http_bind,
        config.data_dir,
        config.regions.len()
    );

    if !std::path::Path::new(&config.data_dir).exists() {
        std::fs::create_dir_all(&config.data_dir).map_err(crate::util::error::HarDataError::Io)?;
    }

    if !std::path::Path::new(&config.metadata).exists() {
        std::fs::create_dir_all(&config.metadata).map_err(crate::util::error::HarDataError::Io)?;
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
            quic_bind: crate::core::constants::DEFAULT_QUIC_BIND_ADDR.to_string(),
            tcp_bind: crate::core::constants::DEFAULT_TCP_BIND_ADDR.to_string(),
        }]
    } else {
        config.regions.clone()
    };

    for region in &regions {
        debug!(
            "Region '{}': quic={}, tcp={}",
            region.name, region.quic_bind, region.tcp_bind
        );
    }

    let regions_for_api = regions.clone();

    let global_index_path = std::path::Path::new(&config.metadata).join(".index");

    if !global_index_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&global_index_path) {
            warn!(
                "Failed to create global index directory {:?}: {}",
                global_index_path, e
            );
        }
    }

    let global_index = match engine::ChunkIndex::new(&global_index_path) {
        Ok(index) => {
            debug!("Global index enabled: {:?}", global_index_path);

            match index.cleanup_stale_entries() {
                Ok(removed) if removed > 0 => {
                    info!("Index cleanup: {} stale entries removed", removed);
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("Index cleanup failed: {}", e);
                }
            }

            let index_arc = std::sync::Arc::new(index);

            let index_clone = Arc::clone(&index_arc);
            let data_dir_clone = config.data_dir.clone();
            let min_chunk_size = crate::util::cdc::DEFAULT_MIN_CHUNK_SIZE;
            let avg_chunk_size = crate::util::cdc::DEFAULT_AVG_CHUNK_SIZE;
            let max_chunk_size = crate::util::cdc::DEFAULT_MAX_CHUNK_SIZE;

            tokio::spawn(async move {
                info!("Starting background scan of local files for global index...");
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
                            "Background scan completed: {} files scanned, {} chunks indexed",
                            scanned, indexed
                        );
                    }
                    Err(e) => {
                        warn!("Background scan failed: {}", e);
                    }
                }
            });

            Some(index_arc)
        }
        Err(e) => {
            warn!("Failed to open global chunk index: {}, disabling", e);
            None
        }
    };

    use crate::core::constants::{
        FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS, SCHEDULER_BATCH_SIZE,
        SCHEDULER_MAX_CONCURRENCY, SCHEDULER_MAX_CONCURRENT_FILES, SCHEDULER_MAX_CONCURRENT_JOBS,
        SCHEDULER_MIN_CONCURRENCY, SCHEDULER_POOL_SIZE,
    };

    info!("Scheduler constants: pool_size={}, concurrency={}-{}, file_cache_max={}, file_cache_ttl={}s",
        SCHEDULER_POOL_SIZE, SCHEDULER_MIN_CONCURRENCY, SCHEDULER_MAX_CONCURRENCY,
        FILE_CACHE_MAX_ENTRIES, FILE_CACHE_TTL_SECS);

    let scheduler_config = engine::scheduler::SchedulerConfig {
        regions,
        data_dir: config.data_dir.clone(),
        chunk_cache_path: config.chunk_cache_path(),
        enable_cache_preheat: true,
        max_concurrent_jobs: SCHEDULER_MAX_CONCURRENT_JOBS,
        min_chunk_size: crate::util::cdc::DEFAULT_MIN_CHUNK_SIZE,
        avg_chunk_size: crate::util::cdc::DEFAULT_AVG_CHUNK_SIZE,
        max_chunk_size: crate::util::cdc::DEFAULT_MAX_CHUNK_SIZE,
        retry_config: crate::util::retry::RetryConfig::default(),
        compression_strategy: crate::util::compression::CompressionStrategy::default(),
        batch_size: SCHEDULER_BATCH_SIZE,
        max_concurrent_files: SCHEDULER_MAX_CONCURRENT_FILES,
        replicate_mode: engine::scheduler::ReplicateMode::default(),
        global_index,
    };

    let scheduler =
        engine::scheduler::SyncScheduler::new(scheduler_config, sync_db_arc.clone()).await?;

    scheduler.start().await?;

    let app = api::create_sync_router(scheduler.clone(), regions_for_api);

    let http_bind = config.http_bind.clone();
    let listener = tokio::net::TcpListener::bind(&http_bind)
        .await
        .map_err(|e| {
            error!("Failed to bind HTTP server to {}: {}", http_bind, e);
            crate::util::error::HarDataError::Io(e)
        })?;

    info!("HTTP API listening on {}", http_bind);

    let api_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            error!("HTTP API server error: {}", e);
        }
    });

    info!("Sync started successfully (Press Ctrl+C to stop)");
    info!("  - HTTP API: {}", http_bind);
    info!(
        "  - Web UI: {}",
        if config.web_ui { "enabled" } else { "disabled" }
    );

    tokio::select! {
        _ = api_handle => {
            info!("HTTP API stopped");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
    }

    info!("Sync shutdown complete");
    Ok(())
}

async fn scan_and_index_local_files(
    data_dir: &str,
    global_index: &Arc<engine::ChunkIndex>,
    min_chunk_size: usize,
    avg_chunk_size: usize,
    max_chunk_size: usize,
) -> Result<(usize, usize)> {
    use crate::util::cdc::{StreamingFastCDC, StreamingFastCDCConfig};
    use futures::stream::{self, StreamExt};

    let data_path = Path::new(data_dir);
    if !data_path.exists() {
        warn!("Data directory does not exist: {}", data_dir);
        return Ok((0, 0));
    }

    let mut files = Vec::new();
    collect_files_recursive(data_path, &mut files).await?;

    info!("Found {} files to scan for global index", files.len());

    let cdc_config = StreamingFastCDCConfig {
        min_chunk_size,
        avg_chunk_size,
        max_chunk_size,
        window_size: 256 * 1024 * 1024,
    };

    let mut total_scanned = 0;
    let mut total_indexed = 0;
    let mut total_processed = 0;

    let mut stream = stream::iter(files)
        .map(|file_path| {
            let cdc_config_clone = cdc_config.clone();
            let global_index_clone = Arc::clone(global_index);

            async move {
                let metadata = match tokio::fs::metadata(&file_path).await {
                    Ok(m) => m,
                    Err(_) => return Ok::<_, crate::util::error::HarDataError>((0, false)),
                };

                let file_size = metadata.len();
                let mtime = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);

                let file_path_str = file_path.to_string_lossy().to_string();

                match global_index_clone.should_reindex_file(&file_path_str, mtime, file_size) {
                    Ok(false) => {
                        debug!("Skipping already indexed file: {}", file_path_str);
                        return Ok((0, false));
                    }
                    Ok(true) => {
                        debug!("Indexing file: {}", file_path_str);
                    }
                    Err(e) => {
                        debug!("Error checking file index status: {}, will reindex", e);
                    }
                }

                let cdc = StreamingFastCDC::new(cdc_config_clone);
                let chunk_entries = match cdc.chunk_file(&file_path).await {
                    Ok(entries) => entries,
                    Err(e) => {
                        warn!("Failed to chunk file {}: {}", file_path_str, e);
                        return Ok((0, false));
                    }
                };

                if chunk_entries.is_empty() {
                    return Ok((0, false));
                }

                let chunk_infos: Vec<engine::ChunkInfo> = chunk_entries
                    .iter()
                    .map(|entry| engine::ChunkInfo {
                        offset: entry.offset,
                        size: entry.length as u64,
                        strong_hash: entry.hash,
                        weak_hash: entry.weak_hash,
                    })
                    .collect();

                match global_index_clone.batch_insert_chunks(
                    &file_path_str,
                    &chunk_infos,
                    mtime,
                    file_size,
                ) {
                    Ok(indexed) => {
                        debug!("Indexed {} chunks for file: {}", indexed, file_path_str);
                        Ok((indexed, true))
                    }
                    Err(e) => {
                        warn!("Failed to index file {}: {}", file_path_str, e);
                        Ok((0, false))
                    }
                }
            }
        })
        .buffer_unordered(4);

    while let Some(result) = stream.next().await {
        total_processed += 1;
        match result {
            Ok((indexed, scanned)) => {
                if scanned {
                    total_scanned += 1;
                }
                total_indexed += indexed;

                if total_processed % 100 == 0 {
                    info!(
                        "Scan progress: {}/{} files indexed, {} chunks",
                        total_scanned, total_processed, total_indexed
                    );
                }
            }
            Err(e) => {
                warn!("Error during file scan: {}", e);
            }
        }
    }

    Ok((total_scanned, total_indexed))
}

fn collect_files_recursive<'a>(
    dir: &'a Path,
    files: &'a mut Vec<PathBuf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let mut entries = tokio::fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;

            if metadata.is_dir() {
                if let Some(name) = path.file_name() {
                    if name.to_string_lossy().starts_with('.') {
                        continue;
                    }
                }
                collect_files_recursive(&path, files).await?;
            } else if metadata.is_file() {
                files.push(path);
            }
        }

        Ok(())
    })
}
