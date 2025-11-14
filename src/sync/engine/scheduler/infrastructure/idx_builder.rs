use crate::sync::engine::{CDCResultCache, ChunkIndex};
use crate::sync::storage::db::Database;
use crate::util::cdc::{StreamingFastCDC, StreamingFastCDCConfig};
use crate::util::error::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::config::SchedulerConfig;

pub struct CacheBuilderStats {
    pub scanned_files: usize,
    pub cdc_cached: usize,
    pub global_indexed: usize,
    pub skipped_running: usize,
    pub skipped_recent: usize,
    pub skipped_already_cached: usize,
    pub total_chunks: usize,
}

pub struct CacheBuilder {
    config: Arc<SchedulerConfig>,
    global_index: Option<Arc<ChunkIndex>>,
    cdc_cache: Arc<CDCResultCache>,
    db: Arc<Database>,
    running: Arc<AtomicBool>,
    scanning: Arc<AtomicBool>,
}

impl CacheBuilder {
    pub fn new(
        config: Arc<SchedulerConfig>,
        global_index: Option<Arc<ChunkIndex>>,
        cdc_cache: Arc<CDCResultCache>,
        db: Arc<Database>,
    ) -> Self {
        Self {
            config,
            global_index,
            cdc_cache,
            db,
            running: Arc::new(AtomicBool::new(false)),
            scanning: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start(self: Arc<Self>) {
        let builder = self.clone();
        tokio::spawn(async move {
            debug!("CacheBuilder started (CDC + GlobalIndex)");
            builder.running.store(true, Ordering::Relaxed);

            sleep(Duration::from_secs(10)).await;

            loop {
                if !builder.running.load(Ordering::Relaxed) {
                    debug!("CacheBuilder stopping...");
                    break;
                }

                if builder
                    .scanning
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    let start = std::time::Instant::now();

                    match builder.scan_and_build().await {
                        Ok(stats) => {
                            let elapsed = start.elapsed();
                            if stats.cdc_cached > 0 || stats.global_indexed > 0 {
                                info!(
                                    "CacheBuilder: scanned={}, cdc_cached={}, global_indexed={}, skipped_running={}, skipped_recent={}, skipped_already={}, chunks={}, elapsed={:.2}s",
                                    stats.scanned_files,
                                    stats.cdc_cached,
                                    stats.global_indexed,
                                    stats.skipped_running,
                                    stats.skipped_recent,
                                    stats.skipped_already_cached,
                                    stats.total_chunks,
                                    elapsed.as_secs_f64()
                                );
                            }
                        }
                        Err(e) => {
                            warn!("CacheBuilder scan failed: {}", e);
                        }
                    }

                    builder.scanning.store(false, Ordering::Relaxed);
                } else {
                    debug!("CacheBuilder: previous scan still running, skipping this round");
                }

                sleep(Duration::from_secs(300)).await;
            }

            debug!("CacheBuilder stopped");
        });
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    async fn scan_and_build(&self) -> Result<CacheBuilderStats> {
        let data_dir = Path::new(&self.config.data_dir);
        if !data_dir.exists() {
            return Ok(CacheBuilderStats {
                scanned_files: 0,
                cdc_cached: 0,
                global_indexed: 0,
                skipped_running: 0,
                skipped_recent: 0,
                skipped_already_cached: 0,
                total_chunks: 0,
            });
        }

        let running_files = self.get_running_job_files().await?;

        let files = self.scan_directory(data_dir).await?;
        let scanned_files = files.len();

        let mut cdc_cached = 0;
        let mut global_indexed = 0;
        let mut skipped_running = 0;
        let mut skipped_recent = 0;
        let mut skipped_already_cached = 0;
        let mut total_chunks = 0;

        let now = SystemTime::now();

        for file_path in files {
            if running_files.contains(&file_path) {
                skipped_running += 1;
                continue;
            }

            let metadata = match fs::metadata(&file_path).await {
                Ok(m) => m,
                Err(e) => {
                    debug!("Failed to get metadata for {:?}: {}", file_path, e);
                    continue;
                }
            };

            let mtime = match metadata.modified() {
                Ok(t) => t,
                Err(_) => continue,
            };

            if let Ok(duration) = now.duration_since(mtime) {
                if duration < Duration::from_secs(60) {
                    skipped_recent += 1;
                    continue;
                }
            } else {
                continue;
            }

            let file_path_str = file_path.to_string_lossy().to_string();
            let file_size = metadata.len();
            let mtime_secs = mtime
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            if let Some(global_index) = &self.global_index {
                match global_index.should_reindex_file(&file_path_str, mtime_secs, file_size) {
                    Ok(false) => {
                        skipped_already_cached += 1;
                        continue;
                    }
                    Ok(true) => {}
                    Err(e) => {
                        debug!("Failed to check reindex status: {}", e);
                    }
                }
            }

            let (chunks, is_cdc_new) =
                match self.cdc_cache.get(&file_path_str, mtime_secs, file_size)? {
                    Some(cache) => (cache.chunks, false),
                    None => {
                        match self
                            .compute_and_cache_chunks(&file_path, mtime_secs, file_size)
                            .await
                        {
                            Ok(chunks) => {
                                cdc_cached += 1;
                                (chunks, true)
                            }
                            Err(e) => {
                                debug!("Failed to compute chunks for {:?}: {}", file_path, e);
                                continue;
                            }
                        }
                    }
                };

            if let Some(global_index) = &self.global_index {
                match global_index.batch_insert_chunks(
                    &file_path_str,
                    &chunks,
                    mtime_secs,
                    file_size,
                ) {
                    Ok(indexed) => {
                        if indexed > 0 {
                            global_indexed += 1;
                            total_chunks += indexed;
                            debug!(
                                "Indexed {} chunks for file: {} (cdc_new={})",
                                indexed, file_path_str, is_cdc_new
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to index file {}: {}", file_path_str, e);
                    }
                }
            }
        }

        Ok(CacheBuilderStats {
            scanned_files,
            cdc_cached,
            global_indexed,
            skipped_running,
            skipped_recent,
            skipped_already_cached,
            total_chunks,
        })
    }

    async fn get_running_job_files(&self) -> Result<std::collections::HashSet<PathBuf>> {
        let mut running_files = std::collections::HashSet::new();

        let jobs = self.db.load_all_jobs().await?;

        for job in jobs {
            if matches!(
                job.status,
                crate::core::job::JobStatus::Pending | crate::core::job::JobStatus::Syncing
            ) {
                let dest_path = Path::new(&self.config.data_dir).join(&job.dest.path);
                if dest_path.exists() {
                    if let Ok(files) = self.get_directory_files(&dest_path).await {
                        running_files.extend(files);
                    }
                }
            }
        }

        Ok(running_files)
    }

    async fn get_directory_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut dirs_to_scan = vec![dir.to_path_buf()];

        while let Some(dir) = dirs_to_scan.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            while let Some(entry) = entries.next_entry().await.transpose() {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let path = entry.path();
                let metadata = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if metadata.is_dir() {
                    dirs_to_scan.push(path);
                } else if metadata.is_file() {
                    files.push(path);
                }
            }
        }

        Ok(files)
    }

    async fn scan_directory(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.get_directory_files(path).await
    }

    async fn compute_and_cache_chunks(
        &self,
        file_path: &Path,
        mtime: i64,
        file_size: u64,
    ) -> Result<Vec<crate::sync::engine::ChunkInfo>> {
        let cdc_config = StreamingFastCDCConfig {
            min_chunk_size: self.config.min_chunk_size,
            avg_chunk_size: self.config.avg_chunk_size,
            max_chunk_size: self.config.max_chunk_size,
            ..Default::default()
        };
        let cdc = StreamingFastCDC::new(cdc_config);

        let chunks = cdc.chunk_file(file_path).await?;

        let result: Vec<crate::sync::engine::ChunkInfo> = chunks
            .into_iter()
            .map(|ce| crate::sync::engine::ChunkInfo {
                offset: ce.offset,
                size: ce.length as u64,
                strong_hash: ce.hash,
                weak_hash: ce.weak_hash,
            })
            .collect();

        let file_path_str = file_path.to_string_lossy().to_string();
        let file_cache = crate::sync::engine::FileChunkCache {
            mtime,
            size: file_size,
            chunks: result.clone(),
        };

        if let Err(e) = self.cdc_cache.put(&file_path_str, file_cache) {
            warn!("Failed to cache CDC result for {:?}: {}", file_path, e);
        }

        Ok(result)
    }
}
