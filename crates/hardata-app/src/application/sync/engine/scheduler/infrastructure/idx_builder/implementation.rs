use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::{CDCResultCache, ChunkIndex};
use crate::shared::cdc::{StreamingFastCDC, StreamingFastCDCConfig};
use crate::shared::error::Result;
use crate::shared::time::metadata_mtime_nanos;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
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
    tmp_write_paths: Arc<DashMap<String, String>>,
    running: Arc<AtomicBool>,
    scanning: Arc<AtomicBool>,
    stop_signal: watch::Sender<bool>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl CacheBuilder {
    pub fn new(
        config: Arc<SchedulerConfig>,
        global_index: Option<Arc<ChunkIndex>>,
        cdc_cache: Arc<CDCResultCache>,
        db: Arc<Database>,
        tmp_write_paths: Arc<DashMap<String, String>>,
    ) -> Self {
        let (stop_signal, _) = watch::channel(false);

        Self {
            config,
            global_index,
            cdc_cache,
            db,
            tmp_write_paths,
            running: Arc::new(AtomicBool::new(false)),
            scanning: Arc::new(AtomicBool::new(false)),
            stop_signal,
            task: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start(self: &Arc<Self>) {
        self.running.store(true, Ordering::Relaxed);
        self.stop_signal.send_replace(false);

        let builder = self.clone();
        let handle = tokio::spawn(async move {
            debug!("CacheBuilder started (CDC + GlobalIndex)");

            if builder.wait_or_stop(Duration::from_secs(10)).await {
                debug!("CacheBuilder stopped before first scan");
                return;
            }

            loop {
                if !builder.running.load(Ordering::Relaxed) {
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
                                    operation = "cache_builder.scan_completed",
                                    scanned_files = stats.scanned_files,
                                    cdc_cached = stats.cdc_cached,
                                    global_indexed = stats.global_indexed,
                                    skipped_running = stats.skipped_running,
                                    skipped_recent = stats.skipped_recent,
                                    skipped_already_cached = stats.skipped_already_cached,
                                    total_chunks = stats.total_chunks,
                                    elapsed_seconds = elapsed.as_secs_f64(),
                                    "cache builder scan completed"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(operation = "cache_builder.scan_failed", error = %e, "cache builder scan failed");
                        }
                    }

                    builder.scanning.store(false, Ordering::Relaxed);
                } else {
                    debug!("CacheBuilder: previous scan still running, skipping this round");
                }

                if builder.wait_or_stop(Duration::from_secs(300)).await {
                    debug!("CacheBuilder received stop while idle");
                    break;
                }
            }

            debug!("CacheBuilder stopped");
        });

        *self.task.lock().await = Some(handle);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        self.stop_signal.send_replace(true);
    }

    pub async fn shutdown(&self) {
        self.stop();
        if let Some(handle) = self.task.lock().await.take() {
            if let Err(e) = handle.await {
                warn!(operation = "cache_builder.task_join_failed", error = %e, "cache builder task join failed");
            }
        }
    }

    async fn wait_or_stop(&self, duration: Duration) -> bool {
        if !self.running.load(Ordering::Relaxed) {
            return true;
        }

        let mut stop_rx = self.stop_signal.subscribe();
        if *stop_rx.borrow() {
            return true;
        }

        tokio::select! {
            _ = sleep(duration) => false,
            changed = stop_rx.changed() => changed.is_err() || *stop_rx.borrow(),
        }
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

        let running_files = self.collect_runtime_skip_paths().await?;

        let files = self.scan_directory(data_dir).await?;
        let scanned_files = files.len();

        if let Some(global_index) = &self.global_index {
            match global_index.cleanup_stale_file_scans() {
                Ok(removed) if removed > 0 => {
                    debug!("Pruned {} stale index entries before scan round", removed);
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(operation = "cache_builder.stale_scan_prune_failed", error = %e, "stale file scan pruning failed");
                }
            }
        }

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
                    debug!(operation = "cache_builder.metadata_read_failed", path = %file_path.display(), error = %e, "cache builder metadata read failed");
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
            let mtime_secs = metadata_mtime_nanos(&metadata);

            if let Some(global_index) = &self.global_index {
                match global_index.should_reindex_file(&file_path_str, mtime_secs, file_size) {
                    Ok(false) => {
                        skipped_already_cached += 1;
                        continue;
                    }
                    Ok(true) => {}
                    Err(e) => {
                        debug!(operation = "cache_builder.reindex_check_failed", path = %file_path_str, error = %e, "cache builder reindex check failed");
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
                                debug!(operation = "cache_builder.chunking_failed", path = %file_path.display(), error = %e, "cache builder chunking failed");
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
                        warn!(operation = "cache_builder.index_file_failed", path = %file_path_str, error = %e, "cache builder file indexing failed");
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

        let jobs = self.db.load_active_job_destinations().await?;

        for (job_id, dest_path) in jobs {
            let dest_path = match resolve_runtime_destination(&self.config, &dest_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!(
                        operation = "cache_builder.destination_skipped",
                        job_id = %job_id,
                        path = %dest_path,
                        error = %e,
                        "cache builder runtime destination skipped"
                    );
                    continue;
                }
            };
            if let Ok(files) = self.get_directory_files(&dest_path).await {
                running_files.extend(files);
            }
        }

        Ok(running_files)
    }

    async fn collect_runtime_skip_paths(&self) -> Result<std::collections::HashSet<PathBuf>> {
        let mut running_files = self.get_running_job_files().await?;
        running_files.extend(
            self.tmp_write_paths
                .iter()
                .map(|entry| PathBuf::from(entry.key().clone()))
                .collect::<std::collections::HashSet<_>>(),
        );
        running_files.extend(self.load_registered_tmp_paths().await?);
        Ok(running_files)
    }

    async fn load_registered_tmp_paths(&self) -> Result<std::collections::HashSet<PathBuf>> {
        let mut tmp_paths = std::collections::HashSet::new();

        for (job_id, path) in self.db.load_tmp_transfer_paths().await? {
            let path_buf = PathBuf::from(&path);
            match fs::symlink_metadata(&path_buf).await {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    tmp_paths.insert(path_buf);
                }
                _ => {
                    if let Err(e) = self.db.delete_tmp_transfer_path(&job_id, &path).await {
                        warn!(
                            operation = "cache_builder.stale_tmp_path_delete_failed",
                            job_id = %job_id,
                            path = %path,
                            error = %e,
                            "stale temporary transfer path deletion failed"
                        );
                    }
                }
            }
        }

        Ok(tmp_paths)
    }

    async fn get_directory_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        collect_regular_files(dir).await
    }

    async fn scan_directory(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.get_directory_files(path).await
    }

    async fn compute_and_cache_chunks(
        &self,
        file_path: &Path,
        mtime: i64,
        file_size: u64,
    ) -> Result<Vec<crate::application::sync::engine::ChunkInfo>> {
        let cdc_config = StreamingFastCDCConfig {
            min_chunk_size: self.config.min_chunk_size,
            avg_chunk_size: self.config.avg_chunk_size,
            max_chunk_size: self.config.max_chunk_size,
            ..Default::default()
        };
        let cdc = StreamingFastCDC::new(cdc_config);

        let chunks = cdc.chunk_file(file_path).await?;

        let result: Vec<crate::application::sync::engine::ChunkInfo> = chunks
            .into_iter()
            .map(|ce| crate::application::sync::engine::ChunkInfo {
                offset: ce.offset,
                size: ce.length as u64,
                strong_hash: ce.hash,
                weak_hash: ce.weak_hash,
            })
            .collect();

        let file_path_str = file_path.to_string_lossy().to_string();
        let file_cache = crate::application::sync::engine::FileChunkCache {
            mtime,
            size: file_size,
            chunks: result.clone(),
        };

        if let Err(e) = self.cdc_cache.put(&file_path_str, file_cache) {
            warn!(operation = "cache_builder.cdc_cache_write_failed", path = %file_path.display(), error = %e, "CDC cache write failed");
        }

        Ok(result)
    }
}

async fn collect_regular_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let root_metadata = match fs::symlink_metadata(dir).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    if root_metadata.file_type().is_symlink() {
        return Ok(Vec::new());
    }

    if root_metadata.is_file() {
        return Ok(vec![dir.to_path_buf()]);
    }

    let mut files = Vec::new();
    let mut dirs_to_scan = vec![dir.to_path_buf()];

    while let Some(dir) = dirs_to_scan.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => continue,
        };

        while let Some(entry) = entries.next_entry().await.transpose() {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let path = entry.path();
            let metadata = match fs::symlink_metadata(&path).await {
                Ok(m) => m,
                Err(_) => continue,
            };

            if metadata.file_type().is_symlink() {
                continue;
            }

            if metadata.is_dir() {
                dirs_to_scan.push(path);
            } else if metadata.is_file() {
                files.push(path);
            }
        }
    }

    Ok(files)
}

fn resolve_runtime_destination(config: &SchedulerConfig, dest: &str) -> Result<PathBuf> {
    config.resolve_runtime_destination_path(dest)
}
