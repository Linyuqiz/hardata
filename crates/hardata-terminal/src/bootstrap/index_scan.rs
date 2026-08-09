use hardata_app::application::sync::engine;
use hardata_app::shared::error::Result;
use hardata_app::shared::time::metadata_mtime_nanos;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

pub(super) async fn scan_and_index_local_files(
    data_dir: &str,
    global_index: &Arc<engine::ChunkIndex>,
    min_chunk_size: usize,
    avg_chunk_size: usize,
    max_chunk_size: usize,
) -> Result<(usize, usize)> {
    use futures::stream::{self, StreamExt};
    use hardata_app::shared::cdc::{StreamingFastCDC, StreamingFastCDCConfig};

    let data_path = Path::new(data_dir);
    if !data_path.exists() {
        warn!(
            operation = "sync.global_index_scan_skipped",
            data_dir = %data_dir,
            reason = "data_directory_missing",
            "global index scan skipped"
        );
        return Ok((0, 0));
    }

    let mut files = Vec::new();
    collect_files_recursive(data_path, &mut files).await?;

    info!(
        operation = "sync.global_index_scan_discovered",
        file_count = files.len(),
        "global index scan files discovered"
    );

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
                    Err(_) => return Ok::<_, hardata_app::shared::error::HarDataError>((0, false)),
                };

                let file_size = metadata.len();
                let mtime = metadata_mtime_nanos(&metadata);

                let file_path_str = file_path.to_string_lossy().to_string();

                match global_index_clone.should_reindex_file(&file_path_str, mtime, file_size) {
                    Ok(false) => {
                        debug!(
                            operation = "sync.global_index_file_skipped",
                            path = %file_path_str,
                            reason = "already_indexed",
                            "global index file skipped"
                        );
                        return Ok((0, false));
                    }
                    Ok(true) => {
                        debug!(
                            operation = "sync.global_index_file_started",
                            path = %file_path_str,
                            "global index file started"
                        );
                    }
                    Err(e) => {
                        debug!(
                            operation = "sync.global_index_reindex_check_failed",
                            path = %file_path_str,
                            error = %e,
                            "global index reindex check failed"
                        );
                    }
                }

                let cdc = StreamingFastCDC::new(cdc_config_clone);
                let chunk_entries = match cdc.chunk_file(&file_path).await {
                    Ok(entries) => entries,
                    Err(e) => {
                        warn!(
                            operation = "sync.global_index_file_chunk_failed",
                            path = %file_path_str,
                            error = %e,
                            "global index file chunking failed"
                        );
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
                        debug!(
                            operation = "sync.global_index_file_completed",
                            path = %file_path_str,
                            indexed_chunks = indexed,
                            "global index file completed"
                        );
                        Ok((indexed, true))
                    }
                    Err(e) => {
                        warn!(
                            operation = "sync.global_index_file_failed",
                            path = %file_path_str,
                            error = %e,
                            "global index file indexing failed"
                        );
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
                        operation = "sync.global_index_scan_progress",
                        scanned_files = total_scanned,
                        processed_files = total_processed,
                        indexed_chunks = total_indexed,
                        "global index scan progress"
                    );
                }
            }
            Err(e) => {
                warn!(
                    operation = "sync.global_index_scan_file_failed",
                    error = %e,
                    "global index file scan failed"
                );
            }
        }
    }

    Ok((total_scanned, total_indexed))
}

pub(super) fn collect_files_recursive<'a>(
    dir: &'a Path,
    files: &'a mut Vec<PathBuf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let mut entries = tokio::fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = tokio::fs::symlink_metadata(&path).await?;

            if metadata.file_type().is_symlink() {
                continue;
            }

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
