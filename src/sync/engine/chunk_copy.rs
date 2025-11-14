use crate::sync::engine::ChunkLocation;
use crate::util::error::Result;
use crate::util::file_ops::{read_file_range, write_file_range};
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyResult {
    Success(u64),
    SourceNotFound,
    SourceModified,
    HashMismatch,
    IOError(String),
}

#[derive(Debug, Clone)]
pub struct CopyTask {
    pub chunk_index: usize,
    pub source: ChunkLocation,
    pub dest_offset: u64,
    pub expected_hash: [u8; 32],
}

#[derive(Debug, Clone, Default)]
pub struct BatchCopyStats {
    pub success_count: usize,
    pub bytes_copied: u64,
    pub verification_failed: usize,
    pub io_failed: usize,
    pub failed_chunks: Vec<usize>,
}

impl BatchCopyStats {
    pub fn failed_count(&self) -> usize {
        self.verification_failed + self.io_failed
    }
}

pub async fn copy_chunk_from_file(
    source: &ChunkLocation,
    dest_path: &str,
    dest_offset: u64,
    expected_hash: [u8; 32],
) -> Result<CopyResult> {
    let metadata = match tokio::fs::metadata(&source.file_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            debug!("Source file not found: {}", source.file_path);
            return Ok(CopyResult::SourceNotFound);
        }
        Err(e) => {
            return Ok(CopyResult::IOError(format!(
                "Failed to stat source file: {}",
                e
            )));
        }
    };

    let current_mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    if current_mtime != source.mtime {
        debug!(
            "Source file modified: {} (expected mtime={}, actual={})",
            source.file_path, source.mtime, current_mtime
        );
        return Ok(CopyResult::SourceModified);
    }

    let data = match read_file_range(&source.file_path, source.offset, source.size).await {
        Ok(d) => d,
        Err(e) => {
            return Ok(CopyResult::IOError(format!(
                "Failed to read source chunk: {}",
                e
            )));
        }
    };

    let actual_hash = blake3::hash(&data);
    if actual_hash.as_bytes() != &expected_hash {
        warn!(
            "Hash mismatch when copying from {} offset {}: expected {} got {}",
            source.file_path,
            source.offset,
            hex::encode(expected_hash),
            hex::encode(actual_hash.as_bytes())
        );
        return Ok(CopyResult::HashMismatch);
    }

    match write_file_range(dest_path, dest_offset, &data).await {
        Ok(_) => {
            debug!(
                "Copied chunk from {} offset {} to {} offset {} ({} bytes)",
                source.file_path,
                source.offset,
                dest_path,
                dest_offset,
                data.len()
            );
            Ok(CopyResult::Success(data.len() as u64))
        }
        Err(e) => Ok(CopyResult::IOError(format!(
            "Failed to write dest chunk: {}",
            e
        ))),
    }
}

pub async fn copy_chunks_batch(tasks: Vec<CopyTask>, dest_path: &str) -> Result<BatchCopyStats> {
    let mut stats = BatchCopyStats::default();

    if tasks.is_empty() {
        return Ok(stats);
    }

    let mut by_source: HashMap<String, Vec<CopyTask>> = HashMap::new();
    for task in tasks {
        by_source
            .entry(task.source.file_path.clone())
            .or_default()
            .push(task);
    }

    info!(
        "Batch copy: {} chunks from {} source files",
        by_source.values().map(|v| v.len()).sum::<usize>(),
        by_source.len()
    );

    for tasks in by_source.values_mut() {
        tasks.sort_by_key(|t| t.source.offset);
    }

    for (source_file, tasks) in by_source {
        debug!("Copying {} chunks from {}", tasks.len(), source_file);

        for task in tasks {
            match copy_chunk_from_file(
                &task.source,
                dest_path,
                task.dest_offset,
                task.expected_hash,
            )
            .await
            {
                Ok(CopyResult::Success(bytes)) => {
                    stats.success_count += 1;
                    stats.bytes_copied += bytes;
                }
                Ok(CopyResult::SourceNotFound) | Ok(CopyResult::SourceModified) => {
                    stats.verification_failed += 1;
                    stats.failed_chunks.push(task.chunk_index);
                }
                Ok(CopyResult::HashMismatch) => {
                    stats.verification_failed += 1;
                    stats.failed_chunks.push(task.chunk_index);
                }
                Ok(CopyResult::IOError(e)) => {
                    warn!("I/O error when copying chunk: {}", e);
                    stats.io_failed += 1;
                    stats.failed_chunks.push(task.chunk_index);
                }
                Err(e) => {
                    warn!("Error when copying chunk: {}", e);
                    stats.io_failed += 1;
                    stats.failed_chunks.push(task.chunk_index);
                }
            }
        }
    }

    info!(
        "Batch copy completed: {} success, {} verification failed, {} I/O failed ({} bytes total)",
        stats.success_count, stats.verification_failed, stats.io_failed, stats.bytes_copied
    );

    Ok(stats)
}
