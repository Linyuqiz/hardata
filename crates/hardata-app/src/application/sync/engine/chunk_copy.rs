use crate::application::sync::engine::ChunkLocation;
use crate::shared::error::Result;
use crate::shared::file_ops::{read_file_range, write_file_range};
use crate::shared::time::{metadata_mtime_nanos, timestamps_match};
use std::collections::HashMap;
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
            debug!(operation = "dedup.source_missing", path = %source.file_path, "dedup source file missing");
            return Ok(CopyResult::SourceNotFound);
        }
        Err(e) => {
            return Ok(CopyResult::IOError(format!(
                "Failed to stat source file: {}",
                e
            )));
        }
    };

    let current_mtime = metadata_mtime_nanos(&metadata);

    if !timestamps_match(current_mtime, source.mtime) {
        debug!(
            operation = "dedup.source_changed",
            path = %source.file_path,
            expected_mtime = source.mtime,
            actual_mtime = current_mtime,
            "dedup source file changed"
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
            operation = "dedup.hash_mismatch",
            path = %source.file_path,
            offset = source.offset,
            expected_hash = %hex::encode(expected_hash),
            actual_hash = %hex::encode(actual_hash.as_bytes()),
            "dedup chunk hash mismatch"
        );
        return Ok(CopyResult::HashMismatch);
    }

    match write_file_range(dest_path, dest_offset, &data).await {
        Ok(_) => {
            debug!(
                operation = "dedup.chunk_copied",
                source = %source.file_path,
                source_offset = source.offset,
                destination = %dest_path,
                destination_offset = dest_offset,
                bytes = data.len(),
                "dedup chunk copied"
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
        operation = "dedup.batch_copy_started",
        chunk_count = by_source.values().map(|v| v.len()).sum::<usize>(),
        source_file_count = by_source.len(),
        destination = %dest_path,
        "dedup batch copy started"
    );

    for tasks in by_source.values_mut() {
        tasks.sort_by_key(|t| t.source.offset);
    }

    for (source_file, tasks) in by_source {
        debug!(operation = "dedup.source_batch_started", source = %source_file, chunk_count = tasks.len(), "dedup source batch started");

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
                    warn!(operation = "dedup.chunk_copy_failed", error = %e, "dedup chunk copy I/O failed");
                    stats.io_failed += 1;
                    stats.failed_chunks.push(task.chunk_index);
                }
                Err(e) => {
                    warn!(operation = "dedup.chunk_copy_failed", error = %e, "dedup chunk copy failed");
                    stats.io_failed += 1;
                    stats.failed_chunks.push(task.chunk_index);
                }
            }
        }
    }

    info!(
        operation = "dedup.batch_copy_completed",
        succeeded = stats.success_count,
        verification_failed = stats.verification_failed,
        io_failed = stats.io_failed,
        bytes_copied = stats.bytes_copied,
        "dedup batch copy completed"
    );

    Ok(stats)
}
