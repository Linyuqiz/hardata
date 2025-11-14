use crate::sync::engine::core::FileChunk;
use crate::util::cdc::{StreamingFastCDC, StreamingFastCDCConfig};
use crate::util::error::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::{debug, info};

use crate::sync::engine::scheduler::infrastructure::config::SchedulerConfig;

pub struct LocalCdcResult {
    pub chunks: Vec<crate::util::cdc::ChunkEntry>,
    pub mtime: i64,
    pub file_size: u64,
}

pub struct WeakHashMatchResult {
    pub existing_weak_hashes: HashSet<u64>,
    pub weak_matched_indices: Vec<usize>,
    pub local_chunk_map: HashMap<u64, (u64, usize)>,
}

pub async fn check_local_cdc(
    config: &SchedulerConfig,
    dest_path: &str,
    chunk_index: Option<&std::sync::Arc<crate::sync::engine::CDCResultCache>>,
) -> Result<Option<LocalCdcResult>> {
    let dest_file_path = Path::new(dest_path);

    if !dest_file_path.exists() {
        info!(
            "Local dest file {} does not exist, no local deduplication possible",
            dest_path
        );
        return Ok(None);
    }

    let metadata = match tokio::fs::metadata(dest_file_path).await {
        Ok(m) => m,
        Err(e) => {
            info!(
                "Failed to read metadata for {}: {}, skipping deduplication",
                dest_path, e
            );
            return Ok(None);
        }
    };

    let file_size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let local_chunks = if let Some(cache) = chunk_index {
        match cache.get(dest_path, mtime, file_size) {
            Ok(Some(cached)) => {
                info!(
                    "CDCResultCache hit for {}: {} chunks (mtime={}, size={})",
                    dest_path,
                    cached.chunks.len(),
                    mtime,
                    file_size
                );
                cached
                    .chunks
                    .into_iter()
                    .map(|ci| crate::util::cdc::ChunkEntry {
                        offset: ci.offset,
                        length: ci.size as usize,
                        weak_hash: ci.weak_hash,
                        hash: ci.strong_hash,
                    })
                    .collect()
            }
            Ok(None) | Err(_) => {
                debug!("CDC cache miss for {}, calculating CDC", dest_path);
                let chunks = compute_cdc(config, dest_file_path).await?;

                save_to_cache(cache, dest_path, &chunks, mtime, file_size);

                chunks
            }
        }
    } else {
        compute_cdc(config, dest_file_path).await?
    };

    if local_chunks.is_empty() {
        info!(
            "Local dest file {} is empty, no deduplication possible",
            dest_path
        );
        return Ok(None);
    }

    info!("Local dest file has {} chunks", local_chunks.len());

    Ok(Some(LocalCdcResult {
        chunks: local_chunks,
        mtime,
        file_size,
    }))
}

async fn compute_cdc(
    config: &SchedulerConfig,
    dest_file_path: &Path,
) -> Result<Vec<crate::util::cdc::ChunkEntry>> {
    let cdc_config = StreamingFastCDCConfig {
        min_chunk_size: config.min_chunk_size,
        avg_chunk_size: config.avg_chunk_size,
        max_chunk_size: config.max_chunk_size,
        ..Default::default()
    };
    let cdc = StreamingFastCDC::new(cdc_config);

    match cdc.chunk_file_weak_only(dest_file_path).await {
        Ok(chunks) => Ok(chunks),
        Err(e) => {
            info!(
                "Failed to chunk local dest file {:?}: {}, skipping deduplication",
                dest_file_path, e
            );
            Err(e)
        }
    }
}

fn save_to_cache(
    cache: &crate::sync::engine::CDCResultCache,
    dest_path: &str,
    chunks: &[crate::util::cdc::ChunkEntry],
    mtime: i64,
    file_size: u64,
) {
    let cache_chunks: Vec<crate::sync::engine::ChunkInfo> = chunks
        .iter()
        .map(|ce| crate::sync::engine::ChunkInfo {
            offset: ce.offset,
            size: ce.length as u64,
            strong_hash: ce.hash,
            weak_hash: ce.weak_hash,
        })
        .collect();

    let file_cache = crate::sync::engine::FileChunkCache {
        mtime,
        size: file_size,
        chunks: cache_chunks,
    };

    if let Err(e) = cache.put(dest_path, file_cache) {
        info!("Failed to update CDCResultCache for {}: {}", dest_path, e);
    } else {
        info!(
            "Updated CDCResultCache for {}: {} chunks",
            dest_path,
            chunks.len()
        );
    }
}

pub fn match_weak_hashes(
    source_chunks: &[FileChunk],
    local_chunks: &[crate::util::cdc::ChunkEntry],
) -> WeakHashMatchResult {
    let mut existing_weak_hashes: HashSet<u64> = HashSet::new();

    for chunk in local_chunks {
        existing_weak_hashes.insert(chunk.weak_hash);
    }

    let local_chunk_map: HashMap<u64, (u64, usize)> = local_chunks
        .iter()
        .map(|c| (c.weak_hash, (c.offset, c.length)))
        .collect();

    let mut weak_matched_indices: Vec<usize> = Vec::new();
    for (idx, chunk) in source_chunks.iter().enumerate() {
        if existing_weak_hashes.contains(&chunk.chunk_hash.weak) {
            weak_matched_indices.push(idx);
        }
    }

    info!(
        "Weak hash matching: {} of {} source chunks match local dest",
        weak_matched_indices.len(),
        source_chunks.len()
    );

    WeakHashMatchResult {
        existing_weak_hashes,
        weak_matched_indices,
        local_chunk_map,
    }
}
