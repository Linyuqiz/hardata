use crate::sync::engine::core::FileChunk;
use crate::sync::net::transport::TransportConnection;
use crate::sync::scanner::ScannedFile;
use crate::util::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info};

use super::super::PrefetchManager;

pub type LocalChunkInfo = HashMap<[u8; 32], (u64, usize)>;

struct ChunkToVerify {
    chunk_idx: usize,
    local_offset: u64,
    local_length: usize,
    local_strong: [u8; 32],
}

pub struct StrongHashVerifyResult {
    pub existing_strong_hashes: HashSet<[u8; 32]>,
    pub local_chunk_info: LocalChunkInfo,
    pub dedup_count: usize,
    pub computed_strong_hashes: HashMap<u64, [u8; 32]>,
}

#[allow(clippy::too_many_arguments)]
pub async fn verify_strong_hashes(
    chunks: &mut [FileChunk],
    weak_matched_indices: &[usize],
    local_chunk_map: &HashMap<u64, (u64, usize)>,
    connection: &mut TransportConnection,
    file: &ScannedFile,
    dest_path: &str,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
) -> Result<StrongHashVerifyResult> {
    let mut existing_strong_hashes: HashSet<[u8; 32]> = HashSet::new();
    let mut local_chunk_info: LocalChunkInfo = HashMap::new();
    let mut dedup_count = 0;
    let mut computed_strong_hashes: HashMap<u64, [u8; 32]> = HashMap::new();

    if weak_matched_indices.is_empty() {
        debug!("No weak hash matches, skipping strong hash verification");
        return Ok(StrongHashVerifyResult {
            existing_strong_hashes,
            local_chunk_info,
            dedup_count,
            computed_strong_hashes,
        });
    }

    info!(
        "Found {} weak hash matches, calculating local strong hashes",
        weak_matched_indices.len()
    );

    let mut chunks_need_remote_verification: Vec<ChunkToVerify> = Vec::new();

    for &idx in weak_matched_indices {
        let source_chunk = &chunks[idx];
        let source_weak = source_chunk.chunk_hash.weak;

        if let Some(&(local_offset, local_length)) = local_chunk_map.get(&source_weak) {
            let local_data = if let Some(pm) = prefetch_manager {
                match pm
                    .get_chunk_data(dest_path, local_offset, local_length as u64)
                    .await
                {
                    Ok(data) => (*data).clone(),
                    Err(_) => continue,
                }
            } else {
                match crate::util::file_ops::read_file_range(
                    dest_path,
                    local_offset,
                    local_length as u64,
                )
                .await
                {
                    Ok(data) => data,
                    Err(_) => continue,
                }
            };

            let local_strong = blake3::hash(&local_data);
            let local_strong_bytes: [u8; 32] = *local_strong.as_bytes();

            computed_strong_hashes.insert(source_weak, local_strong_bytes);

            if let Some(source_strong) = source_chunk.chunk_hash.strong {
                if source_strong == local_strong_bytes {
                    existing_strong_hashes.insert(source_strong);
                    local_chunk_info.insert(source_strong, (local_offset, local_length));
                    dedup_count += 1;
                }
            } else {
                chunks_need_remote_verification.push(ChunkToVerify {
                    chunk_idx: idx,
                    local_offset,
                    local_length,
                    local_strong: local_strong_bytes,
                });
            }
        }
    }

    if !chunks_need_remote_verification.is_empty() {
        verify_with_remote(
            chunks,
            &chunks_need_remote_verification,
            connection,
            file,
            &mut existing_strong_hashes,
            &mut local_chunk_info,
            &mut dedup_count,
        )
        .await?;
    }

    if dedup_count > 0 {
        info!(
            "Deduplication completed: {}/{} chunks already exist ({:.1}%)",
            dedup_count,
            chunks.len(),
            dedup_count as f64 / chunks.len() as f64 * 100.0
        );
    }

    Ok(StrongHashVerifyResult {
        existing_strong_hashes,
        local_chunk_info,
        dedup_count,
        computed_strong_hashes,
    })
}

async fn verify_with_remote(
    chunks: &mut [FileChunk],
    chunks_to_verify: &[ChunkToVerify],
    connection: &mut TransportConnection,
    file: &ScannedFile,
    existing_strong_hashes: &mut HashSet<[u8; 32]>,
    local_chunk_info: &mut LocalChunkInfo,
    dedup_count: &mut usize,
) -> Result<()> {
    info!(
        "Requesting {} strong hashes from remote for batch verification",
        chunks_to_verify.len()
    );

    let chunk_locations: Vec<crate::core::ChunkLocation> = chunks_to_verify
        .iter()
        .map(|cv| {
            let source_chunk = &chunks[cv.chunk_idx];
            crate::core::ChunkLocation {
                offset: source_chunk.offset,
                length: source_chunk.length,
            }
        })
        .collect();

    let remote_file_path = file.path.to_str().unwrap_or("");
    match connection
        .get_strong_hashes(remote_file_path, chunk_locations)
        .await
    {
        Ok(response) => {
            info!(
                "Received {} strong hashes from remote, verifying...",
                response.hashes.len()
            );

            if response.hashes.len() != chunks_to_verify.len() {
                info!(
                    "Remote returned {} hashes but expected {}, treating as verification failure",
                    response.hashes.len(),
                    chunks_to_verify.len()
                );
                return Ok(());
            }

            let mut verified_count = 0;
            for (verify_info, remote_hash_result) in
                chunks_to_verify.iter().zip(response.hashes.iter())
            {
                let remote_strong = remote_hash_result.strong_hash;

                if remote_strong == verify_info.local_strong {
                    chunks[verify_info.chunk_idx].chunk_hash.strong = Some(remote_strong);
                    existing_strong_hashes.insert(remote_strong);
                    local_chunk_info.insert(
                        remote_strong,
                        (verify_info.local_offset, verify_info.local_length),
                    );
                    *dedup_count += 1;
                    verified_count += 1;
                } else {
                    info!(
                        "Weak hash collision detected at offset {}: remote strong hash differs from local",
                        verify_info.local_offset
                    );
                }
            }

            info!(
                "Batch verification completed: {}/{} weak-only chunks verified successfully",
                verified_count,
                chunks_to_verify.len()
            );
        }
        Err(e) => {
            info!(
                "Failed to get remote strong hashes: {}, treating weak-only matches as non-dedup",
                e
            );
        }
    }

    Ok(())
}

pub fn update_cache(
    cache: &crate::sync::engine::CDCResultCache,
    dest_path: &str,
    local_chunks: Vec<crate::util::cdc::ChunkEntry>,
    computed_strong_hashes: &HashMap<u64, [u8; 32]>,
    mtime: i64,
    file_size: u64,
) {
    if computed_strong_hashes.is_empty() {
        return;
    }

    let updated_chunks: Vec<crate::util::cdc::ChunkEntry> = local_chunks
        .into_iter()
        .map(|mut chunk| {
            if let Some(&strong_hash) = computed_strong_hashes.get(&chunk.weak_hash) {
                chunk.hash = Some(strong_hash);
            }
            chunk
        })
        .collect();

    let cache_chunks: Vec<crate::sync::engine::ChunkInfo> = updated_chunks
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
        info!(
            "Failed to update CDCResultCache with computed strong hashes for {}: {}",
            dest_path, e
        );
    } else {
        info!(
            "Updated CDCResultCache for {} with {} newly computed strong hashes",
            dest_path,
            computed_strong_hashes.len()
        );
    }
}
