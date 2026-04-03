use crate::sync::engine::{core::FileChunk, ChunkLocation};
use crate::sync::net::transport::TransportConnection;
use crate::sync::scanner::ScannedFile;
use crate::util::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info};

use super::super::PrefetchManager;

pub type LocalChunkInfo = HashMap<[u8; 32], Vec<ChunkLocation>>;

#[derive(Clone, Copy)]
struct LocalCandidate {
    local_offset: u64,
    local_length: usize,
    local_strong: [u8; 32],
}

struct ChunkToVerify {
    chunk_idx: usize,
    candidates: Vec<LocalCandidate>,
}

pub struct StrongHashVerifyResult {
    pub existing_strong_hashes: HashSet<[u8; 32]>,
    pub local_chunk_info: LocalChunkInfo,
    pub dedup_count: usize,
    pub computed_strong_hashes: HashMap<u64, [u8; 32]>,
}

fn record_local_match(
    local_chunk_info: &mut LocalChunkInfo,
    local_path: &str,
    local_mtime: i64,
    strong_hash: [u8; 32],
    local_offset: u64,
    local_length: usize,
) {
    let locations = local_chunk_info.entry(strong_hash).or_default();
    if !locations
        .iter()
        .any(|location| location.file_path == local_path && location.offset == local_offset)
    {
        locations.push(ChunkLocation {
            file_path: local_path.to_string(),
            offset: local_offset,
            size: local_length as u64,
            mtime: local_mtime,
            strong_hash: Some(strong_hash),
        });
    }
}

async fn read_local_chunk(
    dest_path: &str,
    local_offset: u64,
    local_length: usize,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
) -> Option<Vec<u8>> {
    if let Some(pm) = prefetch_manager {
        pm.get_chunk_data(dest_path, local_offset, local_length as u64)
            .await
            .ok()
            .map(|data| (*data).clone())
    } else {
        crate::util::file_ops::read_file_range(dest_path, local_offset, local_length as u64)
            .await
            .ok()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn verify_strong_hashes(
    chunks: &mut [FileChunk],
    weak_matched_indices: &[usize],
    local_chunk_map: &HashMap<u64, Vec<(u64, usize)>>,
    connection: &mut TransportConnection,
    file: &ScannedFile,
    local_path: &str,
    local_mtime: i64,
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
        let source_weak = chunks[idx].chunk_hash.weak;
        let source_strong = chunks[idx].chunk_hash.strong;
        let Some(local_locations) = local_chunk_map.get(&source_weak) else {
            continue;
        };

        let mut matched_existing_chunk = false;
        let mut candidates = Vec::new();

        for &(local_offset, local_length) in local_locations {
            let Some(local_data) =
                read_local_chunk(local_path, local_offset, local_length, prefetch_manager).await
            else {
                continue;
            };

            let local_strong = *blake3::hash(&local_data).as_bytes();
            computed_strong_hashes.insert(local_offset, local_strong);

            if let Some(source_strong) = source_strong {
                if source_strong == local_strong {
                    chunks[idx].chunk_hash.strong = Some(source_strong);
                    existing_strong_hashes.insert(source_strong);
                    record_local_match(
                        &mut local_chunk_info,
                        local_path,
                        local_mtime,
                        source_strong,
                        local_offset,
                        local_length,
                    );
                    dedup_count += 1;
                    matched_existing_chunk = true;
                    break;
                }
            } else {
                candidates.push(LocalCandidate {
                    local_offset,
                    local_length,
                    local_strong,
                });
            }
        }

        if !matched_existing_chunk && source_strong.is_none() && !candidates.is_empty() {
            chunks_need_remote_verification.push(ChunkToVerify {
                chunk_idx: idx,
                candidates,
            });
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
            local_path,
            local_mtime,
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

#[allow(clippy::too_many_arguments)]
async fn verify_with_remote(
    chunks: &mut [FileChunk],
    chunks_to_verify: &[ChunkToVerify],
    connection: &mut TransportConnection,
    file: &ScannedFile,
    existing_strong_hashes: &mut HashSet<[u8; 32]>,
    local_chunk_info: &mut LocalChunkInfo,
    dedup_count: &mut usize,
    local_path: &str,
    local_mtime: i64,
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
                if let Some(candidate) = verify_info
                    .candidates
                    .iter()
                    .find(|candidate| candidate.local_strong == remote_strong)
                {
                    chunks[verify_info.chunk_idx].chunk_hash.strong = Some(remote_strong);
                    existing_strong_hashes.insert(remote_strong);
                    record_local_match(
                        local_chunk_info,
                        local_path,
                        local_mtime,
                        remote_strong,
                        candidate.local_offset,
                        candidate.local_length,
                    );
                    *dedup_count += 1;
                    verified_count += 1;
                } else {
                    info!(
                        "Weak hash collision detected for chunk {}: {} local candidates did not match remote strong hash",
                        verify_info.chunk_idx,
                        verify_info.candidates.len()
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
            if let Some(&strong_hash) = computed_strong_hashes.get(&chunk.offset) {
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

#[cfg(test)]
mod tests {
    use super::verify_strong_hashes;
    use crate::core::chunk::ChunkHash;
    use crate::sync::engine::{core::FileChunk, ChunkLocation};
    use crate::sync::net::tcp::TcpClient;
    use crate::sync::net::transport::TransportConnection;
    use crate::sync::scanner::ScannedFile;
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[tokio::test]
    async fn verify_strong_hashes_checks_all_local_candidates_for_same_weak_hash() {
        let temp_dir = std::env::temp_dir();
        let dest_path = temp_dir.join(format!("hardata-remote-dedup-{}.bin", uuid::Uuid::new_v4()));
        tokio::fs::write(&dest_path, b"AAAABBBB").await.unwrap();

        let expected_strong = *blake3::hash(b"BBBB").as_bytes();
        let mut chunks = vec![FileChunk {
            file_path: "source.bin".to_string(),
            offset: 0,
            length: 4,
            chunk_hash: ChunkHash {
                weak: 7,
                strong: Some(expected_strong),
            },
        }];
        let mut local_chunk_map = HashMap::new();
        local_chunk_map.insert(7, vec![(0, 4), (4, 4)]);

        let mut connection = TransportConnection::Tcp {
            client: TcpClient::new("127.0.0.1:1".to_string()).unwrap(),
        };
        let file = ScannedFile {
            path: PathBuf::from("source.bin"),
            size: 4,
            modified: 0,
            change_time: None,
            inode: None,
            is_dir: false,
            mode: 0,
            is_symlink: false,
            symlink_target: None,
        };

        let result = verify_strong_hashes(
            &mut chunks,
            &[0],
            &local_chunk_map,
            &mut connection,
            &file,
            dest_path.to_str().unwrap(),
            123,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.dedup_count, 1);
        assert!(result.existing_strong_hashes.contains(&expected_strong));
        assert_eq!(
            result.local_chunk_info.get(&expected_strong),
            Some(&vec![ChunkLocation {
                file_path: dest_path.to_string_lossy().to_string(),
                offset: 4,
                size: 4,
                mtime: 123,
                strong_hash: Some(expected_strong),
            }])
        );
        assert_eq!(result.computed_strong_hashes.len(), 2);

        let _ = tokio::fs::remove_file(dest_path).await;
    }
}
