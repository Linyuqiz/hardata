use super::types::{ChunkLocation, DedupResult};
use crate::core::ChunkLocation as ProtocolChunkLocation;
use crate::sync::net::transport::TransportConnection;
use crate::sync::scanner::ScannedFile;
use crate::util::error::{HarDataError, Result};
use crate::util::time::{metadata_mtime_nanos, timestamps_match};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

#[derive(Clone)]
struct CandidateLocation {
    local_file_path: String,
    local_offset: u64,
    local_strong: [u8; 32],
    location: ChunkLocation,
}

struct ChunkToVerify {
    chunk_idx: usize,
    candidates: Vec<CandidateLocation>,
}

fn select_candidate(
    candidates: &[CandidateLocation],
    remote_strong: [u8; 32],
) -> Option<&CandidateLocation> {
    candidates
        .iter()
        .find(|candidate| candidate.local_strong == remote_strong)
}

#[allow(clippy::too_many_arguments)]
pub async fn query_chunks(
    weak_hash_tree: &sled::Tree,
    hit_count: &AtomicU64,
    weak_filter_count: &AtomicU64,
    chunks: &mut [crate::sync::engine::core::FileChunk],
    dest_path: &str,
    existing_strong_hashes: &HashSet<[u8; 32]>,
    connection: &mut TransportConnection,
    file: &ScannedFile,
) -> Result<DedupResult> {
    let mut global_strong_hashes = HashSet::new();
    let mut global_chunk_info = HashMap::new();
    let mut global_dedup_count = 0;

    let mut weak_candidates: HashMap<usize, Vec<ChunkLocation>> = HashMap::new();
    let mut total_weak_matches = 0;

    for (idx, chunk) in chunks.iter().enumerate() {
        if let Some(strong) = chunk.chunk_hash.strong {
            if existing_strong_hashes.contains(&strong) {
                continue;
            }
        }

        let weak_hash = chunk.chunk_hash.weak;
        let key = weak_hash.to_be_bytes();

        if let Some(value) = weak_hash_tree
            .get(key)
            .map_err(|e| HarDataError::InvalidConfig(format!("Failed to query weak_hash: {}", e)))?
        {
            let locations: Vec<ChunkLocation> = bincode::deserialize(&value).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to deserialize locations: {}", e))
            })?;

            let valid_locations: Vec<ChunkLocation> = locations
                .into_iter()
                .filter(|loc| loc.file_path != dest_path)
                .collect();

            if !valid_locations.is_empty() {
                total_weak_matches += valid_locations.len();
                weak_candidates.insert(idx, valid_locations);
            }
        }
    }

    weak_filter_count.fetch_add(weak_candidates.len() as u64, Ordering::Relaxed);

    debug!(
        "Global index weak filter: {}/{} chunks have weak matches ({} total candidates)",
        weak_candidates.len(),
        chunks.len(),
        total_weak_matches
    );

    if weak_candidates.is_empty() {
        return Ok(DedupResult {
            strong_hashes: global_strong_hashes,
            chunk_info: global_chunk_info,
            dedup_count: 0,
        });
    }

    let mut chunks_need_remote_verification: Vec<ChunkToVerify> = Vec::new();

    for (chunk_idx, candidate_locations) in weak_candidates {
        let chunk = &chunks[chunk_idx];
        let mut verification_candidates = Vec::new();

        for location in candidate_locations {
            if !verify_location_sync(&location)? {
                continue;
            }

            let local_strong = match location.strong_hash {
                Some(hash) => hash,
                None => continue,
            };

            if let Some(target_strong) = chunk.chunk_hash.strong {
                if target_strong == local_strong {
                    global_strong_hashes.insert(local_strong);
                    global_chunk_info
                        .entry(local_strong)
                        .or_insert_with(Vec::new)
                        .push(location.clone());
                    global_dedup_count += 1;
                    hit_count.fetch_add(1, Ordering::Relaxed);

                    debug!(
                        "Global dedup: chunk {} found in {} at offset {}",
                        hex::encode(local_strong),
                        location.file_path,
                        location.offset
                    );

                    verification_candidates.clear();
                    break;
                }
            } else {
                verification_candidates.push(CandidateLocation {
                    local_file_path: location.file_path.clone(),
                    local_offset: location.offset,
                    local_strong,
                    location: location.clone(),
                });
            }
        }

        if chunk.chunk_hash.strong.is_none() && !verification_candidates.is_empty() {
            chunks_need_remote_verification.push(ChunkToVerify {
                chunk_idx,
                candidates: verification_candidates,
            });
        }
    }

    if !chunks_need_remote_verification.is_empty() {
        verify_with_remote(
            chunks,
            &chunks_need_remote_verification,
            connection,
            file,
            &mut global_strong_hashes,
            &mut global_chunk_info,
            &mut global_dedup_count,
            hit_count,
        )
        .await?;
    }

    if global_dedup_count > 0 {
        debug!(
            "Global dedup completed: {}/{} chunks found in other files ({:.1}%)",
            global_dedup_count,
            chunks.len(),
            global_dedup_count as f64 / chunks.len() as f64 * 100.0
        );
    }

    Ok(DedupResult {
        strong_hashes: global_strong_hashes,
        chunk_info: global_chunk_info,
        dedup_count: global_dedup_count,
    })
}

#[allow(clippy::too_many_arguments)]
async fn verify_with_remote(
    chunks: &mut [crate::sync::engine::core::FileChunk],
    chunks_to_verify: &[ChunkToVerify],
    connection: &mut TransportConnection,
    file: &ScannedFile,
    global_strong_hashes: &mut HashSet<[u8; 32]>,
    global_chunk_info: &mut HashMap<[u8; 32], Vec<ChunkLocation>>,
    global_dedup_count: &mut usize,
    hit_count: &AtomicU64,
) -> Result<()> {
    debug!(
        "Requesting {} strong hashes from remote for global dedup verification",
        chunks_to_verify.len()
    );

    let chunk_locations: Vec<ProtocolChunkLocation> = chunks_to_verify
        .iter()
        .map(|cv| {
            let chunk = &chunks[cv.chunk_idx];
            ProtocolChunkLocation {
                offset: chunk.offset,
                length: chunk.length,
            }
        })
        .collect();

    let remote_file_path = file.path.to_str().unwrap_or("");
    match connection
        .get_strong_hashes(remote_file_path, chunk_locations)
        .await
    {
        Ok(response) => {
            debug!(
                "Received {} strong hashes from remote for global dedup, verifying...",
                response.hashes.len()
            );

            if response.hashes.len() != chunks_to_verify.len() {
                warn!(
                    "Remote returned {} strong hashes, expected {}",
                    response.hashes.len(),
                    chunks_to_verify.len()
                );
                return Ok(());
            }

            for (cv, remote_hash_result) in chunks_to_verify.iter().zip(response.hashes.iter()) {
                let remote_strong = remote_hash_result.strong_hash;
                if let Some(candidate) = select_candidate(&cv.candidates, remote_strong) {
                    chunks[cv.chunk_idx].chunk_hash.strong = Some(remote_strong);

                    global_strong_hashes.insert(remote_strong);
                    global_chunk_info
                        .entry(remote_strong)
                        .or_default()
                        .push(candidate.location.clone());
                    *global_dedup_count += 1;
                    hit_count.fetch_add(1, Ordering::Relaxed);

                    debug!(
                        "Global dedup verified: chunk {} found in {} at offset {}",
                        hex::encode(remote_strong),
                        candidate.local_file_path,
                        candidate.local_offset
                    );
                }
            }
        }
        Err(e) => {
            warn!("Failed to get remote strong hashes for global dedup: {}", e);
        }
    }

    Ok(())
}

fn verify_location_sync(location: &ChunkLocation) -> Result<bool> {
    let file_path = Path::new(&location.file_path);

    if !file_path.exists() {
        return Ok(false);
    }

    if let Ok(metadata) = std::fs::metadata(file_path) {
        return Ok(timestamps_match(
            metadata_mtime_nanos(&metadata),
            location.mtime,
        ));
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{select_candidate, CandidateLocation};
    use crate::sync::engine::ChunkLocation;

    #[test]
    fn select_candidate_matches_remote_hash_among_multiple_candidates() {
        let strong_a = [1; 32];
        let strong_b = [2; 32];
        let candidates = vec![
            CandidateLocation {
                local_file_path: "a.bin".to_string(),
                local_offset: 0,
                local_strong: strong_a,
                location: ChunkLocation {
                    file_path: "a.bin".to_string(),
                    offset: 0,
                    size: 4,
                    mtime: 1,
                    strong_hash: Some(strong_a),
                },
            },
            CandidateLocation {
                local_file_path: "b.bin".to_string(),
                local_offset: 8,
                local_strong: strong_b,
                location: ChunkLocation {
                    file_path: "b.bin".to_string(),
                    offset: 8,
                    size: 4,
                    mtime: 1,
                    strong_hash: Some(strong_b),
                },
            },
        ];

        let selected = select_candidate(&candidates, strong_b).unwrap();
        assert_eq!(selected.local_file_path, "b.bin");
        assert_eq!(selected.local_offset, 8);
    }
}
