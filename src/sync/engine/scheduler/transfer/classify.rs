use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::core::FileChunk;
use crate::sync::engine::scheduler::dedup::{GlobalChunkInfo, LocalChunkInfo};
use crate::sync::engine::ChunkLocation;
use std::collections::{HashMap, HashSet};

pub struct ChunkClassification {
    pub chunks_to_transfer: Vec<usize>,
    pub chunks_to_relocate: Vec<(usize, u64, u64, usize)>,
    pub chunks_to_copy: Vec<(usize, ChunkLocation, u64)>,
    pub dedup_count: usize,
    pub skipped_bytes: u64,
}

#[allow(clippy::too_many_arguments)]
pub fn classify_chunks(
    chunks: &[FileChunk],
    state: &mut FileTransferState,
    dest_offset_map: &HashMap<usize, u64>,
    existing_strong_hashes: &HashSet<[u8; 32]>,
    local_chunk_info: &LocalChunkInfo,
    global_chunk_info: &GlobalChunkInfo,
    _dest_path: &str,
) -> ChunkClassification {
    tracing::info!(
        "classify_chunks called: global_chunk_info size={}, local_chunk_info size={}, existing_strong_hashes size={}",
        global_chunk_info.len(),
        local_chunk_info.len(),
        existing_strong_hashes.len()
    );

    for (idx, (hash, locs)) in global_chunk_info.iter().take(3).enumerate() {
        tracing::info!(
            "Sample global_chunk_info[{}]: hash={}, locations={}",
            idx,
            hex::encode(hash),
            locs.len()
        );
    }

    tracing::info!("Total chunks to classify: {}", chunks.len());

    let mut chunks_with_strong = 0;
    let mut chunks_in_global = 0;
    for (i, chunk) in chunks.iter().enumerate() {
        if let Some(strong) = chunk.chunk_hash.strong {
            chunks_with_strong += 1;
            let in_global = global_chunk_info.contains_key(&strong);
            if in_global {
                chunks_in_global += 1;
            }
            if i < 3 {
                let in_local = local_chunk_info.contains_key(&strong);
                let in_existing = existing_strong_hashes.contains(&strong);
                tracing::info!(
                    "Chunk[{}]: hash={}, in_global={}, in_local={}, in_existing={}",
                    i,
                    hex::encode(strong),
                    in_global,
                    in_local,
                    in_existing
                );
            }
        }
    }
    tracing::info!(
        "Chunks summary: total={}, with_strong={}, in_global={}",
        chunks.len(),
        chunks_with_strong,
        chunks_in_global
    );

    let mut chunks_to_transfer = Vec::new();
    let mut chunks_to_relocate = Vec::new();
    let mut chunks_to_copy = Vec::new();
    let mut dedup_count = 0;
    let mut skipped_bytes = 0u64;

    'outer: for (i, chunk) in chunks.iter().enumerate() {
        if state.is_chunk_completed(i) {
            continue;
        }

        let dest_offset = dest_offset_map.get(&i).copied().unwrap_or(0);

        let already_exists = chunk
            .chunk_hash
            .strong
            .map(|hash| existing_strong_hashes.contains(&hash))
            .unwrap_or(false);

        if already_exists {
            if let Some(strong_hash) = chunk.chunk_hash.strong {
                tracing::info!(
                    "Chunk {} already_exists=true, checking local_chunk_info first",
                    i
                );
                if let Some(&(local_offset, local_length)) = local_chunk_info.get(&strong_hash) {
                    if local_offset == dest_offset {
                        state.mark_chunk_completed(i);
                        skipped_bytes += chunk.length;
                        dedup_count += 1;
                        continue;
                    } else {
                        chunks_to_relocate.push((i, local_offset, dest_offset, local_length));
                        continue;
                    }
                }

                if let Some(locations) = global_chunk_info.get(&strong_hash) {
                    tracing::info!(
                        "Chunk {} has {} candidate locations from global index",
                        i,
                        locations.len()
                    );
                    for location in locations {
                        if let Some(loc_strong) = location.strong_hash {
                            if loc_strong == strong_hash {
                                tracing::info!(
                                    "Chunk {} matched! Copying from {} at offset {}",
                                    i,
                                    location.file_path,
                                    location.offset
                                );
                                chunks_to_copy.push((i, location.clone(), dest_offset));
                                continue 'outer;
                            } else {
                                tracing::warn!(
                                    "Chunk {} location strong_hash mismatch: expected {:?}, got {:?}",
                                    i,
                                    hex::encode(strong_hash),
                                    hex::encode(loc_strong)
                                );
                            }
                        } else {
                            tracing::warn!(
                                "Chunk {} location has no strong_hash! file_path={}",
                                i,
                                location.file_path
                            );
                        }
                    }
                } else {
                    tracing::warn!(
                        "Chunk {} strong_hash exists in set but not in global_chunk_info map!",
                        i
                    );
                }
            }
        }

        chunks_to_transfer.push(i);
    }

    ChunkClassification {
        chunks_to_transfer,
        chunks_to_relocate,
        chunks_to_copy,
        dedup_count,
        skipped_bytes,
    }
}
