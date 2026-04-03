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

enum LocalReuseDecision {
    InPlace,
    Relocate { offset: u64, length: usize },
    Copy(ChunkLocation),
}

fn select_local_reuse(
    strong_hash: [u8; 32],
    dest_offset: u64,
    local_chunk_info: &LocalChunkInfo,
    dest_path: &str,
) -> Option<LocalReuseDecision> {
    let locations = local_chunk_info.get(&strong_hash)?;

    if locations
        .iter()
        .any(|location| location.file_path == dest_path && location.offset == dest_offset)
    {
        return Some(LocalReuseDecision::InPlace);
    }

    if let Some(location) = locations
        .iter()
        .find(|location| location.file_path == dest_path)
    {
        return Some(LocalReuseDecision::Relocate {
            offset: location.offset,
            length: location.size as usize,
        });
    }

    locations.first().cloned().map(LocalReuseDecision::Copy)
}

#[allow(clippy::too_many_arguments)]
pub fn classify_chunks(
    chunks: &[FileChunk],
    state: &mut FileTransferState,
    dest_offset_map: &HashMap<usize, u64>,
    existing_strong_hashes: &HashSet<[u8; 32]>,
    local_chunk_info: &LocalChunkInfo,
    global_chunk_info: &GlobalChunkInfo,
    dest_path: &str,
) -> ChunkClassification {
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
                if let Some(local_reuse) =
                    select_local_reuse(strong_hash, dest_offset, local_chunk_info, dest_path)
                {
                    match local_reuse {
                        LocalReuseDecision::InPlace => {
                            state.mark_chunk_completed(i);
                            skipped_bytes += chunk.length;
                            dedup_count += 1;
                            continue;
                        }
                        LocalReuseDecision::Relocate { offset, length } => {
                            chunks_to_relocate.push((i, offset, dest_offset, length));
                            continue;
                        }
                        LocalReuseDecision::Copy(location) => {
                            chunks_to_copy.push((i, location, dest_offset));
                            continue;
                        }
                    }
                }

                if let Some(locations) = global_chunk_info.get(&strong_hash) {
                    for location in locations {
                        if location.strong_hash == Some(strong_hash) {
                            chunks_to_copy.push((i, location.clone(), dest_offset));
                            continue 'outer;
                        }
                    }
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

#[cfg(test)]
mod tests {
    use super::classify_chunks;
    use crate::core::chunk::ChunkHash;
    use crate::core::transfer_state::FileTransferState;
    use crate::sync::engine::{core::FileChunk, ChunkLocation};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn classify_chunks_prefers_existing_destination_offset_for_duplicate_blocks() {
        let strong_hash = [7; 32];
        let chunks = vec![FileChunk {
            file_path: "source.bin".to_string(),
            offset: 0,
            length: 4,
            chunk_hash: ChunkHash {
                weak: 1,
                strong: Some(strong_hash),
            },
        }];
        let mut state = FileTransferState::new("source.bin".to_string(), 1);
        let dest_offset_map = HashMap::from([(0usize, 0u64)]);
        let existing = HashSet::from([strong_hash]);
        let local_chunk_info = HashMap::from([(
            strong_hash,
            vec![
                ChunkLocation {
                    file_path: "dest.bin".to_string(),
                    offset: 8,
                    size: 4,
                    mtime: 1,
                    strong_hash: Some(strong_hash),
                },
                ChunkLocation {
                    file_path: "dest.bin".to_string(),
                    offset: 0,
                    size: 4,
                    mtime: 1,
                    strong_hash: Some(strong_hash),
                },
            ],
        )]);

        let classification = classify_chunks(
            &chunks,
            &mut state,
            &dest_offset_map,
            &existing,
            &local_chunk_info,
            &HashMap::new(),
            "dest.bin",
        );

        assert!(state.is_chunk_completed(0));
        assert_eq!(classification.dedup_count, 1);
        assert_eq!(classification.skipped_bytes, 4);
        assert!(classification.chunks_to_transfer.is_empty());
        assert!(classification.chunks_to_relocate.is_empty());
    }

    #[test]
    fn classify_chunks_copies_from_other_local_file_when_destination_differs() {
        let strong_hash = [9; 32];
        let chunks = vec![FileChunk {
            file_path: "source.bin".to_string(),
            offset: 0,
            length: 4,
            chunk_hash: ChunkHash {
                weak: 2,
                strong: Some(strong_hash),
            },
        }];
        let mut state = FileTransferState::new("source.bin".to_string(), 1);
        let dest_offset_map = HashMap::from([(0usize, 0u64)]);
        let existing = HashSet::from([strong_hash]);
        let local_chunk_info = HashMap::from([(
            strong_hash,
            vec![ChunkLocation {
                file_path: "dest.bin".to_string(),
                offset: 0,
                size: 4,
                mtime: 1,
                strong_hash: Some(strong_hash),
            }],
        )]);

        let classification = classify_chunks(
            &chunks,
            &mut state,
            &dest_offset_map,
            &existing,
            &local_chunk_info,
            &HashMap::new(),
            "dest.bin.tmp",
        );

        assert!(!state.is_chunk_completed(0));
        assert_eq!(classification.dedup_count, 0);
        assert!(classification.chunks_to_transfer.is_empty());
        assert!(classification.chunks_to_relocate.is_empty());
        assert_eq!(classification.chunks_to_copy.len(), 1);
        assert_eq!(classification.chunks_to_copy[0].1.file_path, "dest.bin");
        assert_eq!(classification.chunks_to_copy[0].2, 0);
    }
}
