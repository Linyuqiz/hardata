use super::types::{ChunkLocation, FileScanMetadata};
use crate::util::error::{HarDataError, Result};
use crate::util::time::timestamps_match;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

const MAX_LOCATIONS_PER_HASH: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyFileScanMetadata {
    mtime: i64,
    size: u64,
    chunk_count: usize,
    last_indexed: i64,
}

pub(super) fn deserialize_scan_metadata(value: &[u8]) -> Result<FileScanMetadata> {
    bincode::deserialize(value)
        .or_else(|_| {
            bincode::deserialize::<LegacyFileScanMetadata>(value).map(|legacy| FileScanMetadata {
                mtime: legacy.mtime,
                size: legacy.size,
                chunk_count: legacy.chunk_count,
                last_indexed: legacy.last_indexed,
                weak_hashes: Vec::new(),
                strong_hashes: Vec::new(),
            })
        })
        .map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to deserialize scan metadata: {}", e))
        })
}

fn load_scan_metadata(
    file_scans_tree: &sled::Tree,
    file_path: &str,
) -> Result<Option<FileScanMetadata>> {
    let key = file_path.as_bytes();
    match file_scans_tree
        .get(key)
        .map_err(|e| HarDataError::InvalidConfig(format!("Failed to query file_scans: {}", e)))?
    {
        Some(value) => Ok(Some(deserialize_scan_metadata(&value)?)),
        None => Ok(None),
    }
}

fn collect_weak_hashes(chunks: &[crate::sync::engine::ChunkInfo]) -> Vec<u64> {
    let mut hashes: Vec<u64> = chunks
        .iter()
        .map(|chunk| chunk.weak_hash)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    hashes.sort_unstable();
    hashes
}

fn collect_strong_hashes(chunks: &[crate::sync::engine::ChunkInfo]) -> Vec<[u8; 32]> {
    let mut hashes: Vec<[u8; 32]> = chunks
        .iter()
        .filter_map(|chunk| chunk.strong_hash)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    hashes.sort_unstable();
    hashes
}

fn remove_locations_for_key(tree: &sled::Tree, key: &[u8], file_path: &str) -> Result<usize> {
    let Some(value) = tree.get(key).map_err(|e| {
        HarDataError::InvalidConfig(format!("Failed to query location list: {}", e))
    })?
    else {
        return Ok(0);
    };

    let mut locations: Vec<ChunkLocation> = bincode::deserialize(&value).map_err(|e| {
        HarDataError::InvalidConfig(format!("Failed to deserialize chunk locations: {}", e))
    })?;

    let original_len = locations.len();
    locations.retain(|loc| loc.file_path != file_path);
    let removed = original_len - locations.len();

    if removed == 0 {
        return Ok(0);
    }

    if locations.is_empty() {
        tree.remove(key).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to remove empty location list: {}", e))
        })?;
    } else {
        let encoded = bincode::serialize(&locations).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to serialize chunk locations: {}", e))
        })?;
        tree.insert(key, encoded).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to update location list: {}", e))
        })?;
    }

    Ok(removed)
}

pub(super) fn remove_file_locations_by_hashes(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_path: &str,
    metadata: &FileScanMetadata,
) -> Result<usize> {
    let mut removed = 0usize;

    for weak_hash in &metadata.weak_hashes {
        removed += remove_locations_for_key(weak_hash_tree, &weak_hash.to_be_bytes(), file_path)?;
    }

    for strong_hash in &metadata.strong_hashes {
        removed += remove_locations_for_key(strong_hash_tree, strong_hash.as_slice(), file_path)?;
    }

    if removed > 0 {
        info!(
            "Removed {} stale indexed chunk locations for file {} before reindex",
            removed, file_path
        );
    }

    Ok(removed)
}

pub fn batch_insert_chunks(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_scans_tree: &sled::Tree,
    file_path: &str,
    chunks: &[crate::sync::engine::ChunkInfo],
    mtime: i64,
    file_size: u64,
) -> Result<usize> {
    if let Some(previous) = load_scan_metadata(file_scans_tree, file_path)? {
        let changed = !timestamps_match(previous.mtime, mtime)
            || previous.size != file_size
            || previous.chunk_count != chunks.len();

        if changed {
            if previous.weak_hashes.is_empty() && previous.strong_hashes.is_empty() {
                super::cleanup::remove_file(
                    weak_hash_tree,
                    strong_hash_tree,
                    file_scans_tree,
                    file_path,
                )?;
            } else {
                remove_file_locations_by_hashes(
                    weak_hash_tree,
                    strong_hash_tree,
                    file_path,
                    &previous,
                )?;
            }
        }
    }

    let mut inserted = 0;

    for chunk in chunks {
        let location = ChunkLocation {
            file_path: file_path.to_string(),
            offset: chunk.offset,
            size: chunk.size,
            mtime,
            strong_hash: chunk.strong_hash,
        };

        let weak_key = chunk.weak_hash.to_be_bytes();
        update_location_list(weak_hash_tree, &weak_key, location.clone())?;

        if let Some(strong_hash) = chunk.strong_hash {
            update_location_list(strong_hash_tree, strong_hash.as_slice(), location)?;
            inserted += 1;
        }
    }

    let scan_metadata = FileScanMetadata {
        mtime,
        size: file_size,
        chunk_count: chunks.len(),
        last_indexed: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
        weak_hashes: collect_weak_hashes(chunks),
        strong_hashes: collect_strong_hashes(chunks),
    };

    let scan_key = file_path.as_bytes();
    let scan_value = bincode::serialize(&scan_metadata).map_err(|e| {
        HarDataError::InvalidConfig(format!("Failed to serialize scan metadata: {}", e))
    })?;
    file_scans_tree.insert(scan_key, scan_value).map_err(|e| {
        HarDataError::InvalidConfig(format!("Failed to insert scan metadata: {}", e))
    })?;

    Ok(inserted)
}

fn update_location_list(tree: &sled::Tree, key: &[u8], new_location: ChunkLocation) -> Result<()> {
    let mut update_error = None;
    tree.update_and_fetch(key, |old_value| {
        let mut locations: Vec<ChunkLocation> = match old_value {
            Some(value) => match bincode::deserialize(value) {
                Ok(locations) => locations,
                Err(e) => {
                    update_error = Some(HarDataError::InvalidConfig(format!(
                        "Failed to deserialize chunk locations: {}",
                        e
                    )));
                    return old_value.map(|value| value.to_vec());
                }
            },
            None => Vec::new(),
        };

        locations.retain(|loc| {
            loc.file_path != new_location.file_path || loc.offset != new_location.offset
        });

        if locations.len() >= MAX_LOCATIONS_PER_HASH {
            locations.remove(0);
        }

        locations.push(new_location.clone());

        match bincode::serialize(&locations) {
            Ok(encoded) => Some(encoded),
            Err(e) => {
                update_error = Some(HarDataError::InvalidConfig(format!(
                    "Failed to serialize chunk locations: {}",
                    e
                )));
                old_value.map(|value| value.to_vec())
            }
        }
    })
    .map_err(|e| HarDataError::InvalidConfig(format!("Failed to update location list: {}", e)))?;

    if let Some(err) = update_error {
        return Err(err);
    }

    Ok(())
}

pub fn should_reindex_file(
    file_scans_tree: &sled::Tree,
    file_path: &str,
    current_mtime: i64,
    current_size: u64,
) -> Result<bool> {
    let Some(metadata) = load_scan_metadata(file_scans_tree, file_path)? else {
        return Ok(true);
    };

    if timestamps_match(metadata.mtime, current_mtime) && metadata.size == current_size {
        return Ok(false);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{batch_insert_chunks, should_reindex_file, LegacyFileScanMetadata};
    use crate::sync::engine::chunk_index::ChunkLocation;
    use crate::sync::engine::ChunkInfo;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-chunk-insert-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn open_trees(path: &std::path::Path) -> (sled::Db, sled::Tree, sled::Tree, sled::Tree) {
        let db = sled::open(path).unwrap();
        let weak = db.open_tree("weak_hash").unwrap();
        let strong = db.open_tree("strong_hash").unwrap();
        let scans = db.open_tree("file_scans").unwrap();
        (db, weak, strong, scans)
    }

    #[test]
    fn batch_insert_replaces_stale_entries_for_same_file() {
        let dir = temp_dir("replace-stale");
        let (_db, weak, strong, scans) = open_trees(&dir);
        let old_chunk = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([1; 32]),
            weak_hash: 11,
        };
        let new_chunk = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([2; 32]),
            weak_hash: 22,
        };

        batch_insert_chunks(&weak, &strong, &scans, "file.bin", &[old_chunk], 1, 4).unwrap();
        batch_insert_chunks(&weak, &strong, &scans, "file.bin", &[new_chunk], 2, 4).unwrap();

        assert!(weak.get(11u64.to_be_bytes()).unwrap().is_none());
        assert!(strong.get([1; 32]).unwrap().is_none());

        let weak_locations: Vec<ChunkLocation> =
            bincode::deserialize(&weak.get(22u64.to_be_bytes()).unwrap().unwrap()).unwrap();
        assert_eq!(weak_locations.len(), 1);
        assert_eq!(weak_locations[0].mtime, 2);
        assert_eq!(weak_locations[0].strong_hash, Some([2; 32]));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn should_not_reindex_legacy_second_precision_entries() {
        let dir = temp_dir("legacy-seconds");
        let (_db, weak, strong, scans) = open_trees(&dir);
        let chunk = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([9; 32]),
            weak_hash: 29,
        };

        batch_insert_chunks(
            &weak,
            &strong,
            &scans,
            "file.bin",
            &[chunk],
            1_700_000_000,
            4,
        )
        .unwrap();

        assert!(!should_reindex_file(&scans, "file.bin", 1_700_000_000_123_456_789, 4).unwrap());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn batch_insert_preserves_other_files_sharing_same_hash() {
        let dir = temp_dir("preserve-shared");
        let (_db, weak, strong, scans) = open_trees(&dir);
        let shared = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([3; 32]),
            weak_hash: 33,
        };
        let replacement = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([4; 32]),
            weak_hash: 44,
        };

        batch_insert_chunks(
            &weak,
            &strong,
            &scans,
            "a.bin",
            std::slice::from_ref(&shared),
            1,
            4,
        )
        .unwrap();
        batch_insert_chunks(&weak, &strong, &scans, "b.bin", &[shared], 1, 4).unwrap();
        batch_insert_chunks(&weak, &strong, &scans, "a.bin", &[replacement], 2, 4).unwrap();

        let shared_locations: Vec<ChunkLocation> =
            bincode::deserialize(&weak.get(33u64.to_be_bytes()).unwrap().unwrap()).unwrap();
        assert_eq!(shared_locations.len(), 1);
        assert_eq!(shared_locations[0].file_path, "b.bin");

        let strong_locations: Vec<ChunkLocation> =
            bincode::deserialize(&strong.get([3; 32]).unwrap().unwrap()).unwrap();
        assert_eq!(strong_locations.len(), 1);
        assert_eq!(strong_locations[0].file_path, "b.bin");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn batch_insert_reindexes_legacy_scan_metadata_entries() {
        let dir = temp_dir("legacy-reindex");
        let (_db, weak, strong, scans) = open_trees(&dir);
        let old_location = ChunkLocation {
            file_path: "legacy.bin".to_string(),
            offset: 0,
            size: 4,
            mtime: 1,
            strong_hash: Some([5; 32]),
        };
        weak.insert(
            55u64.to_be_bytes(),
            bincode::serialize(&vec![old_location.clone()]).unwrap(),
        )
        .unwrap();
        strong
            .insert([5; 32], bincode::serialize(&vec![old_location]).unwrap())
            .unwrap();
        scans
            .insert(
                b"legacy.bin",
                bincode::serialize(&LegacyFileScanMetadata {
                    mtime: 1,
                    size: 4,
                    chunk_count: 1,
                    last_indexed: 1,
                })
                .unwrap(),
            )
            .unwrap();

        let replacement = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([6; 32]),
            weak_hash: 66,
        };
        batch_insert_chunks(&weak, &strong, &scans, "legacy.bin", &[replacement], 2, 4).unwrap();

        assert!(weak.get(55u64.to_be_bytes()).unwrap().is_none());
        assert!(strong.get([5; 32]).unwrap().is_none());
        assert!(weak.get(66u64.to_be_bytes()).unwrap().is_some());
        assert!(strong.get([6; 32]).unwrap().is_some());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn batch_insert_rejects_corrupted_existing_location_list() {
        let dir = temp_dir("corrupted-location-list");
        let (_db, weak, strong, scans) = open_trees(&dir);
        let chunk = ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([7; 32]),
            weak_hash: 77,
        };
        let corrupted = vec![0, 1, 2, 3, 4];
        weak.insert(77u64.to_be_bytes(), corrupted.clone()).unwrap();

        let err =
            batch_insert_chunks(&weak, &strong, &scans, "broken.bin", &[chunk], 1, 4).unwrap_err();

        assert!(err
            .to_string()
            .contains("Failed to deserialize chunk locations"));
        assert_eq!(
            weak.get(77u64.to_be_bytes()).unwrap().unwrap().to_vec(),
            corrupted
        );
        assert!(strong.get([7; 32]).unwrap().is_none());
        assert!(scans.get(b"broken.bin").unwrap().is_none());

        let _ = fs::remove_dir_all(dir);
    }
}
