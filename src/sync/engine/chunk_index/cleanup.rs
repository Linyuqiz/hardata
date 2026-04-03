use super::{
    insert::{deserialize_scan_metadata, remove_file_locations_by_hashes},
    types::{ChunkLocation, FileScanMetadata},
};
use crate::util::error::{HarDataError, Result};
use crate::util::time::{metadata_mtime_nanos, timestamps_match};
use std::path::Path;
use tracing::info;

pub fn remove_file(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_scans_tree: &sled::Tree,
    file_path: &str,
) -> Result<usize> {
    let mut removed_locations = 0;
    let mut removed_weak_hashes = 0;
    let mut removed_strong_hashes = 0;

    for entry in weak_hash_tree.iter() {
        let (key, value) = entry.map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to iterate weak_hash tree: {}", e))
        })?;

        let mut locations: Vec<ChunkLocation> = bincode::deserialize(&value).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to deserialize chunk locations: {}", e))
        })?;

        let original_len = locations.len();
        locations.retain(|loc| loc.file_path != file_path);
        let removed = original_len - locations.len();
        removed_locations += removed;

        if locations.is_empty() {
            weak_hash_tree.remove(&key).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to remove weak_hash from index: {}", e))
            })?;
            removed_weak_hashes += 1;
        } else if removed > 0 {
            let new_value = bincode::serialize(&locations).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to serialize chunk locations: {}", e))
            })?;
            weak_hash_tree.insert(&key, new_value).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to update weak_hash in index: {}", e))
            })?;
        }
    }

    for entry in strong_hash_tree.iter() {
        let (key, value) = entry.map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to iterate strong_hash tree: {}", e))
        })?;

        let mut locations: Vec<ChunkLocation> = bincode::deserialize(&value).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to deserialize chunk locations: {}", e))
        })?;

        let original_len = locations.len();
        locations.retain(|loc| loc.file_path != file_path);
        let removed = original_len - locations.len();

        if locations.is_empty() {
            strong_hash_tree.remove(&key).map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Failed to remove strong_hash from index: {}",
                    e
                ))
            })?;
            removed_strong_hashes += 1;
        } else if removed > 0 {
            let new_value = bincode::serialize(&locations).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to serialize chunk locations: {}", e))
            })?;
            strong_hash_tree.insert(&key, new_value).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to update strong_hash in index: {}", e))
            })?;
        }
    }

    let scan_key = file_path.as_bytes();
    file_scans_tree.remove(scan_key).map_err(|e| {
        HarDataError::InvalidConfig(format!("Failed to remove file_scan entry: {}", e))
    })?;

    info!(
        "Removed {} locations (weak_hashes: {}, strong_hashes: {}) for file: {}",
        removed_locations, removed_weak_hashes, removed_strong_hashes, file_path
    );

    Ok(removed_locations)
}

fn scan_entry_is_stale(file_path: &str, metadata: &FileScanMetadata) -> bool {
    let path = Path::new(file_path);
    if !path.exists() {
        return true;
    }

    match std::fs::metadata(path) {
        Ok(current) => {
            !timestamps_match(metadata_mtime_nanos(&current), metadata.mtime)
                || current.len() != metadata.size
        }
        Err(_) => true,
    }
}

fn remove_file_by_scan_metadata(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_scans_tree: &sled::Tree,
    file_path: &str,
    metadata: &FileScanMetadata,
) -> Result<usize> {
    if metadata.weak_hashes.is_empty() && metadata.strong_hashes.is_empty() {
        return remove_file(weak_hash_tree, strong_hash_tree, file_scans_tree, file_path);
    }

    let removed =
        remove_file_locations_by_hashes(weak_hash_tree, strong_hash_tree, file_path, metadata)?;

    file_scans_tree.remove(file_path.as_bytes()).map_err(|e| {
        HarDataError::InvalidConfig(format!("Failed to remove file_scan entry: {}", e))
    })?;

    Ok(removed)
}

pub fn cleanup_stale_file_scans(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_scans_tree: &sled::Tree,
) -> Result<usize> {
    let mut stale_entries = Vec::new();

    for entry in file_scans_tree.iter() {
        let (key, value) = entry.map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to iterate file_scans tree: {}", e))
        })?;

        let metadata = deserialize_scan_metadata(&value)?;
        let file_path = String::from_utf8_lossy(&key).into_owned();

        if scan_entry_is_stale(&file_path, &metadata) {
            stale_entries.push((file_path, metadata));
        }
    }

    let mut removed_locations = 0;
    let mut removed_file_scans = 0;

    for (file_path, metadata) in stale_entries {
        removed_locations += remove_file_by_scan_metadata(
            weak_hash_tree,
            strong_hash_tree,
            file_scans_tree,
            &file_path,
            &metadata,
        )?;
        removed_file_scans += 1;
    }

    if removed_locations > 0 || removed_file_scans > 0 {
        info!(
            "File-scan cleanup completed: removed {} stale locations across {} file scans",
            removed_locations, removed_file_scans
        );
    }

    Ok(removed_locations + removed_file_scans)
}

pub fn cleanup_stale_entries(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_scans_tree: &sled::Tree,
) -> Result<usize> {
    let mut removed_locations = 0;
    let mut removed_weak_hashes = 0;
    let mut removed_strong_hashes = 0;

    for entry in weak_hash_tree.iter() {
        let (key, value) = entry.map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to iterate weak_hash tree: {}", e))
        })?;

        let mut locations: Vec<ChunkLocation> = bincode::deserialize(&value).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to deserialize chunk locations: {}", e))
        })?;

        let original_len = locations.len();

        locations.retain(|loc| {
            let file_path = Path::new(&loc.file_path);
            if !file_path.exists() {
                return false;
            }

            if let Ok(metadata) = std::fs::metadata(file_path) {
                return timestamps_match(metadata_mtime_nanos(&metadata), loc.mtime);
            }
            false
        });

        let removed = original_len - locations.len();
        removed_locations += removed;

        if locations.is_empty() {
            weak_hash_tree.remove(&key).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to remove weak_hash from index: {}", e))
            })?;
            removed_weak_hashes += 1;
        } else if removed > 0 {
            let new_value = bincode::serialize(&locations).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to serialize chunk locations: {}", e))
            })?;
            weak_hash_tree.insert(&key, new_value).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to update weak_hash in index: {}", e))
            })?;
        }
    }

    for entry in strong_hash_tree.iter() {
        let (key, value) = entry.map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to iterate strong_hash tree: {}", e))
        })?;

        let mut locations: Vec<ChunkLocation> = bincode::deserialize(&value).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to deserialize chunk locations: {}", e))
        })?;

        let original_len = locations.len();

        locations.retain(|loc| {
            let file_path = Path::new(&loc.file_path);
            if !file_path.exists() {
                return false;
            }

            if let Ok(metadata) = std::fs::metadata(file_path) {
                return timestamps_match(metadata_mtime_nanos(&metadata), loc.mtime);
            }
            false
        });

        let removed = original_len - locations.len();
        if removed > 0 {
            removed_locations += removed;
        }

        if locations.is_empty() {
            strong_hash_tree.remove(&key).map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Failed to remove strong_hash from index: {}",
                    e
                ))
            })?;
            removed_strong_hashes += 1;
        } else if removed > 0 {
            let new_value = bincode::serialize(&locations).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to serialize chunk locations: {}", e))
            })?;
            strong_hash_tree.insert(&key, new_value).map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to update strong_hash in index: {}", e))
            })?;
        }
    }

    let mut removed_file_scans = 0;

    for entry in file_scans_tree.iter() {
        let (key, value) = entry.map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to iterate file_scans tree: {}", e))
        })?;

        let metadata = deserialize_scan_metadata(&value)?;
        let file_path = String::from_utf8_lossy(&key).into_owned();

        if scan_entry_is_stale(&file_path, &metadata) {
            file_scans_tree.remove(&key).map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Failed to remove stale file_scan entry: {}",
                    e
                ))
            })?;
            removed_file_scans += 1;
        }
    }

    if removed_locations > 0 || removed_file_scans > 0 {
        info!(
            "Cleanup completed: removed {} stale locations ({} weak hashes, {} strong hashes, {} file scans)",
            removed_locations, removed_weak_hashes, removed_strong_hashes, removed_file_scans
        );
    }

    Ok(removed_locations + removed_file_scans)
}

#[cfg(test)]
mod tests {
    use super::{cleanup_stale_entries, cleanup_stale_file_scans};
    use crate::sync::engine::chunk_index::{ChunkLocation, FileScanMetadata};
    use crate::util::time::metadata_mtime_nanos;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-chunk-cleanup-{label}-{unique}"));
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
    fn cleanup_stale_file_scans_removes_deleted_files_during_runtime() {
        let dir = temp_dir("runtime-file-scan-cleanup");
        let file_path = dir.join("runtime-stale.bin");
        fs::write(&file_path, b"stale").unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let mtime = metadata_mtime_nanos(&metadata);
        let size = metadata.len();
        let file_path_str = file_path.to_string_lossy().to_string();

        let (_db, weak, strong, scans) = open_trees(&dir.join("db-runtime"));
        let location = ChunkLocation {
            file_path: file_path_str.clone(),
            offset: 0,
            size,
            mtime,
            strong_hash: Some([8; 32]),
        };

        weak.insert(
            88u64.to_be_bytes(),
            bincode::serialize(&vec![location.clone()]).unwrap(),
        )
        .unwrap();
        strong
            .insert([8; 32], bincode::serialize(&vec![location]).unwrap())
            .unwrap();
        scans
            .insert(
                file_path_str.as_bytes(),
                bincode::serialize(&FileScanMetadata {
                    mtime,
                    size,
                    chunk_count: 1,
                    last_indexed: 1,
                    weak_hashes: vec![88],
                    strong_hashes: vec![[8; 32]],
                })
                .unwrap(),
            )
            .unwrap();

        fs::remove_file(&file_path).unwrap();

        let removed = cleanup_stale_file_scans(&weak, &strong, &scans).unwrap();

        assert_eq!(removed, 3);
        assert!(weak.get(88u64.to_be_bytes()).unwrap().is_none());
        assert!(strong.get([8; 32]).unwrap().is_none());
        assert!(scans.get(file_path_str.as_bytes()).unwrap().is_none());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cleanup_stale_entries_removes_file_scan_metadata_for_deleted_files() {
        let dir = temp_dir("deleted-file-scans");
        let file_path = dir.join("stale.bin");
        fs::write(&file_path, b"stale").unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let mtime = metadata_mtime_nanos(&metadata);
        let size = metadata.len();
        let file_path_str = file_path.to_string_lossy().to_string();

        let (_db, weak, strong, scans) = open_trees(&dir.join("db"));
        let location = ChunkLocation {
            file_path: file_path_str.clone(),
            offset: 0,
            size,
            mtime,
            strong_hash: Some([9; 32]),
        };

        weak.insert(
            99u64.to_be_bytes(),
            bincode::serialize(&vec![location.clone()]).unwrap(),
        )
        .unwrap();
        strong
            .insert([9; 32], bincode::serialize(&vec![location]).unwrap())
            .unwrap();
        scans
            .insert(
                file_path_str.as_bytes(),
                bincode::serialize(&FileScanMetadata {
                    mtime,
                    size,
                    chunk_count: 1,
                    last_indexed: 1,
                    weak_hashes: vec![99],
                    strong_hashes: vec![[9; 32]],
                })
                .unwrap(),
            )
            .unwrap();

        fs::remove_file(&file_path).unwrap();

        let removed = cleanup_stale_entries(&weak, &strong, &scans).unwrap();

        assert_eq!(removed, 3);
        assert!(weak.get(99u64.to_be_bytes()).unwrap().is_none());
        assert!(strong.get([9; 32]).unwrap().is_none());
        assert!(scans.get(file_path_str.as_bytes()).unwrap().is_none());

        let _ = fs::remove_dir_all(dir);
    }
}
