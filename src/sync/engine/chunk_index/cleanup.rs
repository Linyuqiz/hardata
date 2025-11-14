use super::types::ChunkLocation;
use crate::util::error::{HarDataError, Result};
use std::path::Path;
use std::time::UNIX_EPOCH;
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

pub fn cleanup_stale_entries(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
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
                if let Ok(modified) = metadata.modified() {
                    if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
                        let current_mtime = duration.as_secs() as i64;
                        return current_mtime == loc.mtime;
                    }
                }
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
                if let Ok(modified) = metadata.modified() {
                    if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
                        let current_mtime = duration.as_secs() as i64;
                        return current_mtime == loc.mtime;
                    }
                }
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

    if removed_locations > 0 {
        info!(
            "Cleanup completed: removed {} stale locations ({} weak hashes, {} strong hashes)",
            removed_locations, removed_weak_hashes, removed_strong_hashes
        );
    }

    Ok(removed_locations)
}
