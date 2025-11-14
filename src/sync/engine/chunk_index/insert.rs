use super::types::{ChunkLocation, FileScanMetadata};
use crate::util::error::{HarDataError, Result};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_LOCATIONS_PER_HASH: usize = 100;

pub fn batch_insert_chunks(
    weak_hash_tree: &sled::Tree,
    strong_hash_tree: &sled::Tree,
    file_scans_tree: &sled::Tree,
    file_path: &str,
    chunks: &[crate::sync::engine::ChunkInfo],
    mtime: i64,
    file_size: u64,
) -> Result<usize> {
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
    tree.update_and_fetch(key, |old_value| {
        let mut locations: Vec<ChunkLocation> = old_value
            .and_then(|v| bincode::deserialize(v).ok())
            .unwrap_or_default();

        locations.retain(|loc| {
            loc.file_path != new_location.file_path || loc.offset != new_location.offset
        });

        if locations.len() >= MAX_LOCATIONS_PER_HASH {
            locations.remove(0);
        }

        locations.push(new_location.clone());

        bincode::serialize(&locations).ok()
    })
    .map_err(|e| HarDataError::InvalidConfig(format!("Failed to update location list: {}", e)))?;

    Ok(())
}

pub fn should_reindex_file(
    file_scans_tree: &sled::Tree,
    file_path: &str,
    current_mtime: i64,
    current_size: u64,
) -> Result<bool> {
    let key = file_path.as_bytes();

    if let Some(value) = file_scans_tree
        .get(key)
        .map_err(|e| HarDataError::InvalidConfig(format!("Failed to query file_scans: {}", e)))?
    {
        let metadata: FileScanMetadata = bincode::deserialize(&value).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to deserialize scan metadata: {}", e))
        })?;

        if metadata.mtime == current_mtime && metadata.size == current_size {
            return Ok(false);
        }
    }

    Ok(true)
}
