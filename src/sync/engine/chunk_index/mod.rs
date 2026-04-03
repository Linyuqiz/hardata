mod cleanup;
mod insert;
mod query;
mod types;

pub use types::{ChunkLocation, DedupResult, FileScanMetadata};

use crate::util::error::{HarDataError, Result};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::info;

pub struct ChunkIndex {
    db: Arc<sled::Db>,
    weak_hash_tree: sled::Tree,
    strong_hash_tree: sled::Tree,
    file_scans_tree: sled::Tree,
    hit_count: AtomicU64,
    weak_filter_count: AtomicU64,
}

impl ChunkIndex {
    pub fn new(cache_path: &Path) -> Result<Self> {
        let db = sled::open(cache_path).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to open global chunk index: {}", e))
        })?;

        let weak_hash_tree = db.open_tree("weak_hash").map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to open weak_hash tree: {}", e))
        })?;

        let strong_hash_tree = db.open_tree("strong_hash").map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to open strong_hash tree: {}", e))
        })?;

        let file_scans_tree = db.open_tree("file_scans").map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to open file_scans tree: {}", e))
        })?;

        info!("ChunkIndex opened at: {:?}", cache_path);
        info!("  - weak_hash tree: {} entries", weak_hash_tree.len());
        info!("  - strong_hash tree: {} entries", strong_hash_tree.len());
        info!("  - file_scans tree: {} entries", file_scans_tree.len());

        Ok(Self {
            db: Arc::new(db),
            weak_hash_tree,
            strong_hash_tree,
            file_scans_tree,
            hit_count: AtomicU64::new(0),
            weak_filter_count: AtomicU64::new(0),
        })
    }

    pub fn remove_file(&self, file_path: &str) -> Result<usize> {
        cleanup::remove_file(
            &self.weak_hash_tree,
            &self.strong_hash_tree,
            &self.file_scans_tree,
            file_path,
        )
    }

    pub fn cleanup_stale_entries(&self) -> Result<usize> {
        cleanup::cleanup_stale_entries(
            &self.weak_hash_tree,
            &self.strong_hash_tree,
            &self.file_scans_tree,
        )
    }

    pub fn cleanup_stale_file_scans(&self) -> Result<usize> {
        cleanup::cleanup_stale_file_scans(
            &self.weak_hash_tree,
            &self.strong_hash_tree,
            &self.file_scans_tree,
        )
    }

    pub fn clear(&self) -> Result<usize> {
        let count = self.strong_hash_tree.len();

        self.weak_hash_tree.clear().map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to clear weak_hash tree: {}", e))
        })?;

        self.strong_hash_tree.clear().map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to clear strong_hash tree: {}", e))
        })?;

        self.file_scans_tree.clear().map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to clear file_scans tree: {}", e))
        })?;

        info!("ChunkIndex cleared: {} entries", count);

        Ok(count)
    }

    pub async fn query_chunks(
        &self,
        chunks: &mut [crate::sync::engine::core::FileChunk],
        dest_path: &str,
        existing_strong_hashes: &HashSet<[u8; 32]>,
        connection: &mut crate::sync::net::transport::TransportConnection,
        file: &crate::sync::scanner::ScannedFile,
    ) -> Result<DedupResult> {
        query::query_chunks(
            &self.weak_hash_tree,
            &self.hit_count,
            &self.weak_filter_count,
            chunks,
            dest_path,
            existing_strong_hashes,
            connection,
            file,
        )
        .await
    }

    pub fn batch_insert_chunks(
        &self,
        file_path: &str,
        chunks: &[crate::sync::engine::ChunkInfo],
        mtime: i64,
        file_size: u64,
    ) -> Result<usize> {
        insert::batch_insert_chunks(
            &self.weak_hash_tree,
            &self.strong_hash_tree,
            &self.file_scans_tree,
            file_path,
            chunks,
            mtime,
            file_size,
        )
    }

    pub fn should_reindex_file(
        &self,
        file_path: &str,
        current_mtime: i64,
        current_size: u64,
    ) -> Result<bool> {
        insert::should_reindex_file(
            &self.file_scans_tree,
            file_path,
            current_mtime,
            current_size,
        )
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush().map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to flush global index: {}", e))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkIndex;
    use crate::sync::engine::ChunkInfo;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-chunk-index-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn should_reindex_when_only_subsecond_mtime_changes() {
        let dir = temp_dir("mtime-nanos");
        let index = ChunkIndex::new(&dir).unwrap();
        let chunks = vec![ChunkInfo {
            offset: 0,
            size: 4,
            strong_hash: Some([2; 32]),
            weak_hash: 22,
        }];

        index
            .batch_insert_chunks("file.bin", &chunks, 1_700_000_000_000_000_001, 4)
            .unwrap();

        assert!(!index
            .should_reindex_file("file.bin", 1_700_000_000_000_000_001, 4)
            .unwrap());
        assert!(index
            .should_reindex_file("file.bin", 1_700_000_000_000_000_999, 4)
            .unwrap());

        let _ = fs::remove_dir_all(dir);
    }
}
