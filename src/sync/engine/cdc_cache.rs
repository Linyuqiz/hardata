use crate::util::error::{HarDataError, Result};
use crate::util::time::timestamps_match;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub offset: u64,
    pub size: u64,
    pub strong_hash: Option<[u8; 32]>,
    pub weak_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChunkCache {
    pub mtime: i64,
    pub size: u64,
    pub chunks: Vec<ChunkInfo>,
}

pub struct CDCResultCache {
    db: Arc<sled::Db>,
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
}

impl CDCResultCache {
    pub fn new(cache_path: &Path) -> Result<Self> {
        let db = sled::open(cache_path).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to open chunk cache: {}", e))
        })?;

        info!("CDCResultCache opened at: {:?}", cache_path);

        Ok(Self {
            db: Arc::new(db),
            cache_hits: std::sync::atomic::AtomicU64::new(0),
            cache_misses: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn get(
        &self,
        file_path: &str,
        current_mtime: i64,
        current_size: u64,
    ) -> Result<Option<FileChunkCache>> {
        let key = file_path.as_bytes();

        match self.db.get(key) {
            Ok(Some(value_bytes)) => {
                let cache: FileChunkCache = bincode::deserialize(&value_bytes).map_err(|e| {
                    HarDataError::InvalidConfig(format!("Failed to deserialize cache: {}", e))
                })?;

                if timestamps_match(cache.mtime, current_mtime) && cache.size == current_size {
                    self.cache_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    debug!(
                        "CDCResultCache hit: {} ({} chunks)",
                        file_path,
                        cache.chunks.len()
                    );
                    Ok(Some(cache))
                } else {
                    self.cache_misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    debug!("CDCResultCache stale: {} (mtime/size changed)", file_path);
                    Ok(None)
                }
            }
            Ok(None) => {
                self.cache_misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!("CDCResultCache miss: {}", file_path);
                Ok(None)
            }
            Err(e) => Err(HarDataError::InvalidConfig(format!(
                "Failed to query cache: {}",
                e
            ))),
        }
    }

    pub fn put(&self, file_path: &str, cache: FileChunkCache) -> Result<()> {
        let key = file_path.as_bytes();
        let value_bytes = bincode::serialize(&cache).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to serialize cache: {}", e))
        })?;

        self.db
            .insert(key, value_bytes)
            .map_err(|e| HarDataError::InvalidConfig(format!("Failed to update cache: {}", e)))?;

        debug!(
            "CDCResultCache updated: {} ({} chunks)",
            file_path,
            cache.chunks.len()
        );

        Ok(())
    }

    pub fn remove(&self, file_path: &str) -> Result<()> {
        let key = file_path.as_bytes();

        self.db
            .remove(key)
            .map_err(|e| HarDataError::InvalidConfig(format!("Failed to remove cache: {}", e)))?;

        debug!("CDCResultCache removed: {}", file_path);

        Ok(())
    }

    pub fn clear(&self) -> Result<usize> {
        let count = self.db.len();

        self.db
            .clear()
            .map_err(|e| HarDataError::InvalidConfig(format!("Failed to clear cache: {}", e)))?;

        info!("CDCResultCache cleared: {} files", count);

        Ok(count)
    }

    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| HarDataError::InvalidConfig(format!("Failed to flush cache: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CDCResultCache, ChunkInfo, FileChunkCache};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-cdc-cache-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn cache_distinguishes_subsecond_mtime_changes() {
        let dir = temp_dir("mtime-nanos");
        let cache = CDCResultCache::new(&dir).unwrap();
        let entry = FileChunkCache {
            mtime: 1_700_000_000_000_000_001,
            size: 16,
            chunks: vec![ChunkInfo {
                offset: 0,
                size: 16,
                strong_hash: Some([1; 32]),
                weak_hash: 11,
            }],
        };
        cache.put("file.bin", entry).unwrap();

        assert!(cache
            .get("file.bin", 1_700_000_000_000_000_001, 16)
            .unwrap()
            .is_some());
        assert!(cache
            .get("file.bin", 1_700_000_000_000_000_002, 16)
            .unwrap()
            .is_none());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cache_accepts_legacy_second_precision_entries() {
        let dir = temp_dir("mtime-legacy-seconds");
        let cache = CDCResultCache::new(&dir).unwrap();
        let entry = FileChunkCache {
            mtime: 1_700_000_000,
            size: 16,
            chunks: vec![ChunkInfo {
                offset: 0,
                size: 16,
                strong_hash: Some([3; 32]),
                weak_hash: 12,
            }],
        };
        cache.put("file.bin", entry).unwrap();

        assert!(cache
            .get("file.bin", 1_700_000_000_123_456_789, 16)
            .unwrap()
            .is_some());

        let _ = fs::remove_dir_all(dir);
    }
}
