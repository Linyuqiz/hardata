use crate::util::error::{HarDataError, Result};
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

                if cache.mtime == current_mtime && cache.size == current_size {
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
