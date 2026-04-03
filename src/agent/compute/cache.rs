use crate::util::error::{HarDataError, Result};
use crate::util::time::unix_timestamp_nanos;
use std::path::Path;
use std::sync::atomic::Ordering;
use tracing::debug;

use super::types::{ComputeService, CACHE_CLEANUP_THRESHOLD, CACHE_MAX_ENTRIES};

impl ComputeService {
    pub fn cache_stats(&self) -> (u64, u64, usize) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
            self.hash_cache.len(),
        )
    }

    pub(super) fn maybe_cleanup_cache(&self) {
        if self.hash_cache.len() > CACHE_CLEANUP_THRESHOLD {
            let to_remove = self.hash_cache.len() - CACHE_MAX_ENTRIES;

            let mut entries: Vec<(std::path::PathBuf, u64)> = self
                .hash_cache
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().last_access))
                .collect();

            entries.sort_by_key(|(_path, last_access)| *last_access);

            for (key, _) in entries.iter().take(to_remove) {
                self.hash_cache.remove(key);
            }

            debug!(
                "Cache LRU cleanup: removed {} oldest entries, cache size: {}",
                to_remove,
                self.hash_cache.len()
            );
        }
    }

    pub(super) async fn get_file_mtime(path: &Path) -> Result<i64> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to get metadata: {}", e)))?;

        let mtime = unix_timestamp_nanos(
            metadata
                .modified()
                .map_err(|e| HarDataError::FileOperation(format!("Failed to get mtime: {}", e)))?,
        );

        Ok(mtime)
    }
}
