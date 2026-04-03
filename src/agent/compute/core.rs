use crate::util::error::{HarDataError, Result};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use tracing::info;

use super::types::{ComputeService, CACHE_MAX_ENTRIES};

impl ComputeService {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let data_path = PathBuf::from(data_dir);

        info!(
            "ComputeService initialized with data_dir: {}, cache_max_entries: {}",
            data_dir, CACHE_MAX_ENTRIES
        );

        Ok(Self {
            data_dir: data_path,
            hash_cache: DashMap::new(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        })
    }

    pub(super) async fn ensure_regular_file(path: &Path) -> Result<()> {
        let metadata = tokio::fs::symlink_metadata(path).await.map_err(|e| {
            HarDataError::FileOperation(format!("Failed to get path metadata: {}", e))
        })?;
        let file_type = metadata.file_type();

        if file_type.is_symlink() {
            return Err(HarDataError::FileOperation(format!(
                "Refusing to read symlink path: {:?}",
                path
            )));
        }

        if !file_type.is_file() {
            return Err(HarDataError::FileOperation(format!(
                "Not a regular file: {:?}",
                path
            )));
        }

        Ok(())
    }
}
