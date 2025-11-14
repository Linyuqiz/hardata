use crate::util::error::Result;
use dashmap::DashMap;
use std::path::PathBuf;
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
}
