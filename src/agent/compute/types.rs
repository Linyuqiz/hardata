use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub const CACHE_MAX_ENTRIES: usize = 50000;
pub const CACHE_CLEANUP_THRESHOLD: usize = 60000;
pub const CACHE_TTL_SECS: u64 = 3600; // 缓存有效期 1 小时

#[derive(Clone)]
pub(super) struct CacheEntry {
    pub mtime: i64,
    pub file_size: u64,
    pub min_chunk_size: usize,
    pub avg_chunk_size: usize,
    pub max_chunk_size: usize,
    pub chunks: Arc<Vec<crate::core::ChunkMetadata>>,
    pub created_at: u64,  // 创建时间戳（秒）
    pub last_access: u64, // 最后访问时间戳（秒）
}

pub struct ComputeService {
    pub(super) data_dir: PathBuf,
    pub(super) hash_cache: DashMap<PathBuf, CacheEntry>,
    pub(super) cache_hits: AtomicU64,
    pub(super) cache_misses: AtomicU64,
}
