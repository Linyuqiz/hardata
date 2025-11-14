use crate::util::error::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;
use tracing::{debug, info};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ChunkKey {
    pub file_path: String,
    pub offset: u64,
}

impl ChunkKey {
    pub fn new(file_path: &str, offset: u64) -> Self {
        Self {
            file_path: file_path.to_string(),
            offset,
        }
    }
}

struct CacheEntry {
    data: Arc<Vec<u8>>,
    last_access: u64,
    size: usize,
}

pub struct PrefetchManager {
    cache: Mutex<HashMap<ChunkKey, CacheEntry>>,
    max_cache_size: usize,
    current_cache_size: AtomicUsize,
    access_counter: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl PrefetchManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cache: Mutex::new(HashMap::new()),
            max_cache_size: 100 * 1024 * 1024,
            current_cache_size: AtomicUsize::new(0),
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    pub async fn get_chunk_data(
        &self,
        file_path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Arc<Vec<u8>>> {
        let key = ChunkKey::new(file_path, offset);

        {
            let mut cache = self.cache.lock().await;
            if let Some(entry) = cache.get_mut(&key) {
                entry.last_access = self.access_counter.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                debug!("Cache hit for {}:{}", file_path, offset);
                return Ok(entry.data.clone());
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        debug!("Cache miss for {}:{}", file_path, offset);

        let data = self.read_file_range(file_path, offset, length).await?;
        let arc_data = Arc::new(data);

        self.put_cache(key, arc_data.as_ref().clone()).await;

        Ok(arc_data)
    }

    async fn read_file_range(&self, file_path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut file = File::open(file_path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; length as usize];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    async fn put_cache(&self, key: ChunkKey, data: Vec<u8>) {
        let data_size = data.len();

        while self.current_cache_size.load(Ordering::Relaxed) + data_size > self.max_cache_size {
            if !self.evict_lru().await {
                break;
            }
        }

        let entry = CacheEntry {
            data: Arc::new(data),
            last_access: self.access_counter.fetch_add(1, Ordering::Relaxed),
            size: data_size,
        };

        let mut cache = self.cache.lock().await;
        if let Some(old) = cache.insert(key, entry) {
            self.current_cache_size
                .fetch_sub(old.size, Ordering::Relaxed);
        }
        self.current_cache_size
            .fetch_add(data_size, Ordering::Relaxed);
    }

    async fn evict_lru(&self) -> bool {
        let mut cache = self.cache.lock().await;

        if cache.is_empty() {
            return false;
        }

        let oldest_key = cache
            .iter()
            .min_by_key(|(_, v)| v.last_access)
            .map(|(k, _)| k.clone());

        if let Some(key) = oldest_key {
            if let Some(entry) = cache.remove(&key) {
                self.current_cache_size
                    .fetch_sub(entry.size, Ordering::Relaxed);
                debug!("Evicted cache entry: {}:{}", key.file_path, key.offset);
                return true;
            }
        }

        false
    }

    pub async fn remove(&self, file_path: &str, offset: u64) {
        let key = ChunkKey::new(file_path, offset);
        let mut cache = self.cache.lock().await;
        if let Some(entry) = cache.remove(&key) {
            self.current_cache_size
                .fetch_sub(entry.size, Ordering::Relaxed);
        }
    }

    pub async fn clear(&self) {
        let mut cache = self.cache.lock().await;
        cache.clear();
        self.current_cache_size.store(0, Ordering::Relaxed);
        info!("Prefetch cache cleared");
    }

    pub async fn contains(&self, file_path: &str, offset: u64) -> bool {
        let key = ChunkKey::new(file_path, offset);
        let cache = self.cache.lock().await;
        cache.contains_key(&key)
    }
}

impl Default for PrefetchManager {
    fn default() -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
            max_cache_size: 100 * 1024 * 1024,
            current_cache_size: AtomicUsize::new(0),
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileChunkInfo {
    pub file_path: String,
    pub offset: u64,
    pub length: u64,
}

impl FileChunkInfo {
    pub fn new(file_path: &str, offset: u64, length: u64) -> Self {
        Self {
            file_path: file_path.to_string(),
            offset,
            length,
        }
    }
}
