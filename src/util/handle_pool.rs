use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::core::constants::{IDLE_TIMEOUT_SECS, MAX_HANDLES_PER_FILE, MAX_TOTAL_HANDLES};

struct TimestampedHandle {
    file: File,
    last_used: std::time::Instant,
}

struct FileHandles {
    handles: Mutex<VecDeque<TimestampedHandle>>,
    active_count: AtomicUsize,
}

impl FileHandles {
    fn new() -> Self {
        Self {
            handles: Mutex::new(VecDeque::new()),
            active_count: AtomicUsize::new(0),
        }
    }
}

pub struct FileHandlePool {
    handles: DashMap<PathBuf, Arc<FileHandles>>,
    total_semaphore: Arc<Semaphore>,
    hits: AtomicU64,
    misses: AtomicU64,
    active_handles: AtomicUsize,
}

pub struct PooledFileHandle {
    file: Option<File>,
    path: PathBuf,
    pool: Arc<FileHandlePool>,
    _permit: OwnedSemaphorePermit,
}

impl FileHandlePool {
    pub fn new() -> Arc<Self> {
        let pool = Arc::new(Self {
            handles: DashMap::new(),
            total_semaphore: Arc::new(Semaphore::new(MAX_TOTAL_HANDLES)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            active_handles: AtomicUsize::new(0),
        });

        let pool_clone = Arc::clone(&pool);
        tokio::spawn(async move {
            pool_clone.cleanup_loop().await;
        });

        debug!(
            "FileHandlePool initialized: max_per_file={}, max_total={}",
            MAX_HANDLES_PER_FILE, MAX_TOTAL_HANDLES
        );

        pool
    }

    pub async fn acquire(self: &Arc<Self>, path: &Path) -> std::io::Result<PooledFileHandle> {
        let permit = self
            .total_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(std::io::Error::other)?;

        let path_buf = path.to_path_buf();

        let file_handles = self
            .handles
            .entry(path_buf.clone())
            .or_insert_with(|| Arc::new(FileHandles::new()))
            .clone();

        {
            let mut handles = file_handles.handles.lock().await;
            while let Some(timestamped) = handles.pop_front() {
                if timestamped.last_used.elapsed().as_secs() < IDLE_TIMEOUT_SECS {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    self.active_handles.fetch_add(1, Ordering::Relaxed);
                    file_handles.active_count.fetch_add(1, Ordering::Relaxed);

                    return Ok(PooledFileHandle {
                        file: Some(timestamped.file),
                        path: path_buf,
                        pool: Arc::clone(self),
                        _permit: permit,
                    });
                }
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let file = File::open(path).await?;
        self.active_handles.fetch_add(1, Ordering::Relaxed);
        file_handles.active_count.fetch_add(1, Ordering::Relaxed);

        Ok(PooledFileHandle {
            file: Some(file),
            path: path_buf,
            pool: Arc::clone(self),
            _permit: permit,
        })
    }

    async fn release(&self, path: PathBuf, file: File) {
        self.active_handles.fetch_sub(1, Ordering::Relaxed);

        if let Some(file_handles) = self.handles.get(&path) {
            file_handles.active_count.fetch_sub(1, Ordering::Relaxed);

            let mut handles = file_handles.handles.lock().await;
            if handles.len() < MAX_HANDLES_PER_FILE {
                handles.push_back(TimestampedHandle {
                    file,
                    last_used: std::time::Instant::now(),
                });
            }
        }
    }

    async fn cleanup_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        loop {
            interval.tick().await;

            let mut cleaned = 0;
            let mut empty_entries = Vec::new();

            for entry in self.handles.iter() {
                let key = entry.key().clone();
                let file_handles = entry.value();

                let mut handles = file_handles.handles.lock().await;
                let before = handles.len();
                handles.retain(|h| h.last_used.elapsed().as_secs() < IDLE_TIMEOUT_SECS);
                cleaned += before - handles.len();

                if file_handles.active_count.load(Ordering::Relaxed) == 0 && handles.is_empty() {
                    empty_entries.push(key);
                }
            }

            for key in &empty_entries {
                self.handles.remove(key);
            }

            if cleaned > 0 || !empty_entries.is_empty() {
                debug!(
                    "FileHandlePool cleanup: removed {} idle handles, {} empty entries",
                    cleaned,
                    empty_entries.len()
                );
            }
        }
    }
}

impl Default for FileHandlePool {
    fn default() -> Self {
        Self {
            handles: DashMap::new(),
            total_semaphore: Arc::new(Semaphore::new(MAX_TOTAL_HANDLES)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            active_handles: AtomicUsize::new(0),
        }
    }
}

impl PooledFileHandle {
    pub async fn read_at(&mut self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        let file = self.file.as_mut().unwrap();
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }
}

impl Drop for PooledFileHandle {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let pool = Arc::clone(&self.pool);
            let path = self.path.clone();
            tokio::spawn(async move {
                pool.release(path, file).await;
            });
        }
    }
}

use std::sync::OnceLock;

static GLOBAL_FILE_HANDLE_POOL: OnceLock<Arc<FileHandlePool>> = OnceLock::new();

pub fn global_file_handle_pool() -> &'static Arc<FileHandlePool> {
    GLOBAL_FILE_HANDLE_POOL.get_or_init(FileHandlePool::new)
}

pub async fn acquire_file_handle(path: &Path) -> std::io::Result<PooledFileHandle> {
    global_file_handle_pool().acquire(path).await
}
