use bytes::{Bytes, BytesMut};
use crossbeam::queue::SegQueue;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    pub buffer_size: usize,
    pub initial_capacity: usize,
    pub max_capacity: usize,
    pub adaptive_size: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            buffer_size: 256 * 1024,
            initial_capacity: 50,
            max_capacity: 500,
            adaptive_size: true,
        }
    }
}

pub struct BufferPool {
    config: BufferPoolConfig,
    free_buffers: Arc<SegQueue<BytesMut>>,
    stats: Arc<BufferPoolStats>,
}

#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub total_allocations: std::sync::atomic::AtomicU64,
    pub pool_hits: std::sync::atomic::AtomicU64,
    pub pool_misses: std::sync::atomic::AtomicU64,
    pub returns: std::sync::atomic::AtomicU64,
}

impl BufferPool {
    pub fn new(config: BufferPoolConfig) -> Self {
        let pool = Self {
            config: config.clone(),
            free_buffers: Arc::new(SegQueue::new()),
            stats: Arc::new(BufferPoolStats::default()),
        };

        for _ in 0..config.initial_capacity {
            let buffer = BytesMut::with_capacity(config.buffer_size);
            pool.free_buffers.push(buffer);
        }

        debug!(
            "BufferPool initialized: buffer_size={}, initial_capacity={}, max_capacity={}",
            config.buffer_size, config.initial_capacity, config.max_capacity
        );

        pool
    }

    pub fn acquire(&self) -> PooledBuffer {
        self.stats
            .total_allocations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if let Some(mut buffer) = self.free_buffers.pop() {
            buffer.clear();
            self.stats
                .pool_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            PooledBuffer {
                buffer: Some(buffer),
                pool: self.free_buffers.clone(),
                config: self.config.clone(),
                stats: self.stats.clone(),
            }
        } else {
            self.stats
                .pool_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let buffer = BytesMut::with_capacity(self.config.buffer_size);
            PooledBuffer {
                buffer: Some(buffer),
                pool: self.free_buffers.clone(),
                config: self.config.clone(),
                stats: self.stats.clone(),
            }
        }
    }
}

pub struct PooledBuffer {
    buffer: Option<BytesMut>,
    pool: Arc<SegQueue<BytesMut>>,
    config: BufferPoolConfig,
    stats: Arc<BufferPoolStats>,
}

impl PooledBuffer {
    pub fn get_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().unwrap()
    }

    pub fn freeze(mut self) -> Bytes {
        self.buffer.take().unwrap().freeze()
    }

    pub fn len(&self) -> usize {
        self.buffer.as_ref().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.as_ref().unwrap().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.as_ref().unwrap().capacity()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer.as_ref().unwrap()[..]
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            if self.config.max_capacity == 0 || self.pool.len() < self.config.max_capacity {
                self.stats
                    .returns
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.pool.push(buffer);
            }
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut().unwrap()
    }
}

use std::sync::OnceLock;

static GLOBAL_BUFFER_POOL: OnceLock<BufferPool> = OnceLock::new();

pub fn global_buffer_pool() -> &'static BufferPool {
    GLOBAL_BUFFER_POOL.get_or_init(|| BufferPool::new(BufferPoolConfig::default()))
}
