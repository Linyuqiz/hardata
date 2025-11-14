use std::time::Duration;

pub const MAX_ITEMS_PER_REQUEST: usize = 50;
pub const SAMPLE_WINDOW_SIZE: usize = 20;
pub const MIN_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_RECONNECT_ATTEMPTS: u32 = 5;
pub const RECONNECT_DELAY_MS: u64 = 1000;

pub const QUICK_HASH_SIZE: usize = 4 * 1024;
pub const CACHE_FILE_NAME: &str = ".hardata_file_cache.json";

pub const MAX_HANDLES_PER_FILE: usize = 4;
pub const MAX_TOTAL_HANDLES: usize = 512;
pub const IDLE_TIMEOUT_SECS: u64 = 60;

pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

pub const SCHEDULER_POOL_SIZE: usize = 64;
pub const SCHEDULER_MIN_CONCURRENCY: usize = 4;
pub const SCHEDULER_MAX_CONCURRENCY: usize = 64;
pub const SCHEDULER_MAX_CONCURRENT_JOBS: usize = 32;
pub const SCHEDULER_BATCH_SIZE: usize = 5000;
pub const SCHEDULER_MAX_CONCURRENT_FILES: usize = 8;

pub const FILE_CACHE_MAX_ENTRIES: usize = 100000;
pub const FILE_CACHE_TTL_SECS: u64 = 86400;
pub const FILE_CACHE_CLEANUP_INTERVAL_SECS: u64 = 300;

pub const EWMA_BANDWIDTH_ALPHA: f64 = 0.3;

pub const DEFAULT_QUIC_BIND_ADDR: &str = "127.0.0.1:9443";
pub const DEFAULT_TCP_BIND_ADDR: &str = "127.0.0.1:9444";

pub const RETRY_MIN_DELAY_MS: u64 = 100;
pub const RETRY_MAX_DELAY_MS: u64 = 5000;

pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_IO_TIMEOUT_SECS: u64 = 30;

pub const SYNC_ENGINE_BATCH_SIZE: usize = 100;
pub const SYNC_ENGINE_PARALLEL_WORKERS: usize = 100;

pub const QUIC_MAX_CONCURRENT_STREAMS: u32 = 1000;
