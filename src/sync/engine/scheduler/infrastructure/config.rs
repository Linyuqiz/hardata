use crate::core::constants::{DEFAULT_QUIC_BIND_ADDR, DEFAULT_TCP_BIND_ADDR};
use crate::core::job::JobStatus;
use crate::sync::net::bandwidth::NetworkQuality;
use crate::sync::net::quic::QuicClient;
use crate::sync::net::tcp::TcpClient;
use crate::sync::RegionConfig;
use crate::util::compression::CompressionStrategy;
use crate::util::retry::RetryConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct SchedulerConfig {
    pub regions: Vec<RegionConfig>,
    pub data_dir: String,
    pub chunk_cache_path: String,
    pub enable_cache_preheat: bool,
    pub max_concurrent_jobs: usize,
    pub max_concurrent_files: usize,
    pub min_chunk_size: usize,
    pub avg_chunk_size: usize,
    pub max_chunk_size: usize,
    pub retry_config: RetryConfig,
    pub compression_strategy: CompressionStrategy,
    pub batch_size: usize,
    pub replicate_mode: ReplicateMode,
    pub global_index: Option<Arc<crate::sync::engine::ChunkIndex>>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        use crate::util::cdc::{
            DEFAULT_AVG_CHUNK_SIZE, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_MIN_CHUNK_SIZE,
        };

        Self {
            regions: vec![RegionConfig {
                name: "local".to_string(),
                quic_bind: DEFAULT_QUIC_BIND_ADDR.to_string(),
                tcp_bind: DEFAULT_TCP_BIND_ADDR.to_string(),
            }],
            data_dir: ".hardata/sync".to_string(),
            chunk_cache_path: ".hardata/chunk_cache".to_string(),
            enable_cache_preheat: true,
            max_concurrent_jobs: 8,
            max_concurrent_files: 16,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            avg_chunk_size: DEFAULT_AVG_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            retry_config: RetryConfig::default(),
            compression_strategy: CompressionStrategy::default(),
            batch_size: 50,
            replicate_mode: ReplicateMode::default(),
            global_index: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRuntimeStatus {
    pub job_id: String,
    pub status: JobStatus,
    pub progress: u8,
    pub current_size: u64,
    pub total_size: u64,
    pub region: String,
    pub error_message: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub trait JobStatusCallback: Send + Sync {
    fn on_job_started(&self, job_id: &str);
    fn on_job_completed(&self, job_id: &str);
    fn on_job_failed(&self, job_id: &str, error: &str);
    fn on_job_progress(&self, job_id: &str, progress: u8, current_size: u64);
}

pub struct ConnectionPool {
    pub quic_connection: Option<quinn::Connection>,
    pub protocol: Option<crate::sync::net::eyeballs::TransportProtocol>,
    pub quic_client: Option<QuicClient>,
    pub tcp_client: Option<TcpClient>,
    pub health_checker: Option<Arc<crate::sync::net::health::HealthChecker>>,
    pub health_task: Option<JoinHandle<()>>,
}

impl ConnectionPool {
    pub fn new(quic_client: Option<QuicClient>, tcp_client: Option<TcpClient>) -> Self {
        Self {
            quic_connection: None,
            protocol: None,
            quic_client,
            tcp_client,
            health_checker: None,
            health_task: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DynamicTransferConfig {
    pub min_chunk_size: usize,
    pub avg_chunk_size: usize,
    pub max_chunk_size: usize,
    pub concurrent_streams: usize,
    pub batch_size: usize,
    pub max_concurrent_files: usize,
    pub compression_algorithm: String,
    pub compression_level: i32,
    pub receive_window: usize,
    pub send_window: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
}

impl DynamicTransferConfig {
    pub fn for_quality(quality: NetworkQuality) -> Self {
        match quality {
            NetworkQuality::Excellent => Self::excellent(),
            NetworkQuality::Good => Self::good(),
            NetworkQuality::Fair => Self::fair(),
            NetworkQuality::Poor | NetworkQuality::Unknown => Self::poor(),
        }
    }

    pub fn excellent() -> Self {
        Self {
            min_chunk_size: 512 * 1024,
            avg_chunk_size: 4 * 1024 * 1024,
            max_chunk_size: 16 * 1024 * 1024,
            concurrent_streams: 64,
            batch_size: 100,
            max_concurrent_files: 8,
            compression_algorithm: "none".to_string(),
            compression_level: 0,
            receive_window: 200 * 1024 * 1024,
            send_window: 128 * 1024 * 1024,
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
        }
    }

    pub fn good() -> Self {
        Self {
            min_chunk_size: 256 * 1024,
            avg_chunk_size: 2 * 1024 * 1024,
            max_chunk_size: 8 * 1024 * 1024,
            concurrent_streams: 32,
            batch_size: 50,
            max_concurrent_files: 4,
            compression_algorithm: "lz4".to_string(),
            compression_level: 1,
            receive_window: 100 * 1024 * 1024,
            send_window: 64 * 1024 * 1024,
            max_retries: 5,
            initial_retry_delay: Duration::from_millis(200),
            max_retry_delay: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(15),
            read_timeout: Duration::from_secs(45),
            write_timeout: Duration::from_secs(45),
            idle_timeout: Duration::from_secs(45),
        }
    }

    pub fn fair() -> Self {
        Self {
            min_chunk_size: 128 * 1024,
            avg_chunk_size: 512 * 1024,
            max_chunk_size: 2 * 1024 * 1024,
            concurrent_streams: 16,
            batch_size: 20,
            max_concurrent_files: 2,
            compression_algorithm: "zstd".to_string(),
            compression_level: 3,
            receive_window: 32 * 1024 * 1024,
            send_window: 16 * 1024 * 1024,
            max_retries: 10,
            initial_retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(60),
        }
    }

    pub fn poor() -> Self {
        Self {
            min_chunk_size: 64 * 1024,
            avg_chunk_size: 256 * 1024,
            max_chunk_size: 1024 * 1024,
            concurrent_streams: 4,
            batch_size: 10,
            max_concurrent_files: 1,
            compression_algorithm: "zstd".to_string(),
            compression_level: 9,
            receive_window: 8 * 1024 * 1024,
            send_window: 4 * 1024 * 1024,
            max_retries: 20,
            initial_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(60),
            read_timeout: Duration::from_secs(120),
            write_timeout: Duration::from_secs(120),
            idle_timeout: Duration::from_secs(120),
        }
    }
}

impl Default for DynamicTransferConfig {
    fn default() -> Self {
        Self::good()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicateMode {
    #[default]
    Append,
    Tmp,
}
