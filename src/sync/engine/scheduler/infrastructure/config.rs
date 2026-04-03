use crate::core::constants::{DEFAULT_QUIC_BIND_ADDR, DEFAULT_TCP_BIND_ADDR};
use crate::core::job::JobStatus;
use crate::sync::net::bandwidth::NetworkQuality;
use crate::sync::net::quic::QuicClient;
use crate::sync::net::tcp::TcpClient;
use crate::sync::RegionConfig;
use crate::util::compression::CompressionStrategy;
use crate::util::error::{HarDataError, Result};
use crate::util::retry::RetryConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
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
    pub stability_threshold: Duration,
    pub replicate_mode: ReplicateMode,
    pub allow_external_destinations: bool,
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
            stability_threshold: Duration::from_secs(20),
            replicate_mode: ReplicateMode::default(),
            allow_external_destinations: false,
            global_index: None,
        }
    }
}

impl SchedulerConfig {
    pub fn normalized_data_dir(&self) -> PathBuf {
        normalize_path(Path::new(self.data_dir.trim_end_matches('/')))
    }

    pub fn resolve_destination_path(&self, dest: &str) -> Result<PathBuf> {
        let normalized_dest = normalize_path(Path::new(dest));
        let data_dir = self.normalized_data_dir();
        let resolved = if Path::new(dest).is_absolute() || normalized_dest.starts_with(&data_dir) {
            normalized_dest
        } else {
            normalize_path(&data_dir.join(&normalized_dest))
        };

        if !self.allow_external_destinations && !resolved.starts_with(&data_dir) {
            return Err(HarDataError::FileOperation(format!(
                "Destination path '{}' resolves outside sync.data_dir '{}'",
                dest,
                data_dir.display()
            )));
        }

        Ok(resolved)
    }

    pub fn resolve_runtime_destination_path(&self, dest: &str) -> Result<PathBuf> {
        let resolved = self.resolve_destination_path(dest)?;
        self.validate_runtime_destination_ancestry(&resolved)?;
        Ok(resolved)
    }

    fn validate_runtime_destination_ancestry(&self, resolved: &Path) -> Result<()> {
        if self.allow_external_destinations {
            return Ok(());
        }

        let data_dir = self.normalized_data_dir();
        let canonical_data_dir = match std::fs::canonicalize(&data_dir) {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(HarDataError::FileOperation(format!(
                    "Failed to canonicalize sync.data_dir '{}': {}",
                    data_dir.display(),
                    e
                )));
            }
        };

        let nearest_existing = nearest_existing_ancestor(resolved)?;
        let canonical_existing = std::fs::canonicalize(&nearest_existing).map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to canonicalize destination ancestor '{}': {}",
                nearest_existing.display(),
                e
            ))
        })?;

        if !canonical_existing.starts_with(&canonical_data_dir) {
            return Err(HarDataError::FileOperation(format!(
                "Destination path '{}' escapes sync.data_dir '{}' through existing ancestor '{}'",
                resolved.display(),
                data_dir.display(),
                nearest_existing.display()
            )));
        }

        Ok(())
    }
}

pub fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                let _ = normalized.pop();
            }
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut current = Some(path);

    while let Some(candidate) = current {
        if std::fs::symlink_metadata(candidate).is_ok() {
            return Ok(candidate.to_path_buf());
        }
        current = candidate.parent();
    }

    Err(HarDataError::FileOperation(format!(
        "Destination path '{}' has no existing ancestor",
        path.display()
    )))
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

#[cfg(test)]
mod tests {
    use super::SchedulerConfig;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("hardata-config-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn resolve_runtime_destination_path_rejects_symlink_ancestor_outside_data_dir() {
        let root = temp_dir("runtime-dest-symlink");
        let data_dir = root.join("sync-data");
        let outside = root.join("outside");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, data_dir.join("escape")).unwrap();

        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };

        let err = config
            .resolve_runtime_destination_path("escape/file.txt")
            .unwrap_err();
        assert!(err.to_string().contains("escapes sync.data_dir"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_runtime_destination_path_allows_regular_subpath() {
        let root = temp_dir("runtime-dest-regular");
        let data_dir = root.join("sync-data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let config = SchedulerConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..SchedulerConfig::default()
        };

        let resolved = config
            .resolve_runtime_destination_path("nested/file.txt")
            .unwrap();
        assert_eq!(resolved, data_dir.join("nested/file.txt"));

        std::fs::remove_dir_all(root).unwrap();
    }
}

impl Default for DynamicTransferConfig {
    fn default() -> Self {
        Self::good()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReplicateMode {
    Append,
    #[default]
    Tmp,
}
