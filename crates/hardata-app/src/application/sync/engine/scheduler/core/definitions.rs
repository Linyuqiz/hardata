use crate::adapters::outbound::persistence::db::Database;
use crate::adapters::outbound::transport::bandwidth::BandwidthProbe;
use crate::adapters::outbound::transport::gateway::ProtocolSelector;
use crate::adapters::outbound::transport::quic::QuicClient;
use crate::adapters::outbound::transport::tcp::TcpClient;
use crate::application::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::domain::JobStatus;
use crate::shared::error::Result;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::infrastructure::config::{
    ConnectionPool, JobRuntimeStatus, JobStatusCallback, SchedulerConfig,
};
use super::infrastructure::connection;
use super::{
    AdaptiveConcurrencyController, DelayedQueue, NetworkAdaptiveController, PrefetchManager,
    PriorityQueue, SizeFreezer, SmartRetryPolicy,
};

fn build_region_tcp_client(
    region: &crate::application::config::RegionConfig,
    pool_size: usize,
    quic_available: bool,
) -> Result<Option<TcpClient>> {
    match TcpClient::with_pool(region.tcp_bind.clone(), Some(pool_size)) {
        Ok(client) => Ok(Some(client)),
        Err(pool_error) => {
            warn!(
                operation = "scheduler.tcp_pool_create_failed",
                region = %region.name,
                error = %pool_error,
                "pooled TCP client unavailable; falling back to direct client"
            );

            match TcpClient::new(region.tcp_bind.clone()) {
                Ok(client) => Ok(Some(client)),
                Err(direct_error) if quic_available => {
                    warn!(
                        operation = "scheduler.tcp_client_unavailable",
                        region = %region.name,
                        pool_error = %pool_error,
                        direct_error = %direct_error,
                        fallback = "quic_only",
                        "TCP client unavailable; continuing with QUIC only"
                    );
                    Ok(None)
                }
                Err(direct_error) => {
                    Err(crate::shared::error::HarDataError::InvalidConfig(format!(
                        "Region '{}' has no usable transport client (tcp pool: {}; tcp direct: {})",
                        region.name, pool_error, direct_error
                    )))
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileSyncState {
    pub size: u64,
    pub mtime: i64,
    pub change_time: Option<i64>,
    pub inode: Option<u64>,
    pub dest_mtime: Option<i64>,
    pub dest_change_time: Option<i64>,
    pub dest_inode: Option<u64>,
    pub updated_at: i64,
}

#[derive(Clone)]
pub struct SyncScheduler {
    pub(super) job_queue: Arc<PriorityQueue<SyncJob>>,
    pub(super) job_notify: Arc<Notify>,
    pub(super) job_status_cache: Arc<DashMap<String, JobRuntimeStatus>>,
    pub(super) job_cache: Arc<DashMap<String, SyncJob>>,
    pub(super) cancelled_jobs: Arc<DashMap<String, ()>>,
    pub(super) running_jobs: Arc<DashMap<String, ()>>,
    pub(super) config: Arc<SchedulerConfig>,
    pub(super) transfer_manager_pool: Arc<TransferManagerPool>,
    pub(super) connection_pools: Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    pub(super) semaphore: Arc<Semaphore>,
    pub(super) shutdown: Arc<AtomicBool>,
    pub(super) shutdown_signal: watch::Sender<bool>,
    pub(super) queue_update_lock: Arc<Mutex<()>>,
    pub(super) workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    pub(super) retry_scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) delayed_scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) delayed_queue: Arc<DelayedQueue<SyncJob>>,
    pub(super) cache_cleaner: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) cache_builder: Arc<Mutex<Option<Arc<super::CacheBuilder>>>>,
    pub(super) status_callback: Arc<Mutex<Option<Arc<dyn JobStatusCallback>>>>,
    pub(super) db: Arc<Database>,
    pub(super) chunk_index: Arc<crate::application::sync::engine::CDCResultCache>,
    pub(super) adaptive_controller: Arc<NetworkAdaptiveController>,
    pub(super) size_freezers: Arc<DashMap<String, Arc<SizeFreezer>>>,
    pub(super) concurrency_controllers: Arc<DashMap<String, Arc<AdaptiveConcurrencyController>>>,
    pub(super) prefetch_manager: Arc<PrefetchManager>,
    pub(super) retry_policy: Arc<SmartRetryPolicy>,
    pub(super) protocol_selector: Arc<ProtocolSelector>,
    pub(super) synced_files_cache: Arc<DashMap<String, DashMap<String, FileSyncState>>>,
}
