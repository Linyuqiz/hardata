use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::net::transport::{ProtocolSelector, TransportConnection};
use crate::sync::scanner::ScannedFile;
use crate::util::error::{HarDataError, Result};
use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use super::super::adaptive::{AdaptiveConcurrencyController, NetworkAdaptiveController};
use super::super::infrastructure::config::{ConnectionPool, JobRuntimeStatus, SchedulerConfig};
use super::super::infrastructure::connection;
use super::super::optimization::PrefetchManager;
use super::super::retry::SmartRetryPolicy;
use super::concurrent::sync_files_concurrent;
use super::sequential::sync_files_sequential;

#[allow(clippy::too_many_arguments)]
pub async fn sync_files(
    config: &Arc<SchedulerConfig>,
    transfer_manager_pool: &Arc<TransferManagerPool>,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    shutdown: &Arc<AtomicBool>,
    job_status_cache: &Arc<DashMap<String, JobRuntimeStatus>>,
    job: &SyncJob,
    files: Vec<ScannedFile>,
    adaptive_controller: &Arc<NetworkAdaptiveController>,
    concurrency_controller: &Arc<AdaptiveConcurrencyController>,
    retry_policy: &Arc<SmartRetryPolicy>,
    protocol_selector: &Arc<ProtocolSelector>,
    prefetch_manager: &Arc<PrefetchManager>,
    chunk_index: &Arc<crate::sync::engine::CDCResultCache>,
    notify_progress: impl Fn(&str, u8, u64, u64) + Send + Sync + Clone + 'static,
) -> Result<()> {
    info!(
        "Syncing {} files for job {} (region='{}')",
        files.len(),
        job.job_id,
        job.region
    );

    let connection_pool = connection_pools
        .get(&job.region)
        .ok_or_else(|| HarDataError::InvalidConfig(format!("Region '{}' not found", job.region)))?
        .clone();

    let conn = connection::get_transport_connection(&connection_pool).await?;
    let is_quic = matches!(conn, TransportConnection::Quic { .. });
    drop(conn);

    let total_size: u64 = files.iter().map(|f| f.size).sum();
    info!(
        "Job {} calculated total_size={} from {} files",
        job.job_id,
        total_size,
        files.len()
    );
    for (idx, f) in files.iter().enumerate() {
        info!("  File {}: path={}, size={}", idx, f.path.display(), f.size);
    }

    notify_progress(&job.job_id, 0, 0, total_size);

    let concurrent_streams = concurrency_controller.get_concurrency();
    info!(
        "Job {} using AIMD concurrency: {} streams",
        job.job_id, concurrent_streams
    );

    if is_quic {
        sync_files_concurrent(
            config,
            transfer_manager_pool,
            connection_pools,
            &job.region,
            shutdown,
            job_status_cache,
            job,
            files,
            adaptive_controller,
            concurrency_controller,
            retry_policy,
            protocol_selector,
            prefetch_manager,
            chunk_index,
            total_size,
            concurrent_streams,
            notify_progress,
        )
        .await
    } else {
        sync_files_sequential(
            config,
            transfer_manager_pool,
            connection_pools,
            &job.region,
            shutdown,
            job_status_cache,
            job,
            files,
            adaptive_controller,
            concurrency_controller,
            retry_policy,
            prefetch_manager,
            chunk_index,
            total_size,
            notify_progress,
        )
        .await
    }
}
