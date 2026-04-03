use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::net::transport::{ProtocolSelector, TransportConnection};
use crate::sync::scanner::ScannedFile;
use crate::util::error::Result;
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
use super::calculate_progress;
use super::concurrent::sync_files_concurrent;
use super::sequential::sync_files_sequential;
use super::single::{calculate_dest_path, sync_directory_entry};

fn sort_directory_entries(entries: &mut [ScannedFile]) {
    entries.sort_by(|left, right| {
        left.path
            .components()
            .count()
            .cmp(&right.path.components().count())
            .then_with(|| left.path.cmp(&right.path))
    });
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_files(
    config: &Arc<SchedulerConfig>,
    transfer_manager_pool: &Arc<TransferManagerPool>,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    shutdown: &Arc<AtomicBool>,
    job_status_cache: &Arc<DashMap<String, JobRuntimeStatus>>,
    cancelled_jobs: &Arc<DashMap<String, ()>>,
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

    let total_entries = files.len();
    let mut directory_entries = Vec::new();
    let mut remaining_entries = Vec::new();
    for file in files {
        if file.is_dir {
            directory_entries.push(file);
        } else {
            remaining_entries.push(file);
        }
    }

    if !directory_entries.is_empty() {
        sort_directory_entries(&mut directory_entries);
        info!(
            "Syncing {} directories before file payload transfer for job {}",
            directory_entries.len(),
            job.job_id
        );
        for file in &directory_entries {
            let source_file_path = file.path.to_string_lossy().to_string();
            let source_str = job.source.to_string_lossy();
            let relative_path = source_file_path
                .strip_prefix(source_str.trim_end_matches('/'))
                .unwrap_or(&source_file_path)
                .trim_start_matches('/');
            let dest_file_path = calculate_dest_path(config, job, relative_path, total_entries)?;
            sync_directory_entry(file, &dest_file_path).await?;
        }
    }

    if remaining_entries.is_empty() {
        return Ok(());
    }

    let initial_connection = connection::get_connection_with_retry_for_region_with_selector(
        config,
        connection_pools,
        &job.region,
        shutdown,
        Some(protocol_selector),
    )
    .await?;
    let is_quic = matches!(initial_connection, TransportConnection::Quic { .. });
    drop(initial_connection);

    let total_size: u64 = remaining_entries.iter().map(|f| f.size).sum();
    let initial_current = job_status_cache
        .get(&job.job_id)
        .map(|status| status.current_size.min(total_size))
        .unwrap_or(0);
    let initial_progress = calculate_progress(initial_current, total_size);
    info!(
        "Job {} calculated total_size={} from {} files",
        job.job_id,
        total_size,
        remaining_entries.len()
    );
    for (idx, f) in remaining_entries.iter().enumerate() {
        info!("  File {}: path={}, size={}", idx, f.path.display(), f.size);
    }

    notify_progress(&job.job_id, initial_progress, initial_current, total_size);

    let concurrent_streams = concurrency_controller.get_concurrency();
    info!(
        "Job {} using AIMD concurrency: {} streams",
        job.job_id, concurrent_streams
    );

    let should_sync_concurrently = is_quic || remaining_entries.len() > 1;
    if should_sync_concurrently {
        sync_files_concurrent(
            config,
            transfer_manager_pool,
            connection_pools,
            &job.region,
            shutdown,
            job_status_cache,
            cancelled_jobs,
            job,
            remaining_entries,
            adaptive_controller,
            concurrency_controller,
            retry_policy,
            protocol_selector,
            prefetch_manager,
            chunk_index,
            is_quic,
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
            cancelled_jobs,
            job,
            remaining_entries,
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
