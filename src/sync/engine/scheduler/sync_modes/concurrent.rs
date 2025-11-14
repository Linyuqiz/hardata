use crate::core::job::JobStatus;
use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::scanner::ScannedFile;
use crate::util::error::HarDataError;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info};

use super::super::adaptive::{AdaptiveConcurrencyController, NetworkAdaptiveController};
use super::super::infrastructure::config::{ConnectionPool, JobRuntimeStatus, SchedulerConfig};
use super::super::infrastructure::connection;
use super::super::optimization::PrefetchManager;
use super::super::retry::ErrorCategory;
use super::super::retry::SmartRetryPolicy;
use super::single::{calculate_dest_path, sync_single_file};
use crate::sync::net::transport::ProtocolSelector;

#[allow(clippy::too_many_arguments)]
pub async fn sync_files_concurrent(
    config: &Arc<SchedulerConfig>,
    transfer_manager_pool: &Arc<TransferManagerPool>,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    region: &str,
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
    total_size: u64,
    concurrent_streams: usize,
    notify_progress: impl Fn(&str, u8, u64, u64) + Send + Sync + Clone + 'static,
) -> crate::util::error::Result<()> {
    let actual_concurrency = concurrent_streams.max(1).min(config.max_concurrent_files);
    info!(
        "QUIC: Processing {} files concurrently (dynamic_concurrent={})",
        files.len(),
        actual_concurrency
    );

    let semaphore = Arc::new(Semaphore::new(actual_concurrency));
    let adaptive_controller = adaptive_controller.clone();
    let concurrency_controller = concurrency_controller.clone();
    let success = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let total_transferred = Arc::new(AtomicU64::new(0));
    let last_reported_progress = Arc::new(AtomicU64::new(0));
    let job_id = job.job_id.clone();
    let files_len = files.len();

    let config_clone = config.clone();
    let connection_pools_clone = connection_pools.clone();
    let region_clone = region.to_string();
    let shutdown_clone = shutdown.clone();

    let mut futures = FuturesUnordered::new();

    for file in files {
        let sem = semaphore.clone();
        let success = success.clone();
        let failed = failed.clone();
        let total_transferred = total_transferred.clone();
        let last_reported_progress = last_reported_progress.clone();
        let job_status_cache = job_status_cache.clone();
        let job_id = job_id.clone();
        let job = job.clone();
        let config = config_clone.clone();
        let transfer_manager_pool = transfer_manager_pool.clone();
        let connection_pools_ref = connection_pools_clone.clone();
        let region_ref = region_clone.clone();
        let shutdown_ref = shutdown_clone.clone();
        let notify_progress = notify_progress.clone();
        let adaptive_ctrl = adaptive_controller.clone();
        let concurrency_ctrl = concurrency_controller.clone();
        let retry_pol = retry_policy.clone();
        let proto_selector = protocol_selector.clone();
        let prefetch_mgr = prefetch_manager.clone();

        let source_file_path = file.path.to_string_lossy().to_string();

        let source_str = job.source.to_string_lossy();
        let relative_path = source_file_path
            .strip_prefix(source_str.trim_end_matches('/'))
            .unwrap_or(&source_file_path)
            .trim_start_matches('/');

        let dest_file_path = calculate_dest_path(&config, &job, relative_path, files_len);

        let file_name = file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        futures.push(async move {
            let _permit = sem.acquire().await;

            if let Some(status) = job_status_cache.get(&job_id) {
                if status.status == JobStatus::Cancelled {
                    return Err(HarDataError::Unknown("Job cancelled".to_string()));
                }
            }

            let mut connection =
                match connection::get_connection_with_retry_for_region_with_selector(
                    &config,
                    &connection_pools_ref,
                    &region_ref,
                    &shutdown_ref,
                    Some(&proto_selector),
                )
                .await
                {
                    Ok(conn) => conn,
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                };

            let total_transferred_ref = total_transferred.clone();
            let last_progress_ref = last_reported_progress.clone();
            let total_size_copy = total_size;
            let job_id_copy = job_id.clone();
            let notify_progress_ref = notify_progress.clone();

            let file_size = file.size;
            let concurrent_streams_copy = concurrent_streams;

            let start_time = Instant::now();
            match sync_single_file(
                &config,
                &transfer_manager_pool,
                &job_status_cache,
                &job,
                &file,
                &source_file_path,
                &dest_file_path,
                &mut connection,
                concurrent_streams_copy,
                move |delta| {
                    let new_total =
                        total_transferred_ref.fetch_add(delta, Ordering::Relaxed) + delta;
                    let progress = ((new_total as f64 / total_size_copy as f64) * 100.0) as u8;

                    let last_progress = last_progress_ref.load(Ordering::Relaxed) as u8;
                    if progress > last_progress {
                        last_progress_ref.store(progress as u64, Ordering::Relaxed);
                        info!("Job {} Progress: {}%", job_id_copy, progress);
                    }

                    notify_progress_ref(&job_id_copy, progress, new_total, total_size_copy);
                },
                Some(&prefetch_mgr),
                Some(chunk_index),
                config.global_index.as_ref(),
            )
            .await
            {
                Ok(_) => {
                    success.fetch_add(1, Ordering::Relaxed);
                    adaptive_ctrl.record_transfer_success(file_size, start_time.elapsed());
                    concurrency_ctrl.record_success();
                    Ok(file_size)
                }
                Err(e) => {
                    let category = retry_pol.categorize_error(&e);
                    match category {
                        ErrorCategory::Fatal => {
                            error!("Fatal error syncing file {}: {}", file_name, e);
                        }
                        ErrorCategory::Transient => {
                            error!("Transient error syncing file {}: {}", file_name, e);
                        }
                        _ => {
                            error!(
                                "Failed to sync file {}: {} (category={:?})",
                                file_name, e, category
                            );
                        }
                    }
                    failed.fetch_add(1, Ordering::Relaxed);
                    adaptive_ctrl.record_transfer_failure();
                    concurrency_ctrl.record_failure();
                    Err(e)
                }
            }
        });
    }

    while (futures.next().await).is_some() {}

    let success_count = success.load(Ordering::Relaxed);
    let failed_count = failed.load(Ordering::Relaxed);

    info!(
        "Job {} completed: {} succeeded, {} failed",
        job_id, success_count, failed_count
    );

    let quality = adaptive_controller.assess_network_quality().await;
    concurrency_controller.adjust_concurrency(quality);
    info!(
        "Job {} completed, adjusted concurrency to {}",
        job_id,
        concurrency_controller.get_concurrency()
    );

    if failed_count > 0 {
        Err(HarDataError::Unknown(format!(
            "{} files failed",
            failed_count
        )))
    } else {
        Ok(())
    }
}
