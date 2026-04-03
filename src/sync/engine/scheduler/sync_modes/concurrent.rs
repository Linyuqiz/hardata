use crate::core::constants::SCHEDULER_POOL_SIZE;
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
use super::{aggregate_failed_sync_error, calculate_progress, update_representative_failure};
use crate::sync::net::transport::ProtocolSelector;

fn protocol_label(is_quic: bool) -> &'static str {
    if is_quic {
        "QUIC"
    } else {
        "TCP"
    }
}

fn per_file_stream_budget(
    is_quic: bool,
    requested_streams: usize,
    file_concurrency: usize,
) -> usize {
    let requested_streams = requested_streams.max(1);
    if is_quic {
        requested_streams
    } else {
        requested_streams.min((SCHEDULER_POOL_SIZE / file_concurrency.max(1)).max(1))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_files_concurrent(
    config: &Arc<SchedulerConfig>,
    transfer_manager_pool: &Arc<TransferManagerPool>,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    region: &str,
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
    is_quic: bool,
    total_size: u64,
    concurrent_streams: usize,
    notify_progress: impl Fn(&str, u8, u64, u64) + Send + Sync + Clone + 'static,
) -> crate::util::error::Result<()> {
    let actual_concurrency = concurrent_streams.max(1).min(config.max_concurrent_files);
    let per_file_streams = per_file_stream_budget(is_quic, concurrent_streams, actual_concurrency);
    info!(
        "{}: Processing {} files concurrently (file_concurrency={}, per_file_streams={})",
        protocol_label(is_quic),
        files.len(),
        actual_concurrency,
        per_file_streams
    );

    let semaphore = Arc::new(Semaphore::new(actual_concurrency));
    let adaptive_controller = adaptive_controller.clone();
    let concurrency_controller = concurrency_controller.clone();
    let success = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let representative_failure = Arc::new(Mutex::new(None::<(ErrorCategory, String)>));
    let initial_transferred = job_status_cache
        .get(&job.job_id)
        .map(|status| status.current_size.min(total_size))
        .unwrap_or(0);
    let initial_progress = u64::from(calculate_progress(initial_transferred, total_size));
    let total_transferred = Arc::new(AtomicU64::new(initial_transferred));
    let last_reported_progress = Arc::new(AtomicU64::new(initial_progress));
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
        let representative_failure_ref = representative_failure.clone();
        let job_status_cache = job_status_cache.clone();
        let cancelled_jobs = cancelled_jobs.clone();
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

        let dest_file_path = calculate_dest_path(&config, &job, relative_path, files_len)?;

        let file_name = file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        futures.push(async move {
            let _permit = sem.acquire().await;

            if cancelled_jobs.contains_key(&job_id)
                || job_status_cache
                    .get(&job_id)
                    .map(|status| status.status == JobStatus::Cancelled)
                    .unwrap_or(false)
            {
                return Err(HarDataError::Unknown("Job cancelled".to_string()));
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
            let per_file_streams_copy = per_file_streams;

            let start_time = Instant::now();
            match sync_single_file(
                &config,
                &transfer_manager_pool,
                &job_status_cache,
                &cancelled_jobs,
                &job,
                &file,
                &source_file_path,
                &dest_file_path,
                &mut connection,
                per_file_streams_copy,
                move |delta| {
                    let new_total =
                        total_transferred_ref.fetch_add(delta, Ordering::Relaxed) + delta;
                    let reported_total = new_total.min(total_size_copy);
                    let progress = calculate_progress(reported_total, total_size_copy);

                    let last_progress = last_progress_ref.load(Ordering::Relaxed) as u8;
                    if progress > last_progress {
                        last_progress_ref.store(progress as u64, Ordering::Relaxed);
                        info!("Job {} Progress: {}%", job_id_copy, progress);
                    }

                    notify_progress_ref(&job_id_copy, progress, reported_total, total_size_copy);
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
                    {
                        let mut representative = representative_failure_ref.lock().await;
                        update_representative_failure(&mut representative, category, e.to_string());
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
    let representative_failure = representative_failure.lock().await.take();

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
        Err(aggregate_failed_sync_error(
            failed_count as usize,
            representative_failure,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::per_file_stream_budget;

    #[test]
    fn tcp_stream_budget_is_capped_by_pool_size() {
        assert_eq!(per_file_stream_budget(false, 64, 8), 8);
        assert_eq!(per_file_stream_budget(false, 32, 4), 16);
    }

    #[test]
    fn quic_stream_budget_preserves_requested_parallelism() {
        assert_eq!(per_file_stream_budget(true, 64, 8), 64);
        assert_eq!(per_file_stream_budget(true, 1, 8), 1);
    }
}
