use crate::application::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::application::sync::scanner::ScannedFile;
use crate::domain::job::JobStatus;
use crate::shared::error::HarDataError;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{error, info};

use super::super::adaptive::{AdaptiveConcurrencyController, NetworkAdaptiveController};
use super::super::infrastructure::config::{ConnectionPool, JobRuntimeStatus, SchedulerConfig};
use super::super::infrastructure::connection;
use super::super::optimization::PrefetchManager;
use super::super::retry::ErrorCategory;
use super::super::retry::SmartRetryPolicy;
use super::single::{calculate_dest_path, sync_single_file};
use super::{aggregate_failed_sync_error, calculate_progress, update_representative_failure};

#[allow(clippy::too_many_arguments)]
pub async fn sync_files_sequential(
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
    prefetch_manager: &Arc<PrefetchManager>,
    chunk_index: &Arc<crate::application::sync::engine::CDCResultCache>,
    total_size: u64,
    notify_progress: impl Fn(&str, u8, u64, u64) + Send + Sync + Clone + 'static,
) -> crate::shared::error::Result<()> {
    let mut connection = connection::get_connection_with_retry_for_region(
        config,
        connection_pools,
        region,
        shutdown,
    )
    .await?;

    let protocol = if matches!(
        connection,
        crate::adapters::outbound::transport::gateway::TransportConnection::Quic { .. }
    ) {
        "QUIC"
    } else {
        "TCP"
    };
    info!(
        operation = "job.sequential_sync_started",
        job_id = %job.job_id,
        region = %region,
        protocol = %protocol,
        file_count = files.len(),
        "sequential file synchronization started"
    );
    let mut success = 0;
    let mut failed = 0;
    let mut representative_failure: Option<(ErrorCategory, String)> = None;
    let initial_transferred = job_status_cache
        .get(&job.job_id)
        .map(|status| status.current_size.min(total_size))
        .unwrap_or(0);
    let total_transferred = Arc::new(AtomicU64::new(initial_transferred));
    let files_len = files.len();

    for file in &files {
        if cancelled_jobs.contains_key(&job.job_id)
            || job_status_cache
                .get(&job.job_id)
                .map(|status| status.status == JobStatus::Cancelled)
                .unwrap_or(false)
        {
            info!(operation = "job.cancelled_during_transfer", job_id = %job.job_id, "job cancelled during file synchronization");
            return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
        }

        let source_file_path = file.path.to_string_lossy().to_string();

        let source_str = job.source.to_string_lossy();
        let relative_path = source_file_path
            .strip_prefix(source_str.trim_end_matches('/'))
            .unwrap_or(&source_file_path)
            .trim_start_matches('/');

        let dest_file_path = calculate_dest_path(config, job, relative_path, files_len)?;

        let notify_progress_clone = notify_progress.clone();
        let job_id_clone = job.job_id.clone();
        let total_transferred_clone = total_transferred.clone();

        let file_size = file.size;
        let concurrent_streams = concurrency_controller.get_concurrency();

        let start_time = Instant::now();
        match sync_single_file(
            config,
            transfer_manager_pool,
            job_status_cache,
            cancelled_jobs,
            job,
            file,
            &source_file_path,
            &dest_file_path,
            &mut connection,
            concurrent_streams,
            move |delta| {
                let new_total = total_transferred_clone.fetch_add(delta, Ordering::Relaxed) + delta;
                let reported_total = new_total.min(total_size);
                let progress = calculate_progress(reported_total, total_size);
                notify_progress_clone(&job_id_clone, progress, reported_total, total_size);
            },
            Some(prefetch_manager),
            Some(chunk_index),
            config.global_index.as_ref(),
        )
        .await
        {
            Ok(_) => {
                success += 1;
                adaptive_controller.record_transfer_success(file_size, start_time.elapsed());
            }
            Err(e) => {
                let category = retry_policy.categorize_error(&e);
                match category {
                    ErrorCategory::Fatal => {
                        error!(operation = "job.file_sync_failed", job_id = %job.job_id, path = %file.path.display(), category = "fatal", error = %e, "file synchronization failed");
                    }
                    ErrorCategory::Transient => {
                        error!(operation = "job.file_sync_failed", job_id = %job.job_id, path = %file.path.display(), category = "transient", error = %e, "file synchronization failed");
                    }
                    _ => {
                        error!(
                            operation = "job.file_sync_failed",
                            job_id = %job.job_id,
                            path = %file.path.display(),
                            category = ?category,
                            error = %e,
                            "file synchronization failed"
                        );
                    }
                }
                update_representative_failure(&mut representative_failure, category, e.to_string());
                failed += 1;
                adaptive_controller.record_transfer_failure();
            }
        }
    }

    info!(
        operation = "job.file_sync_completed",
        job_id = %job.job_id,
        succeeded = success,
        failed,
        "sequential file synchronization completed"
    );

    let quality = adaptive_controller.assess_network_quality().await;
    concurrency_controller.adjust_concurrency(quality);

    if failed > 0 {
        Err(aggregate_failed_sync_error(failed, representative_failure))
    } else {
        Ok(())
    }
}
