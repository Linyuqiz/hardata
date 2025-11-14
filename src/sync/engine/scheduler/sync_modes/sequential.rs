use crate::core::job::JobStatus;
use crate::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::sync::scanner::ScannedFile;
use crate::util::error::HarDataError;
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

#[allow(clippy::too_many_arguments)]
pub async fn sync_files_sequential(
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
    prefetch_manager: &Arc<PrefetchManager>,
    chunk_index: &Arc<crate::sync::engine::CDCResultCache>,
    total_size: u64,
    notify_progress: impl Fn(&str, u8, u64, u64) + Send + Sync + Clone + 'static,
) -> crate::util::error::Result<()> {
    let mut connection = connection::get_connection_with_retry_for_region(
        config,
        connection_pools,
        region,
        shutdown,
    )
    .await?;

    let protocol = if matches!(
        connection,
        crate::sync::net::transport::TransportConnection::Quic { .. }
    ) {
        "QUIC"
    } else {
        "TCP"
    };
    info!(
        "Sequential mode: Processing {} files one-by-one (region='{}', protocol={})",
        files.len(),
        region,
        protocol
    );
    let mut success = 0;
    let mut failed = 0;
    let total_transferred = Arc::new(AtomicU64::new(0));
    let files_len = files.len();

    for file in &files {
        if let Some(status) = job_status_cache.get(&job.job_id) {
            if status.status == JobStatus::Cancelled {
                info!("Job {} was cancelled during file sync", job.job_id);
                return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
            }
        }

        let source_file_path = file.path.to_string_lossy().to_string();

        let source_str = job.source.to_string_lossy();
        let relative_path = source_file_path
            .strip_prefix(source_str.trim_end_matches('/'))
            .unwrap_or(&source_file_path)
            .trim_start_matches('/');

        let dest_file_path = calculate_dest_path(config, job, relative_path, files_len);

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
            job,
            file,
            &source_file_path,
            &dest_file_path,
            &mut connection,
            concurrent_streams,
            move |delta| {
                let new_total = total_transferred_clone.fetch_add(delta, Ordering::Relaxed) + delta;
                let progress = ((new_total as f64 / total_size as f64) * 100.0) as u8;
                notify_progress_clone(&job_id_clone, progress, new_total, total_size);
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
                        error!("Fatal error syncing file {:?}: {}", file.path, e);
                    }
                    ErrorCategory::Transient => {
                        error!("Transient error syncing file {:?}: {}", file.path, e);
                    }
                    _ => {
                        error!(
                            "Failed to sync file {:?}: {} (category={:?})",
                            file.path, e, category
                        );
                    }
                }
                failed += 1;
                adaptive_controller.record_transfer_failure();
            }
        }
    }

    info!(
        "Job {} completed: {} succeeded, {} failed",
        job.job_id, success, failed
    );

    let quality = adaptive_controller.assess_network_quality().await;
    concurrency_controller.adjust_concurrency(quality);

    if failed > 0 {
        Err(HarDataError::Unknown(format!("{} files failed", failed)))
    } else {
        Ok(())
    }
}
