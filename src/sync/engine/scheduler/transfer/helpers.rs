use crate::sync::engine::core::FileChunk;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use crate::sync::transfer::batch::ProgressCallback;

pub type ProgressCallbackResult = (
    Arc<AtomicU64>,
    ProgressCallback,
    Arc<dyn Fn(u64) + Send + Sync>,
    Arc<AtomicU64>,
);

pub fn build_offset_map(chunks: &[FileChunk]) -> HashMap<usize, u64> {
    let mut dest_offset_map = HashMap::new();
    let mut cumulative_offset = 0u64;

    for (idx, chunk) in chunks.iter().enumerate() {
        dest_offset_map.insert(idx, cumulative_offset);
        cumulative_offset += chunk.length;
    }

    tracing::debug!(
        "Calculated dest_offset_map for {} chunks, total size: {}",
        chunks.len(),
        cumulative_offset
    );

    dest_offset_map
}

pub fn create_progress_callback<F>(
    on_batch_progress: F,
    initial_transferred: u64,
) -> ProgressCallbackResult
where
    F: Fn(u64) + Send + Sync + 'static,
{
    let realtime_transferred = Arc::new(AtomicU64::new(initial_transferred));
    let last_reported = Arc::new(AtomicU64::new(initial_transferred));
    let last_report_time = Arc::new(AtomicU64::new(0));

    let rt = realtime_transferred.clone();
    let last_reported_clone = last_reported.clone();
    let last_report_time_clone = last_report_time.clone();
    let progress_callback = Arc::new(on_batch_progress);
    let progress_callback_clone = progress_callback.clone();

    const REPORT_SIZE_INTERVAL: u64 = 10 * 1024 * 1024;
    const REPORT_TIME_INTERVAL_MS: u64 = 500;

    let chunk_progress_callback: ProgressCallback = Arc::new(move |bytes| {
        let new_total = rt.fetch_add(bytes, Ordering::Relaxed) + bytes;

        let last_size = last_reported_clone.load(Ordering::Relaxed);
        let last_time = last_report_time_clone.load(Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let size_trigger = new_total - last_size >= REPORT_SIZE_INTERVAL;
        let time_trigger = now_ms - last_time >= REPORT_TIME_INTERVAL_MS;

        if size_trigger || time_trigger {
            let delta = new_total - last_size;
            last_reported_clone.store(new_total, Ordering::Relaxed);
            last_report_time_clone.store(now_ms, Ordering::Relaxed);
            progress_callback_clone(delta);
        }
    });

    (
        realtime_transferred,
        chunk_progress_callback,
        progress_callback,
        last_reported,
    )
}
