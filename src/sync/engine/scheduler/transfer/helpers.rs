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

fn progress_delta(new_total: u64, last_size: u64) -> u64 {
    new_total.saturating_sub(last_size)
}

fn elapsed_millis(now_ms: u64, last_time: u64) -> u64 {
    now_ms.saturating_sub(last_time)
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

        let delta = progress_delta(new_total, last_size);
        let elapsed_ms = elapsed_millis(now_ms, last_time);
        let size_trigger = delta >= REPORT_SIZE_INTERVAL;
        let time_trigger = elapsed_ms >= REPORT_TIME_INTERVAL_MS;

        if delta > 0 && (size_trigger || time_trigger) {
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

#[cfg(test)]
mod tests {
    use super::{elapsed_millis, progress_delta};

    #[test]
    fn progress_delta_is_saturating_when_callbacks_arrive_out_of_order() {
        assert_eq!(progress_delta(10, 12), 0);
        assert_eq!(progress_delta(12, 10), 2);
    }

    #[test]
    fn elapsed_millis_is_saturating_when_clock_moves_backward() {
        assert_eq!(elapsed_millis(100, 250), 0);
        assert_eq!(elapsed_millis(750, 250), 500);
    }
}
