use crate::util::time::{duration_nanos, unix_timestamp_nanos};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

struct SizeItem {
    size: u64,
    freeze_at: Instant,
    mtime: i64,
}

pub struct SizeFreezer {
    size_map: Mutex<HashMap<String, SizeItem>>,
    freeze_threshold: Duration,
    exclude_extensions: Vec<String>,
}

impl Default for SizeFreezer {
    fn default() -> Self {
        Self::new()
    }
}

impl SizeFreezer {
    pub fn new() -> Self {
        Self::with_threshold(Duration::from_secs(20))
    }

    pub(crate) fn with_threshold(freeze_threshold: Duration) -> Self {
        Self {
            size_map: Mutex::new(HashMap::new()),
            freeze_threshold,
            exclude_extensions: Vec::new(),
        }
    }

    fn is_excluded(&self, file_name: &str) -> bool {
        let ext = Path::new(file_name)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e.to_lowercase()))
            .unwrap_or_default();

        self.exclude_extensions
            .iter()
            .any(|e| e.to_lowercase() == ext)
    }

    pub async fn check_stable_and_update(&self, file_name: &str, size: u64, mtime: i64) -> bool {
        if !self.exclude_extensions.is_empty() && self.is_excluded(file_name) {
            return true;
        }

        let now = Instant::now();
        let current_time = unix_timestamp_nanos(std::time::SystemTime::now());
        let threshold_nanos = duration_nanos(self.freeze_threshold);

        let mtime_age = current_time.saturating_sub(mtime);
        if mtime_age > threshold_nanos {
            debug!(
                "File {} is stable (mtime age: {}ns > threshold: {}ns)",
                file_name, mtime_age, threshold_nanos
            );
            return true;
        }

        let mut size_map = self.size_map.lock().await;

        if let Some(item) = size_map.get_mut(file_name) {
            if item.size == size && item.mtime == mtime {
                let elapsed = now.duration_since(item.freeze_at);
                if elapsed >= self.freeze_threshold {
                    debug!(
                        "File {} is stable (size={}, frozen for {:.1}s)",
                        file_name,
                        size,
                        elapsed.as_secs_f64()
                    );
                    return true;
                }
                return false;
            } else {
                item.size = size;
                item.mtime = mtime;
                item.freeze_at = now;
                debug!(
                    "File {} changed (size={}, mtime={}), restarting freeze timer",
                    file_name, size, mtime
                );
                return false;
            }
        }

        size_map.insert(
            file_name.to_string(),
            SizeItem {
                size,
                mtime,
                freeze_at: now,
            },
        );
        debug!(
            "File {} first seen (size={}, mtime={}), starting freeze timer",
            file_name, size, mtime
        );
        false
    }

    pub async fn get_stable_files(&self, files: &[(String, u64, i64)]) -> Vec<String> {
        let mut stable_files = Vec::new();
        for (file_name, size, mtime) in files {
            if self.check_stable_and_update(file_name, *size, *mtime).await {
                stable_files.push(file_name.clone());
            }
        }
        let removed = self.prune_expired_entries().await;
        if removed > 0 {
            debug!(
                "Pruned {} expired stability tracking entries after scan",
                removed
            );
        }
        stable_files
    }

    pub async fn remove(&self, file_name: &str) {
        let mut size_map = self.size_map.lock().await;
        size_map.remove(file_name);
    }

    pub async fn retain_paths(&self, current_paths: &HashSet<String>) -> usize {
        let mut size_map = self.size_map.lock().await;
        let before = size_map.len();
        size_map.retain(|path, _| current_paths.contains(path));
        before.saturating_sub(size_map.len())
    }

    pub async fn clear(&self) {
        let mut size_map = self.size_map.lock().await;
        size_map.clear();
    }

    async fn prune_expired_entries(&self) -> usize {
        let threshold_nanos = duration_nanos(self.freeze_threshold);
        let current_time = unix_timestamp_nanos(std::time::SystemTime::now());
        let mut size_map = self.size_map.lock().await;
        let before = size_map.len();
        size_map.retain(|_, item| current_time.saturating_sub(item.mtime) <= threshold_nanos);
        before.saturating_sub(size_map.len())
    }
}

#[cfg(test)]
mod tests {
    use super::SizeFreezer;
    use crate::util::time::unix_timestamp_nanos;
    use std::collections::HashSet;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn text_files_do_not_bypass_stability_wait_by_default() {
        let freezer = SizeFreezer::new();
        let mtime = unix_timestamp_nanos(SystemTime::now());

        assert!(
            !freezer
                .check_stable_and_update("/tmp/current.log", 128, mtime)
                .await
        );
        assert!(
            !freezer
                .check_stable_and_update("/tmp/current.txt", 128, mtime)
                .await
        );
    }

    #[tokio::test]
    async fn separate_freezers_do_not_share_stability_state() {
        let freezer_a = SizeFreezer::with_threshold(Duration::from_millis(1));
        let freezer_b = SizeFreezer::with_threshold(Duration::from_millis(1));
        let mtime_a = unix_timestamp_nanos(SystemTime::now());

        assert!(
            !freezer_a
                .check_stable_and_update("/tmp/file.bin", 64, mtime_a)
                .await
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(
            freezer_a
                .check_stable_and_update("/tmp/file.bin", 64, mtime_a)
                .await
        );
        let mtime_b = unix_timestamp_nanos(SystemTime::now());
        assert!(
            !freezer_b
                .check_stable_and_update("/tmp/file.bin", 64, mtime_b)
                .await
        );
    }

    #[tokio::test]
    async fn retain_paths_removes_stale_entries_from_stability_state() {
        let freezer = SizeFreezer::with_threshold(Duration::from_millis(1));
        let file = "/tmp/file.bin";
        let mtime = unix_timestamp_nanos(SystemTime::now()) + 1_000_000_000;

        assert!(!freezer.check_stable_and_update(file, 64, mtime).await);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(freezer.check_stable_and_update(file, 64, mtime).await);

        let removed = freezer.retain_paths(&HashSet::new()).await;
        assert_eq!(removed, 1);
        assert!(!freezer.check_stable_and_update(file, 64, mtime).await);
    }

    #[tokio::test]
    async fn get_stable_files_prunes_entries_once_mtime_ages_past_threshold() {
        let freezer = SizeFreezer::with_threshold(Duration::from_millis(10));
        let file = "/tmp/file.bin".to_string();
        let mtime = unix_timestamp_nanos(SystemTime::now());

        assert!(freezer
            .get_stable_files(&[(file.clone(), 64, mtime)])
            .await
            .is_empty());
        assert_eq!(freezer.size_map.lock().await.len(), 1);

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            freezer.get_stable_files(&[(file.clone(), 64, mtime)]).await,
            vec![file]
        );
        assert_eq!(freezer.size_map.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn get_stable_files_keeps_entries_when_stability_relies_on_local_freeze_time() {
        let freezer = SizeFreezer::with_threshold(Duration::from_millis(1));
        let file = "/tmp/file.bin".to_string();
        let mtime = unix_timestamp_nanos(SystemTime::now()) + 1_000_000_000;

        assert!(freezer
            .get_stable_files(&[(file.clone(), 64, mtime)])
            .await
            .is_empty());
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(
            freezer.get_stable_files(&[(file.clone(), 64, mtime)]).await,
            vec![file]
        );
        assert_eq!(freezer.size_map.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn check_stable_and_update_handles_extreme_past_mtime_without_overflow() {
        let freezer = SizeFreezer::with_threshold(Duration::from_millis(1));

        assert!(
            freezer
                .check_stable_and_update("/tmp/file.bin", 64, i64::MIN)
                .await
        );
    }
}
