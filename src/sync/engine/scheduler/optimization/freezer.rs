use std::collections::HashMap;
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

impl SizeFreezer {
    pub fn new(_task_id: &str) -> Self {
        Self {
            size_map: Mutex::new(HashMap::new()),
            freeze_threshold: Duration::from_secs(20),
            exclude_extensions: vec![".log".to_string(), ".txt".to_string()],
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
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let mtime_age = current_time - mtime;
        if mtime_age > self.freeze_threshold.as_secs() as i64 {
            debug!(
                "File {} is stable (mtime age: {}s > threshold: {}s)",
                file_name,
                mtime_age,
                self.freeze_threshold.as_secs()
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
        stable_files
    }

    pub async fn remove(&self, file_name: &str) {
        let mut size_map = self.size_map.lock().await;
        size_map.remove(file_name);
    }

    pub async fn clear(&self) {
        let mut size_map = self.size_map.lock().await;
        size_map.clear();
    }
}
