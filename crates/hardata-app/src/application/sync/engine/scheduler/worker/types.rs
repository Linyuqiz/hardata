use crate::application::sync::engine::job::SyncJob;
use crate::application::sync::scanner::ScannedFile;
use crate::domain::job::JobStatus;
use crate::shared::error::{HarDataError, Result};
use crate::shared::time::{
    metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos, optional_timestamps_match,
    timestamps_match,
};
use dashmap::DashMap;
use regex::Regex;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::core::{FileSyncState, SyncScheduler};
use super::infrastructure::config::normalize_path;
use super::infrastructure::connection;
use super::infrastructure::notify::is_cancelled_error;
use super::sync_modes::files as sync_files;

const MIN_STABILITY_RETRY_DELAY_MS: u64 = 100;

struct RunningJobGuard {
    job_id: String,
    running_jobs: Arc<DashMap<String, ()>>,
    cancelled_jobs: Arc<DashMap<String, ()>>,
}

impl RunningJobGuard {
    fn new(
        job_id: String,
        running_jobs: Arc<DashMap<String, ()>>,
        cancelled_jobs: Arc<DashMap<String, ()>>,
    ) -> Self {
        running_jobs.insert(job_id.clone(), ());
        Self {
            job_id,
            running_jobs,
            cancelled_jobs,
        }
    }
}

fn source_file_matches_cached_state(
    file: &ScannedFile,
    size: u64,
    mtime: i64,
    change_time: Option<i64>,
    inode: Option<u64>,
) -> bool {
    size == file.size
        && timestamps_match(mtime, file.modified)
        && optional_timestamps_match(change_time, file.change_time)
        && inode == file.inode
}

impl Drop for RunningJobGuard {
    fn drop(&mut self) {
        self.running_jobs.remove(&self.job_id);
        self.cancelled_jobs.remove(&self.job_id);
    }
}

struct ScanFilter {
    exclude: Vec<Regex>,
    include: Vec<Regex>,
}

struct RemoteScanResult {
    files: Vec<ScannedFile>,
    source_is_single_file: bool,
    root_excluded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobExecutionResult {
    NoTransfer { retry_due_to_stability: bool },
    Transferred { retry_due_to_stability: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DestinationSyncState {
    requires_sync: bool,
    dest_mtime: Option<i64>,
    dest_change_time: Option<i64>,
    dest_inode: Option<u64>,
}

impl JobExecutionResult {
    fn retry_due_to_stability(self) -> bool {
        match self {
            Self::NoTransfer {
                retry_due_to_stability,
            }
            | Self::Transferred {
                retry_due_to_stability,
            } => retry_due_to_stability,
        }
    }

    fn transferred(self) -> bool {
        matches!(self, Self::Transferred { .. })
    }
}

impl ScanFilter {
    fn new(exclude_patterns: &[String], include_patterns: &[String]) -> Result<Self> {
        Ok(Self {
            exclude: compile_patterns(exclude_patterns, "exclude_regex")?,
            include: compile_patterns(include_patterns, "include_regex")?,
        })
    }

    fn excludes(&self, path: &str) -> bool {
        self.exclude.iter().any(|regex| regex.is_match(path))
    }

    fn include_matches(&self, path: &str) -> bool {
        self.include.is_empty() || self.include.iter().any(|regex| regex.is_match(path))
    }

    fn should_scan_dir(&self, path: &str) -> bool {
        !self.excludes(path)
    }

    fn should_include_dir(&self, path: &str) -> bool {
        self.should_scan_dir(path) && self.include_matches(path)
    }

    fn should_include_file(&self, path: &str) -> bool {
        self.should_scan_dir(path) && self.include_matches(path)
    }
}
