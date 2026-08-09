use crate::adapters::outbound::persistence::db::JobRetry;
use crate::application::sync::engine::job::SyncJob;
use crate::application::sync::engine::scheduler::sync_modes::calculate_progress;
use crate::domain::job::JobStatus;
use crate::domain::{Job, JobConfig, JobPath, JobType};
use crate::shared::error::{HarDataError, Result};
use dashmap::DashMap;
use std::path::PathBuf;
use tracing::{info, warn};

use super::super::core::SyncScheduler;
use super::super::infrastructure::config::JobRuntimeStatus;

/// Snapshot used to restore a job when finalize fails.
struct FinalizeRollbackState {
    snapshot: Option<Job>,
    retry_record: Option<JobRetry>,
    runtime_status: Option<JobRuntimeStatus>,
    sync_job: Option<SyncJob>,
    synced_file_cache: Vec<(String, super::super::core::FileSyncState)>,
}
