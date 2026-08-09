use super::{
    cleanup_deleted_targets, has_active_scan_filters, next_sync_schedule_delay,
    parent_listing_confirms_single_file, parent_lookup_path, pending_stability_file_count,
    should_cleanup_deleted_targets, single_file_root_candidate, source_file_matches_cached_state,
    trim_deleted_source_tracking, JobExecutionResult, ScanFilter, MIN_STABILITY_RETRY_DELAY_MS,
};
use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::job::SyncJob;
use crate::application::sync::engine::scheduler::core::FileSyncState;
use crate::application::sync::engine::scheduler::SchedulerConfig;
use crate::application::sync::scanner::ScannedFile;
use crate::domain::{JobStatus, JobType};
use crate::protocol::FileInfo;
use crate::shared::time::unix_timestamp_nanos;
use hardata_infra_agent::agent_server::tcp::TcpServer;
use hardata_infra_agent::compute::ComputeService;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn temp_dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

async fn open_raw_pool(db_path: &str) -> SqlitePool {
    SqlitePool::connect(db_path).await.unwrap()
}

include!("part_01.rs");
include!("part_02.rs");
include!("part_03.rs");
include!("part_04.rs");
include!("part_05.rs");
include!("part_06.rs");
