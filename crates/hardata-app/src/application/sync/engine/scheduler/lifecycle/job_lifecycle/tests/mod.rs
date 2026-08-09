use super::super::super::core::SyncScheduler;
use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::job::SyncJob;
use crate::application::sync::engine::scheduler::core::FileSyncState;
use crate::application::sync::engine::scheduler::{JobRuntimeStatus, SchedulerConfig};
use crate::domain::{FileTransferState, Job, JobPath, JobStatus, JobType};
use dashmap::DashMap;
use sqlx::SqlitePool;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hardata-{name}-{unique}"));
    fs::create_dir_all(&path).unwrap();
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
