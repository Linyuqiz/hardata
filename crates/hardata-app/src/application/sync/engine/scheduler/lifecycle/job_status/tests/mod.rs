use super::super::super::core::SyncScheduler;
use super::resolve_persisted_page_window;
use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::scheduler::{JobRuntimeStatus, SchedulerConfig};
use crate::domain::{Job, JobPath, JobStatus, JobType};
use chrono::{Duration, Utc};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn create_temp_dir(label: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hardata-job-status-{label}-{unique}"));
    fs::create_dir_all(&path).unwrap();
    path
}

async fn open_raw_pool(db_path: &str) -> SqlitePool {
    SqlitePool::connect(db_path).await.unwrap()
}

include!("part_01.rs");
include!("part_02.rs");
