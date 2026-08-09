use super::super::super::core::SyncScheduler;
use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::scheduler::SchedulerConfig;
use crate::domain::{FileTransferState, Job, JobPath, JobStatus, JobType};
use chrono::{Duration, Utc};
use sqlx::sqlite::SqlitePool;
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn create_temp_dir(label: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hardata-retry-scheduler-{label}-{unique}"));
    fs::create_dir_all(&path).unwrap();
    path
}

include!("part_01.rs");
include!("part_02.rs");
