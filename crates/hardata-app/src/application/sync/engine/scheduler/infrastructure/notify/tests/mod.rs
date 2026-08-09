use super::super::super::core::SyncScheduler;
use crate::adapters::outbound::persistence::db::Database;
use crate::application::sync::engine::scheduler::retry::ErrorCategory;
use crate::application::sync::engine::scheduler::SchedulerConfig;
use crate::domain::FileTransferState;
use crate::domain::{Job, JobPath, JobStatus, JobType};
use sqlx::{Row, SqlitePool};
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn create_temp_dir(label: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hardata-notify-{label}-{unique}"));
    fs::create_dir_all(&path).unwrap();
    path
}

async fn create_scheduler(label: &str) -> (std::path::PathBuf, Arc<Database>, Arc<SyncScheduler>) {
    let temp_dir = create_temp_dir(label);
    let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
    let db = Arc::new(Database::new(&db_path).await.unwrap());
    let config = SchedulerConfig {
        data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
        chunk_cache_path: temp_dir.join("chunk-cache").to_string_lossy().to_string(),
        ..SchedulerConfig::default()
    };
    std::fs::create_dir_all(&config.data_dir).unwrap();
    let scheduler = SyncScheduler::new(config, db.clone()).await.unwrap();
    (temp_dir, db, scheduler)
}

async fn open_raw_pool(db_path: &str) -> SqlitePool {
    SqlitePool::connect(db_path).await.unwrap()
}

fn cancelled_job(job_id: &str) -> Job {
    let mut job = Job::new(
        job_id.to_string(),
        JobPath {
            path: "/tmp/source.bin".to_string(),
            client_id: String::new(),
        },
        JobPath {
            path: "dest.bin".to_string(),
            client_id: String::new(),
        },
    )
    .with_job_type(JobType::Sync);
    job.status = JobStatus::Cancelled;
    job
}

include!("part_01.rs");
include!("part_02.rs");
include!("part_03.rs");
