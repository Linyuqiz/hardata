use super::{
    calculate_dest_path, finalize_file, load_transfer_state_for_current_source,
    prepare_regular_file_destination, resolve_base_dest_path, resolve_dedup_source_path,
    should_cleanup_tmp_after_transfer_error, sync_single_file, sync_single_file_with_mode,
};
use crate::adapters::outbound::persistence::db::Database;
use crate::adapters::outbound::transport::gateway::TransportConnection;
use crate::adapters::outbound::transport::tcp::TcpClient;
use crate::application::sync::engine::job::{SyncJob, TransferManagerPool};
use crate::application::sync::engine::scheduler::{
    JobRuntimeStatus, ReplicateMode, SchedulerConfig,
};
use crate::application::sync::scanner::ScannedFile;
use crate::domain::{FileTransferState, JobStatus, JobType};
use crate::shared::error::HarDataError;
use crate::shared::file_ops;
use crate::shared::time::{metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos};
use dashmap::DashMap;
use hardata_infra_agent::agent_server::tcp::TcpServer;
use hardata_infra_agent::compute::ComputeService;
use sqlx::SqlitePool;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

include!("part_01.rs");
include!("part_02.rs");
include!("part_03.rs");
