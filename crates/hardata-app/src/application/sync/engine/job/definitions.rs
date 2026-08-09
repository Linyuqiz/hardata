use crate::application::ports::TransferStateStore;
use crate::domain::{FileTransferState, JobType};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

#[derive(Clone)]
pub struct SyncJob {
    pub job_id: String,
    pub source: PathBuf,
    pub dest: String,
    pub region: String,
    pub job_type: JobType,
    pub priority: i32,
    pub round_id: i64,
    pub is_first_round: bool,
    pub is_last_round: bool,
    pub scan_interval: std::time::Duration,
    pub exclude_regex: Vec<String>,
    pub include_regex: Vec<String>,
}

impl SyncJob {
    pub fn new(job_id: String, source: PathBuf, dest: String, region: String) -> Self {
        Self {
            job_id,
            source,
            dest,
            region,
            job_type: JobType::Once,
            priority: 0,
            round_id: 0,
            is_first_round: true,
            is_last_round: false,
            scan_interval: std::time::Duration::from_secs(10),
            exclude_regex: Vec::new(),
            include_regex: Vec::new(),
        }
    }

    pub fn with_job_type(mut self, job_type: JobType) -> Self {
        self.job_type = job_type;
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_filters(mut self, exclude_regex: Vec<String>, include_regex: Vec<String>) -> Self {
        self.exclude_regex = exclude_regex;
        self.include_regex = include_regex;
        self
    }

    pub fn start_new_round(&mut self) {
        self.round_id += 1;
        self.is_first_round = false;
    }

    pub fn restore_round_state(&mut self, round_id: i64, is_last_round: bool) {
        self.round_id = round_id;
        self.is_last_round = is_last_round;
        self.is_first_round = round_id == 0 && !is_last_round;
    }

    pub fn mark_resumed_round(&mut self) {
        self.is_first_round = false;
        if self.round_id == 0 {
            self.round_id = 1;
        }
    }

    pub fn start_final_round(&mut self) {
        self.round_id += 1;
        self.is_first_round = false;
        self.is_last_round = true;
    }

    pub fn ensure_final_round_state(&mut self) {
        self.is_first_round = false;
        if self.round_id == 0 {
            self.round_id = 1;
        }
        self.is_last_round = true;
    }

    pub fn try_start_new_round(&mut self, elapsed: std::time::Duration) -> bool {
        if self.is_first_round {
            self.is_first_round = false;
            self.start_new_round();
            return true;
        }

        if self.job_type.is_sync() && !self.is_last_round && elapsed >= self.scan_interval {
            self.start_new_round();
            return true;
        }

        false
    }

    pub fn is_final_transfer(&self) -> bool {
        self.job_type == JobType::Once || self.job_type == JobType::Full || self.is_last_round
    }

    pub fn is_completed(&self) -> bool {
        match self.job_type {
            JobType::Once | JobType::Full => true,
            JobType::Sync => self.is_last_round,
        }
    }
}

enum StateOperation {
    Save {
        job_id: String,
        generation: u64,
        state: FileTransferState,
        response: oneshot::Sender<crate::shared::error::Result<()>>,
    },
    Delete {
        job_id: String,
        generation: u64,
        file_path: String,
        response: oneshot::Sender<crate::shared::error::Result<()>>,
    },
    ClearJob {
        job_id: String,
        generation: u64,
        response: oneshot::Sender<crate::shared::error::Result<()>>,
    },
    TmpSave {
        job_id: String,
        generation: u64,
        path: String,
        response: oneshot::Sender<crate::shared::error::Result<()>>,
    },
    TmpDelete {
        job_id: String,
        generation: u64,
        path: String,
        response: oneshot::Sender<crate::shared::error::Result<()>>,
    },
    TmpClearJob {
        job_id: String,
        generation: u64,
        response: oneshot::Sender<crate::shared::error::Result<()>>,
    },
}

pub struct TransferManagerPool {
    db: Arc<dyn TransferStateStore>,
    states: Arc<DashMap<String, DashMap<String, FileTransferState>>>,
    tmp_write_paths: Arc<DashMap<String, String>>,
    state_generations: Arc<DashMap<String, u64>>,
    tmp_path_generations: Arc<DashMap<String, u64>>,
    state_tx: Arc<Mutex<Option<mpsc::Sender<StateOperation>>>>,
    writer_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}
