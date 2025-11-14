use crate::core::{FileTransferState, JobType};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
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

    pub fn start_new_round(&mut self) {
        self.round_id += 1;
        self.is_first_round = false;
    }

    pub fn start_final_round(&mut self) {
        self.round_id += 1;
        self.is_first_round = false;
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
        state: FileTransferState,
    },
    Delete {
        job_id: String,
        file_path: String,
    },
    ClearJob {
        job_id: String,
    },
}

pub struct TransferManagerPool {
    db: Arc<crate::sync::storage::db::Database>,
    states: Arc<DashMap<String, DashMap<String, FileTransferState>>>,
    state_tx: mpsc::UnboundedSender<StateOperation>,
}

impl TransferManagerPool {
    pub fn new(db: Arc<crate::sync::storage::db::Database>) -> Self {
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let db_clone = db.clone();

        tokio::spawn(Self::state_writer_loop(db_clone, state_rx));

        Self {
            db,
            states: Arc::new(DashMap::new()),
            state_tx,
        }
    }

    async fn state_writer_loop(
        db: Arc<crate::sync::storage::db::Database>,
        mut rx: mpsc::UnboundedReceiver<StateOperation>,
    ) {
        info!("Transfer state writer started");

        while let Some(op) = rx.recv().await {
            match op {
                StateOperation::Save { job_id, state } => {
                    if let Err(e) = db.save_transfer_state(&job_id, &state).await {
                        warn!("Async save transfer state failed: {}", e);
                    }
                }
                StateOperation::Delete { job_id, file_path } => {
                    if let Err(e) = db.delete_transfer_state(&job_id, &file_path).await {
                        warn!("Async delete transfer state failed: {}", e);
                    }
                }
                StateOperation::ClearJob { job_id } => {
                    if let Err(e) = db.delete_job_transfer_states(&job_id).await {
                        warn!("Async clear job states failed: {}", e);
                    }
                }
            }
        }

        info!("Transfer state writer stopped");
    }

    pub async fn load_state(&self, job_id: &str, file_path: &str) -> Option<FileTransferState> {
        if let Some(job_states) = self.states.get(job_id) {
            if let Some(state) = job_states.get(file_path) {
                return Some(state.clone());
            }
        }

        match self.db.load_transfer_state(job_id, file_path).await {
            Ok(Some(state)) => {
                info!(
                    "Loaded transfer state for job {} file {}: {}/{} chunks completed",
                    job_id,
                    file_path,
                    state.completed_chunks.len(),
                    state.total_chunks
                );

                self.states
                    .entry(job_id.to_string())
                    .or_default()
                    .insert(file_path.to_string(), state.clone());

                Some(state)
            }
            Ok(None) => None,
            Err(e) => {
                warn!("Failed to load transfer state: {}", e);
                None
            }
        }
    }

    pub async fn save_state(
        &self,
        job_id: &str,
        state: &FileTransferState,
    ) -> crate::util::error::Result<()> {
        self.states
            .entry(job_id.to_string())
            .or_default()
            .insert(state.file_path.clone(), state.clone());

        let _ = self.state_tx.send(StateOperation::Save {
            job_id: job_id.to_string(),
            state: state.clone(),
        });

        Ok(())
    }

    pub async fn delete_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> crate::util::error::Result<()> {
        if let Some(job_states) = self.states.get(job_id) {
            job_states.remove(file_path);
        }

        let _ = self.state_tx.send(StateOperation::Delete {
            job_id: job_id.to_string(),
            file_path: file_path.to_string(),
        });

        info!(
            "Deleted transfer state for job {} file {}",
            job_id, file_path
        );
        Ok(())
    }

    pub async fn clear_job_states(&self, job_id: &str) -> crate::util::error::Result<()> {
        self.states.remove(job_id);

        let _ = self.state_tx.send(StateOperation::ClearJob {
            job_id: job_id.to_string(),
        });

        info!("Cleared all transfer states for job {}", job_id);
        Ok(())
    }
}
