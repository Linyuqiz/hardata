use crate::core::{FileTransferState, JobType};
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
        response: oneshot::Sender<crate::util::error::Result<()>>,
    },
    Delete {
        job_id: String,
        generation: u64,
        file_path: String,
        response: oneshot::Sender<crate::util::error::Result<()>>,
    },
    ClearJob {
        job_id: String,
        generation: u64,
        response: oneshot::Sender<crate::util::error::Result<()>>,
    },
    TmpSave {
        job_id: String,
        generation: u64,
        path: String,
        response: oneshot::Sender<crate::util::error::Result<()>>,
    },
    TmpDelete {
        job_id: String,
        generation: u64,
        path: String,
        response: oneshot::Sender<crate::util::error::Result<()>>,
    },
    TmpClearJob {
        job_id: String,
        generation: u64,
        response: oneshot::Sender<crate::util::error::Result<()>>,
    },
}

pub struct TransferManagerPool {
    db: Arc<crate::sync::storage::db::Database>,
    states: Arc<DashMap<String, DashMap<String, FileTransferState>>>,
    tmp_write_paths: Arc<DashMap<String, String>>,
    state_generations: Arc<DashMap<String, u64>>,
    tmp_path_generations: Arc<DashMap<String, u64>>,
    state_tx: Arc<Mutex<Option<mpsc::Sender<StateOperation>>>>,
    writer_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl TransferManagerPool {
    pub fn new(db: Arc<crate::sync::storage::db::Database>) -> Self {
        let (state_tx, state_rx) = mpsc::channel(1024);
        let db_clone = db.clone();
        let state_generations = Arc::new(DashMap::new());
        let tmp_path_generations = Arc::new(DashMap::new());
        let writer_task = tokio::spawn(Self::state_writer_loop(
            db_clone,
            state_generations.clone(),
            tmp_path_generations.clone(),
            state_rx,
        ));

        Self {
            db,
            states: Arc::new(DashMap::new()),
            tmp_write_paths: Arc::new(DashMap::new()),
            state_generations,
            tmp_path_generations,
            state_tx: Arc::new(Mutex::new(Some(state_tx))),
            writer_task: Arc::new(Mutex::new(Some(writer_task))),
        }
    }

    async fn state_writer_loop(
        db: Arc<crate::sync::storage::db::Database>,
        state_generations: Arc<DashMap<String, u64>>,
        tmp_path_generations: Arc<DashMap<String, u64>>,
        mut rx: mpsc::Receiver<StateOperation>,
    ) {
        info!("Transfer state writer started");

        while let Some(op) = rx.recv().await {
            match op {
                StateOperation::Save {
                    job_id,
                    generation,
                    state,
                    response,
                } => {
                    let result = if is_stale_generation(&state_generations, &job_id, generation) {
                        info!(
                            "Ignoring stale transfer state save for job {} generation {}",
                            job_id, generation
                        );
                        Ok(())
                    } else {
                        db.save_transfer_state(&job_id, &state).await
                    };
                    if let Err(ref e) = result {
                        warn!("Async save transfer state failed: {}", e);
                    }
                    let _ = response.send(result);
                }
                StateOperation::Delete {
                    job_id,
                    generation,
                    file_path,
                    response,
                } => {
                    let result = if is_stale_generation(&state_generations, &job_id, generation) {
                        info!(
                            "Ignoring stale transfer state delete for job {} generation {}",
                            job_id, generation
                        );
                        Ok(())
                    } else {
                        db.delete_transfer_state(&job_id, &file_path).await
                    };
                    if let Err(ref e) = result {
                        warn!("Async delete transfer state failed: {}", e);
                    }
                    let _ = response.send(result);
                }
                StateOperation::ClearJob {
                    job_id,
                    generation,
                    response,
                } => {
                    let result = if is_stale_generation(&state_generations, &job_id, generation) {
                        info!(
                            "Ignoring stale transfer state clear for job {} generation {}",
                            job_id, generation
                        );
                        Ok(())
                    } else {
                        db.delete_job_transfer_states(&job_id).await
                    };
                    if let Err(ref e) = result {
                        warn!("Async clear job states failed: {}", e);
                    }
                    let _ = response.send(result);
                }
                StateOperation::TmpSave {
                    job_id,
                    generation,
                    path,
                    response,
                } => {
                    let result = if is_stale_generation(&tmp_path_generations, &job_id, generation)
                    {
                        info!(
                            "Ignoring stale tmp path save for job {} generation {}",
                            job_id, generation
                        );
                        Ok(())
                    } else {
                        db.save_tmp_transfer_path(&job_id, &path).await
                    };
                    if let Err(ref e) = result {
                        warn!("Async save tmp transfer path failed: {}", e);
                    }
                    let _ = response.send(result);
                }
                StateOperation::TmpDelete {
                    job_id,
                    generation,
                    path,
                    response,
                } => {
                    let result = if is_stale_generation(&tmp_path_generations, &job_id, generation)
                    {
                        info!(
                            "Ignoring stale tmp path delete for job {} generation {}",
                            job_id, generation
                        );
                        Ok(())
                    } else {
                        db.delete_tmp_transfer_path(&job_id, &path).await
                    };
                    if let Err(ref e) = result {
                        warn!("Async delete tmp transfer path failed: {}", e);
                    }
                    let _ = response.send(result);
                }
                StateOperation::TmpClearJob {
                    job_id,
                    generation,
                    response,
                } => {
                    let result = if is_stale_generation(&tmp_path_generations, &job_id, generation)
                    {
                        info!(
                            "Ignoring stale tmp path clear for job {} generation {}",
                            job_id, generation
                        );
                        Ok(())
                    } else {
                        db.delete_job_tmp_transfer_paths(&job_id).await
                    };
                    if let Err(ref e) = result {
                        warn!("Async clear job tmp paths failed: {}", e);
                    }
                    let _ = response.send(result);
                }
            }
        }

        info!("Transfer state writer stopped");
    }

    async fn enqueue_state_operation<F>(&self, build: F) -> crate::util::error::Result<()>
    where
        F: FnOnce(oneshot::Sender<crate::util::error::Result<()>>) -> StateOperation,
    {
        let tx = {
            let guard = self.state_tx.lock().await;
            guard.clone()
        };

        let (response_tx, response_rx) = oneshot::channel();
        let op = build(response_tx);

        let Some(tx) = tx else {
            return Err(crate::util::error::HarDataError::ConnectionError(
                "Transfer state writer already shut down".to_string(),
            ));
        };

        tx.send(op).await.map_err(|_| {
            crate::util::error::HarDataError::ConnectionError(
                "Transfer state writer channel closed".to_string(),
            )
        })?;

        response_rx.await.map_err(|_| {
            crate::util::error::HarDataError::ConnectionError(
                "Transfer state writer response channel closed".to_string(),
            )
        })?
    }

    fn current_generation(&self, job_id: &str) -> u64 {
        self.state_generations
            .get(job_id)
            .map(|generation| *generation)
            .unwrap_or(0)
    }

    fn current_tmp_generation(&self, job_id: &str) -> u64 {
        self.tmp_path_generations
            .get(job_id)
            .map(|generation| *generation)
            .unwrap_or(0)
    }

    fn bump_generation(&self, job_id: &str) -> u64 {
        let mut generation = self
            .state_generations
            .entry(job_id.to_string())
            .or_insert(0);
        *generation += 1;
        *generation
    }

    fn bump_tmp_generation(&self, job_id: &str) -> u64 {
        let mut generation = self
            .tmp_path_generations
            .entry(job_id.to_string())
            .or_insert(0);
        *generation += 1;
        *generation
    }

    pub async fn load_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> crate::util::error::Result<Option<FileTransferState>> {
        let cached_state = self
            .states
            .get(job_id)
            .and_then(|job_states| job_states.get(file_path).map(|state| state.clone()));
        if let Some(state) = cached_state {
            if state.cache_only || state.is_completed() {
                if let Some(job_states) = self.states.get(job_id) {
                    job_states.remove(file_path);
                }
            } else {
                return Ok(Some(state));
            }
        }

        match self.db.load_transfer_state(job_id, file_path).await {
            Ok(Some(state)) => {
                if state.cache_only || state.is_completed() {
                    info!(
                        "Ignoring completed transfer checkpoint for job {} file {}",
                        job_id, file_path
                    );
                    return Ok(None);
                }

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

                Ok(Some(state))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn save_state(
        &self,
        job_id: &str,
        state: &FileTransferState,
    ) -> crate::util::error::Result<()> {
        let previous_state = self
            .states
            .entry(job_id.to_string())
            .or_default()
            .insert(state.file_path.clone(), state.clone());

        let generation = self.current_generation(job_id);
        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::Save {
                job_id: job_id.to_string(),
                generation,
                state: state.clone(),
                response,
            })
            .await
        {
            self.rollback_saved_state(job_id, &state.file_path, state, previous_state);
            return Err(e);
        }

        Ok(())
    }

    pub async fn delete_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> crate::util::error::Result<()> {
        let removed_state = self
            .states
            .get(job_id)
            .and_then(|job_states| job_states.remove(file_path).map(|(_, state)| state));

        let generation = self.current_generation(job_id);
        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::Delete {
                job_id: job_id.to_string(),
                generation,
                file_path: file_path.to_string(),
                response,
            })
            .await
        {
            self.rollback_deleted_state(job_id, file_path, removed_state);
            return Err(e);
        }

        info!(
            "Deleted transfer state for job {} file {}",
            job_id, file_path
        );
        Ok(())
    }

    pub async fn checkpoint_state(
        &self,
        job_id: &str,
        state: &FileTransferState,
    ) -> crate::util::error::Result<()> {
        let removed_state = self
            .states
            .get(job_id)
            .and_then(|job_states| job_states.remove(&state.file_path).map(|(_, state)| state));

        let generation = self.current_generation(job_id);
        let checkpoint = state.clone().mark_cache_only();
        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::Save {
                job_id: job_id.to_string(),
                generation,
                state: checkpoint.clone(),
                response,
            })
            .await
        {
            self.rollback_deleted_state(job_id, &state.file_path, removed_state);
            return Err(e);
        }

        info!(
            "Saved completed transfer checkpoint for job {} file {}",
            job_id, state.file_path
        );
        Ok(())
    }

    pub async fn clear_job_states(&self, job_id: &str) -> crate::util::error::Result<()> {
        let removed_states = self
            .states
            .remove(job_id)
            .map(|(_, states)| {
                states
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let generation = self.bump_generation(job_id);

        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::ClearJob {
                job_id: job_id.to_string(),
                generation,
                response,
            })
            .await
        {
            self.rollback_cleared_states(job_id, removed_states);
            return Err(e);
        }

        info!("Cleared all transfer states for job {}", job_id);
        Ok(())
    }

    pub async fn register_tmp_write_path(
        &self,
        job_id: &str,
        write_path: &str,
    ) -> crate::util::error::Result<()> {
        let previous_owner = self
            .tmp_write_paths
            .insert(write_path.to_string(), job_id.to_string());

        let generation = self.current_tmp_generation(job_id);
        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::TmpSave {
                job_id: job_id.to_string(),
                generation,
                path: write_path.to_string(),
                response,
            })
            .await
        {
            self.rollback_registered_tmp_write_path(job_id, write_path, previous_owner);
            return Err(e);
        }

        Ok(())
    }

    pub async fn unregister_tmp_write_path(
        &self,
        job_id: &str,
        write_path: &str,
    ) -> crate::util::error::Result<()> {
        let previous_owner = self
            .tmp_write_paths
            .remove(write_path)
            .map(|(_, owner)| owner);

        let generation = self.current_tmp_generation(job_id);
        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::TmpDelete {
                job_id: job_id.to_string(),
                generation,
                path: write_path.to_string(),
                response,
            })
            .await
        {
            self.rollback_unregistered_tmp_write_path(write_path, previous_owner);
            return Err(e);
        }

        Ok(())
    }

    pub async fn clear_job_tmp_write_paths(&self, job_id: &str) -> crate::util::error::Result<()> {
        let paths_to_remove: Vec<String> = self
            .tmp_write_paths
            .iter()
            .filter_map(|entry| {
                if entry.value().as_str() == job_id {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for path in &paths_to_remove {
            self.tmp_write_paths.remove(path);
        }

        let generation = self.bump_tmp_generation(job_id);
        if let Err(e) = self
            .enqueue_state_operation(|response| StateOperation::TmpClearJob {
                job_id: job_id.to_string(),
                generation,
                response,
            })
            .await
        {
            self.rollback_cleared_tmp_write_paths(job_id, &paths_to_remove);
            return Err(e);
        }

        Ok(())
    }

    pub fn job_tmp_write_paths(&self, job_id: &str) -> Vec<String> {
        self.tmp_write_paths
            .iter()
            .filter_map(|entry| {
                if entry.value().as_str() == job_id {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn tmp_write_paths(&self) -> Arc<DashMap<String, String>> {
        Arc::clone(&self.tmp_write_paths)
    }

    pub async fn shutdown(&self) {
        if let Some(sender) = self.state_tx.lock().await.take() {
            drop(sender);
        }

        if let Some(task) = self.writer_task.lock().await.take() {
            if let Err(e) = task.await {
                warn!("Transfer state writer join failed: {}", e);
            }
        }
    }

    fn rollback_saved_state(
        &self,
        job_id: &str,
        file_path: &str,
        expected_state: &FileTransferState,
        previous_state: Option<FileTransferState>,
    ) {
        let Some(job_states) = self.states.get(job_id) else {
            return;
        };
        let current_state = job_states.get(file_path).map(|state| state.clone());
        let is_empty_before = job_states.is_empty();
        drop(job_states);

        if current_state.as_ref() != Some(expected_state) {
            return;
        }

        if let Some(job_states) = self.states.get(job_id) {
            match previous_state {
                Some(previous_state) => {
                    job_states.insert(file_path.to_string(), previous_state);
                }
                None => {
                    job_states.remove(file_path);
                    if !is_empty_before && job_states.is_empty() {
                        drop(job_states);
                        self.states.remove(job_id);
                    }
                }
            }
        }
    }

    fn rollback_deleted_state(
        &self,
        job_id: &str,
        file_path: &str,
        removed_state: Option<FileTransferState>,
    ) {
        let Some(removed_state) = removed_state else {
            return;
        };

        if let Some(job_states) = self.states.get(job_id) {
            if job_states.contains_key(file_path) {
                return;
            }
            job_states.insert(file_path.to_string(), removed_state);
            return;
        }

        self.states
            .entry(job_id.to_string())
            .or_default()
            .insert(file_path.to_string(), removed_state);
    }

    fn rollback_cleared_states(
        &self,
        job_id: &str,
        removed_states: Vec<(String, FileTransferState)>,
    ) {
        if removed_states.is_empty() || self.states.contains_key(job_id) {
            return;
        }

        let restored = DashMap::new();
        for (file_path, state) in removed_states {
            restored.insert(file_path, state);
        }
        self.states.insert(job_id.to_string(), restored);
    }

    fn rollback_registered_tmp_write_path(
        &self,
        job_id: &str,
        write_path: &str,
        previous_owner: Option<String>,
    ) {
        let current_owner = self
            .tmp_write_paths
            .get(write_path)
            .map(|owner| owner.clone());
        if current_owner.as_deref() != Some(job_id) {
            return;
        }

        match previous_owner {
            Some(previous_owner) => {
                self.tmp_write_paths
                    .insert(write_path.to_string(), previous_owner);
            }
            None => {
                self.tmp_write_paths.remove(write_path);
            }
        }
    }

    fn rollback_unregistered_tmp_write_path(
        &self,
        write_path: &str,
        previous_owner: Option<String>,
    ) {
        let Some(previous_owner) = previous_owner else {
            return;
        };
        if self.tmp_write_paths.contains_key(write_path) {
            return;
        }
        self.tmp_write_paths
            .insert(write_path.to_string(), previous_owner);
    }

    fn rollback_cleared_tmp_write_paths(&self, job_id: &str, removed_paths: &[String]) {
        for path in removed_paths {
            if !self.tmp_write_paths.contains_key(path) {
                self.tmp_write_paths
                    .insert(path.clone(), job_id.to_string());
            }
        }
    }
}

fn is_stale_generation(
    state_generations: &DashMap<String, u64>,
    job_id: &str,
    generation: u64,
) -> bool {
    state_generations
        .get(job_id)
        .map(|current| generation < *current)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{StateOperation, TransferManagerPool};
    use crate::core::FileTransferState;
    use crate::sync::storage::db::Database;
    use sqlx::SqlitePool;
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-transfer-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn clear_job_states_ignores_stale_save_operations() {
        let temp_dir = create_temp_dir("stale-save");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let state = FileTransferState::new("nested/file.bin".to_string(), 8);
        let job_id = "job-stale";

        pool.enqueue_state_operation(|response| StateOperation::Save {
            job_id: job_id.to_string(),
            generation: 0,
            state: state.clone(),
            response,
        })
        .await
        .unwrap();
        pool.clear_job_states(job_id).await.unwrap();
        pool.enqueue_state_operation(|response| StateOperation::Save {
            job_id: job_id.to_string(),
            generation: 0,
            state,
            response,
        })
        .await
        .unwrap();

        pool.shutdown().await;

        let loaded = db
            .load_transfer_state(job_id, "nested/file.bin")
            .await
            .unwrap();
        assert!(loaded.is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn save_state_after_clear_uses_new_generation() {
        let temp_dir = create_temp_dir("new-generation");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let state = FileTransferState::new("nested/file.bin".to_string(), 8);
        let job_id = "job-fresh";

        pool.clear_job_states(job_id).await.unwrap();
        pool.save_state(job_id, &state).await.unwrap();
        pool.shutdown().await;

        let loaded = db
            .load_transfer_state(job_id, "nested/file.bin")
            .await
            .unwrap();
        assert!(loaded.is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_state_returns_error_when_db_lookup_fails() {
        let temp_dir = create_temp_dir("load-state-db-failure");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db);

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("DROP TABLE transfer_states")
            .execute(&raw_pool)
            .await
            .unwrap();

        let err = pool
            .load_state("job-load-failure", "nested/file.bin")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such table: transfer_states"));

        raw_pool.close().await;
        pool.shutdown().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn save_state_returns_error_and_reverts_cache_when_db_persist_fails() {
        let temp_dir = create_temp_dir("save-state-db-failure");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let job_id = "job-save-failure";
        let state = FileTransferState::new("nested/file.bin".to_string(), 8);

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER reject_transfer_state_insert
            BEFORE INSERT ON transfer_states
            WHEN NEW.job_id = 'job-save-failure'
            BEGIN
                SELECT RAISE(FAIL, 'reject transfer state insert');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let err = pool.save_state(job_id, &state).await.unwrap_err();
        assert!(err.to_string().contains("reject transfer state insert"));
        assert!(pool
            .load_state(job_id, "nested/file.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_transfer_state(job_id, "nested/file.bin")
            .await
            .unwrap()
            .is_none());

        raw_pool.close().await;
        pool.shutdown().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn clear_job_tmp_write_paths_ignores_stale_tmp_save_operations() {
        let temp_dir = create_temp_dir("stale-tmp-save");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let job_id = "job-tmp-stale";
        let tmp_path = temp_dir.join("stale.tmp");
        let tmp_path = tmp_path.to_string_lossy().to_string();

        pool.enqueue_state_operation(|response| StateOperation::TmpSave {
            job_id: job_id.to_string(),
            generation: 0,
            path: tmp_path.clone(),
            response,
        })
        .await
        .unwrap();
        pool.clear_job_tmp_write_paths(job_id).await.unwrap();
        pool.enqueue_state_operation(|response| StateOperation::TmpSave {
            job_id: job_id.to_string(),
            generation: 0,
            path: tmp_path.clone(),
            response,
        })
        .await
        .unwrap();

        pool.shutdown().await;

        let loaded = db.load_tmp_transfer_paths_by_job(job_id).await.unwrap();
        assert!(loaded.is_empty());

        let _ = fs::remove_dir_all(temp_dir);
    }
}
