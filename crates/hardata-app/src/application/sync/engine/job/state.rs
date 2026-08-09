impl TransferManagerPool {
    pub async fn load_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> crate::shared::error::Result<Option<FileTransferState>> {
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
                    info!(operation = "job.transfer_checkpoint_ignored", job_id = %job_id, path = %file_path, reason = "already_completed", "completed transfer checkpoint ignored");
                    return Ok(None);
                }

                info!(operation = "job.transfer_state_loaded", job_id = %job_id, path = %file_path, completed_chunks = state.completed_chunks.len(), total_chunks = state.total_chunks, "transfer state loaded");

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
    ) -> crate::shared::error::Result<()> {
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
    ) -> crate::shared::error::Result<()> {
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

        info!(operation = "job.transfer_state_deleted", job_id = %job_id, path = %file_path, "transfer state deleted");
        Ok(())
    }

    pub async fn checkpoint_state(
        &self,
        job_id: &str,
        state: &FileTransferState,
    ) -> crate::shared::error::Result<()> {
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

        info!(operation = "job.transfer_checkpoint_saved", job_id = %job_id, path = %state.file_path, "completed transfer checkpoint saved");
        Ok(())
    }

    pub async fn clear_job_states(&self, job_id: &str) -> crate::shared::error::Result<()> {
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

        info!(operation = "job.transfer_state_cleared", job_id = %job_id, "all transfer states cleared");
        Ok(())
    }

    pub async fn register_tmp_write_path(
        &self,
        job_id: &str,
        write_path: &str,
    ) -> crate::shared::error::Result<()> {
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
    ) -> crate::shared::error::Result<()> {
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

    pub async fn clear_job_tmp_write_paths(
        &self,
        job_id: &str,
    ) -> crate::shared::error::Result<()> {
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
                warn!(
                    operation = "job.transfer_state_writer_join_failed",
                    error = %e,
                    "transfer state writer shutdown failed"
                );
            }
        }
    }
}
