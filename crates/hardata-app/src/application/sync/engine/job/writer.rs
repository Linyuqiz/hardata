impl TransferManagerPool {
    pub fn new(db: Arc<dyn TransferStateStore>) -> Self {
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
        db: Arc<dyn TransferStateStore>,
        state_generations: Arc<DashMap<String, u64>>,
        tmp_path_generations: Arc<DashMap<String, u64>>,
        mut rx: mpsc::Receiver<StateOperation>,
    ) {
        while let Some(op) = rx.recv().await {
            match op {
                StateOperation::Save {
                    job_id,
                    generation,
                    state,
                    response,
                } => {
                    let result = if is_stale_generation(&state_generations, &job_id, generation) {
                        info!(operation = "job.transfer_state_save_ignored", job_id = %job_id, generation, reason = "stale_generation", "stale transfer state save ignored");
                        Ok(())
                    } else {
                        db.save_transfer_state(&job_id, &state).await
                    };
                    if let Err(ref e) = result {
                        warn!(operation = "job.transfer_state_save_failed", job_id = %job_id, error = %e, "transfer state save failed");
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
                        info!(operation = "job.transfer_state_delete_ignored", job_id = %job_id, generation, reason = "stale_generation", "stale transfer state delete ignored");
                        Ok(())
                    } else {
                        db.delete_transfer_state(&job_id, &file_path).await
                    };
                    if let Err(ref e) = result {
                        warn!(operation = "job.transfer_state_delete_failed", job_id = %job_id, path = %file_path, error = %e, "transfer state delete failed");
                    }
                    let _ = response.send(result);
                }
                StateOperation::ClearJob {
                    job_id,
                    generation,
                    response,
                } => {
                    let result = if is_stale_generation(&state_generations, &job_id, generation) {
                        info!(operation = "job.transfer_state_clear_ignored", job_id = %job_id, generation, reason = "stale_generation", "stale transfer state clear ignored");
                        Ok(())
                    } else {
                        db.delete_job_transfer_states(&job_id).await
                    };
                    if let Err(ref e) = result {
                        warn!(operation = "job.transfer_state_clear_failed", job_id = %job_id, error = %e, "transfer state clear failed");
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
                        info!(operation = "job.tmp_path_save_ignored", job_id = %job_id, generation, reason = "stale_generation", "stale temporary path save ignored");
                        Ok(())
                    } else {
                        db.save_tmp_transfer_path(&job_id, &path).await
                    };
                    if let Err(ref e) = result {
                        warn!(operation = "job.tmp_path_save_failed", job_id = %job_id, path = %path, error = %e, "temporary transfer path save failed");
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
                        info!(operation = "job.tmp_path_delete_ignored", job_id = %job_id, generation, reason = "stale_generation", "stale temporary path delete ignored");
                        Ok(())
                    } else {
                        db.delete_tmp_transfer_path(&job_id, &path).await
                    };
                    if let Err(ref e) = result {
                        warn!(operation = "job.tmp_path_delete_failed", job_id = %job_id, path = %path, error = %e, "temporary transfer path delete failed");
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
                        info!(operation = "job.tmp_path_clear_ignored", job_id = %job_id, generation, reason = "stale_generation", "stale temporary path clear ignored");
                        Ok(())
                    } else {
                        db.delete_job_tmp_transfer_paths(&job_id).await
                    };
                    if let Err(ref e) = result {
                        warn!(operation = "job.tmp_path_clear_failed", job_id = %job_id, error = %e, "temporary transfer path clear failed");
                    }
                    let _ = response.send(result);
                }
            }
        }
    }

    async fn enqueue_state_operation<F>(&self, build: F) -> crate::shared::error::Result<()>
    where
        F: FnOnce(oneshot::Sender<crate::shared::error::Result<()>>) -> StateOperation,
    {
        let tx = {
            let guard = self.state_tx.lock().await;
            guard.clone()
        };

        let (response_tx, response_rx) = oneshot::channel();
        let op = build(response_tx);

        let Some(tx) = tx else {
            return Err(crate::shared::error::HarDataError::ConnectionError(
                "Transfer state writer already shut down".to_string(),
            ));
        };

        tx.send(op).await.map_err(|_| {
            crate::shared::error::HarDataError::ConnectionError(
                "Transfer state writer channel closed".to_string(),
            )
        })?;

        response_rx.await.map_err(|_| {
            crate::shared::error::HarDataError::ConnectionError(
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

}
