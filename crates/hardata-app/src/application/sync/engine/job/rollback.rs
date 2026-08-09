impl TransferManagerPool {
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
