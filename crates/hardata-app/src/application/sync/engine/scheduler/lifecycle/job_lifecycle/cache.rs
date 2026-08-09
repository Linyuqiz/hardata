impl SyncScheduler {
    fn snapshot_synced_file_cache(
        &self,
        job_id: &str,
    ) -> Vec<(String, super::super::core::FileSyncState)> {
        self.synced_files_cache
            .get(job_id)
            .map(|cache| {
                cache
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn restore_synced_file_cache(
        &self,
        job_id: &str,
        snapshot: &[(String, super::super::core::FileSyncState)],
    ) {
        if snapshot.is_empty() {
            self.synced_files_cache.remove(job_id);
            return;
        }

        let job_cache = DashMap::new();
        for (path, state) in snapshot {
            job_cache.insert(path.clone(), state.clone());
        }
        self.synced_files_cache
            .insert(job_id.to_string(), job_cache);
    }

    pub(in crate::application::sync::engine::scheduler) async fn restore_synced_file_cache_from_transfer_states(
        &self,
        job_id: &str,
    ) -> Result<()> {
        let versions = self
            .db
            .load_completed_transfer_source_versions(job_id)
            .await?;

        if versions.is_empty() {
            self.synced_files_cache.remove(job_id);
            return Ok(());
        }

        let restored_count = versions.len();
        let restored_at = chrono::Utc::now().timestamp();
        let job_cache = DashMap::new();
        for (path, size, mtime, change_time, inode, dest_mtime, dest_change_time, dest_inode) in
            versions
        {
            job_cache.insert(
                path,
                super::super::core::FileSyncState {
                    size,
                    mtime,
                    change_time,
                    inode,
                    dest_mtime,
                    dest_change_time,
                    dest_inode,
                    updated_at: restored_at,
                },
            );
        }
        self.synced_files_cache
            .insert(job_id.to_string(), job_cache);
        info!(
            operation = "job.synced_cache_restored",
            job_id = %job_id,
            restored_entries = restored_count,
            "synced file cache restored"
        );
        Ok(())
    }

    async fn ensure_destination_available(&self, job_id: &str, dest: &str) -> Result<()> {
        let requested_dest = self.config.resolve_runtime_destination_path(dest)?;

        for (active_job_id, active_dest) in self.db.load_active_job_destinations().await? {
            if active_job_id == job_id {
                continue;
            }
            if is_final_job_pair(job_id, &active_job_id) {
                continue;
            }

            let resolved_active_dest = match self
                .config
                .resolve_runtime_destination_path(&active_dest)
            {
                Ok(path) => path,
                Err(e) => {
                    warn!(
                        operation = "job.destination_conflict_check_skipped",
                        job_id = %job_id,
                        active_job_id = %active_job_id,
                        path = %active_dest,
                        error = %e,
                        "destination conflict check skipped"
                    );
                    continue;
                }
            };

            if destinations_overlap(&requested_dest, &resolved_active_dest) {
                return Err(HarDataError::InvalidConfig(format!(
                    "destination '{}' overlaps active job {} destination '{}'",
                    requested_dest.display(),
                    active_job_id,
                    resolved_active_dest.display()
                )));
            }
        }

        Ok(())
    }

}
