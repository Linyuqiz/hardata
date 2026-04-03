use crate::core::FileTransferState;
use crate::util::error::{HarDataError, Result};
use sqlx::Row;

use super::types::Database;

impl Database {
    pub async fn save_transfer_state(&self, job_id: &str, state: &FileTransferState) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let completed_chunks = serde_json::to_string(&state.completed_chunks)?;

        sqlx::query(
            r#"
            INSERT INTO transfer_states (
                job_id,
                file_path,
                total_chunks,
                completed_chunks,
                progress,
                source_size,
                source_modified,
                source_change_time,
                source_inode,
                dest_size,
                dest_modified,
                dest_change_time,
                dest_inode,
                cache_only,
                created_at,
                updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(job_id, file_path) DO UPDATE SET
                total_chunks = ?3,
                completed_chunks = ?4,
                progress = ?5,
                source_size = ?6,
                source_modified = ?7,
                source_change_time = ?8,
                source_inode = ?9,
                dest_size = ?10,
                dest_modified = ?11,
                dest_change_time = ?12,
                dest_inode = ?13,
                cache_only = ?14,
                updated_at = ?16
            "#,
        )
        .bind(job_id)
        .bind(&state.file_path)
        .bind(state.total_chunks as i64)
        .bind(&completed_chunks)
        .bind(state.progress as i64)
        .bind(state.source_size.map(|size| size as i64))
        .bind(state.source_modified)
        .bind(state.source_change_time)
        .bind(state.source_inode.map(|inode| inode as i64))
        .bind(state.dest_size.map(|size| size as i64))
        .bind(state.dest_modified)
        .bind(state.dest_change_time)
        .bind(state.dest_inode.map(|inode| inode as i64))
        .bind(if state.cache_only { 1_i64 } else { 0_i64 })
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn load_transfer_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> Result<Option<FileTransferState>> {
        let row = sqlx::query(
            r#"
            SELECT
                file_path,
                total_chunks,
                completed_chunks,
                progress,
                source_size,
                source_modified,
                source_change_time,
                source_inode,
                dest_size,
                dest_modified,
                dest_change_time,
                dest_inode,
                cache_only
            FROM transfer_states
            WHERE job_id = ?1 AND file_path = ?2
            "#,
        )
        .bind(job_id)
        .bind(file_path)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let total_chunks: i64 = row.try_get("total_chunks")?;
            let completed_chunks_str: String = row.try_get("completed_chunks")?;
            let completed_chunks = serde_json::from_str(&completed_chunks_str)?;
            let progress: i64 = row.try_get("progress")?;
            let source_size = match row.try_get::<Option<i64>, _>("source_size")? {
                Some(size) if size >= 0 => Some(size as u64),
                Some(size) => {
                    return Err(HarDataError::SerializationError(format!(
                        "Invalid source_size for transfer state '{}': {}",
                        file_path, size
                    )));
                }
                None => None,
            };
            let source_modified: Option<i64> = row.try_get("source_modified")?;
            let source_change_time: Option<i64> = row.try_get("source_change_time")?;
            let source_inode = match row.try_get::<Option<i64>, _>("source_inode")? {
                Some(inode) if inode >= 0 => Some(inode as u64),
                Some(inode) => {
                    return Err(HarDataError::SerializationError(format!(
                        "Invalid source_inode for transfer state '{}': {}",
                        file_path, inode
                    )));
                }
                None => None,
            };
            let dest_size = match row.try_get::<Option<i64>, _>("dest_size")? {
                Some(size) if size >= 0 => Some(size as u64),
                Some(size) => {
                    return Err(HarDataError::SerializationError(format!(
                        "Invalid dest_size for transfer state '{}': {}",
                        file_path, size
                    )));
                }
                None => None,
            };
            let dest_modified: Option<i64> = row.try_get("dest_modified")?;
            let dest_change_time: Option<i64> = row.try_get("dest_change_time")?;
            let dest_inode = match row.try_get::<Option<i64>, _>("dest_inode")? {
                Some(inode) if inode >= 0 => Some(inode as u64),
                Some(inode) => {
                    return Err(HarDataError::SerializationError(format!(
                        "Invalid dest_inode for transfer state '{}': {}",
                        file_path, inode
                    )));
                }
                None => None,
            };
            let cache_only = row.try_get::<i64, _>("cache_only")? != 0;

            let state = FileTransferState {
                file_path: row.try_get("file_path")?,
                total_chunks: total_chunks as usize,
                completed_chunks,
                progress: progress as u8,
                source_size,
                source_modified,
                source_change_time,
                source_inode,
                dest_size,
                dest_modified,
                dest_change_time,
                dest_inode,
                cache_only,
            };

            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    pub async fn load_completed_transfer_source_versions(
        &self,
        job_id: &str,
    ) -> Result<
        Vec<(
            String,
            u64,
            i64,
            Option<i64>,
            Option<u64>,
            Option<i64>,
            Option<i64>,
            Option<u64>,
        )>,
    > {
        let rows = sqlx::query(
            r#"
            SELECT file_path, source_size, source_modified, source_change_time, source_inode, dest_modified, dest_change_time, dest_inode
            FROM transfer_states
            WHERE job_id = ?1
              AND (cache_only = 1 OR progress = 100)
              AND source_size IS NOT NULL
              AND source_modified IS NOT NULL
            "#,
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let file_path: String = row.try_get("file_path")?;
                let source_size = row.try_get::<i64, _>("source_size")?;
                let source_modified = row.try_get::<i64, _>("source_modified")?;
                if source_size < 0 {
                    return Err(HarDataError::SerializationError(format!(
                        "Invalid source_size for transfer state '{}': {}",
                        file_path, source_size
                    )));
                }
                let dest_modified: Option<i64> = row.try_get("dest_modified")?;
                let source_change_time: Option<i64> = row.try_get("source_change_time")?;
                let source_inode = match row.try_get::<Option<i64>, _>("source_inode")? {
                    Some(inode) if inode >= 0 => Some(inode as u64),
                    Some(inode) => {
                        return Err(HarDataError::SerializationError(format!(
                            "Invalid source_inode for transfer state '{}': {}",
                            file_path, inode
                        )));
                    }
                    None => None,
                };
                let dest_change_time: Option<i64> = row.try_get("dest_change_time")?;
                let dest_inode = match row.try_get::<Option<i64>, _>("dest_inode")? {
                    Some(inode) if inode >= 0 => Some(inode as u64),
                    Some(inode) => {
                        return Err(HarDataError::SerializationError(format!(
                            "Invalid dest_inode for transfer state '{}': {}",
                            file_path, inode
                        )));
                    }
                    None => None,
                };
                Ok((
                    file_path,
                    source_size as u64,
                    source_modified,
                    source_change_time,
                    source_inode,
                    dest_modified,
                    dest_change_time,
                    dest_inode,
                ))
            })
            .collect()
    }

    pub async fn delete_transfer_state(&self, job_id: &str, file_path: &str) -> Result<()> {
        sqlx::query("DELETE FROM transfer_states WHERE job_id = ?1 AND file_path = ?2")
            .bind(job_id)
            .bind(file_path)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete_job_transfer_states(&self, job_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM transfer_states WHERE job_id = ?1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
