use crate::core::FileTransferState;
use crate::util::error::Result;
use sqlx::Row;

use super::types::Database;

impl Database {
    pub async fn save_transfer_state(&self, job_id: &str, state: &FileTransferState) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let completed_chunks = serde_json::to_string(&state.completed_chunks)?;

        sqlx::query(
            r#"
            INSERT INTO transfer_states (job_id, file_path, total_chunks, completed_chunks, progress, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(job_id, file_path) DO UPDATE SET
                total_chunks = ?3,
                completed_chunks = ?4,
                progress = ?5,
                updated_at = ?7
            "#,
        )
        .bind(job_id)
        .bind(&state.file_path)
        .bind(state.total_chunks as i64)
        .bind(&completed_chunks)
        .bind(state.progress as i64)
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
            SELECT file_path, total_chunks, completed_chunks, progress
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

            let state = FileTransferState {
                file_path: row.try_get("file_path")?,
                total_chunks: total_chunks as usize,
                completed_chunks,
                progress: progress as u8,
            };

            Ok(Some(state))
        } else {
            Ok(None)
        }
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
