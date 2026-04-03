use crate::util::error::Result;
use sqlx::Row;

use super::types::Database;

impl Database {
    pub async fn save_tmp_transfer_path(&self, job_id: &str, path: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO tmp_transfer_paths (job_id, path, updated_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(job_id, path) DO UPDATE SET
                updated_at = excluded.updated_at
            "#,
        )
        .bind(job_id)
        .bind(path)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn delete_tmp_transfer_path(&self, job_id: &str, path: &str) -> Result<()> {
        sqlx::query("DELETE FROM tmp_transfer_paths WHERE job_id = ?1 AND path = ?2")
            .bind(job_id)
            .bind(path)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete_job_tmp_transfer_paths(&self, job_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM tmp_transfer_paths WHERE job_id = ?1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn load_tmp_transfer_paths(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, path
            FROM tmp_transfer_paths
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.get("job_id"), row.get("path")))
            .collect())
    }

    pub async fn load_tmp_transfer_paths_by_job(&self, job_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT path
            FROM tmp_transfer_paths
            WHERE job_id = ?1
            ORDER BY updated_at DESC
            "#,
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|row| row.get("path")).collect())
    }
}
