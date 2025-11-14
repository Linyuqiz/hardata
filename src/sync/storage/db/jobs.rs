use crate::core::{Job, JobConfig, JobPath, JobStatus, JobType};
use crate::util::error::{HarDataError, Result};
use sqlx::Row;
use tracing::info;

use super::types::Database;

impl Database {
    pub async fn save_job(&self, job: &Job) -> Result<()> {
        info!("Saving job to database: {}", job.job_id);

        let exclude_regex = serde_json::to_string(&job.exclude_regex)?;
        let include_regex = serde_json::to_string(&job.include_regex)?;
        let options = serde_json::to_string(&job.options)?;

        sqlx::query(
            r#"
            INSERT INTO jobs (
                job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                status, progress, current_size, total_size, priority,
                exclude_regex, include_regex, job_type, options, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
            ON CONFLICT(job_id) DO UPDATE SET
                region = ?2,
                status = ?7,
                progress = ?8,
                current_size = ?9,
                total_size = ?10,
                priority = ?11,
                exclude_regex = ?12,
                include_regex = ?13,
                job_type = ?14,
                options = ?15,
                updated_at = ?17
            "#,
        )
        .bind(&job.job_id)
        .bind(&job.region)
        .bind(&job.source.path)
        .bind(&job.source.client_id)
        .bind(&job.dest.path)
        .bind(&job.dest.client_id)
        .bind(job.status.as_str())
        .bind(job.progress as i64)
        .bind(job.current_size as i64)
        .bind(job.total_size as i64)
        .bind(job.priority)
        .bind(exclude_regex)
        .bind(include_regex)
        .bind(job.job_type.as_str())
        .bind(options)
        .bind(job.created_at.to_rfc3339())
        .bind(job.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;

        info!("Job saved successfully: {}", job.job_id);
        Ok(())
    }

    pub async fn load_all_jobs(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority,
                   exclude_regex, include_regex, job_type, options, created_at, updated_at
            FROM jobs
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let jobs: Result<Vec<Job>> = rows.into_iter().map(|row| self.row_to_job(row)).collect();
        jobs
    }

    pub async fn update_job_status(
        &self,
        job_id: &str,
        status: JobStatus,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            UPDATE jobs SET status = ?2, progress = ?3, current_size = ?4, total_size = ?5, updated_at = ?6
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .bind(status.as_str())
        .bind(progress as i64)
        .bind(current_size as i64)
        .bind(total_size as i64)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_job_progress(
        &self,
        job_id: &str,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            UPDATE jobs SET progress = ?2, current_size = ?3, total_size = ?4, updated_at = ?5
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .bind(progress as i64)
        .bind(current_size as i64)
        .bind(total_size as i64)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn cleanup_old_jobs(&self, days: i64) -> Result<usize> {
        let cutoff_date = chrono::Utc::now() - chrono::Duration::days(days);
        let cutoff_str = cutoff_date.to_rfc3339();

        let result = sqlx::query(
            r#"
            DELETE FROM jobs
            WHERE (status = 'completed' OR status = 'failed')
            AND updated_at < ?1
            "#,
        )
        .bind(&cutoff_str)
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected() as usize;
        info!("Cleaned up {} old jobs (older than {} days)", deleted, days);

        Ok(deleted)
    }

    fn row_to_job(&self, row: sqlx::sqlite::SqliteRow) -> Result<Job> {
        let status_str: String = row.try_get("status")?;
        let status = match status_str.as_str() {
            "pending" => JobStatus::Pending,
            "syncing" => JobStatus::Syncing,
            "paused" => JobStatus::Paused,
            "completed" => JobStatus::Completed,
            "failed" => JobStatus::Failed,
            "cancelled" => JobStatus::Cancelled,
            _ => JobStatus::Pending,
        };

        let exclude_regex_str: String = row.try_get("exclude_regex")?;
        let exclude_regex: Vec<String> = serde_json::from_str(&exclude_regex_str)?;

        let include_regex_str: String = row.try_get("include_regex")?;
        let include_regex: Vec<String> = serde_json::from_str(&include_regex_str)?;

        let options_str: String = row.try_get("options")?;
        let options: JobConfig = serde_json::from_str(&options_str)?;

        let job_type_str: String = row.try_get("job_type")?;
        let job_type = JobType::parse(&job_type_str);

        let created_at_str: String = row.try_get("created_at")?;
        let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .map_err(|e| HarDataError::SerializationError(format!("Invalid created_at: {}", e)))?
            .with_timezone(&chrono::Utc);

        let updated_at_str: String = row.try_get("updated_at")?;
        let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_str)
            .map_err(|e| HarDataError::SerializationError(format!("Invalid updated_at: {}", e)))?
            .with_timezone(&chrono::Utc);

        let job = Job {
            job_id: row.try_get("job_id")?,
            region: row.try_get("region")?,
            source: JobPath {
                path: row.try_get("source_path")?,
                client_id: row.try_get("source_client_id")?,
            },
            dest: JobPath {
                path: row.try_get("dest_path")?,
                client_id: row.try_get("dest_client_id")?,
            },
            status,
            progress: row.try_get::<i64, _>("progress")? as u8,
            current_size: row.try_get::<i64, _>("current_size")? as u64,
            total_size: row.try_get::<i64, _>("total_size")? as u64,
            priority: row.try_get("priority")?,
            exclude_regex,
            include_regex,
            job_type,
            options,
            created_at,
            updated_at,
        };

        Ok(job)
    }
}
