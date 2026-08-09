use hardata_domain::{Job, JobConfig, JobPath, JobStatus, JobType};
use hardata_shared::error::{HarDataError, Result};
use sqlx::{QueryBuilder, Row, Sqlite};
use std::collections::HashMap;
use tracing::info;

use super::types::Database;

impl Database {
    pub async fn save_job(&self, job: &Job) -> Result<()> {
        let exclude_regex = serde_json::to_string(&job.exclude_regex)?;
        let include_regex = serde_json::to_string(&job.include_regex)?;
        let options = serde_json::to_string(&job.options)?;

        sqlx::query(
            r#"
            INSERT INTO jobs (
                job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                status, progress, current_size, total_size, priority, round_id, is_last_round,
                exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)
            ON CONFLICT(job_id) DO UPDATE SET
                region = ?2,
                source_path = ?3,
                source_client_id = ?4,
                dest_path = ?5,
                dest_client_id = ?6,
                status = ?7,
                progress = ?8,
                current_size = ?9,
                total_size = ?10,
                priority = ?11,
                round_id = ?12,
                is_last_round = ?13,
                exclude_regex = ?14,
                include_regex = ?15,
                job_type = ?16,
                options = ?17,
                error_message = ?18,
                updated_at = ?20
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
        .bind(job.round_id)
        .bind(job.is_last_round)
        .bind(exclude_regex)
        .bind(include_regex)
        .bind(job.job_type.as_str())
        .bind(options)
        .bind(&job.error_message)
        .bind(job.created_at.to_rfc3339())
        .bind(job.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn load_all_jobs(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            ORDER BY created_at DESC, job_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let jobs: Result<Vec<Job>> = rows.into_iter().map(|row| self.row_to_job(row)).collect();
        jobs
    }

    pub async fn count_jobs(&self) -> Result<usize> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM jobs
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(row.try_get::<i64, _>("count")? as usize)
    }

    pub async fn count_public_jobs(&self) -> Result<usize> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE job_id NOT LIKE '%\_final' ESCAPE '\'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(row.try_get::<i64, _>("count")? as usize)
    }

    pub async fn load_jobs_page(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            ORDER BY created_at DESC, job_id ASC
            LIMIT ?1 OFFSET ?2
            "#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let jobs: Result<Vec<Job>> = rows.into_iter().map(|row| self.row_to_job(row)).collect();
        jobs
    }

    pub async fn load_job_page_refs(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, chrono::DateTime<chrono::Utc>)>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, created_at
            FROM jobs
            ORDER BY created_at DESC, job_id ASC
            LIMIT ?1 OFFSET ?2
            "#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let created_at_str: String = row.try_get("created_at")?;
                let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        HarDataError::SerializationError(format!("Invalid created_at: {}", e))
                    })?
                    .with_timezone(&chrono::Utc);
                Ok((row.try_get("job_id")?, created_at))
            })
            .collect()
    }

    pub async fn load_public_job_page_refs(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, chrono::DateTime<chrono::Utc>)>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, created_at
            FROM jobs
            WHERE job_id NOT LIKE '%\_final' ESCAPE '\'
            ORDER BY created_at DESC, job_id ASC
            LIMIT ?1 OFFSET ?2
            "#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let created_at_str: String = row.try_get("created_at")?;
                let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        HarDataError::SerializationError(format!("Invalid created_at: {}", e))
                    })?
                    .with_timezone(&chrono::Utc);
                Ok((row.try_get("job_id")?, created_at))
            })
            .collect()
    }

    /// Read one page of all jobs, including internal final jobs.
    pub async fn load_job_page(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            ORDER BY created_at DESC, job_id ASC
            LIMIT ?1 OFFSET ?2
            "#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| self.row_to_job(row).ok())
            .collect())
    }

    /// Read one page of public jobs, excluding internal final jobs.
    pub async fn load_public_job_page(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            WHERE job_id NOT LIKE '%\_final' ESCAPE '\'
            ORDER BY created_at DESC, job_id ASC
            LIMIT ?1 OFFSET ?2
            "#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| self.row_to_job(row).ok())
            .collect())
    }

    pub async fn load_active_jobs(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            WHERE status IN ('pending', 'syncing', 'paused')
            ORDER BY created_at DESC, job_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let jobs: Result<Vec<Job>> = rows.into_iter().map(|row| self.row_to_job(row)).collect();
        jobs
    }

    pub async fn load_terminal_jobs(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            WHERE status IN ('completed', 'failed', 'cancelled')
            ORDER BY created_at DESC, job_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let jobs: Result<Vec<Job>> = rows.into_iter().map(|row| self.row_to_job(row)).collect();
        jobs
    }

    pub async fn load_job(&self, job_id: &str) -> Result<Option<Job>> {
        let row = sqlx::query(
            r#"
            SELECT job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                   status, progress, current_size, total_size, priority, round_id, is_last_round,
                   exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            FROM jobs
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| self.row_to_job(row)).transpose()
    }

    pub async fn load_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        let row = sqlx::query(
            r#"
            SELECT status
            FROM jobs
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let status_str: String = row.try_get("status")?;
        let status = JobStatus::try_parse(&status_str).ok_or_else(|| {
            HarDataError::SerializationError(format!("Invalid job status '{}'", status_str))
        })?;
        Ok(Some(status))
    }

    pub async fn load_job_statuses(
        &self,
        job_ids: &[String],
    ) -> Result<HashMap<String, JobStatus>> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut query = QueryBuilder::<Sqlite>::new(
            r#"
            SELECT job_id, status
            FROM jobs
            WHERE job_id IN (
            "#,
        );
        let mut separated = query.separated(", ");
        for job_id in job_ids {
            separated.push_bind(job_id);
        }
        query.push(")");

        let rows = query.build().fetch_all(&self.pool).await?;
        let mut statuses = HashMap::with_capacity(rows.len());
        for row in rows {
            let status_str: String = row.try_get("status")?;
            let status = JobStatus::try_parse(&status_str).ok_or_else(|| {
                HarDataError::SerializationError(format!("Invalid job status '{}'", status_str))
            })?;
            statuses.insert(row.try_get("job_id")?, status);
        }

        Ok(statuses)
    }

    pub async fn count_jobs_by_status(&self) -> Result<HashMap<JobStatus, i64>> {
        let rows = sqlx::query(
            r#"
            SELECT status, COUNT(*) AS count
            FROM jobs
            GROUP BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut counts = HashMap::new();
        for row in rows {
            let status_str: String = row.try_get("status")?;
            let status = JobStatus::try_parse(&status_str).ok_or_else(|| {
                HarDataError::SerializationError(format!("Invalid job status '{}'", status_str))
            })?;
            counts.insert(status, row.try_get::<i64, _>("count")?);
        }

        Ok(counts)
    }

    pub async fn count_public_jobs_by_status(&self) -> Result<HashMap<JobStatus, i64>> {
        let rows = sqlx::query(
            r#"
            SELECT status, COUNT(*) AS count
            FROM jobs
            WHERE job_id NOT LIKE '%\_final' ESCAPE '\'
            GROUP BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut counts = HashMap::new();
        for row in rows {
            let status_str: String = row.try_get("status")?;
            let status = JobStatus::try_parse(&status_str).ok_or_else(|| {
                HarDataError::SerializationError(format!("Invalid job status '{}'", status_str))
            })?;
            counts.insert(status, row.try_get::<i64, _>("count")?);
        }

        Ok(counts)
    }

    pub async fn load_active_job_destinations(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, dest_path
            FROM jobs
            WHERE status IN ('pending', 'syncing', 'paused')
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let destinations: Result<Vec<(String, String)>> = rows
            .into_iter()
            .map(|row| Ok((row.try_get("job_id")?, row.try_get("dest_path")?)))
            .collect();

        destinations
    }

    pub async fn update_job_status(
        &self,
        job_id: &str,
        status: JobStatus,
        progress: u8,
        current_size: u64,
        total_size: u64,
        error_message: Option<&str>,
    ) -> Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let result = sqlx::query(
            r#"
            UPDATE jobs SET status = ?2, progress = ?3, current_size = ?4, total_size = ?5, error_message = ?6, updated_at = ?7
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .bind(status.as_str())
        .bind(progress as i64)
        .bind(current_size as i64)
        .bind(total_size as i64)
        .bind(error_message)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_job_round_state(
        &self,
        job_id: &str,
        round_id: i64,
        is_last_round: bool,
    ) -> Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET round_id = ?2, is_last_round = ?3, updated_at = ?4
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .bind(round_id)
        .bind(is_last_round)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_job_progress(
        &self,
        job_id: &str,
        progress: u8,
        current_size: u64,
        total_size: u64,
    ) -> Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET progress = ?2, current_size = ?3, total_size = ?4, updated_at = ?5
            WHERE job_id = ?1
              AND status IN ('pending', 'syncing')
            "#,
        )
        .bind(job_id)
        .bind(progress as i64)
        .bind(current_size as i64)
        .bind(total_size as i64)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

}
