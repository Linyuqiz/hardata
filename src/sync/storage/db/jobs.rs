use crate::core::{Job, JobConfig, JobPath, JobStatus, JobType};
use crate::util::error::{HarDataError, Result};
use sqlx::{QueryBuilder, Row, Sqlite};
use std::collections::HashMap;
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

        info!("Job saved successfully: {}", job.job_id);
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

    /// 整页读取所有任务（含 _final），消除分页 ref-then-load 竞争窗口
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

    /// 整页读取公开任务（排除 _final），消除分页 ref-then-load 竞争窗口
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

    pub async fn cleanup_old_jobs(&self, days: i64) -> Result<usize> {
        let cutoff_date = chrono::Utc::now() - chrono::Duration::days(days);
        let cutoff_str = cutoff_date.to_rfc3339();
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            DELETE FROM transfer_states
            WHERE job_id IN (
                SELECT job_id
                FROM jobs
                WHERE (status = 'completed' OR status = 'failed' OR status = 'cancelled')
                AND updated_at < ?1
            )
            "#,
        )
        .bind(&cutoff_str)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM job_retries
            WHERE job_id IN (
                SELECT job_id
                FROM jobs
                WHERE (status = 'completed' OR status = 'failed' OR status = 'cancelled')
                AND updated_at < ?1
            )
            "#,
        )
        .bind(&cutoff_str)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM tmp_transfer_paths
            WHERE job_id IN (
                SELECT job_id
                FROM jobs
                WHERE (status = 'completed' OR status = 'failed' OR status = 'cancelled')
                AND updated_at < ?1
            )
            "#,
        )
        .bind(&cutoff_str)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM api_idempotency_keys
            WHERE job_id IN (
                SELECT job_id
                FROM jobs
                WHERE (status = 'completed' OR status = 'failed' OR status = 'cancelled')
                AND updated_at < ?1
            )
            "#,
        )
        .bind(&cutoff_str)
        .execute(&mut *tx)
        .await?;

        let result = sqlx::query(
            r#"
            DELETE FROM jobs
            WHERE (status = 'completed' OR status = 'failed' OR status = 'cancelled')
            AND updated_at < ?1
            "#,
        )
        .bind(&cutoff_str)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM api_idempotency_keys
            WHERE updated_at < ?1
              AND NOT EXISTS (
                  SELECT 1
                  FROM jobs
                  WHERE jobs.job_id = api_idempotency_keys.job_id
              )
            "#,
        )
        .bind(&cutoff_str)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let deleted = result.rows_affected() as usize;
        info!("Cleaned up {} old jobs (older than {} days)", deleted, days);

        Ok(deleted)
    }

    fn row_to_job(&self, row: sqlx::sqlite::SqliteRow) -> Result<Job> {
        let status_str: String = row.try_get("status")?;
        let status = JobStatus::try_parse(&status_str).ok_or_else(|| {
            HarDataError::SerializationError(format!("Invalid job status '{}'", status_str))
        })?;

        let exclude_regex_str: String = row.try_get("exclude_regex")?;
        let exclude_regex: Vec<String> = serde_json::from_str(&exclude_regex_str)?;

        let include_regex_str: String = row.try_get("include_regex")?;
        let include_regex: Vec<String> = serde_json::from_str(&include_regex_str)?;

        let options_str: String = row.try_get("options")?;
        let options: JobConfig = serde_json::from_str(&options_str)?;

        let job_type_str: String = row.try_get("job_type")?;
        let job_type = JobType::try_parse(&job_type_str).ok_or_else(|| {
            HarDataError::SerializationError(format!("Invalid job type '{}'", job_type_str))
        })?;

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
            round_id: row.try_get("round_id")?,
            is_last_round: row.try_get::<i64, _>("is_last_round")? != 0,
            exclude_regex,
            include_regex,
            job_type,
            options,
            created_at,
            updated_at,
            error_message: row.try_get("error_message")?,
        };

        Ok(job)
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::core::{FileTransferState, Job, JobPath, JobStatus, JobType};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-jobs-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn sample_job(job_id: &str, status: JobStatus) -> Job {
        let mut job = Job::new(
            job_id.to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.status = status;
        job
    }

    #[tokio::test]
    async fn save_job_persists_error_message() {
        let temp_dir = create_temp_dir("save-job-error-message");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let mut job = sample_job("job-error-message", JobStatus::Failed);
        job.error_message = Some("disk full".to_string());
        db.save_job(&job).await.unwrap();

        let loaded = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(loaded.error_message.as_deref(), Some("disk full"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn save_job_persists_round_state() {
        let temp_dir = create_temp_dir("save-job-round-state");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let mut job = sample_job("job-round-state", JobStatus::Paused);
        job.round_id = 4;
        job.is_last_round = true;
        db.save_job(&job).await.unwrap();

        let loaded = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(loaded.round_id, 4);
        assert!(loaded.is_last_round);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn update_job_status_updates_error_message_column() {
        let temp_dir = create_temp_dir("update-job-status-error-message");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let job = sample_job("job-update-error-message", JobStatus::Pending);
        db.save_job(&job).await.unwrap();

        let updated = db
            .update_job_status(
                &job.job_id,
                JobStatus::Failed,
                55,
                55,
                100,
                Some("network down"),
            )
            .await
            .unwrap();
        assert!(updated);

        let loaded = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(loaded.status, JobStatus::Failed);
        assert_eq!(loaded.error_message.as_deref(), Some("network down"));

        let updated = db
            .update_job_status(&job.job_id, JobStatus::Pending, 10, 10, 100, None)
            .await
            .unwrap();
        assert!(updated);

        let loaded = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(loaded.status, JobStatus::Pending);
        assert!(loaded.error_message.is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cleanup_old_jobs_removes_cancelled_jobs_and_related_records() {
        let temp_dir = create_temp_dir("cleanup-old-jobs");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();
        let completed_job = sample_job("job-completed-old", JobStatus::Completed);
        let cancelled_job = sample_job("job-cancelled-old", JobStatus::Cancelled);
        let pending_job = sample_job("job-pending-recent", JobStatus::Pending);

        db.save_job(&completed_job).await.unwrap();
        db.save_job(&cancelled_job).await.unwrap();
        db.save_job(&pending_job).await.unwrap();

        db.save_transfer_state(
            &cancelled_job.job_id,
            &FileTransferState::new("remote/source.bin".to_string(), 4),
        )
        .await
        .unwrap();
        db.save_retry(&cancelled_job.job_id, "stale retry")
            .await
            .unwrap();
        db.save_tmp_transfer_path(&cancelled_job.job_id, "/tmp/cancelled.tmp")
            .await
            .unwrap();
        db.reserve_api_idempotency_key(
            "create_job",
            "cleanup-old-idempotency",
            "fingerprint",
            &cancelled_job.job_id,
        )
        .await
        .unwrap();

        let old_timestamp = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        sqlx::query("UPDATE jobs SET updated_at = ?2 WHERE job_id IN (?1, ?3)")
            .bind(&completed_job.job_id)
            .bind(&old_timestamp)
            .bind(&cancelled_job.job_id)
            .execute(&db.pool)
            .await
            .unwrap();

        let deleted = db.cleanup_old_jobs(7).await.unwrap();
        assert_eq!(deleted, 2);

        let jobs = db.load_all_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, pending_job.job_id);

        let transfer_state = db
            .load_transfer_state(&cancelled_job.job_id, "remote/source.bin")
            .await
            .unwrap();
        assert!(transfer_state.is_none());
        assert!(db.get_retry(&cancelled_job.job_id).await.unwrap().is_none());
        assert!(db
            .load_tmp_transfer_paths_by_job(&cancelled_job.job_id)
            .await
            .unwrap()
            .is_empty());
        assert!(db
            .load_api_idempotency_record("create_job", "cleanup-old-idempotency")
            .await
            .unwrap()
            .is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn cleanup_old_jobs_removes_stale_orphan_idempotency_keys() {
        let temp_dir = create_temp_dir("cleanup-orphan-idempotency");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        db.reserve_api_idempotency_key(
            "create_job",
            "orphan-old-idempotency",
            "fingerprint-old",
            "missing-job",
        )
        .await
        .unwrap();
        db.reserve_api_idempotency_key(
            "create_job",
            "orphan-recent-idempotency",
            "fingerprint-recent",
            "missing-job-recent",
        )
        .await
        .unwrap();

        let old_timestamp = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        sqlx::query(
            "UPDATE api_idempotency_keys SET created_at = ?1, updated_at = ?1 WHERE idempotency_key = ?2",
        )
        .bind(&old_timestamp)
        .bind("orphan-old-idempotency")
        .execute(&db.pool)
        .await
        .unwrap();

        let deleted = db.cleanup_old_jobs(7).await.unwrap();
        assert_eq!(deleted, 0);
        assert!(db
            .load_api_idempotency_record("create_job", "orphan-old-idempotency")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_api_idempotency_record("create_job", "orphan-recent-idempotency")
            .await
            .unwrap()
            .is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn save_job_updates_paths_and_client_ids_on_conflict() {
        let temp_dir = create_temp_dir("save-job-updates-paths");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let mut job = sample_job("job-upsert-paths", JobStatus::Pending);
        job.source.client_id = "source-a".to_string();
        job.dest.client_id = "dest-a".to_string();
        db.save_job(&job).await.unwrap();

        job.source.path = "/tmp/source-updated.bin".to_string();
        job.source.client_id = "source-b".to_string();
        job.dest.path = "dest-updated.bin".to_string();
        job.dest.client_id = "dest-b".to_string();
        job.status = JobStatus::Syncing;
        db.save_job(&job).await.unwrap();

        let jobs = db.load_all_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, job.job_id);
        assert_eq!(jobs[0].source.path, "/tmp/source-updated.bin");
        assert_eq!(jobs[0].source.client_id, "source-b");
        assert_eq!(jobs[0].dest.path, "dest-updated.bin");
        assert_eq!(jobs[0].dest.client_id, "dest-b");
        assert_eq!(jobs[0].status, JobStatus::Syncing);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_returns_only_requested_row() {
        let temp_dir = create_temp_dir("load-single-job");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let requested = sample_job("job-requested", JobStatus::Syncing);
        let other = sample_job("job-other", JobStatus::Completed);
        db.save_job(&requested).await.unwrap();
        db.save_job(&other).await.unwrap();

        let loaded = db.load_job(&requested.job_id).await.unwrap().unwrap();

        assert_eq!(loaded.job_id, requested.job_id);
        assert_eq!(loaded.status, JobStatus::Syncing);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_status_returns_only_status_column() {
        let temp_dir = create_temp_dir("load-job-status");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let job = sample_job("job-status-only", JobStatus::Paused);
        db.save_job(&job).await.unwrap();

        let status = db.load_job_status(&job.job_id).await.unwrap();

        assert_eq!(status, Some(JobStatus::Paused));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_job_statuses_returns_only_requested_rows() {
        let temp_dir = create_temp_dir("load-job-statuses");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let pending = sample_job("job-statuses-pending", JobStatus::Pending);
        let paused = sample_job("job-statuses-paused", JobStatus::Paused);
        let completed = sample_job("job-statuses-completed", JobStatus::Completed);
        db.save_job(&pending).await.unwrap();
        db.save_job(&paused).await.unwrap();
        db.save_job(&completed).await.unwrap();

        let statuses = db
            .load_job_statuses(&[pending.job_id.clone(), completed.job_id.clone()])
            .await
            .unwrap();

        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses.get(&pending.job_id), Some(&JobStatus::Pending));
        assert_eq!(statuses.get(&completed.job_id), Some(&JobStatus::Completed));
        assert!(!statuses.contains_key(&paused.job_id));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn count_jobs_by_status_aggregates_without_loading_full_rows() {
        let temp_dir = create_temp_dir("count-jobs-by-status");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        db.save_job(&sample_job("job-pending-a", JobStatus::Pending))
            .await
            .unwrap();
        db.save_job(&sample_job("job-pending-b", JobStatus::Pending))
            .await
            .unwrap();
        db.save_job(&sample_job("job-paused", JobStatus::Paused))
            .await
            .unwrap();

        let counts = db.count_jobs_by_status().await.unwrap();

        assert_eq!(counts.get(&JobStatus::Pending), Some(&2));
        assert_eq!(counts.get(&JobStatus::Paused), Some(&1));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn public_job_queries_exclude_internal_final_rows() {
        let temp_dir = create_temp_dir("public-job-queries");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let public_a = sample_job("job-public-a", JobStatus::Pending);
        let public_b = sample_job("job-public-b", JobStatus::Completed);
        let internal_final = sample_job("job-public-a_final", JobStatus::Syncing);
        db.save_job(&public_a).await.unwrap();
        db.save_job(&public_b).await.unwrap();
        db.save_job(&internal_final).await.unwrap();

        let total = db.count_public_jobs().await.unwrap();
        let refs = db.load_public_job_page_refs(10, 0).await.unwrap();
        let counts = db.count_public_jobs_by_status().await.unwrap();

        assert_eq!(total, 2);
        assert_eq!(refs.len(), 2);
        assert!(refs.iter().all(|(job_id, _)| !job_id.ends_with("_final")));
        assert_eq!(counts.get(&JobStatus::Pending), Some(&1));
        assert_eq!(counts.get(&JobStatus::Completed), Some(&1));
        assert!(!counts.contains_key(&JobStatus::Syncing));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_active_job_destinations_includes_paused_and_excludes_terminal() {
        let temp_dir = create_temp_dir("load-active-destinations");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let mut paused = sample_job("job-paused-dest", JobStatus::Paused);
        paused.dest.path = "paused.bin".to_string();
        db.save_job(&paused).await.unwrap();

        let mut syncing = sample_job("job-syncing-dest", JobStatus::Syncing);
        syncing.dest.path = "syncing.bin".to_string();
        db.save_job(&syncing).await.unwrap();

        let mut completed = sample_job("job-completed-dest", JobStatus::Completed);
        completed.dest.path = "completed.bin".to_string();
        db.save_job(&completed).await.unwrap();

        let destinations = db.load_active_job_destinations().await.unwrap();

        assert!(destinations.contains(&(paused.job_id.clone(), "paused.bin".to_string())));
        assert!(destinations.contains(&(syncing.job_id.clone(), "syncing.bin".to_string())));
        assert!(!destinations.contains(&(completed.job_id.clone(), "completed.bin".to_string())));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_all_jobs_rejects_unknown_status_values() {
        let temp_dir = create_temp_dir("load-jobs-invalid-status");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let job = sample_job("job-invalid-status", JobStatus::Pending);
        db.save_job(&job).await.unwrap();
        sqlx::query("UPDATE jobs SET status = 'broken-status' WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&db.pool)
            .await
            .unwrap();

        let err = db.load_all_jobs().await.unwrap_err();
        assert!(err.to_string().contains("Invalid job status"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_all_jobs_rejects_unknown_job_type_values() {
        let temp_dir = create_temp_dir("load-jobs-invalid-job-type");
        let db_path = format!("sqlite://{}", temp_dir.join("jobs.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let job = sample_job("job-invalid-job-type", JobStatus::Pending);
        db.save_job(&job).await.unwrap();
        sqlx::query("UPDATE jobs SET job_type = 'broken-type' WHERE job_id = ?1")
            .bind(&job.job_id)
            .execute(&db.pool)
            .await
            .unwrap();

        let err = db.load_all_jobs().await.unwrap_err();
        assert!(err.to_string().contains("Invalid job type"));

        let _ = fs::remove_dir_all(temp_dir);
    }
}
