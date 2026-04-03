use crate::util::error::Result;
use chrono::{DateTime, Duration, Utc};
use sqlx::Row;
use std::collections::HashSet;
use tracing::{info, warn};

use super::types::Database;

#[derive(Debug, Clone)]
pub struct JobRetry {
    pub job_id: String,
    pub retry_count: i32,
    pub max_retries: i32,
    pub last_retry_at: Option<DateTime<Utc>>,
    pub next_retry_at: DateTime<Utc>,
    pub last_error: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn retry_delay_for_count(retry_count: i32) -> Duration {
    match retry_count {
        i32::MIN..=0 => Duration::minutes(1),
        1 => Duration::minutes(3),
        2 => Duration::minutes(6),
        _ => Duration::minutes(10),
    }
}

fn parse_optional_retry_time(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|raw| {
            DateTime::parse_from_rfc3339(&raw)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| {
                    crate::util::error::HarDataError::SerializationError(format!(
                        "Invalid {}: {}",
                        field, e
                    ))
                })
        })
        .transpose()
}

impl Database {
    pub async fn save_retry(&self, job_id: &str, error: &str) -> Result<()> {
        let now = Utc::now();
        let existing_retry = self.get_retry(job_id).await?;
        let next_retry = now
            + retry_delay_for_count(
                existing_retry
                    .as_ref()
                    .map(|retry| retry.retry_count)
                    .unwrap_or(0),
            );

        if existing_retry.is_some() {
            sqlx::query(
                r#"
                UPDATE job_retries
                SET last_error = ?2, next_retry_at = ?3, updated_at = ?4
                WHERE job_id = ?1
                "#,
            )
            .bind(job_id)
            .bind(error)
            .bind(next_retry.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO job_retries (
                    job_id, retry_count, max_retries, last_retry_at, next_retry_at,
                    last_error, created_at, updated_at
                ) VALUES (?1, 0, 3, NULL, ?2, ?3, ?4, ?5)
                "#,
            )
            .bind(job_id)
            .bind(next_retry.to_rfc3339())
            .bind(error)
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&self.pool)
            .await?;
        }

        info!(
            "Retry record saved for job {}, next retry at {}",
            job_id,
            next_retry.to_rfc3339()
        );
        Ok(())
    }

    pub async fn get_pending_retries(&self) -> Result<Vec<JobRetry>> {
        let now = Utc::now().to_rfc3339();
        let rows = sqlx::query(
            r#"
            SELECT job_id, retry_count, max_retries, last_retry_at, next_retry_at,
                   last_error, created_at, updated_at
            FROM job_retries
            WHERE retry_count < max_retries AND next_retry_at <= ?1
            ORDER BY next_retry_at ASC
            LIMIT 100
            "#,
        )
        .bind(&now)
        .fetch_all(&self.pool)
        .await?;

        let retries: Result<Vec<JobRetry>> = rows
            .into_iter()
            .map(|row| {
                let last_retry_str: Option<String> = row.try_get("last_retry_at")?;
                let last_retry_at = parse_optional_retry_time(last_retry_str, "last_retry_at")?;

                let next_retry_str: String = row.try_get("next_retry_at")?;
                let next_retry_at = DateTime::parse_from_rfc3339(&next_retry_str)
                    .map_err(|e| {
                        crate::util::error::HarDataError::SerializationError(format!(
                            "Invalid next_retry_at: {}",
                            e
                        ))
                    })?
                    .with_timezone(&Utc);

                let created_at_str: String = row.try_get("created_at")?;
                let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        crate::util::error::HarDataError::SerializationError(format!(
                            "Invalid created_at: {}",
                            e
                        ))
                    })?
                    .with_timezone(&Utc);

                let updated_at_str: String = row.try_get("updated_at")?;
                let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                    .map_err(|e| {
                        crate::util::error::HarDataError::SerializationError(format!(
                            "Invalid updated_at: {}",
                            e
                        ))
                    })?
                    .with_timezone(&Utc);

                Ok(JobRetry {
                    job_id: row.try_get("job_id")?,
                    retry_count: row.try_get::<i64, _>("retry_count")? as i32,
                    max_retries: row.try_get::<i64, _>("max_retries")? as i32,
                    last_retry_at,
                    next_retry_at,
                    last_error: row.try_get("last_error")?,
                    created_at,
                    updated_at,
                })
            })
            .collect();

        retries
    }

    pub async fn update_retry_attempt(&self, job_id: &str, success: bool) -> Result<bool> {
        if success {
            let result = sqlx::query("DELETE FROM job_retries WHERE job_id = ?1")
                .bind(job_id)
                .execute(&self.pool)
                .await?;
            return Ok(result.rows_affected() > 0);
        }

        let retry = self.get_retry(job_id).await?;
        if retry.is_none() {
            warn!("Retry record not found for job {}", job_id);
            return Ok(false);
        }

        let retry = retry.unwrap();
        let new_count = retry.retry_count + 1;
        let now = Utc::now();

        if new_count >= retry.max_retries {
            sqlx::query(
                r#"
                UPDATE job_retries
                SET retry_count = ?2, last_retry_at = ?3, updated_at = ?4
                WHERE job_id = ?1
                "#,
            )
            .bind(job_id)
            .bind(retry.max_retries as i64)
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&self.pool)
            .await?;
            info!(
                "Max retries reached for job {}, retry record marked exhausted",
                job_id
            );
            return Ok(true);
        }

        let next_retry = now + retry_delay_for_count(new_count);

        sqlx::query(
            r#"
            UPDATE job_retries
            SET retry_count = ?2, last_retry_at = ?3, next_retry_at = ?4, updated_at = ?5
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .bind(new_count as i64)
        .bind(now.to_rfc3339())
        .bind(next_retry.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await?;

        info!(
            "Retry attempt {} for job {} recorded, next retry at {}",
            new_count,
            job_id,
            next_retry.to_rfc3339()
        );
        Ok(true)
    }

    pub async fn get_retry(&self, job_id: &str) -> Result<Option<JobRetry>> {
        let row = sqlx::query(
            r#"
            SELECT job_id, retry_count, max_retries, last_retry_at, next_retry_at,
                   last_error, created_at, updated_at
            FROM job_retries
            WHERE job_id = ?1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let last_retry_str: Option<String> = row.try_get("last_retry_at")?;
            let last_retry_at = parse_optional_retry_time(last_retry_str, "last_retry_at")?;

            let next_retry_str: String = row.try_get("next_retry_at")?;
            let next_retry_at = DateTime::parse_from_rfc3339(&next_retry_str)
                .map_err(|e| {
                    crate::util::error::HarDataError::SerializationError(format!(
                        "Invalid next_retry_at: {}",
                        e
                    ))
                })?
                .with_timezone(&Utc);

            let created_at_str: String = row.try_get("created_at")?;
            let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                .map_err(|e| {
                    crate::util::error::HarDataError::SerializationError(format!(
                        "Invalid created_at: {}",
                        e
                    ))
                })?
                .with_timezone(&Utc);

            let updated_at_str: String = row.try_get("updated_at")?;
            let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                .map_err(|e| {
                    crate::util::error::HarDataError::SerializationError(format!(
                        "Invalid updated_at: {}",
                        e
                    ))
                })?
                .with_timezone(&Utc);

            Ok(Some(JobRetry {
                job_id: row.try_get("job_id")?,
                retry_count: row.try_get::<i64, _>("retry_count")? as i32,
                max_retries: row.try_get::<i64, _>("max_retries")? as i32,
                last_retry_at,
                next_retry_at,
                last_error: row.try_get("last_error")?,
                created_at,
                updated_at,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn load_retryable_job_ids(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id
            FROM job_retries
            WHERE retry_count < max_retries
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let job_ids: Result<HashSet<String>> = rows
            .into_iter()
            .map(|row| Ok(row.try_get("job_id")?))
            .collect();

        job_ids
    }

    pub async fn delete_retry(&self, job_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM job_retries WHERE job_id = ?1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 将 retry 记录精确恢复到快照状态（用于 finalize 失败回滚）
    pub async fn restore_retry(&self, retry: &JobRetry) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO job_retries (
                job_id, retry_count, max_retries, last_retry_at, next_retry_at,
                last_error, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(job_id) DO UPDATE SET
                retry_count = excluded.retry_count,
                max_retries = excluded.max_retries,
                last_retry_at = excluded.last_retry_at,
                next_retry_at = excluded.next_retry_at,
                last_error = excluded.last_error,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&retry.job_id)
        .bind(retry.retry_count as i64)
        .bind(retry.max_retries as i64)
        .bind(retry.last_retry_at.map(|ts| ts.to_rfc3339()))
        .bind(retry.next_retry_at.to_rfc3339())
        .bind(&retry.last_error)
        .bind(retry.created_at.to_rfc3339())
        .bind(retry.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use chrono::{Duration, Utc};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-retry-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn save_retry_refreshes_next_retry_from_failure_time() {
        let temp_dir = create_temp_dir("refresh-next-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("retry.db").display());
        let db = Database::new(&db_path).await.unwrap();
        let job_id = "job-retry-refresh";

        db.save_retry(job_id, "first failure").await.unwrap();
        db.update_retry_attempt(job_id, false).await.unwrap();

        sqlx::query("UPDATE job_retries SET next_retry_at = ?2 WHERE job_id = ?1")
            .bind(job_id)
            .bind((Utc::now() - Duration::minutes(10)).to_rfc3339())
            .execute(&db.pool)
            .await
            .unwrap();

        db.save_retry(job_id, "failed again").await.unwrap();
        let retry = db.get_retry(job_id).await.unwrap().unwrap();

        assert_eq!(retry.retry_count, 1);
        assert!(retry.next_retry_at > Utc::now() + Duration::minutes(2));
        assert_eq!(retry.last_error, "failed again");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn update_retry_attempt_success_returns_false_when_record_missing() {
        let temp_dir = create_temp_dir("missing-success-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("retry.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let deleted = db.update_retry_attempt("job-missing", true).await.unwrap();

        assert!(!deleted);

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn update_retry_attempt_success_deletes_existing_retry_record() {
        let temp_dir = create_temp_dir("delete-success-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("retry.db").display());
        let db = Database::new(&db_path).await.unwrap();
        let job_id = "job-success-retry";

        db.save_retry(job_id, "temporary failure").await.unwrap();

        let deleted = db.update_retry_attempt(job_id, true).await.unwrap();

        assert!(deleted);
        assert!(db.get_retry(job_id).await.unwrap().is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_retryable_job_ids_excludes_exhausted_records() {
        let temp_dir = create_temp_dir("load-retryable-ids");
        let db_path = format!("sqlite://{}", temp_dir.join("retry.db").display());
        let db = Database::new(&db_path).await.unwrap();

        db.save_retry("job-active-retry", "temporary failure")
            .await
            .unwrap();
        db.save_retry("job-exhausted-retry", "temporary failure")
            .await
            .unwrap();
        sqlx::query("UPDATE job_retries SET retry_count = max_retries WHERE job_id = ?1")
            .bind("job-exhausted-retry")
            .execute(&db.pool)
            .await
            .unwrap();

        let retryable = db.load_retryable_job_ids().await.unwrap();

        assert!(retryable.contains("job-active-retry"));
        assert!(!retryable.contains("job-exhausted-retry"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn get_retry_rejects_invalid_last_retry_timestamp() {
        let temp_dir = create_temp_dir("invalid-last-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("retry.db").display());
        let db = Database::new(&db_path).await.unwrap();
        let job_id = "job-invalid-last-retry";

        db.save_retry(job_id, "temporary failure").await.unwrap();
        sqlx::query("UPDATE job_retries SET last_retry_at = 'not-a-timestamp' WHERE job_id = ?1")
            .bind(job_id)
            .execute(&db.pool)
            .await
            .unwrap();

        let err = db.get_retry(job_id).await.unwrap_err();
        assert!(err.to_string().contains("Invalid last_retry_at"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn get_pending_retries_rejects_invalid_last_retry_timestamp() {
        let temp_dir = create_temp_dir("invalid-pending-last-retry");
        let db_path = format!("sqlite://{}", temp_dir.join("retry.db").display());
        let db = Database::new(&db_path).await.unwrap();
        let job_id = "job-invalid-pending-last-retry";

        db.save_retry(job_id, "temporary failure").await.unwrap();
        sqlx::query(
            "UPDATE job_retries SET last_retry_at = 'not-a-timestamp', next_retry_at = ?2 WHERE job_id = ?1",
        )
        .bind(job_id)
        .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
        .execute(&db.pool)
        .await
        .unwrap();

        let err = db.get_pending_retries().await.unwrap_err();
        assert!(err.to_string().contains("Invalid last_retry_at"));

        let _ = fs::remove_dir_all(temp_dir);
    }
}
