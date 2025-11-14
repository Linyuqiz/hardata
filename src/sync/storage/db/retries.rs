use crate::util::error::Result;
use chrono::{DateTime, Duration, Utc};
use sqlx::Row;
use tracing::{info, warn};

use super::types::Database;

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

impl Database {
    pub async fn save_retry(&self, job_id: &str, error: &str) -> Result<()> {
        let now = Utc::now();
        let next_retry = now + Duration::minutes(1);

        sqlx::query(
            r#"
            INSERT INTO job_retries (
                job_id, retry_count, max_retries, last_retry_at, next_retry_at,
                last_error, created_at, updated_at
            ) VALUES (?1, 0, 3, NULL, ?2, ?3, ?4, ?5)
            ON CONFLICT(job_id) DO UPDATE SET
                last_error = ?3,
                updated_at = ?5
            "#,
        )
        .bind(job_id)
        .bind(next_retry.to_rfc3339())
        .bind(error)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await?;

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
                let last_retry_at = last_retry_str
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc));

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

    pub async fn update_retry_attempt(&self, job_id: &str, success: bool) -> Result<()> {
        if success {
            sqlx::query("DELETE FROM job_retries WHERE job_id = ?1")
                .bind(job_id)
                .execute(&self.pool)
                .await?;
            info!(
                "Retry record deleted for successfully retried job {}",
                job_id
            );
            return Ok(());
        }

        let retry = self.get_retry(job_id).await?;
        if retry.is_none() {
            warn!("Retry record not found for job {}", job_id);
            return Ok(());
        }

        let retry = retry.unwrap();
        let new_count = retry.retry_count + 1;

        if new_count >= retry.max_retries {
            sqlx::query("DELETE FROM job_retries WHERE job_id = ?1")
                .bind(job_id)
                .execute(&self.pool)
                .await?;
            info!(
                "Max retries reached for job {}, removing retry record",
                job_id
            );
            return Ok(());
        }

        let now = Utc::now();
        let next_retry = match new_count {
            1 => now + Duration::minutes(3),
            2 => now + Duration::minutes(6),
            _ => now + Duration::minutes(10),
        };

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
        Ok(())
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
            let last_retry_at = last_retry_str
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc));

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

    pub async fn delete_retry(&self, job_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM job_retries WHERE job_id = ?1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
