use crate::util::error::{HarDataError, Result};
use sqlx::Row;

use super::types::Database;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiIdempotencyRecord {
    pub scope: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub job_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Database {
    pub async fn reserve_api_idempotency_key(
        &self,
        scope: &str,
        idempotency_key: &str,
        request_fingerprint: &str,
        job_id: &str,
    ) -> Result<ApiIdempotencyRecord> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO api_idempotency_keys (
                scope, idempotency_key, request_fingerprint, job_id, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(scope, idempotency_key) DO UPDATE SET
                updated_at = excluded.updated_at
            WHERE api_idempotency_keys.request_fingerprint = excluded.request_fingerprint
            "#,
        )
        .bind(scope)
        .bind(idempotency_key)
        .bind(request_fingerprint)
        .bind(job_id)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        self.load_api_idempotency_record(scope, idempotency_key)
            .await?
            .ok_or_else(|| {
                HarDataError::Unknown(format!(
                    "api idempotency record {scope}:{idempotency_key} missing after reserve"
                ))
            })
    }

    pub async fn load_api_idempotency_record(
        &self,
        scope: &str,
        idempotency_key: &str,
    ) -> Result<Option<ApiIdempotencyRecord>> {
        let row = sqlx::query(
            r#"
            SELECT scope, idempotency_key, request_fingerprint, job_id, created_at, updated_at
            FROM api_idempotency_keys
            WHERE scope = ?1 AND idempotency_key = ?2
            "#,
        )
        .bind(scope)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;

        row.map(row_to_api_idempotency_record).transpose()
    }
}

fn row_to_api_idempotency_record(row: sqlx::sqlite::SqliteRow) -> Result<ApiIdempotencyRecord> {
    let created_at_str: String = row.try_get("created_at")?;
    let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
        .map_err(|e| HarDataError::SerializationError(format!("Invalid created_at: {}", e)))?
        .with_timezone(&chrono::Utc);

    let updated_at_str: String = row.try_get("updated_at")?;
    let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_str)
        .map_err(|e| HarDataError::SerializationError(format!("Invalid updated_at: {}", e)))?
        .with_timezone(&chrono::Utc);

    Ok(ApiIdempotencyRecord {
        scope: row.try_get("scope")?,
        idempotency_key: row.try_get("idempotency_key")?,
        request_fingerprint: row.try_get("request_fingerprint")?,
        job_id: row.try_get("job_id")?,
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::Database;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-idempotency-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn reserve_api_idempotency_key_reuses_existing_job_id_for_same_fingerprint() {
        let temp_dir = create_temp_dir("reuse-existing");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Database::new(&db_path).await.unwrap();

        let first = db
            .reserve_api_idempotency_key("create_job", "idem-1", "fingerprint-a", "job-a")
            .await
            .unwrap();
        let second = db
            .reserve_api_idempotency_key("create_job", "idem-1", "fingerprint-a", "job-b")
            .await
            .unwrap();

        assert_eq!(first.job_id, "job-a");
        assert_eq!(second.job_id, "job-a");
        assert_eq!(second.request_fingerprint, "fingerprint-a");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn reserve_api_idempotency_key_preserves_existing_fingerprint_on_conflict() {
        let temp_dir = create_temp_dir("preserve-conflict");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Database::new(&db_path).await.unwrap();

        db.reserve_api_idempotency_key("create_job", "idem-1", "fingerprint-a", "job-a")
            .await
            .unwrap();

        let record = db
            .reserve_api_idempotency_key("create_job", "idem-1", "fingerprint-b", "job-b")
            .await
            .unwrap();

        assert_eq!(record.job_id, "job-a");
        assert_eq!(record.request_fingerprint, "fingerprint-a");

        let _ = fs::remove_dir_all(temp_dir);
    }
}
