use crate::util::error::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::Row;
use std::str::FromStr;
use tracing::info;

use super::types::Database;

impl Database {
    pub async fn new(db_path: &str) -> Result<Self> {
        let file_path = db_path
            .strip_prefix("sqlite://")
            .or_else(|| db_path.strip_prefix("sqlite:"))
            .unwrap_or(db_path);

        if let Some(parent_dir) = std::path::Path::new(file_path).parent() {
            if !parent_dir.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent_dir).await?;
            }
        }

        let options = SqliteConnectOptions::from_str(db_path)?.create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        let db = Self { pool };
        db.init_schema().await?;

        Ok(db)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                region TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_client_id TEXT NOT NULL,
                dest_path TEXT NOT NULL,
                dest_client_id TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                current_size INTEGER NOT NULL DEFAULT 0,
                total_size INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 100,
                round_id INTEGER NOT NULL DEFAULT 0,
                is_last_round INTEGER NOT NULL DEFAULT 0,
                exclude_regex TEXT,
                include_regex TEXT,
                job_type TEXT NOT NULL DEFAULT 'once',
                options TEXT NOT NULL DEFAULT '{"compression":"zstd","workers":10,"chunk_size":4194304}',
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        let job_columns = sqlx::query("PRAGMA table_info(jobs)")
            .fetch_all(&self.pool)
            .await?;
        let has_error_message = job_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "error_message")
                .unwrap_or(false)
        });
        let has_round_id = job_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "round_id")
                .unwrap_or(false)
        });
        let has_is_last_round = job_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "is_last_round")
                .unwrap_or(false)
        });

        if !has_error_message {
            sqlx::query("ALTER TABLE jobs ADD COLUMN error_message TEXT")
                .execute(&self.pool)
                .await?;
        }

        if !has_round_id {
            sqlx::query("ALTER TABLE jobs ADD COLUMN round_id INTEGER NOT NULL DEFAULT 0")
                .execute(&self.pool)
                .await?;
        }

        if !has_is_last_round {
            sqlx::query("ALTER TABLE jobs ADD COLUMN is_last_round INTEGER NOT NULL DEFAULT 0")
                .execute(&self.pool)
                .await?;
        }

        sqlx::query(
            r#"
            UPDATE jobs
            SET round_id = CASE WHEN round_id < 1 THEN 1 ELSE round_id END,
                is_last_round = 1
            WHERE job_id LIKE '%\_final' ESCAPE '\'
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            UPDATE jobs
            SET round_id = CASE WHEN round_id < 1 THEN 1 ELSE round_id END,
                is_last_round = 1
            WHERE job_type = 'sync'
              AND job_id NOT LIKE '%\_final' ESCAPE '\'
              AND EXISTS (
                  SELECT 1
                  FROM jobs AS final_jobs
                  WHERE final_jobs.job_id = jobs.job_id || '_final'
              )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS transfer_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                total_chunks INTEGER NOT NULL,
                completed_chunks TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(job_id, file_path)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_transfer_states_job_id ON transfer_states(job_id);
            CREATE INDEX IF NOT EXISTS idx_transfer_states_file_path ON transfer_states(file_path);
            "#,
        )
        .execute(&self.pool)
        .await?;

        let transfer_state_columns = sqlx::query("PRAGMA table_info(transfer_states)")
            .fetch_all(&self.pool)
            .await?;
        let has_source_size = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "source_size")
                .unwrap_or(false)
        });
        let has_source_modified = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "source_modified")
                .unwrap_or(false)
        });
        let has_source_change_time = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "source_change_time")
                .unwrap_or(false)
        });
        let has_source_inode = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "source_inode")
                .unwrap_or(false)
        });
        let has_dest_modified = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "dest_modified")
                .unwrap_or(false)
        });
        let has_dest_change_time = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "dest_change_time")
                .unwrap_or(false)
        });
        let has_dest_inode = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "dest_inode")
                .unwrap_or(false)
        });
        let has_dest_size = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "dest_size")
                .unwrap_or(false)
        });
        let has_cache_only = transfer_state_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "cache_only")
                .unwrap_or(false)
        });

        if !has_source_size {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN source_size INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_source_modified {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN source_modified INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_source_change_time {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN source_change_time INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_source_inode {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN source_inode INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_dest_modified {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN dest_modified INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_dest_change_time {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN dest_change_time INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_dest_inode {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN dest_inode INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_dest_size {
            sqlx::query("ALTER TABLE transfer_states ADD COLUMN dest_size INTEGER")
                .execute(&self.pool)
                .await?;
        }

        if !has_cache_only {
            sqlx::query(
                "ALTER TABLE transfer_states ADD COLUMN cache_only INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tmp_transfer_paths (
                job_id TEXT NOT NULL,
                path TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (job_id, path)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_tmp_transfer_paths_job_id ON tmp_transfer_paths(job_id);
            CREATE INDEX IF NOT EXISTS idx_tmp_transfer_paths_path ON tmp_transfer_paths(path);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS job_retries (
                job_id TEXT PRIMARY KEY,
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                last_retry_at TEXT,
                next_retry_at TEXT NOT NULL,
                last_error TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_job_retries_next_retry ON job_retries(next_retry_at);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS api_idempotency_keys (
                scope TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                job_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (scope, idempotency_key)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_api_idempotency_keys_job_id
            ON api_idempotency_keys(job_id);
            "#,
        )
        .execute(&self.pool)
        .await?;

        info!("Sync database schema initialized");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::SqlitePool;
    use std::fs;
    use std::str::FromStr;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-schema-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn init_schema_backfills_round_metadata_for_legacy_final_jobs() {
        let temp_dir = create_temp_dir("legacy-final-round-backfill");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let raw_pool = SqlitePool::connect_with(
            SqliteConnectOptions::from_str(&db_path)
                .unwrap()
                .create_if_missing(true),
        )
        .await
        .unwrap();

        sqlx::query(
            r#"
            CREATE TABLE jobs (
                job_id TEXT PRIMARY KEY,
                region TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_client_id TEXT NOT NULL,
                dest_path TEXT NOT NULL,
                dest_client_id TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                current_size INTEGER NOT NULL DEFAULT 0,
                total_size INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 100,
                exclude_regex TEXT,
                include_regex TEXT,
                job_type TEXT NOT NULL DEFAULT 'once',
                options TEXT NOT NULL DEFAULT '{"compression":"zstd","workers":10,"chunk_size":4194304}',
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let now = chrono::Utc::now().to_rfc3339();
        let options = r#"{"compression":"zstd","workers":10,"chunk_size":4194304}"#;
        let empty_filters = "[]";

        sqlx::query(
            r#"
            INSERT INTO jobs (
                job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                status, progress, current_size, total_size, priority,
                exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            ) VALUES (?1, 'default', '/tmp/source.bin', '', 'dest.bin', '', 'failed', 0, 0, 0, 100, ?2, ?2, 'sync', ?3, NULL, ?4, ?4)
            "#,
        )
        .bind("legacy-sync")
        .bind(empty_filters)
        .bind(options)
        .bind(&now)
        .execute(&raw_pool)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO jobs (
                job_id, region, source_path, source_client_id, dest_path, dest_client_id,
                status, progress, current_size, total_size, priority,
                exclude_regex, include_regex, job_type, options, error_message, created_at, updated_at
            ) VALUES (?1, 'default', '/tmp/source.bin', '', 'dest.bin', '', 'failed', 0, 0, 0, 200, ?2, ?2, 'once', ?3, NULL, ?4, ?4)
            "#,
        )
        .bind("legacy-sync_final")
        .bind(empty_filters)
        .bind(options)
        .bind(&now)
        .execute(&raw_pool)
        .await
        .unwrap();

        raw_pool.close().await;

        let db = Database::new(&db_path).await.unwrap();
        let original = db.load_job("legacy-sync").await.unwrap().unwrap();
        let final_job = db.load_job("legacy-sync_final").await.unwrap().unwrap();

        assert_eq!(original.round_id, 1);
        assert!(original.is_last_round);
        assert_eq!(final_job.round_id, 1);
        assert!(final_job.is_last_round);

        let _ = fs::remove_dir_all(temp_dir);
    }
}
