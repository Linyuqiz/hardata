use crate::util::error::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
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
                exclude_regex TEXT,
                include_regex TEXT,
                job_type TEXT NOT NULL DEFAULT 'once',
                options TEXT NOT NULL DEFAULT '{"compression":"zstd","workers":10,"chunk_size":4194304}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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

        info!("Sync database schema initialized");
        Ok(())
    }
}
