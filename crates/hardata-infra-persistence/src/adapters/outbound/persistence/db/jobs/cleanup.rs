impl Database {
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
        info!(
            operation = "job.retention_cleanup_completed",
            deleted_count = deleted,
            older_than_days = days,
            "old job cleanup completed"
        );

        Ok(deleted)
    }
}
