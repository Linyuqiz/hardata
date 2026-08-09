impl Database {
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
