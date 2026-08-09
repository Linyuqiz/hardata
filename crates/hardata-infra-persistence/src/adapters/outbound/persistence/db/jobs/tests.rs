    use super::Database;
    use hardata_domain::{FileTransferState, Job, JobPath, JobStatus, JobType};
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
