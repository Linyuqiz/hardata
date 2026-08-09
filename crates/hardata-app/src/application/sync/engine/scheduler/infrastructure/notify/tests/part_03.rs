    #[tokio::test]
    async fn notify_job_failed_updates_original_sync_runtime_error_message_for_final_job() {
        let (temp_dir, db, scheduler) =
            create_scheduler("final-failed-updates-original-runtime-error").await;

        let mut original = Job::new(
            "job-final-parent-runtime-error".to_string(),
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
        original.status = JobStatus::Syncing;
        original.progress = 20;
        original.current_size = 200;
        original.total_size = 1000;
        db.save_job(&original).await.unwrap();

        scheduler.job_status_cache.insert(
            original.job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: original.job_id.clone(),
                status: JobStatus::Syncing,
                progress: 20,
                current_size: 200,
                total_size: 1000,
                region: original.region.clone(),
                error_message: Some("stale original error".to_string()),
                created_at: original.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        let final_job_id = format!("{}_final", original.job_id);
        let mut final_job = Job::new(
            final_job_id.clone(),
            original.source.clone(),
            original.dest.clone(),
        )
        .with_job_type(JobType::Once);
        final_job.status = JobStatus::Syncing;
        final_job.progress = 70;
        final_job.current_size = 700;
        final_job.total_size = 1000;
        db.save_job(&final_job).await.unwrap();

        scheduler.job_status_cache.insert(
            final_job_id.clone(),
            crate::application::sync::engine::scheduler::JobRuntimeStatus {
                job_id: final_job_id.clone(),
                status: JobStatus::Syncing,
                progress: 70,
                current_size: 700,
                total_size: 1000,
                region: final_job.region.clone(),
                error_message: None,
                created_at: final_job.created_at,
                updated_at: chrono::Utc::now(),
            },
        );

        scheduler
            .notify_job_failed(
                &final_job_id,
                "temporary network error",
                ErrorCategory::Retriable,
            )
            .await;

        let original_runtime = scheduler.get_job_status(&original.job_id).unwrap();
        assert_eq!(original_runtime.status, JobStatus::Failed);
        assert_eq!(original_runtime.progress, 70);
        assert_eq!(original_runtime.current_size, 700);
        assert_eq!(original_runtime.total_size, 1000);
        assert_eq!(
            original_runtime.error_message.as_deref(),
            Some("temporary network error")
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn notify_job_failed_persists_error_message_for_inactive_job() {
        let (temp_dir, db, scheduler) = create_scheduler("failed-persists-error-message").await;

        let mut job = Job::new(
            "job-failed-persists-error".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "dest.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Once);
        job.status = JobStatus::Syncing;
        job.progress = 61;
        job.current_size = 610;
        job.total_size = 1000;
        db.save_job(&job).await.unwrap();

        scheduler
            .notify_job_failed(&job.job_id, "remote write failed", ErrorCategory::Fatal)
            .await;

        let snapshot = db.load_job(&job.job_id).await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Failed);
        assert_eq!(
            snapshot.error_message.as_deref(),
            Some("remote write failed")
        );

        let _ = fs::remove_dir_all(temp_dir);
    }
