    #[tokio::test]
    async fn finalize_job_is_idempotent_after_final_transfer_completed() {
        let (temp_dir, db, scheduler) = create_scheduler("finalize-idempotent-completed").await;
        let mut job = Job::new(
            "job-finalize-idempotent-completed".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };

        let Json(first_response) = finalize_job(
            State(state.clone()),
            Path(public_job_id.clone()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert!(db
            .update_job_status(&public_job_id, JobStatus::Completed, 100, 1, 1, None)
            .await
            .unwrap());
        assert!(db
            .update_job_status(
                &internal_final_job_id(&public_job_id),
                JobStatus::Completed,
                100,
                1,
                1,
                None,
            )
            .await
            .unwrap());
        let Json(second_response) =
            finalize_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
                .await
                .unwrap();

        assert_eq!(first_response.job_id, public_job_id);
        assert_eq!(second_response.job_id, public_job_id);
        assert_eq!(
            scheduler
                .load_job_snapshot(&public_job_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
        assert_eq!(
            db.load_job(&internal_final_job_id(&public_job_id))
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );

        fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn cancel_job_is_idempotent_when_job_is_already_cancelled() {
        let (temp_dir, db, scheduler) = create_scheduler("cancel-idempotent").await;
        let mut job = Job::new(
            "job-cancel-idempotent".to_string(),
            JobPath {
                path: "/tmp/source.bin".to_string(),
                client_id: String::new(),
            },
            JobPath {
                path: "mirror/output.bin".to_string(),
                client_id: String::new(),
            },
        )
        .with_job_type(JobType::Sync);
        job.region = "local".to_string();
        let public_job_id = job.job_id.clone();
        db.save_job(&job).await.unwrap();

        let state = SyncApiState {
            scheduler: scheduler.clone(),
            regions: vec![],
            data_dir: temp_dir.join("sync-data").to_string_lossy().to_string(),
            allow_external_destinations: false,
            web_ui: false,
            api_token: None,
        };

        cancel_job(
            State(state.clone()),
            Path(public_job_id.clone()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        cancel_job(State(state), Path(public_job_id.clone()), HeaderMap::new())
            .await
            .unwrap();

        let snapshot = scheduler
            .load_job_snapshot(&public_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, JobStatus::Cancelled);

        fs::remove_dir_all(temp_dir).unwrap();
    }
