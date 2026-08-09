    use super::{StateOperation, TransferManagerPool};
    use crate::adapters::outbound::persistence::db::Database;
    use crate::domain::FileTransferState;
    use sqlx::SqlitePool;
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-transfer-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn clear_job_states_ignores_stale_save_operations() {
        let temp_dir = create_temp_dir("stale-save");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let state = FileTransferState::new("nested/file.bin".to_string(), 8);
        let job_id = "job-stale";

        pool.enqueue_state_operation(|response| StateOperation::Save {
            job_id: job_id.to_string(),
            generation: 0,
            state: state.clone(),
            response,
        })
        .await
        .unwrap();
        pool.clear_job_states(job_id).await.unwrap();
        pool.enqueue_state_operation(|response| StateOperation::Save {
            job_id: job_id.to_string(),
            generation: 0,
            state,
            response,
        })
        .await
        .unwrap();

        pool.shutdown().await;

        let loaded = db
            .load_transfer_state(job_id, "nested/file.bin")
            .await
            .unwrap();
        assert!(loaded.is_none());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn save_state_after_clear_uses_new_generation() {
        let temp_dir = create_temp_dir("new-generation");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let state = FileTransferState::new("nested/file.bin".to_string(), 8);
        let job_id = "job-fresh";

        pool.clear_job_states(job_id).await.unwrap();
        pool.save_state(job_id, &state).await.unwrap();
        pool.shutdown().await;

        let loaded = db
            .load_transfer_state(job_id, "nested/file.bin")
            .await
            .unwrap();
        assert!(loaded.is_some());

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn load_state_returns_error_when_db_lookup_fails() {
        let temp_dir = create_temp_dir("load-state-db-failure");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db);

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query("DROP TABLE transfer_states")
            .execute(&raw_pool)
            .await
            .unwrap();

        let err = pool
            .load_state("job-load-failure", "nested/file.bin")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such table: transfer_states"));

        raw_pool.close().await;
        pool.shutdown().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn save_state_returns_error_and_reverts_cache_when_db_persist_fails() {
        let temp_dir = create_temp_dir("save-state-db-failure");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let job_id = "job-save-failure";
        let state = FileTransferState::new("nested/file.bin".to_string(), 8);

        let raw_pool = SqlitePool::connect(&db_path).await.unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER reject_transfer_state_insert
            BEFORE INSERT ON transfer_states
            WHEN NEW.job_id = 'job-save-failure'
            BEGIN
                SELECT RAISE(FAIL, 'reject transfer state insert');
            END;
            "#,
        )
        .execute(&raw_pool)
        .await
        .unwrap();

        let err = pool.save_state(job_id, &state).await.unwrap_err();
        assert!(err.to_string().contains("reject transfer state insert"));
        assert!(pool
            .load_state(job_id, "nested/file.bin")
            .await
            .unwrap()
            .is_none());
        assert!(db
            .load_transfer_state(job_id, "nested/file.bin")
            .await
            .unwrap()
            .is_none());

        raw_pool.close().await;
        pool.shutdown().await;
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn clear_job_tmp_write_paths_ignores_stale_tmp_save_operations() {
        let temp_dir = create_temp_dir("stale-tmp-save");
        let db_path = format!("sqlite://{}", temp_dir.join("state.db").display());
        let db = Arc::new(Database::new(&db_path).await.unwrap());
        let pool = TransferManagerPool::new(db.clone());
        let job_id = "job-tmp-stale";
        let tmp_path = temp_dir.join("stale.tmp");
        let tmp_path = tmp_path.to_string_lossy().to_string();

        pool.enqueue_state_operation(|response| StateOperation::TmpSave {
            job_id: job_id.to_string(),
            generation: 0,
            path: tmp_path.clone(),
            response,
        })
        .await
        .unwrap();
        pool.clear_job_tmp_write_paths(job_id).await.unwrap();
        pool.enqueue_state_operation(|response| StateOperation::TmpSave {
            job_id: job_id.to_string(),
            generation: 0,
            path: tmp_path.clone(),
            response,
        })
        .await
        .unwrap();

        pool.shutdown().await;

        let loaded = db.load_tmp_transfer_paths_by_job(job_id).await.unwrap();
        assert!(loaded.is_empty());

        let _ = fs::remove_dir_all(temp_dir);
    }
