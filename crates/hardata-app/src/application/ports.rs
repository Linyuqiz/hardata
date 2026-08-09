use crate::domain::FileTransferState;
use crate::shared::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait TransferStateStore: Send + Sync {
    async fn save_transfer_state(&self, job_id: &str, state: &FileTransferState) -> Result<()>;
    async fn load_transfer_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> Result<Option<FileTransferState>>;
    async fn delete_transfer_state(&self, job_id: &str, file_path: &str) -> Result<()>;
    async fn delete_job_transfer_states(&self, job_id: &str) -> Result<()>;
    async fn save_tmp_transfer_path(&self, job_id: &str, path: &str) -> Result<()>;
    async fn delete_tmp_transfer_path(&self, job_id: &str, path: &str) -> Result<()>;
    async fn delete_job_tmp_transfer_paths(&self, job_id: &str) -> Result<()>;
    async fn load_tmp_transfer_paths_by_job(&self, job_id: &str) -> Result<Vec<String>>;
}

// Keep the application port independent from the concrete SQLite adapter.
#[async_trait]
impl TransferStateStore for hardata_infra_persistence::Database {
    async fn save_transfer_state(&self, job_id: &str, state: &FileTransferState) -> Result<()> {
        hardata_infra_persistence::Database::save_transfer_state(self, job_id, state).await
    }

    async fn load_transfer_state(
        &self,
        job_id: &str,
        file_path: &str,
    ) -> Result<Option<FileTransferState>> {
        hardata_infra_persistence::Database::load_transfer_state(self, job_id, file_path).await
    }

    async fn delete_transfer_state(&self, job_id: &str, file_path: &str) -> Result<()> {
        hardata_infra_persistence::Database::delete_transfer_state(self, job_id, file_path).await
    }

    async fn delete_job_transfer_states(&self, job_id: &str) -> Result<()> {
        hardata_infra_persistence::Database::delete_job_transfer_states(self, job_id).await
    }

    async fn save_tmp_transfer_path(&self, job_id: &str, path: &str) -> Result<()> {
        hardata_infra_persistence::Database::save_tmp_transfer_path(self, job_id, path).await
    }

    async fn delete_tmp_transfer_path(&self, job_id: &str, path: &str) -> Result<()> {
        hardata_infra_persistence::Database::delete_tmp_transfer_path(self, job_id, path).await
    }

    async fn delete_job_tmp_transfer_paths(&self, job_id: &str) -> Result<()> {
        hardata_infra_persistence::Database::delete_job_tmp_transfer_paths(self, job_id).await
    }

    async fn load_tmp_transfer_paths_by_job(&self, job_id: &str) -> Result<Vec<String>> {
        hardata_infra_persistence::Database::load_tmp_transfer_paths_by_job(self, job_id).await
    }
}
