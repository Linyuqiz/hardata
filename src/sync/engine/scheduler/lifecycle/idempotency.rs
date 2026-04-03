use crate::sync::storage::db::ApiIdempotencyRecord;
use crate::util::error::Result;

use super::super::core::SyncScheduler;

impl SyncScheduler {
    pub async fn reserve_create_job_idempotency_key(
        &self,
        idempotency_key: &str,
        request_fingerprint: &str,
        job_id: &str,
    ) -> Result<ApiIdempotencyRecord> {
        self.db
            .reserve_api_idempotency_key("create_job", idempotency_key, request_fingerprint, job_id)
            .await
    }
}
