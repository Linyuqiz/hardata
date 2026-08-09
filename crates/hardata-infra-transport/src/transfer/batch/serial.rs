use crate::adapters::outbound::transport::quic::QuicClient;
use bytes::Bytes;
use hardata_protocol::{ISyncMessage, MessageType, ReadBlockItem, ReadBlockRequest};
use hardata_shared::error::{HarDataError, Result};
use quinn::Connection;
use tracing::{info, warn};

use super::types::{BatchTransferItem, BatchTransferResult};

impl QuicClient {
    pub async fn read_and_write_batch(
        &self,
        connection: &Connection,
        items: Vec<BatchTransferItem>,
        job_id: &str,
    ) -> Result<BatchTransferResult> {
        if items.is_empty() {
            return Ok(BatchTransferResult {
                succeeded: 0,
                failed: 0,
                total_bytes: 0,
                cancelled: false,
                succeeded_indices: Vec::new(),
                failed_indices: Vec::new(),
            });
        }

        let total_items = items.len();
        let total_bytes: u64 = items.iter().map(|item| item.length).sum();

        info!(
            operation = "transport.batch_read_started",
            job_id = %job_id,
            item_count = total_items,
            total_bytes,
            "batch read started"
        );

        let (mut send, mut recv) = connection.open_bi().await?;

        let mut succeeded = 0;
        let mut failed = 0;
        let mut total_transferred = 0u64;
        let mut succeeded_indices = Vec::new();
        let mut failed_indices = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            let read_request = ReadBlockRequest {
                items: vec![ReadBlockItem {
                    file_path: item.source_path.clone(),
                    offset: item.source_offset,
                    length: item.length,
                }],
            };

            let request_bytes = bincode::serialize(&read_request).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to serialize ReadBlockRequest: {}",
                    e
                ))
            })?;

            let request =
                ISyncMessage::new(MessageType::ReadBlockRequest, Bytes::from(request_bytes));

            if let Err(e) = send.write_all(&request.encode()).await {
                warn!(
                    operation = "transport.batch_request_failed",
                    job_id = %job_id,
                    item_index = idx,
                    item_count = total_items,
                    error = %e,
                    "batch read request send failed"
                );
                failed += 1;
                failed_indices.push(idx);
                continue;
            }

            match self.receive_read_response(&mut recv).await {
                Ok(data) => {
                    match hardata_shared::file_ops::write_file_range(
                        &item.dest_path,
                        item.dest_offset,
                        &data,
                    )
                    .await
                    {
                        Ok(_) => {
                            succeeded += 1;
                            total_transferred += data.len() as u64;
                            succeeded_indices.push(idx);
                        }
                        Err(e) => {
                            warn!(operation = "transport.batch_write_failed", job_id = %job_id, item_index = idx, item_count = total_items, error = %e, "batch read destination write failed");
                            failed += 1;
                            failed_indices.push(idx);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        operation = "transport.batch_response_failed",
                        job_id = %job_id,
                        item_index = idx,
                        item_count = total_items,
                        error = %e,
                        "batch read response failed"
                    );
                    failed += 1;
                    failed_indices.push(idx);
                }
            }
        }

        send.finish()?;

        info!(
            operation = "transport.batch_read_completed",
            job_id = %job_id,
            succeeded,
            failed,
            total_items,
            total_bytes = total_transferred,
            "batch read completed"
        );

        Ok(BatchTransferResult {
            succeeded,
            failed,
            total_bytes: total_transferred,
            cancelled: false,
            succeeded_indices,
            failed_indices,
        })
    }
}
