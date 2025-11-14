use crate::core::protocol::{ISyncMessage, MessageType, ReadBlockItem, ReadBlockRequest};
use crate::sync::net::quic::QuicClient;
use crate::util::error::{HarDataError, Result};
use bytes::Bytes;
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
            });
        }

        let total_items = items.len();
        let total_bytes: u64 = items.iter().map(|item| item.length).sum();

        info!(
            "Batch read from Agent: {} chunks, {} bytes total, job_id: {}",
            total_items, total_bytes, job_id
        );

        let (mut send, mut recv) = connection.open_bi().await?;

        let mut succeeded = 0;
        let mut failed = 0;
        let mut total_transferred = 0u64;

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
                    "Failed to send ReadBlockRequest {}/{}: {}",
                    idx + 1,
                    total_items,
                    e
                );
                failed += 1;
                continue;
            }

            match self.receive_read_response(&mut recv).await {
                Ok(data) => {
                    match crate::util::file_ops::write_file_range(
                        &item.dest_path,
                        item.dest_offset,
                        &data,
                    )
                    .await
                    {
                        Ok(_) => {
                            succeeded += 1;
                            total_transferred += data.len() as u64;
                        }
                        Err(e) => {
                            warn!("Failed to write file {}/{}: {}", idx + 1, total_items, e);
                            failed += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to receive ReadResponse {}/{}: {}",
                        idx + 1,
                        total_items,
                        e
                    );
                    failed += 1;
                }
            }
        }

        send.finish()?;

        info!(
            "Batch read completed: {}/{} succeeded, {} bytes",
            succeeded, total_items, total_transferred
        );

        Ok(BatchTransferResult {
            succeeded,
            failed,
            total_bytes: total_transferred,
        })
    }
}
