use crate::core::protocol::{ISyncMessage, MessageType, ReadBlockItem, ReadBlockRequest};
use crate::sync::transfer::batch::ProgressCallback;
use crate::util::compression::decompress_with_algorithm;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

use super::client::TcpClient;

use crate::core::constants::MAX_ITEMS_PER_REQUEST;

impl TcpClient {
    pub async fn read_and_write_batch_concurrent(
        &self,
        items: Vec<crate::sync::transfer::batch::BatchTransferItem>,
        job_id: &str,
        concurrency: usize,
    ) -> crate::util::error::Result<crate::sync::transfer::batch::BatchTransferResult> {
        self.read_and_write_batch_concurrent_with_progress(items, job_id, concurrency, None)
            .await
    }

    pub async fn read_and_write_batch_concurrent_with_progress(
        &self,
        items: Vec<crate::sync::transfer::batch::BatchTransferItem>,
        job_id: &str,
        concurrency: usize,
        progress_callback: Option<ProgressCallback>,
    ) -> crate::util::error::Result<crate::sync::transfer::batch::BatchTransferResult> {
        if items.is_empty() {
            return Ok(crate::sync::transfer::batch::BatchTransferResult {
                succeeded: 0,
                failed: 0,
                total_bytes: 0,
            });
        }

        let total_items = items.len();
        let total_bytes: u64 = items.iter().map(|item| item.length).sum();

        debug!(
            "TCP concurrent batch read: {} chunks, {} bytes, job_id: {}, concurrency: {}",
            total_items, total_bytes, job_id, concurrency
        );

        let succeeded = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        let total_transferred = Arc::new(AtomicU64::new(0));

        let chunk_size = total_items.div_ceil(concurrency);
        let item_chunks: Vec<_> = items.chunks(chunk_size).map(|c| c.to_vec()).collect();

        debug!(
            "TCP: Split {} items into {} groups ({} items per group)",
            total_items,
            item_chunks.len(),
            chunk_size
        );

        let tasks: Vec<_> = item_chunks
            .into_iter()
            .enumerate()
            .map(|(group_idx, group_items)| {
                let client = self.clone();
                let succeeded = succeeded.clone();
                let failed = failed.clone();
                let total_transferred = total_transferred.clone();
                let progress_callback = progress_callback.clone();

                tokio::spawn(async move {
                    let mut conn = match client.get_pooled_connection().await {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(
                                "TCP: Failed to get pooled connection for group {}: {}",
                                group_idx, e
                            );
                            failed.fetch_add(group_items.len() as u64, Ordering::Relaxed);
                            return;
                        }
                    };

                    debug!(
                        "TCP: Group {} processing {} items (pooled connection)",
                        group_idx,
                        group_items.len()
                    );

                    for batch in group_items.chunks(MAX_ITEMS_PER_REQUEST) {
                        if let Err(e) = process_batch(
                            &mut conn,
                            batch,
                            &succeeded,
                            &failed,
                            &total_transferred,
                            &progress_callback,
                        )
                        .await
                        {
                            warn!("TCP Group {}: Batch failed: {}", group_idx, e);
                        }
                    }

                    debug!(
                        "TCP: Group {} completed (connection returned to pool)",
                        group_idx
                    );
                })
            })
            .collect();

        for task in tasks {
            let _ = task.await;
        }

        let final_succeeded = succeeded.load(Ordering::Relaxed);
        let final_failed = failed.load(Ordering::Relaxed);
        let final_transferred = total_transferred.load(Ordering::Relaxed);

        debug!(
            "TCP concurrent batch transfer completed: {}/{} succeeded, {} bytes transferred",
            final_succeeded, total_items, final_transferred
        );

        Ok(crate::sync::transfer::batch::BatchTransferResult {
            succeeded: final_succeeded as usize,
            failed: final_failed as usize,
            total_bytes: final_transferred,
        })
    }
}

async fn process_batch(
    stream: &mut tokio::net::TcpStream,
    items: &[crate::sync::transfer::batch::BatchTransferItem],
    succeeded: &Arc<AtomicU64>,
    failed: &Arc<AtomicU64>,
    total_transferred: &Arc<AtomicU64>,
    progress_callback: &Option<ProgressCallback>,
) -> Result<(), String> {
    if items.is_empty() {
        return Ok(());
    }

    let read_items: Vec<ReadBlockItem> = items
        .iter()
        .map(|item| ReadBlockItem {
            file_path: item.source_path.clone(),
            offset: item.source_offset,
            length: item.length,
        })
        .collect();

    let read_request = ReadBlockRequest { items: read_items };

    let request_bytes = bincode::serialize(&read_request)
        .map_err(|e| format!("Failed to serialize request: {}", e))?;

    let request = ISyncMessage::new(MessageType::ReadBlockRequest, Bytes::from(request_bytes));

    stream
        .write_all(&request.encode())
        .await
        .map_err(|e| format!("Failed to send batch request: {}", e))?;

    let mut header_buf = vec![0u8; ISyncMessage::HEADER_SIZE];
    stream
        .read_exact(&mut header_buf)
        .await
        .map_err(|e| format!("Failed to read response header: {}", e))?;

    let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)
        .map_err(|e| format!("Failed to decode header: {}", e))?;

    if msg_type != MessageType::ReadBlockResponse {
        failed.fetch_add(items.len() as u64, Ordering::Relaxed);
        return Err(format!("Expected ReadBlockResponse, got {:?}", msg_type));
    }

    let mut payload = vec![0u8; payload_len as usize];
    if payload_len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| format!("Failed to read response payload: {}", e))?;
    }

    let response: crate::core::ReadBlockResponse = bincode::deserialize(&payload)
        .map_err(|e| format!("Failed to deserialize response: {}", e))?;

    for result in response.results {
        let idx = result.index as usize;
        if idx >= items.len() {
            warn!("Invalid result index: {} >= {}", idx, items.len());
            failed.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let item = &items[idx];

        if !result.success {
            failed.fetch_add(1, Ordering::Relaxed);
            warn!(
                "Read failed for item {}: {}",
                idx,
                result.error.unwrap_or_else(|| "Unknown error".to_string())
            );
            continue;
        }

        let data = if let Some(compression_info) = &result.compression {
            match decompress_with_algorithm(&result.data, &compression_info.algorithm) {
                Ok(decompressed) => {
                    if decompressed.len() as u64 != compression_info.original_size {
                        warn!(
                            "Decompressed size mismatch: expected {}, got {}",
                            compression_info.original_size,
                            decompressed.len()
                        );
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    decompressed
                }
                Err(e) => {
                    warn!("Decompression failed for item {}: {}", idx, e);
                    failed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
        } else {
            result.data
        };

        match crate::util::file_ops::write_file_range(&item.dest_path, item.dest_offset, &data)
            .await
        {
            Ok(_) => {
                let bytes_written = data.len() as u64;
                succeeded.fetch_add(1, Ordering::Relaxed);
                total_transferred.fetch_add(bytes_written, Ordering::Relaxed);
                if let Some(callback) = progress_callback {
                    callback(bytes_written);
                }
            }
            Err(e) => {
                failed.fetch_add(1, Ordering::Relaxed);
                warn!("Failed to write item {}: {}", idx, e);
            }
        }
    }

    Ok(())
}
