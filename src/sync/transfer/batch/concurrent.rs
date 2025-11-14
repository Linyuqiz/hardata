use crate::core::protocol::{ISyncMessage, MessageType, ReadBlockItem, ReadBlockRequest};
use crate::sync::net::quic::QuicClient;
use crate::util::compression::decompress_with_algorithm;
use crate::util::error::Result;
use bytes::Bytes;
use quinn::Connection;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

use super::types::{BatchTransferItem, BatchTransferResult, ProgressCallback};

use crate::core::constants::MAX_ITEMS_PER_REQUEST;

impl QuicClient {
    pub async fn read_and_write_batch_concurrent(
        &self,
        connection: &Connection,
        items: Vec<BatchTransferItem>,
        job_id: &str,
        max_concurrent_streams: usize,
    ) -> Result<BatchTransferResult> {
        self.read_and_write_batch_concurrent_with_progress(
            connection,
            items,
            job_id,
            max_concurrent_streams,
            None,
        )
        .await
    }

    pub async fn read_and_write_batch_concurrent_with_progress(
        &self,
        connection: &Connection,
        items: Vec<BatchTransferItem>,
        job_id: &str,
        max_concurrent_streams: usize,
        progress_callback: Option<ProgressCallback>,
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

        let chunk_size = total_items.div_ceil(max_concurrent_streams);
        let item_chunks: Vec<Vec<BatchTransferItem>> =
            items.chunks(chunk_size).map(|c| c.to_vec()).collect();

        debug!(
            "QUIC batch transfer: {} chunks, {} bytes, {} streams, {} per stream, job_id: {}",
            total_items,
            total_bytes,
            item_chunks.len(),
            chunk_size,
            job_id
        );

        let succeeded = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let total_transferred = Arc::new(AtomicU64::new(0));

        let tasks: Vec<_> = item_chunks
            .into_iter()
            .enumerate()
            .map(|(group_idx, group_items)| {
                let connection = connection.clone();
                let succeeded = succeeded.clone();
                let failed = failed.clone();
                let total_transferred = total_transferred.clone();
                let progress_callback = progress_callback.clone();

                tokio::spawn(async move {
                    let stream_result = connection.open_bi().await;
                    let (mut send, mut recv) = match stream_result {
                        Ok(streams) => streams,
                        Err(e) => {
                            warn!("QUIC: Failed to open stream for group {}: {}", group_idx, e);
                            failed.fetch_add(group_items.len(), Ordering::Relaxed);
                            return;
                        }
                    };

                    debug!(
                        "QUIC: Group {} processing {} items",
                        group_idx,
                        group_items.len()
                    );

                    for batch in group_items.chunks(MAX_ITEMS_PER_REQUEST) {
                        if let Err(e) = process_batch_on_stream(
                            &mut send,
                            &mut recv,
                            batch,
                            &succeeded,
                            &failed,
                            &total_transferred,
                            &progress_callback,
                        )
                        .await
                        {
                            warn!("QUIC Group {}: Batch failed: {}", group_idx, e);
                        }
                    }

                    if let Err(e) = send.finish() {
                        warn!(
                            "QUIC: Failed to finish stream for group {}: {}",
                            group_idx, e
                        );
                    }

                    debug!("QUIC: Group {} completed", group_idx);
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
            "QUIC batch transfer completed: {}/{} succeeded, {} bytes",
            final_succeeded, total_items, final_transferred
        );

        Ok(BatchTransferResult {
            succeeded: final_succeeded,
            failed: final_failed,
            total_bytes: final_transferred,
        })
    }
}

async fn process_batch_on_stream(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    items: &[BatchTransferItem],
    succeeded: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    total_transferred: &Arc<AtomicU64>,
    progress_callback: &Option<ProgressCallback>,
) -> std::result::Result<(), String> {
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

    send.write_all(&request.encode())
        .await
        .map_err(|e| format!("Failed to send batch request: {}", e))?;

    let response = QuicClient::receive_batch_read_response_static(recv)
        .await
        .map_err(|e| format!("Failed to receive batch response: {}", e))?;

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
                debug!("Item {} completed: {} bytes", idx, bytes_written);
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
