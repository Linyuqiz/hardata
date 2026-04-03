use crate::core::constants::MAX_ITEMS_PER_REQUEST;
use crate::core::protocol::{ISyncMessage, MessageType, ReadBlockItem, ReadBlockRequest};
use crate::sync::transfer::batch::{
    BatchTransferItem, BatchTransferResult, CancelCallback, ProgressCallback,
};
use crate::util::compression::decompress_with_algorithm;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

use super::client::TcpClient;

#[derive(Default)]
struct BatchOutcome {
    cancelled: bool,
    succeeded_indices: Vec<usize>,
    failed_indices: Vec<usize>,
    transferred_bytes: u64,
}

impl BatchOutcome {
    fn merge(&mut self, mut other: Self) {
        self.cancelled |= other.cancelled;
        self.succeeded_indices.append(&mut other.succeeded_indices);
        self.failed_indices.append(&mut other.failed_indices);
        self.transferred_bytes += other.transferred_bytes;
    }
}

fn is_cancelled_batch_error(error: &str) -> bool {
    error.contains("Job cancelled by user")
}

fn record_batch_failure(
    outcome: &mut BatchOutcome,
    batch: &[(usize, BatchTransferItem)],
    error: &str,
) -> bool {
    outcome
        .failed_indices
        .extend(batch.iter().map(|(idx, _)| *idx));
    if is_cancelled_batch_error(error) {
        outcome.cancelled = true;
        return true;
    }
    false
}

fn empty_batch_transfer_result() -> BatchTransferResult {
    BatchTransferResult {
        succeeded: 0,
        failed: 0,
        total_bytes: 0,
        cancelled: false,
        succeeded_indices: Vec::new(),
        failed_indices: Vec::new(),
    }
}

fn normalize_batch_concurrency(concurrency: usize) -> usize {
    concurrency.max(1)
}

fn finalize_batch_outcome(mut outcome: BatchOutcome) -> BatchTransferResult {
    outcome.succeeded_indices.sort_unstable();
    outcome.failed_indices.sort_unstable();

    BatchTransferResult {
        succeeded: outcome.succeeded_indices.len(),
        failed: outcome.failed_indices.len(),
        total_bytes: outcome.transferred_bytes,
        cancelled: outcome.cancelled,
        succeeded_indices: outcome.succeeded_indices,
        failed_indices: outcome.failed_indices,
    }
}

async fn process_group(
    client: TcpClient,
    group_idx: usize,
    group_items: Vec<(usize, BatchTransferItem)>,
    progress_callback: Option<ProgressCallback>,
    cancel_callback: Option<CancelCallback>,
) -> BatchOutcome {
    let mut group_outcome = BatchOutcome::default();
    let mut conn = match client.get_pooled_connection().await {
        Ok(c) => c,
        Err(e) => {
            warn!(
                "TCP: Failed to get pooled connection for group {}: {}",
                group_idx, e
            );
            group_outcome.failed_indices = group_items.iter().map(|(idx, _)| *idx).collect();
            return group_outcome;
        }
    };

    debug!(
        "TCP: Group {} processing {} items (pooled connection)",
        group_idx,
        group_items.len()
    );

    for batch in group_items.chunks(MAX_ITEMS_PER_REQUEST) {
        match process_batch(&mut conn, batch, &progress_callback, &cancel_callback).await {
            Ok(batch_outcome) => {
                let cancelled = batch_outcome.cancelled;
                group_outcome.merge(batch_outcome);
                if cancelled {
                    break;
                }
            }
            Err(e) => {
                warn!("TCP Group {}: Batch failed: {}", group_idx, e);
                if record_batch_failure(&mut group_outcome, batch, &e) {
                    break;
                }
            }
        }
    }

    debug!(
        "TCP: Group {} completed (connection returned to pool)",
        group_idx
    );
    group_outcome
}

fn mark_unhandled_items_failed(
    outcome: &mut BatchOutcome,
    handled: &[bool],
    items: &[(usize, BatchTransferItem)],
) {
    for (handled_flag, (global_idx, _)) in handled.iter().zip(items.iter()) {
        if !handled_flag {
            outcome.failed_indices.push(*global_idx);
        }
    }
}

impl TcpClient {
    pub async fn read_and_write_batch_concurrent(
        &self,
        items: Vec<BatchTransferItem>,
        job_id: &str,
        concurrency: usize,
    ) -> crate::util::error::Result<BatchTransferResult> {
        self.read_and_write_batch_concurrent_with_progress(items, job_id, concurrency, None, None)
            .await
    }

    pub async fn read_and_write_batch_concurrent_with_progress(
        &self,
        items: Vec<BatchTransferItem>,
        job_id: &str,
        concurrency: usize,
        progress_callback: Option<ProgressCallback>,
        cancel_callback: Option<CancelCallback>,
    ) -> crate::util::error::Result<BatchTransferResult> {
        if items.is_empty() {
            return Ok(empty_batch_transfer_result());
        }

        let total_items = items.len();
        let total_bytes: u64 = items.iter().map(|item| item.length).sum();

        debug!(
            "TCP concurrent batch read: {} chunks, {} bytes, job_id: {}, concurrency: {}",
            total_items, total_bytes, job_id, concurrency
        );

        let concurrency = normalize_batch_concurrency(concurrency);
        let chunk_size = total_items.div_ceil(concurrency);
        let indexed_items: Vec<(usize, BatchTransferItem)> =
            items.into_iter().enumerate().collect();
        let item_chunks: Vec<_> = indexed_items
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        debug!(
            "TCP: Split {} items into {} groups ({} items per group)",
            total_items,
            item_chunks.len(),
            chunk_size
        );

        if item_chunks.len() == 1 {
            let final_outcome = process_group(
                self.clone(),
                0,
                item_chunks
                    .into_iter()
                    .next()
                    .expect("single group must exist"),
                progress_callback,
                cancel_callback,
            )
            .await;
            let result = finalize_batch_outcome(final_outcome);
            debug!(
                "TCP inline batch transfer completed: {}/{} succeeded, {} bytes transferred",
                result.succeeded, total_items, result.total_bytes
            );
            return Ok(result);
        }

        let tasks: Vec<_> = item_chunks
            .into_iter()
            .enumerate()
            .map(|(group_idx, group_items)| {
                let client = self.clone();
                let progress_callback = progress_callback.clone();
                let cancel_callback = cancel_callback.clone();
                let fallback_failed_indices: Vec<usize> =
                    group_items.iter().map(|(idx, _)| *idx).collect();

                (
                    fallback_failed_indices,
                    tokio::spawn(process_group(
                        client,
                        group_idx,
                        group_items,
                        progress_callback,
                        cancel_callback,
                    )),
                )
            })
            .collect();

        let mut final_outcome = BatchOutcome::default();
        for (fallback_failed_indices, task) in tasks {
            match task.await {
                Ok(outcome) => final_outcome.merge(outcome),
                Err(e) => {
                    warn!("TCP group task failed: {}", e);
                    final_outcome.failed_indices.extend(fallback_failed_indices);
                }
            }
        }

        let result = finalize_batch_outcome(final_outcome);
        debug!(
            "TCP concurrent batch transfer completed: {}/{} succeeded, {} bytes transferred",
            result.succeeded, total_items, result.total_bytes
        );

        Ok(result)
    }
}

async fn process_batch(
    stream: &mut tokio::net::TcpStream,
    items: &[(usize, BatchTransferItem)],
    progress_callback: &Option<ProgressCallback>,
    cancel_callback: &Option<CancelCallback>,
) -> Result<BatchOutcome, String> {
    if items.is_empty() {
        return Ok(BatchOutcome::default());
    }

    if cancel_callback.as_ref().is_some_and(|callback| callback()) {
        return Err("Job cancelled by user".to_string());
    }

    let read_items: Vec<ReadBlockItem> = items
        .iter()
        .map(|(_, item)| ReadBlockItem {
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
    let payload_len = ISyncMessage::validate_payload_len(payload_len)
        .map_err(|e| format!("Failed to validate payload length: {}", e))?;

    if msg_type == MessageType::Error {
        let mut error_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            stream
                .read_exact(&mut error_buf)
                .await
                .map_err(|e| format!("Failed to read error payload: {}", e))?;
        }
        let error_msg = String::from_utf8_lossy(&error_buf);
        return Err(format!("Server error: {}", error_msg));
    }

    if msg_type != MessageType::ReadBlockResponse {
        return Err(format!("Expected ReadBlockResponse, got {:?}", msg_type));
    }

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| format!("Failed to read response payload: {}", e))?;
    }

    let response: crate::core::ReadBlockResponse = bincode::deserialize(&payload)
        .map_err(|e| format!("Failed to deserialize response: {}", e))?;

    let mut outcome = BatchOutcome::default();
    let mut handled = vec![false; items.len()];

    for result in response.results {
        if cancel_callback.as_ref().is_some_and(|callback| callback()) {
            outcome.cancelled = true;
            mark_unhandled_items_failed(&mut outcome, &handled, items);
            return Ok(outcome);
        }

        let idx = result.index as usize;
        if idx >= items.len() {
            warn!("Invalid result index: {} >= {}", idx, items.len());
            continue;
        }

        if handled[idx] {
            warn!("Duplicate result index received: {}", idx);
            continue;
        }
        handled[idx] = true;

        let (global_idx, item) = &items[idx];

        if !result.success {
            outcome.failed_indices.push(*global_idx);
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
                        outcome.failed_indices.push(*global_idx);
                        continue;
                    }
                    decompressed
                }
                Err(e) => {
                    warn!("Decompression failed for item {}: {}", idx, e);
                    outcome.failed_indices.push(*global_idx);
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
                outcome.succeeded_indices.push(*global_idx);
                outcome.transferred_bytes += bytes_written;
                if let Some(callback) = progress_callback {
                    callback(bytes_written);
                }
            }
            Err(e) => {
                outcome.failed_indices.push(*global_idx);
                warn!("Failed to write item {}: {}", idx, e);
            }
        }
    }

    mark_unhandled_items_failed(&mut outcome, &handled, items);

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::{normalize_batch_concurrency, record_batch_failure, BatchOutcome};
    use crate::sync::transfer::batch::BatchTransferItem;

    #[test]
    fn normalize_batch_concurrency_clamps_zero_to_one() {
        assert_eq!(normalize_batch_concurrency(0), 1);
        assert_eq!(normalize_batch_concurrency(8), 8);
    }

    #[test]
    fn record_batch_failure_marks_cancellation_and_stops_processing() {
        let mut outcome = BatchOutcome::default();
        let batch = vec![
            (
                1,
                BatchTransferItem::new("src-a".to_string(), "dest".to_string(), 0, 0, 4, 1),
            ),
            (
                2,
                BatchTransferItem::new("src-b".to_string(), "dest".to_string(), 4, 4, 4, 2),
            ),
        ];

        let should_stop = record_batch_failure(&mut outcome, &batch, "Job cancelled by user");

        assert!(should_stop);
        assert!(outcome.cancelled);
        assert_eq!(outcome.failed_indices, vec![1, 2]);
    }
}
