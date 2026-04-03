use crate::core::protocol::{ISyncMessage, MessageType, ReadBlockItem, ReadBlockRequest};
use crate::sync::net::quic::QuicClient;
use crate::util::compression::decompress_with_algorithm;
use crate::util::error::Result;
use bytes::Bytes;
use quinn::Connection;
use tracing::{debug, warn};

use super::types::{BatchTransferItem, BatchTransferResult, CancelCallback, ProgressCallback};

use crate::core::constants::MAX_ITEMS_PER_REQUEST;

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

fn normalize_stream_concurrency(max_concurrent_streams: usize) -> usize {
    max_concurrent_streams.max(1)
}

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
        cancel_callback: Option<CancelCallback>,
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

        let max_concurrent_streams = normalize_stream_concurrency(max_concurrent_streams);
        let chunk_size = total_items.div_ceil(max_concurrent_streams);
        let indexed_items: Vec<(usize, BatchTransferItem)> =
            items.into_iter().enumerate().collect();
        let item_chunks: Vec<Vec<(usize, BatchTransferItem)>> = indexed_items
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        debug!(
            "QUIC batch transfer: {} chunks, {} bytes, {} streams, {} per stream, job_id: {}",
            total_items,
            total_bytes,
            item_chunks.len(),
            chunk_size,
            job_id
        );

        let tasks: Vec<_> = item_chunks
            .into_iter()
            .enumerate()
            .map(|(group_idx, group_items)| {
                let connection = connection.clone();
                let progress_callback = progress_callback.clone();
                let cancel_callback = cancel_callback.clone();
                let fallback_failed_indices: Vec<usize> =
                    group_items.iter().map(|(idx, _)| *idx).collect();

                (
                    fallback_failed_indices,
                    tokio::spawn(async move {
                        let mut group_outcome = BatchOutcome::default();

                        let stream_result = connection.open_bi().await;
                        let (mut send, mut recv) = match stream_result {
                            Ok(streams) => streams,
                            Err(e) => {
                                warn!("QUIC: Failed to open stream for group {}: {}", group_idx, e);
                                group_outcome.failed_indices =
                                    group_items.iter().map(|(idx, _)| *idx).collect();
                                return group_outcome;
                            }
                        };

                        debug!(
                            "QUIC: Group {} processing {} items",
                            group_idx,
                            group_items.len()
                        );

                        for batch in group_items.chunks(MAX_ITEMS_PER_REQUEST) {
                            match process_batch_on_stream(
                                &mut send,
                                &mut recv,
                                batch,
                                &progress_callback,
                                &cancel_callback,
                            )
                            .await
                            {
                                Ok(batch_outcome) => {
                                    let cancelled = batch_outcome.cancelled;
                                    group_outcome.merge(batch_outcome);
                                    if cancelled {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("QUIC Group {}: Batch failed: {}", group_idx, e);
                                    if record_batch_failure(&mut group_outcome, batch, &e) {
                                        break;
                                    }
                                }
                            }
                        }

                        if let Err(e) = send.finish() {
                            warn!(
                                "QUIC: Failed to finish stream for group {}: {}",
                                group_idx, e
                            );
                        }

                        debug!("QUIC: Group {} completed", group_idx);
                        group_outcome
                    }),
                )
            })
            .collect();

        let mut final_outcome = BatchOutcome::default();
        for (fallback_failed_indices, task) in tasks {
            match task.await {
                Ok(outcome) => final_outcome.merge(outcome),
                Err(e) => {
                    warn!("QUIC group task failed: {}", e);
                    final_outcome.failed_indices.extend(fallback_failed_indices);
                }
            }
        }

        final_outcome.succeeded_indices.sort_unstable();
        final_outcome.failed_indices.sort_unstable();
        let final_succeeded = final_outcome.succeeded_indices.len();
        let final_failed = final_outcome.failed_indices.len();
        let final_transferred = final_outcome.transferred_bytes;

        debug!(
            "QUIC batch transfer completed: {}/{} succeeded, {} bytes",
            final_succeeded, total_items, final_transferred
        );

        Ok(BatchTransferResult {
            succeeded: final_succeeded,
            failed: final_failed,
            total_bytes: final_transferred,
            cancelled: final_outcome.cancelled,
            succeeded_indices: final_outcome.succeeded_indices,
            failed_indices: final_outcome.failed_indices,
        })
    }
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

async fn process_batch_on_stream(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    items: &[(usize, BatchTransferItem)],
    progress_callback: &Option<ProgressCallback>,
    cancel_callback: &Option<CancelCallback>,
) -> std::result::Result<BatchOutcome, String> {
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

    send.write_all(&request.encode())
        .await
        .map_err(|e| format!("Failed to send batch request: {}", e))?;

    let response = QuicClient::receive_batch_read_response_static(recv)
        .await
        .map_err(|e| format!("Failed to receive batch response: {}", e))?;

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
                debug!("Item {} completed: {} bytes", idx, bytes_written);
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
    use super::{normalize_stream_concurrency, record_batch_failure, BatchOutcome};
    use crate::sync::transfer::batch::BatchTransferItem;

    #[test]
    fn normalize_stream_concurrency_clamps_zero_to_one() {
        assert_eq!(normalize_stream_concurrency(0), 1);
        assert_eq!(normalize_stream_concurrency(16), 16);
    }

    #[test]
    fn record_batch_failure_marks_cancellation_and_stops_processing() {
        let mut outcome = BatchOutcome::default();
        let batch = vec![
            (
                3,
                BatchTransferItem::new("src-a".to_string(), "dest".to_string(), 0, 0, 4, 1),
            ),
            (
                7,
                BatchTransferItem::new("src-b".to_string(), "dest".to_string(), 4, 4, 4, 2),
            ),
        ];

        let should_stop = record_batch_failure(&mut outcome, &batch, "Job cancelled by user");

        assert!(should_stop);
        assert!(outcome.cancelled);
        assert_eq!(outcome.failed_indices, vec![3, 7]);
    }
}
