use crate::agent_server::common::{compress_block_data, resolve_request_path, MAX_BLOCK_SIZE};
use crate::compute::ComputeService;
use hardata_protocol::{ISyncMessage, MessageType};
use hardata_shared::error::{HarDataError, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, warn};

async fn send_error(send: &mut quinn::SendStream, message: &str) -> Result<()> {
    let response = ISyncMessage::new(
        MessageType::Error,
        bytes::Bytes::copy_from_slice(message.as_bytes()),
    );
    send.write_all(&response.encode()).await?;
    Ok(())
}

pub async fn handle_list_directory(
    send: &mut quinn::SendStream,
    payload_buf: &[u8],
    compute: &Arc<ComputeService>,
    data_dir: &Path,
) -> Result<()> {
    match bincode::deserialize::<hardata_protocol::ListDirectoryRequest>(payload_buf) {
        Ok(request) => {
            let target_path = match resolve_request_path(data_dir, &request.directory_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!(operation = "agent.path_validation_failed", protocol = "quic", request = "list_directory", path = %request.directory_path, error = %e, "agent request path rejected");
                    let error_msg = format!("Path validation failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                    return Ok(());
                }
            };

            debug!(operation = "agent.list_directory_started", protocol = "quic", path = %target_path.display(), "agent directory listing started");

            match compute.list_directory(&target_path).await {
                Ok(files) => {
                    let response_data = hardata_protocol::ListDirectoryResponse {
                        directory_path: request.directory_path,
                        files,
                    };
                    let response_bytes = bincode::serialize(&response_data)
                        .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;
                    let response = ISyncMessage::new(
                        MessageType::ListDirectoryResponse,
                        bytes::Bytes::from(response_bytes),
                    );
                    send.write_all(&response.encode()).await?;
                }
                Err(e) => {
                    warn!(operation = "agent.list_directory_failed", protocol = "quic", path = %target_path.display(), error = %e, "agent directory listing failed");
                    let error_msg = format!("List directory failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                }
            }
        }
        Err(e) => {
            warn!(operation = "agent.request_decode_failed", protocol = "quic", request = "list_directory", error = %e, "agent request decode failed");
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}

pub async fn handle_get_file_hashes(
    send: &mut quinn::SendStream,
    payload_buf: &[u8],
    compute: &Arc<ComputeService>,
    data_dir: &Path,
) -> Result<()> {
    match bincode::deserialize::<hardata_protocol::GetFileHashesRequest>(payload_buf) {
        Ok(request) => {
            let target_path = match resolve_request_path(data_dir, &request.file_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!(operation = "agent.path_validation_failed", protocol = "quic", request = "get_file_hashes", path = %request.file_path, error = %e, "agent request path rejected");
                    let error_msg = format!("Path validation failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                    return Ok(());
                }
            };

            debug!(operation = "agent.file_hashes_started", protocol = "quic", path = %target_path.display(), "agent file hash calculation started");

            match compute
                .get_file_hashes(
                    &target_path,
                    request.min_chunk_size,
                    request.avg_chunk_size,
                    request.max_chunk_size,
                )
                .await
            {
                Ok((file_size, chunks)) => {
                    let response_data = hardata_protocol::GetFileHashesResponse {
                        file_path: request.file_path,
                        file_size,
                        chunks: (*chunks).clone(),
                    };
                    let response_bytes = bincode::serialize(&response_data)
                        .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;
                    let response = ISyncMessage::new(
                        MessageType::GetFileHashesResponse,
                        bytes::Bytes::from(response_bytes),
                    );
                    send.write_all(&response.encode()).await?;
                }
                Err(e) => {
                    warn!(operation = "agent.file_hashes_failed", protocol = "quic", path = %target_path.display(), error = %e, "agent file hash calculation failed");
                    let error_msg = format!("Get file hashes failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                }
            }
        }
        Err(e) => {
            warn!(operation = "agent.request_decode_failed", protocol = "quic", request = "get_file_hashes", error = %e, "agent request decode failed");
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}

pub async fn handle_read_block(
    send: &mut quinn::SendStream,
    payload_buf: &[u8],
    compute: &Arc<ComputeService>,
    data_dir: &Path,
) -> Result<()> {
    match bincode::deserialize::<hardata_protocol::ReadBlockRequest>(payload_buf) {
        Ok(request) => {
            let items_count = request.items.len();
            if let Err(error) = crate::agent_server::common::validate_request_item_count(
                "ReadBlockRequest",
                items_count,
            ) {
                return send_error(send, &error.to_string()).await;
            }
            debug!(
                operation = "agent.read_block_started",
                protocol = "quic",
                item_count = items_count,
                "agent block read started"
            );

            let mut results = Vec::with_capacity(items_count);

            for (index, item) in request.items.iter().enumerate() {
                if item.length > MAX_BLOCK_SIZE {
                    results.push(hardata_protocol::ReadBlockResult {
                        index: index as u32,
                        success: false,
                        data: Vec::new(),
                        compression: None,
                        error: Some(format!(
                            "Block size {} exceeds maximum {}",
                            item.length, MAX_BLOCK_SIZE
                        )),
                    });
                    continue;
                }

                let target_path = match resolve_request_path(data_dir, &item.file_path) {
                    Ok(path) => path,
                    Err(e) => {
                        results.push(hardata_protocol::ReadBlockResult {
                            index: index as u32,
                            success: false,
                            data: Vec::new(),
                            compression: None,
                            error: Some(format!("Path validation failed: {}", e)),
                        });
                        continue;
                    }
                };

                match compute
                    .read_block_by_offset(&target_path, item.offset, item.length)
                    .await
                {
                    Ok(data) => {
                        let (compressed_data, compression_info) =
                            compress_block_data(&target_path, data);
                        results.push(hardata_protocol::ReadBlockResult {
                            index: index as u32,
                            success: true,
                            data: compressed_data,
                            compression: compression_info,
                            error: None,
                        });
                    }
                    Err(e) => {
                        results.push(hardata_protocol::ReadBlockResult {
                            index: index as u32,
                            success: false,
                            data: Vec::new(),
                            compression: None,
                            error: Some(format!("Read failed: {}", e)),
                        });
                    }
                }
            }

            let response_data = hardata_protocol::ReadBlockResponse { results };
            let response_bytes = bincode::serialize(&response_data)
                .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;

            let response = ISyncMessage::new(
                MessageType::ReadBlockResponse,
                bytes::Bytes::from(response_bytes),
            );
            send.write_all(&response.encode()).await?;
        }
        Err(e) => {
            warn!(operation = "agent.request_decode_failed", protocol = "quic", request = "read_block", error = %e, "agent request decode failed");
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}

pub async fn handle_get_strong_hashes(
    send: &mut quinn::SendStream,
    payload_buf: &[u8],
    compute: &Arc<ComputeService>,
    data_dir: &Path,
) -> Result<()> {
    match bincode::deserialize::<hardata_protocol::GetStrongHashesRequest>(payload_buf) {
        Ok(request) => {
            if let Err(error) = crate::agent_server::common::validate_request_item_count(
                "GetStrongHashesRequest",
                request.chunks.len(),
            ) {
                return send_error(send, &error.to_string()).await;
            }
            let target_path = match resolve_request_path(data_dir, &request.file_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!(operation = "agent.path_validation_failed", protocol = "quic", request = "get_strong_hashes", path = %request.file_path, error = %e, "agent request path rejected");
                    let error_msg = format!("Path validation failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                    return Ok(());
                }
            };

            debug!(operation = "agent.strong_hashes_started", protocol = "quic", path = %target_path.display(), chunk_count = request.chunks.len(), "agent strong hash calculation started");

            match compute
                .get_strong_hashes(&target_path, &request.chunks)
                .await
            {
                Ok(hashes) => {
                    let response_data = hardata_protocol::GetStrongHashesResponse {
                        file_path: request.file_path,
                        hashes,
                    };
                    let response_bytes = bincode::serialize(&response_data)
                        .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;
                    let response = ISyncMessage::new(
                        MessageType::GetStrongHashesResponse,
                        bytes::Bytes::from(response_bytes),
                    );
                    send.write_all(&response.encode()).await?;
                }
                Err(e) => {
                    warn!(operation = "agent.strong_hashes_failed", protocol = "quic", path = %target_path.display(), error = %e, "agent strong hash calculation failed");
                    let error_msg = format!("Get strong hashes failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                }
            }
        }
        Err(e) => {
            warn!(operation = "agent.request_decode_failed", protocol = "quic", request = "get_strong_hashes", error = %e, "agent request decode failed");
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}

pub async fn handle_ping(send: &mut quinn::SendStream, payload_buf: &[u8]) -> Result<()> {
    match bincode::deserialize::<hardata_protocol::PingRequest>(payload_buf) {
        Ok(request) => {
            let response_data = hardata_protocol::PongResponse {
                timestamp: request.timestamp,
            };
            let response_bytes = bincode::serialize(&response_data)
                .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;
            let response = ISyncMessage::new(MessageType::Pong, bytes::Bytes::from(response_bytes));
            send.write_all(&response.encode()).await?;
        }
        Err(e) => {
            warn!(operation = "agent.request_decode_failed", protocol = "quic", request = "ping", error = %e, "agent request decode failed");
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}
