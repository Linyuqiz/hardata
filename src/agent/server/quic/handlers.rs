use crate::agent::compute::ComputeService;
use crate::agent::server::common::{compress_block_data, resolve_request_path, MAX_BLOCK_SIZE};
use crate::core::protocol::{ISyncMessage, MessageType};
use crate::util::error::{HarDataError, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, warn};

pub async fn handle_list_directory(
    send: &mut quinn::SendStream,
    payload_buf: &[u8],
    compute: &Arc<ComputeService>,
    data_dir: &Path,
) -> Result<()> {
    match bincode::deserialize::<crate::core::ListDirectoryRequest>(payload_buf) {
        Ok(request) => {
            let target_path = match resolve_request_path(data_dir, &request.directory_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!("Path validation failed: {}", e);
                    let error_msg = format!("Path validation failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                    return Ok(());
                }
            };

            debug!("List directory: {:?}", target_path);

            match compute.list_directory(&target_path).await {
                Ok(files) => {
                    let response_data = crate::core::ListDirectoryResponse {
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
                    warn!("Failed to list directory: {}", e);
                    let error_msg = format!("List directory failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                }
            }
        }
        Err(e) => {
            warn!("Invalid ListDirectoryRequest: {}", e);
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
    match bincode::deserialize::<crate::core::GetFileHashesRequest>(payload_buf) {
        Ok(request) => {
            let target_path = match resolve_request_path(data_dir, &request.file_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!("Path validation failed: {}", e);
                    let error_msg = format!("Path validation failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                    return Ok(());
                }
            };

            debug!("Get file hashes: {:?}", target_path);

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
                    let response_data = crate::core::GetFileHashesResponse {
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
                    warn!("Failed to get file hashes: {}", e);
                    let error_msg = format!("Get file hashes failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                }
            }
        }
        Err(e) => {
            warn!("Invalid GetFileHashesRequest: {}", e);
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
    match bincode::deserialize::<crate::core::ReadBlockRequest>(payload_buf) {
        Ok(request) => {
            let items_count = request.items.len();
            debug!("Read block: {} items", items_count);

            let mut results = Vec::with_capacity(items_count);

            for (index, item) in request.items.iter().enumerate() {
                if item.length > MAX_BLOCK_SIZE {
                    results.push(crate::core::ReadBlockResult {
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
                        results.push(crate::core::ReadBlockResult {
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
                        results.push(crate::core::ReadBlockResult {
                            index: index as u32,
                            success: true,
                            data: compressed_data,
                            compression: compression_info,
                            error: None,
                        });
                    }
                    Err(e) => {
                        results.push(crate::core::ReadBlockResult {
                            index: index as u32,
                            success: false,
                            data: Vec::new(),
                            compression: None,
                            error: Some(format!("Read failed: {}", e)),
                        });
                    }
                }
            }

            let response_data = crate::core::ReadBlockResponse { results };
            let response_bytes = bincode::serialize(&response_data)
                .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;

            let response = ISyncMessage::new(
                MessageType::ReadBlockResponse,
                bytes::Bytes::from(response_bytes),
            );
            send.write_all(&response.encode()).await?;
        }
        Err(e) => {
            warn!("Invalid ReadBlockRequest: {}", e);
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
    match bincode::deserialize::<crate::core::GetStrongHashesRequest>(payload_buf) {
        Ok(request) => {
            let target_path = match resolve_request_path(data_dir, &request.file_path) {
                Ok(path) => path,
                Err(e) => {
                    warn!("Path validation failed: {}", e);
                    let error_msg = format!("Path validation failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                    return Ok(());
                }
            };

            debug!("Get strong hashes: {} chunks", request.chunks.len());

            match compute
                .get_strong_hashes(&target_path, &request.chunks)
                .await
            {
                Ok(hashes) => {
                    let response_data = crate::core::GetStrongHashesResponse {
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
                    warn!("Failed to get strong hashes: {}", e);
                    let error_msg = format!("Get strong hashes failed: {}", e);
                    let response =
                        ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                    send.write_all(&response.encode()).await?;
                }
            }
        }
        Err(e) => {
            warn!("Invalid GetStrongHashesRequest: {}", e);
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}

pub async fn handle_ping(send: &mut quinn::SendStream, payload_buf: &[u8]) -> Result<()> {
    match bincode::deserialize::<crate::core::protocol::PingRequest>(payload_buf) {
        Ok(request) => {
            let response_data = crate::core::protocol::PongResponse {
                timestamp: request.timestamp,
            };
            let response_bytes = bincode::serialize(&response_data)
                .map_err(|e| HarDataError::SerializationError(format!("{}", e)))?;
            let response = ISyncMessage::new(MessageType::Pong, bytes::Bytes::from(response_bytes));
            send.write_all(&response.encode()).await?;
        }
        Err(e) => {
            warn!("Invalid PingRequest: {}", e);
            let error_msg = format!("Invalid request: {}", e);
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
        }
    }
    Ok(())
}
