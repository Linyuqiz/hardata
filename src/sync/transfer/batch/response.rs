use crate::core::protocol::{ISyncMessage, MessageType};
use crate::core::ReadBlockResponse;
use crate::sync::net::quic::QuicClient;
use crate::util::error::{HarDataError, Result};

impl QuicClient {
    pub(super) async fn receive_batch_read_response_static(
        recv: &mut quinn::RecvStream,
    ) -> Result<ReadBlockResponse> {
        let mut header_buf = vec![0u8; ISyncMessage::HEADER_SIZE];
        recv.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len as usize];
            if payload_len > 0 {
                recv.read_exact(&mut error_buf).await?;
            }
            let error_msg = String::from_utf8_lossy(&error_buf).to_string();
            return Err(HarDataError::ProtocolError(format!(
                "Read failed: {}",
                error_msg
            )));
        }

        if msg_type != MessageType::ReadBlockResponse {
            return Err(HarDataError::ProtocolError(format!(
                "Expected ReadResponse, got {:?}",
                msg_type
            )));
        }

        let payload_data: Vec<u8> = if payload_len > 0 {
            let mut buf = vec![0u8; payload_len as usize];
            recv.read_exact(&mut buf).await?;
            buf
        } else {
            vec![]
        };

        let response: ReadBlockResponse = bincode::deserialize(&payload_data).map_err(|e| {
            HarDataError::SerializationError(format!("Failed to deserialize: {}", e))
        })?;

        Ok(response)
    }

    pub(super) async fn receive_read_response(
        &self,
        recv: &mut quinn::RecvStream,
    ) -> Result<Vec<u8>> {
        let mut header_buf = vec![0u8; ISyncMessage::HEADER_SIZE];
        recv.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len as usize];
            if payload_len > 0 {
                recv.read_exact(&mut error_buf).await?;
            }
            let error_msg = String::from_utf8_lossy(&error_buf).to_string();
            return Err(HarDataError::ProtocolError(format!(
                "Read failed: {}",
                error_msg
            )));
        }

        if msg_type != MessageType::ReadBlockResponse {
            return Err(HarDataError::ProtocolError(format!(
                "Expected ReadResponse, got {:?}",
                msg_type
            )));
        }

        use crate::util::buffer_pool::global_buffer_pool;
        let pooled_buffer = if payload_len > 0 {
            let pool = global_buffer_pool();
            let mut buffer = pool.acquire();
            buffer.resize(payload_len as usize, 0);
            recv.read_exact(&mut buffer[..payload_len as usize]).await?;
            Some(buffer)
        } else {
            None
        };

        let payload_data: &[u8] = match &pooled_buffer {
            Some(buf) => &buf[..payload_len as usize],
            None => &[],
        };

        let response: crate::core::ReadBlockResponse =
            bincode::deserialize(payload_data).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize ReadBlockResponse: {}",
                    e
                ))
            })?;

        let result = response
            .results
            .into_iter()
            .next()
            .ok_or_else(|| HarDataError::ProtocolError("Empty response results".to_string()))?;

        if !result.success {
            return Err(HarDataError::ProtocolError(format!(
                "Read failed: {}",
                result.error.unwrap_or_else(|| "Unknown error".to_string())
            )));
        }

        let data = if let Some(compression_info) = &result.compression {
            use crate::util::compression::decompress_with_algorithm;

            let decompressed = decompress_with_algorithm(&result.data, &compression_info.algorithm)
                .map_err(|e| HarDataError::Compression(format!("Decompression failed: {}", e)))?;

            if decompressed.len() as u64 != compression_info.original_size {
                return Err(HarDataError::Compression(format!(
                    "Decompressed size mismatch: expected {}, got {}",
                    compression_info.original_size,
                    decompressed.len()
                )));
            }

            tracing::debug!(
                "Decompressed block: {} bytes -> {} bytes (algorithm: {})",
                compression_info.compressed_size,
                compression_info.original_size,
                compression_info.algorithm
            );

            decompressed
        } else {
            result.data
        };

        Ok(data)
    }
}
