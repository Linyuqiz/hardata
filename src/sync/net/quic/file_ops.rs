use crate::core::protocol::{ISyncMessage, MessageType};
use crate::util::error::{HarDataError, Result};
use tracing::{debug, info};

use super::client::QuicClient;

impl QuicClient {
    pub async fn get_file_hashes(
        &self,
        connection: &quinn::Connection,
        file_path: &str,
        min_chunk_size: usize,
        avg_chunk_size: usize,
        max_chunk_size: usize,
    ) -> Result<crate::core::protocol::GetFileHashesResponse> {
        debug!("Get file hashes: {}", file_path);

        let (mut send, mut recv) = connection.open_bi().await?;

        let request = crate::core::protocol::GetFileHashesRequest {
            file_path: file_path.to_string(),
            min_chunk_size,
            avg_chunk_size,
            max_chunk_size,
        };

        let payload = bincode::serialize(&request).map_err(|e| {
            HarDataError::SerializationError(format!(
                "Failed to serialize GetFileHashesRequest: {}",
                e
            ))
        })?;

        let message = ISyncMessage::new(
            MessageType::GetFileHashesRequest,
            bytes::Bytes::from(payload),
        );

        send.write_all(&message.encode()).await?;
        send.finish()?;

        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        recv.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len as usize];
            recv.read_exact(&mut error_buf).await?;
            let error_msg = String::from_utf8_lossy(&error_buf);
            return Err(HarDataError::ProtocolError(format!(
                "Server error: {}",
                error_msg
            )));
        }

        if msg_type != MessageType::GetFileHashesResponse {
            return Err(HarDataError::ProtocolError(format!(
                "Expected GetFileHashesResponse, got {:?}",
                msg_type
            )));
        }

        let mut payload_buf = vec![0u8; payload_len as usize];
        if payload_len > 0 {
            recv.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::GetFileHashesResponse =
            bincode::deserialize(&payload_buf).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize GetFileHashesResponse: {}",
                    e
                ))
            })?;

        info!(
            "QUIC server returned file hashes: {} chunks, size={}",
            response.chunks.len(),
            response.file_size
        );

        Ok(response)
    }

    pub async fn get_strong_hashes(
        &self,
        connection: &quinn::Connection,
        file_path: &str,
        chunks: Vec<crate::core::ChunkLocation>,
    ) -> Result<crate::core::protocol::GetStrongHashesResponse> {
        info!(
            "Getting strong hashes via QUIC: {}, {} chunks",
            file_path,
            chunks.len()
        );

        let (mut send, mut recv) = connection.open_bi().await?;

        let request = crate::core::protocol::GetStrongHashesRequest {
            file_path: file_path.to_string(),
            chunks,
        };

        let payload = bincode::serialize(&request).map_err(|e| {
            HarDataError::SerializationError(format!(
                "Failed to serialize GetStrongHashesRequest: {}",
                e
            ))
        })?;

        let message = ISyncMessage::new(
            MessageType::GetStrongHashesRequest,
            bytes::Bytes::from(payload),
        );

        send.write_all(&message.encode()).await?;
        send.finish()?;

        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        recv.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len as usize];
            recv.read_exact(&mut error_buf).await?;
            let error_msg = String::from_utf8_lossy(&error_buf).to_string();
            return Err(HarDataError::ProtocolError(format!(
                "QUIC GetStrongHashes failed: {}",
                error_msg
            )));
        }

        if msg_type != MessageType::GetStrongHashesResponse {
            return Err(HarDataError::ProtocolError(format!(
                "Expected GetStrongHashesResponse, got {:?}",
                msg_type
            )));
        }

        let mut payload_buf = vec![0u8; payload_len as usize];
        if payload_len > 0 {
            recv.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::GetStrongHashesResponse =
            bincode::deserialize(&payload_buf).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize GetStrongHashesResponse: {}",
                    e
                ))
            })?;

        info!(
            "QUIC server returned {} strong hashes",
            response.hashes.len()
        );

        Ok(response)
    }
}
