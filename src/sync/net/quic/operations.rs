use crate::core::protocol::{ISyncMessage, MessageType};
use crate::util::error::{HarDataError, Result};
use tracing::{debug, info};

use super::client::QuicClient;

impl QuicClient {
    pub async fn list_directory(
        &self,
        connection: &quinn::Connection,
        directory_path: &str,
    ) -> Result<crate::core::protocol::ListDirectoryResponse> {
        debug!("List directory: {}", directory_path);

        let (mut send, mut recv) = connection.open_bi().await?;

        let request = crate::core::protocol::ListDirectoryRequest {
            directory_path: directory_path.to_string(),
        };

        let payload = bincode::serialize(&request).map_err(|e| {
            HarDataError::SerializationError(format!(
                "Failed to serialize ListDirectoryRequest: {}",
                e
            ))
        })?;

        let message = ISyncMessage::new(
            MessageType::ListDirectoryRequest,
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

        if msg_type != MessageType::ListDirectoryResponse {
            return Err(HarDataError::ProtocolError(format!(
                "Expected ListDirectoryResponse, got {:?}",
                msg_type
            )));
        }

        let mut payload_buf = vec![0u8; payload_len as usize];
        if payload_len > 0 {
            recv.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::ListDirectoryResponse =
            bincode::deserialize(&payload_buf).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize ListDirectoryResponse: {}",
                    e
                ))
            })?;

        info!(
            "QUIC server returned {} files/directories",
            response.files.len()
        );

        Ok(response)
    }

    pub async fn ping(&self, connection: &quinn::Connection) -> Result<u64> {
        tracing::debug!("ping() called");

        let (mut send, mut recv) = connection.open_bi().await?;
        tracing::debug!("Bidirectional stream opened");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let request = crate::core::protocol::PingRequest { timestamp };

        let payload = bincode::serialize(&request).map_err(|e| {
            HarDataError::SerializationError(format!("Failed to serialize PingRequest: {}", e))
        })?;

        let message = ISyncMessage::new(MessageType::Ping, bytes::Bytes::from(payload));

        tracing::debug!("Sending Ping message");
        send.write_all(&message.encode()).await?;
        send.finish()?;
        tracing::debug!("Ping message sent and send stream finished");

        tracing::debug!("Waiting for Pong response header");
        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        recv.read_exact(&mut header_buf).await?;
        tracing::debug!("Pong response header received");

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;
        tracing::debug!(
            "Decoded header: msg_type={:?}, payload_len={}",
            msg_type,
            payload_len
        );

        if msg_type != MessageType::Pong {
            return Err(HarDataError::ProtocolError(format!(
                "Expected Pong, got {:?}",
                msg_type
            )));
        }

        tracing::debug!("Reading payload: {} bytes", payload_len);
        let mut payload_buf = vec![0u8; payload_len as usize];
        if payload_len > 0 {
            recv.read_exact(&mut payload_buf).await?;
        }
        tracing::debug!("Payload read successfully");

        let response: crate::core::protocol::PongResponse = bincode::deserialize(&payload_buf)
            .map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize PongResponse: {}",
                    e
                ))
            })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let rtt = now.saturating_sub(response.timestamp);

        Ok(rtt)
    }
}
