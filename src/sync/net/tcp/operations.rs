use crate::core::protocol::{ISyncMessage, MessageType};
use crate::util::error::{HarDataError, Result};
use crate::util::time::unix_timestamp_millis;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use super::client::TcpClient;

impl TcpClient {
    pub async fn get_file_hashes(
        &self,
        stream: &mut TcpStream,
        file_path: &str,
        min_chunk_size: usize,
        avg_chunk_size: usize,
        max_chunk_size: usize,
    ) -> Result<crate::core::protocol::GetFileHashesResponse> {
        debug!("Get file hashes: {}", file_path);

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

        stream.write_all(&message.encode()).await?;
        stream.flush().await?;

        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        stream.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;
        let payload_len = ISyncMessage::validate_payload_len(payload_len)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut error_buf).await?;
            }
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

        let mut payload_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::GetFileHashesResponse =
            bincode::deserialize(&payload_buf).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize GetFileHashesResponse: {}",
                    e
                ))
            })?;

        debug!(
            "TCP server scanned file: {} chunks, size={}",
            response.chunks.len(),
            response.file_size
        );

        Ok(response)
    }

    pub async fn get_strong_hashes(
        &self,
        stream: &mut TcpStream,
        file_path: &str,
        chunks: Vec<crate::core::ChunkLocation>,
    ) -> Result<crate::core::protocol::GetStrongHashesResponse> {
        debug!(
            "Getting strong hashes via TCP: {}, {} chunks",
            file_path,
            chunks.len()
        );

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

        stream.write_all(&message.encode()).await?;
        stream.flush().await?;

        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        stream.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;
        let payload_len = ISyncMessage::validate_payload_len(payload_len)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut error_buf).await?;
            }
            let error_msg = String::from_utf8_lossy(&error_buf).to_string();
            return Err(HarDataError::ProtocolError(format!(
                "TCP GetStrongHashes failed: {}",
                error_msg
            )));
        }

        if msg_type != MessageType::GetStrongHashesResponse {
            return Err(HarDataError::ProtocolError(format!(
                "Expected GetStrongHashesResponse, got {:?}",
                msg_type
            )));
        }

        let mut payload_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::GetStrongHashesResponse =
            bincode::deserialize(&payload_buf).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize GetStrongHashesResponse: {}",
                    e
                ))
            })?;

        debug!(
            "TCP server returned {} strong hashes",
            response.hashes.len()
        );

        Ok(response)
    }

    pub async fn list_directory(
        &self,
        stream: &mut TcpStream,
        directory_path: &str,
    ) -> Result<crate::core::protocol::ListDirectoryResponse> {
        debug!("List directory: {}", directory_path);

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

        stream.write_all(&message.encode()).await?;
        stream.flush().await?;

        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        stream.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;
        let payload_len = ISyncMessage::validate_payload_len(payload_len)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut error_buf).await?;
            }
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

        let mut payload_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::ListDirectoryResponse =
            bincode::deserialize(&payload_buf).map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize ListDirectoryResponse: {}",
                    e
                ))
            })?;

        debug!(
            "TCP server returned {} files/directories",
            response.files.len()
        );

        Ok(response)
    }

    pub async fn ping(&self) -> Result<u64> {
        let mut connection = self.get_pooled_connection().await?;
        self.ping_stream(&mut connection).await
    }

    async fn ping_stream(&self, stream: &mut TcpStream) -> Result<u64> {
        let timestamp = unix_timestamp_millis(std::time::SystemTime::now());

        let request = crate::core::protocol::PingRequest { timestamp };
        let payload = bincode::serialize(&request).map_err(|e| {
            HarDataError::SerializationError(format!("Failed to serialize PingRequest: {}", e))
        })?;

        let message = ISyncMessage::new(MessageType::Ping, bytes::Bytes::from(payload));
        stream.write_all(&message.encode()).await?;
        stream.flush().await?;

        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        stream.read_exact(&mut header_buf).await?;

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;
        let payload_len = ISyncMessage::validate_payload_len(payload_len)?;

        if msg_type == MessageType::Error {
            let mut error_buf = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut error_buf).await?;
            }
            let error_msg = String::from_utf8_lossy(&error_buf);
            return Err(HarDataError::ProtocolError(format!(
                "Ping failed: {}",
                error_msg
            )));
        }

        if msg_type != MessageType::Pong {
            return Err(HarDataError::ProtocolError(format!(
                "Expected Pong, got {:?}",
                msg_type
            )));
        }

        let mut payload_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload_buf).await?;
        }

        let response: crate::core::protocol::PongResponse = bincode::deserialize(&payload_buf)
            .map_err(|e| {
                HarDataError::SerializationError(format!(
                    "Failed to deserialize PongResponse: {}",
                    e
                ))
            })?;

        let now = unix_timestamp_millis(std::time::SystemTime::now());

        Ok(now.saturating_sub(response.timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::TcpClient;
    use crate::core::protocol::{GetStrongHashesRequest, ISyncMessage, MessageType};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn get_strong_hashes_rejects_unexpected_response_type() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
            socket.read_exact(&mut header_buf).await.unwrap();
            let (_, payload_len) = ISyncMessage::decode_header(&header_buf).unwrap();
            let payload_len = ISyncMessage::validate_payload_len(payload_len).unwrap();

            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                socket.read_exact(&mut payload).await.unwrap();
            }

            let _: GetStrongHashesRequest = bincode::deserialize(&payload).unwrap();
            let response =
                ISyncMessage::new(MessageType::ListDirectoryResponse, bytes::Bytes::new());
            socket.write_all(&response.encode()).await.unwrap();
        });

        let client = TcpClient::new(addr.to_string()).unwrap();
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let err = client
            .get_strong_hashes(&mut stream, "sample.bin", Vec::new())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Expected GetStrongHashesResponse"));

        server.await.unwrap();
    }
}
