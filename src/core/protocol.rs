use crate::util::error::HarDataError;
use crate::util::error::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    ListDirectoryRequest = 0x01,
    ListDirectoryResponse = 0x02,
    GetFileHashesRequest = 0x03,
    GetFileHashesResponse = 0x04,
    GetStrongHashesRequest = 0x05,
    GetStrongHashesResponse = 0x06,

    ReadBlockRequest = 0x10,
    ReadBlockResponse = 0x11,

    CreateEmptyFileRequest = 0x12,
    CreateEmptyFileResponse = 0x13,

    Ping = 0x20,
    Pong = 0x21,

    Error = 0xFF,
}

impl MessageType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Self::ListDirectoryRequest),
            0x02 => Ok(Self::ListDirectoryResponse),
            0x03 => Ok(Self::GetFileHashesRequest),
            0x04 => Ok(Self::GetFileHashesResponse),
            0x05 => Ok(Self::GetStrongHashesRequest),
            0x06 => Ok(Self::GetStrongHashesResponse),
            0x10 => Ok(Self::ReadBlockRequest),
            0x11 => Ok(Self::ReadBlockResponse),
            0x12 => Ok(Self::CreateEmptyFileRequest),
            0x13 => Ok(Self::CreateEmptyFileResponse),
            0x20 => Ok(Self::Ping),
            0x21 => Ok(Self::Pong),
            0xFF => Ok(Self::Error),
            _ => Err(HarDataError::InvalidProtocol(format!(
                "Invalid message type: {:#x}",
                value
            ))),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

#[derive(Debug, Clone)]
pub struct ISyncMessage {
    pub msg_type: MessageType,
    pub payload_len: u32,
    pub payload: Bytes,
}

impl ISyncMessage {
    pub const HEADER_SIZE: usize = 5;

    pub fn new(msg_type: MessageType, payload: Bytes) -> Self {
        let payload_len = payload.len() as u32;
        Self {
            msg_type,
            payload_len,
            payload,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::HEADER_SIZE + self.payload.len());
        buf.put_u8(self.msg_type.as_u8());
        buf.put_u32_le(self.payload_len);
        buf.put(self.payload.clone());

        buf.freeze()
    }

    pub fn decode_header(buf: &[u8]) -> Result<(MessageType, u32)> {
        if buf.len() < Self::HEADER_SIZE {
            return Err(HarDataError::InvalidProtocol(
                "Buffer too small for header".to_string(),
            ));
        }
        let mut cursor = &buf[..Self::HEADER_SIZE];
        let msg_type = MessageType::from_u8(cursor.get_u8())?;
        let payload_len = cursor.get_u32_le();
        Ok((msg_type, payload_len))
    }

    pub fn validate_payload_len(payload_len: u32) -> Result<usize> {
        let payload_len = usize::try_from(payload_len).map_err(|_| {
            HarDataError::InvalidProtocol(format!(
                "Payload size {} exceeds platform limits",
                payload_len
            ))
        })?;

        if payload_len > crate::core::constants::MAX_PROTOCOL_PAYLOAD_SIZE {
            return Err(HarDataError::InvalidProtocol(format!(
                "Payload size {} exceeds maximum allowed size {}",
                payload_len,
                crate::core::constants::MAX_PROTOCOL_PAYLOAD_SIZE
            )));
        }

        Ok(payload_len)
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        let (msg_type, payload_len) = Self::decode_header(buf)?;
        let validated_payload_len = Self::validate_payload_len(payload_len)?;
        let total_len = Self::HEADER_SIZE + validated_payload_len;
        if buf.len() < total_len {
            return Err(HarDataError::InvalidProtocol(format!(
                "Buffer too small: expected {}, got {}",
                total_len,
                buf.len()
            )));
        }
        let payload = Bytes::copy_from_slice(&buf[Self::HEADER_SIZE..total_len]);
        Ok(Self {
            msg_type,
            payload_len,
            payload,
        })
    }

    pub fn total_len(&self) -> usize {
        Self::HEADER_SIZE + self.payload_len as usize
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDirectoryRequest {
    pub directory_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub is_directory: bool,
    #[serde(default)]
    pub modified: i64,
    #[serde(default)]
    pub change_time: Option<i64>,
    #[serde(default)]
    pub inode: Option<u64>,
    #[serde(default)]
    pub mode: u32,
    #[serde(default)]
    pub is_symlink: bool,
    #[serde(default)]
    pub symlink_target: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDirectoryResponse {
    pub directory_path: String,
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetFileHashesRequest {
    pub file_path: String,
    pub min_chunk_size: usize,
    pub avg_chunk_size: usize,
    pub max_chunk_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    pub offset: u64,
    pub length: u64,
    pub weak_hash: u64,
    pub strong_hash: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetFileHashesResponse {
    pub file_path: String,
    pub file_size: u64,
    pub chunks: Vec<ChunkMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStrongHashesRequest {
    pub file_path: String,
    pub chunks: Vec<ChunkLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrongHashResult {
    pub offset: u64,
    pub strong_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStrongHashesResponse {
    pub file_path: String,
    pub hashes: Vec<StrongHashResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionInfo {
    pub algorithm: String,
    pub original_size: u64,
    pub compressed_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBlockItem {
    pub file_path: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBlockRequest {
    pub items: Vec<ReadBlockItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBlockResult {
    pub index: u32,
    pub success: bool,
    pub data: Vec<u8>,
    pub compression: Option<CompressionInfo>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBlockResponse {
    pub results: Vec<ReadBlockResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongResponse {
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEmptyFileRequest {
    pub file_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEmptyFileResponse {
    pub success: bool,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::ISyncMessage;
    use crate::core::constants::MAX_PROTOCOL_PAYLOAD_SIZE;

    #[test]
    fn validate_payload_len_accepts_configured_limit() {
        assert_eq!(
            ISyncMessage::validate_payload_len(MAX_PROTOCOL_PAYLOAD_SIZE as u32).unwrap(),
            MAX_PROTOCOL_PAYLOAD_SIZE
        );
    }

    #[test]
    fn validate_payload_len_rejects_oversized_payload() {
        let err =
            ISyncMessage::validate_payload_len((MAX_PROTOCOL_PAYLOAD_SIZE + 1) as u32).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum allowed size"));
    }
}
