use crate::core::chunk::ChunkHash;

#[derive(Debug, Clone)]
pub struct FileChunk {
    pub file_path: String,
    pub offset: u64,
    pub length: u64,
    pub chunk_hash: ChunkHash,
}
