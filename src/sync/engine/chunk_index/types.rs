use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkLocation {
    pub file_path: String,
    pub offset: u64,
    pub size: u64,
    pub mtime: i64,
    pub strong_hash: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileScanMetadata {
    pub mtime: i64,
    pub size: u64,
    pub chunk_count: usize,
    pub last_indexed: i64,
}

#[derive(Debug, Clone)]
pub struct DedupResult {
    pub strong_hashes: HashSet<[u8; 32]>,
    pub chunk_info: HashMap<[u8; 32], Vec<ChunkLocation>>,
    pub dedup_count: usize,
}
