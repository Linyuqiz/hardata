use crate::adapters::outbound::transport::gateway::TransportConnection;
use crate::application::sync::engine::core::FileChunk;
use crate::application::sync::engine::{ChunkIndex, ChunkLocation};
use crate::application::sync::scanner::ScannedFile;
use crate::shared::error::Result;
use std::collections::{HashMap, HashSet};
use tracing::debug;

pub type GlobalChunkInfo = HashMap<[u8; 32], Vec<ChunkLocation>>;

pub async fn check_global_dedup(
    chunks: &mut [FileChunk],
    existing_strong_hashes: &HashSet<[u8; 32]>,
    global_index: &ChunkIndex,
    dest_path: &str,
    connection: &mut TransportConnection,
    file: &ScannedFile,
) -> Result<(HashSet<[u8; 32]>, GlobalChunkInfo, usize)> {
    let result = global_index
        .query_chunks(chunks, dest_path, existing_strong_hashes, connection, file)
        .await?;

    debug!(
        "Global dedup check completed: {} chunks found, chunk_info size: {}",
        result.dedup_count,
        result.chunk_info.len()
    );

    Ok((result.strong_hashes, result.chunk_info, result.dedup_count))
}
