use crate::core::chunk::ChunkHash;
use crate::sync::engine::core::FileChunk;
use crate::sync::net::transport::TransportConnection;
use crate::util::error::Result;
use tracing::{debug, info};

use crate::sync::engine::scheduler::infrastructure::config::SchedulerConfig;

pub async fn chunk_file(
    config: &SchedulerConfig,
    path: &std::path::Path,
    connection: &mut TransportConnection,
) -> Result<Vec<FileChunk>> {
    let path_str = path.to_string_lossy().to_string();

    let relative_path = path_str
        .strip_prefix(&config.data_dir)
        .map(|p| p.trim_start_matches('/'))
        .unwrap_or(&path_str);

    info!(
        "Requesting remote scan for file: {} (relative: {})",
        path_str, relative_path
    );

    let scan_response = match connection {
        TransportConnection::Quic { client, connection } => {
            client
                .get_file_hashes(
                    connection,
                    relative_path,
                    config.min_chunk_size,
                    config.avg_chunk_size,
                    config.max_chunk_size,
                )
                .await?
        }
        TransportConnection::Tcp { client } => {
            let mut stream = client.connect().await?;
            client
                .get_file_hashes(
                    &mut stream,
                    relative_path,
                    config.min_chunk_size,
                    config.avg_chunk_size,
                    config.max_chunk_size,
                )
                .await?
        }
    };

    debug!(
        "Remote scan completed: file_size={}, chunks={}",
        scan_response.file_size,
        scan_response.chunks.len()
    );

    let chunks: Vec<FileChunk> = scan_response
        .chunks
        .into_iter()
        .map(|chunk_meta| {
            let chunk_hash = ChunkHash {
                weak: chunk_meta.weak_hash,
                strong: chunk_meta.strong_hash,
            };

            FileChunk {
                file_path: relative_path.to_string(),
                offset: chunk_meta.offset,
                length: chunk_meta.length,
                chunk_hash,
            }
        })
        .collect();

    debug!(
        "Converted {} chunks from remote scan response",
        chunks.len()
    );

    Ok(chunks)
}
