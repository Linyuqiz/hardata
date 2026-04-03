use crate::core::chunk::ChunkHash;
use crate::sync::engine::core::FileChunk;
use crate::sync::net::transport::TransportConnection;
use crate::util::error::Result;
use tracing::debug;

use crate::sync::engine::scheduler::infrastructure::config::SchedulerConfig;

pub async fn chunk_file(
    config: &SchedulerConfig,
    path: &std::path::Path,
    connection: &mut TransportConnection,
) -> Result<Vec<FileChunk>> {
    let path_str = build_remote_scan_request_path(path);

    debug!("Requesting remote scan for file: {}", path_str,);

    let scan_response = match connection {
        TransportConnection::Quic { client, connection } => {
            client
                .get_file_hashes(
                    connection,
                    &path_str,
                    config.min_chunk_size,
                    config.avg_chunk_size,
                    config.max_chunk_size,
                )
                .await?
        }
        TransportConnection::Tcp { client } => {
            let mut stream = client.get_pooled_connection().await?;
            client
                .get_file_hashes(
                    &mut stream,
                    &path_str,
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
                file_path: path_str.clone(),
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

fn build_remote_scan_request_path(path: &std::path::Path) -> String {
    path.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use super::build_remote_scan_request_path;
    use std::path::Path;

    #[test]
    fn build_remote_scan_request_path_does_not_strip_local_data_dir_prefix() {
        let path = Path::new("/tmp/agent-data/file.txt");
        assert_eq!(
            build_remote_scan_request_path(path),
            "/tmp/agent-data/file.txt"
        );
    }
}
