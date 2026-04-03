use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::core::FileChunk;
use crate::sync::engine::{copy_chunk_from_file, ChunkLocation};
use crate::util::{
    error::{HarDataError, Result},
    file_ops,
};
use tracing::{debug, warn};

pub async fn relocate_local_chunks(
    chunks_to_relocate: &[(usize, u64, u64, usize)],
    dest_path: &str,
    state: &mut FileTransferState,
    cancel_callback: &crate::sync::transfer::batch::CancelCallback,
) -> Result<u64> {
    if chunks_to_relocate.is_empty() {
        return Ok(0);
    }

    debug!(
        "Relocating {} chunks within local file",
        chunks_to_relocate.len()
    );

    let mut relocate_data: Vec<(usize, u64, Vec<u8>)> = Vec::new();
    for (idx, local_offset, dest_offset, length) in chunks_to_relocate {
        if cancel_callback() {
            return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
        }
        let data = file_ops::read_file_range(dest_path, *local_offset, *length as u64).await?;
        relocate_data.push((*idx, *dest_offset, data));
    }

    relocate_data.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    let mut relocated_bytes = 0u64;
    for (idx, dest_offset, data) in relocate_data {
        if cancel_callback() {
            return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
        }
        file_ops::write_file_range(dest_path, dest_offset, &data).await?;
        state.mark_chunk_completed(idx);
        relocated_bytes += data.len() as u64;
    }

    Ok(relocated_bytes)
}

pub async fn copy_cross_file_chunks(
    chunks: &[FileChunk],
    chunks_to_copy: &[(usize, ChunkLocation, u64)],
    dest_path: &str,
    state: &mut FileTransferState,
    cancel_callback: &crate::sync::transfer::batch::CancelCallback,
) -> Result<(u64, Vec<usize>)> {
    if chunks_to_copy.is_empty() {
        return Ok((0, Vec::new()));
    }

    debug!("Copying {} chunks from other files", chunks_to_copy.len());

    let mut copy_tasks = Vec::with_capacity(chunks_to_copy.len());
    let mut failed_chunks = Vec::new();
    let mut bytes_copied = 0u64;

    for (chunk_idx, location, dest_offset) in chunks_to_copy {
        let chunk = &chunks[*chunk_idx];
        let Some(expected_hash) = chunk.chunk_hash.strong else {
            warn!(
                "Skipping cross-file copy for chunk {} because strong hash is missing",
                chunk_idx
            );
            failed_chunks.push(*chunk_idx);
            continue;
        };

        copy_tasks.push((*chunk_idx, location.clone(), *dest_offset, expected_hash));
    }

    copy_tasks.sort_by(|a, b| {
        a.1.file_path
            .cmp(&b.1.file_path)
            .then_with(|| a.1.offset.cmp(&b.1.offset))
    });

    let mut success_count = 0usize;
    let mut verification_failed = 0usize;
    let mut io_failed = 0usize;

    for (chunk_idx, source, dest_offset, expected_hash) in copy_tasks {
        if cancel_callback() {
            return Err(HarDataError::Unknown("Job cancelled by user".to_string()));
        }

        match copy_chunk_from_file(&source, dest_path, dest_offset, expected_hash).await? {
            crate::sync::engine::CopyResult::Success(bytes) => {
                success_count += 1;
                bytes_copied += bytes;
                state.mark_chunk_completed(chunk_idx);
            }
            crate::sync::engine::CopyResult::SourceNotFound
            | crate::sync::engine::CopyResult::SourceModified
            | crate::sync::engine::CopyResult::HashMismatch => {
                verification_failed += 1;
                failed_chunks.push(chunk_idx);
            }
            crate::sync::engine::CopyResult::IOError(e) => {
                warn!("I/O error when copying chunk: {}", e);
                io_failed += 1;
                failed_chunks.push(chunk_idx);
            }
        }
    }

    failed_chunks.sort_unstable();
    failed_chunks.dedup();

    debug!(
        "Cross-file copy completed: {} success, {} failed, {} bytes copied",
        success_count,
        verification_failed + io_failed,
        bytes_copied
    );

    Ok((bytes_copied, failed_chunks))
}

#[cfg(test)]
mod tests {
    use super::{copy_cross_file_chunks, relocate_local_chunks};
    use crate::core::chunk::ChunkHash;
    use crate::core::transfer_state::FileTransferState;
    use crate::sync::engine::core::FileChunk;
    use crate::sync::engine::ChunkLocation;

    #[tokio::test]
    async fn missing_strong_hash_falls_back_without_marking_completed() {
        let temp_dir = std::env::temp_dir();
        let dest_path = temp_dir.join("hardata-local-copy-dest.bin");
        let _ = tokio::fs::write(&dest_path, b"").await;
        let mut state = FileTransferState::new("dest.bin".to_string(), 1);
        let chunks = vec![FileChunk {
            file_path: "source.bin".to_string(),
            offset: 0,
            length: 4,
            chunk_hash: ChunkHash {
                weak: 42,
                strong: None,
            },
        }];
        let location = ChunkLocation {
            file_path: "source.bin".to_string(),
            offset: 0,
            size: 4,
            mtime: 0,
            strong_hash: Some([1; 32]),
        };

        let (bytes_copied, failed_chunks) = copy_cross_file_chunks(
            &chunks,
            &[(0, location, 0)],
            dest_path.to_str().unwrap(),
            &mut state,
            &(std::sync::Arc::new(|| false) as crate::sync::transfer::batch::CancelCallback),
        )
        .await
        .unwrap();

        assert_eq!(bytes_copied, 0);
        assert_eq!(failed_chunks, vec![0]);
        assert!(!state.is_chunk_completed(0));

        let _ = tokio::fs::remove_file(dest_path).await;
    }

    #[tokio::test]
    async fn copy_cross_file_chunks_stops_when_cancelled() {
        let temp_dir = std::env::temp_dir();
        let source_path = temp_dir.join(format!(
            "hardata-local-copy-source-{}.bin",
            uuid::Uuid::new_v4()
        ));
        let dest_path = temp_dir.join(format!(
            "hardata-local-copy-cancel-{}.bin",
            uuid::Uuid::new_v4()
        ));
        let source_data = b"AAAABBBB";
        tokio::fs::write(&source_path, source_data).await.unwrap();
        tokio::fs::write(&dest_path, b"XXXXXXXX").await.unwrap();
        let metadata = tokio::fs::metadata(&source_path).await.unwrap();
        let mtime = crate::util::time::metadata_mtime_nanos(&metadata);

        let mut state = FileTransferState::new("dest.bin".to_string(), 2);
        let chunks = vec![
            FileChunk {
                file_path: "source.bin".to_string(),
                offset: 0,
                length: 4,
                chunk_hash: ChunkHash {
                    weak: 1,
                    strong: Some(*blake3::hash(b"AAAA").as_bytes()),
                },
            },
            FileChunk {
                file_path: "source.bin".to_string(),
                offset: 4,
                length: 4,
                chunk_hash: ChunkHash {
                    weak: 2,
                    strong: Some(*blake3::hash(b"BBBB").as_bytes()),
                },
            },
        ];
        let locations = [
            ChunkLocation {
                file_path: source_path.to_string_lossy().to_string(),
                offset: 0,
                size: 4,
                mtime,
                strong_hash: Some(*blake3::hash(b"AAAA").as_bytes()),
            },
            ChunkLocation {
                file_path: source_path.to_string_lossy().to_string(),
                offset: 4,
                size: 4,
                mtime,
                strong_hash: Some(*blake3::hash(b"BBBB").as_bytes()),
            },
        ];
        let cancel_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cancel_counter_clone = cancel_counter.clone();
        let cancel: crate::sync::transfer::batch::CancelCallback = std::sync::Arc::new(move || {
            cancel_counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 1
        });

        let error = copy_cross_file_chunks(
            &chunks,
            &[(0, locations[0].clone(), 0), (1, locations[1].clone(), 4)],
            dest_path.to_str().unwrap(),
            &mut state,
            &cancel,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Job cancelled by user"));
        assert!(state.is_chunk_completed(0));
        assert!(!state.is_chunk_completed(1));
        assert_eq!(tokio::fs::read(&dest_path).await.unwrap(), b"AAAAXXXX");

        let _ = tokio::fs::remove_file(source_path).await;
        let _ = tokio::fs::remove_file(dest_path).await;
    }

    #[tokio::test]
    async fn relocate_local_chunks_stops_when_cancelled() {
        let temp_dir = std::env::temp_dir();
        let dest_path = temp_dir.join(format!(
            "hardata-local-relocate-cancel-{}.bin",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&dest_path, b"AAAABBBB").await.unwrap();
        let mut state = FileTransferState::new("dest.bin".to_string(), 2);
        let cancel_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cancel_counter_clone = cancel_counter.clone();
        let cancel: crate::sync::transfer::batch::CancelCallback = std::sync::Arc::new(move || {
            cancel_counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 2
        });

        let error = relocate_local_chunks(
            &[(0, 0, 4, 4), (1, 4, 0, 4)],
            dest_path.to_str().unwrap(),
            &mut state,
            &cancel,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Job cancelled by user"));
        assert!(!state.is_chunk_completed(0));
        assert!(!state.is_chunk_completed(1));
        assert_eq!(tokio::fs::read(&dest_path).await.unwrap(), b"AAAABBBB");

        let _ = tokio::fs::remove_file(dest_path).await;
    }
}
