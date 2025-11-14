use crate::core::transfer_state::FileTransferState;
use crate::sync::engine::core::FileChunk;
use crate::sync::engine::{copy_chunks_batch, ChunkLocation, CopyTask};
use crate::util::{error::Result, file_ops};
use tracing::info;

pub async fn relocate_local_chunks(
    chunks_to_relocate: &[(usize, u64, u64, usize)],
    dest_path: &str,
    state: &mut FileTransferState,
) -> Result<u64> {
    if chunks_to_relocate.is_empty() {
        return Ok(0);
    }

    info!(
        "Relocating {} chunks within local file",
        chunks_to_relocate.len()
    );

    let mut relocate_data: Vec<(usize, u64, Vec<u8>)> = Vec::new();
    for (idx, local_offset, dest_offset, length) in chunks_to_relocate {
        let data = file_ops::read_file_range(dest_path, *local_offset, *length as u64).await?;
        relocate_data.push((*idx, *dest_offset, data));
    }

    relocate_data.sort_by(|a, b| b.1.cmp(&a.1));

    let mut relocated_bytes = 0u64;
    for (idx, dest_offset, data) in relocate_data {
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
) -> Result<(u64, Vec<usize>)> {
    if chunks_to_copy.is_empty() {
        return Ok((0, Vec::new()));
    }

    info!("Copying {} chunks from other files", chunks_to_copy.len());

    let copy_tasks: Vec<CopyTask> = chunks_to_copy
        .iter()
        .map(|(chunk_idx, location, dest_offset)| {
            let chunk = &chunks[*chunk_idx];
            CopyTask {
                chunk_index: *chunk_idx,
                source: location.clone(),
                dest_offset: *dest_offset,
                expected_hash: chunk.chunk_hash.strong.unwrap(),
            }
        })
        .collect();

    let stats = copy_chunks_batch(copy_tasks, dest_path).await?;

    for &chunk_idx in chunks_to_copy.iter().map(|(idx, _, _)| idx) {
        if !stats.failed_chunks.contains(&chunk_idx) {
            state.mark_chunk_completed(chunk_idx);
        }
    }

    info!(
        "Cross-file copy completed: {} success, {} failed, {} bytes copied",
        stats.success_count,
        stats.failed_count(),
        stats.bytes_copied
    );

    Ok((stats.bytes_copied, stats.failed_chunks))
}
