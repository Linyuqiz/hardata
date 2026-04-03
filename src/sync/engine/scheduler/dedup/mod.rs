mod global;
mod local;
mod remote;

use crate::sync::engine::core::FileChunk;
use crate::sync::engine::ChunkIndex;
use crate::sync::net::transport::TransportConnection;
use crate::sync::scanner::ScannedFile;
use crate::util::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

use super::infrastructure::config::SchedulerConfig;
use super::optimization::PrefetchManager;

pub use global::GlobalChunkInfo;
pub use remote::LocalChunkInfo;

#[allow(clippy::too_many_arguments)]
pub async fn check_deduplication(
    config: &SchedulerConfig,
    chunks: &mut [FileChunk],
    connection: &mut TransportConnection,
    file: &ScannedFile,
    dest_path: &str,
    prefetch_manager: Option<&Arc<PrefetchManager>>,
    chunk_index: Option<&Arc<crate::sync::engine::CDCResultCache>>,
    global_index: Option<&Arc<ChunkIndex>>,
) -> Result<(
    HashSet<[u8; 32]>,
    HashSet<u64>,
    usize,
    LocalChunkInfo,
    GlobalChunkInfo,
)> {
    let mut existing_strong_hashes: HashSet<[u8; 32]> = HashSet::new();
    let mut existing_weak_hashes: HashSet<u64> = HashSet::new();
    let mut local_chunk_info: LocalChunkInfo = HashMap::new();
    let mut global_chunk_info: GlobalChunkInfo = HashMap::new();
    let mut dedup_count = 0;

    let cdc_result = local::check_local_cdc(config, dest_path, chunk_index).await?;

    if let Some(cdc_result) = cdc_result {
        let match_result = local::match_weak_hashes(chunks, &cdc_result.chunks);
        existing_weak_hashes = match_result.existing_weak_hashes;

        if !match_result.weak_matched_indices.is_empty() {
            let verify_result = remote::verify_strong_hashes(
                chunks,
                &match_result.weak_matched_indices,
                &match_result.local_chunk_map,
                connection,
                file,
                dest_path,
                cdc_result.mtime,
                prefetch_manager,
            )
            .await?;

            existing_strong_hashes = verify_result.existing_strong_hashes.clone();
            local_chunk_info = verify_result.local_chunk_info;
            dedup_count = verify_result.dedup_count;

            if !verify_result.computed_strong_hashes.is_empty() {
                if let Some(cache) = chunk_index {
                    remote::update_cache(
                        cache,
                        dest_path,
                        cdc_result.chunks,
                        &verify_result.computed_strong_hashes,
                        cdc_result.mtime,
                        cdc_result.file_size,
                    );
                }
            }
        }
    }

    if let Some(gindex) = global_index {
        let (global_hashes, global_locations, global_count) = global::check_global_dedup(
            chunks,
            &existing_strong_hashes,
            gindex,
            dest_path,
            connection,
            file,
        )
        .await?;

        debug!(
            "Merging global dedup results: global_locations size={}, global_count={}",
            global_locations.len(),
            global_count
        );

        existing_strong_hashes.extend(global_hashes);
        global_chunk_info.extend(global_locations);
        dedup_count += global_count;

        debug!(
            "After merge: global_chunk_info size={}",
            global_chunk_info.len()
        );
    }

    Ok((
        existing_strong_hashes,
        existing_weak_hashes,
        dedup_count,
        local_chunk_info,
        global_chunk_info,
    ))
}
