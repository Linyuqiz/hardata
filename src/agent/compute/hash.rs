use crate::util::error::{HarDataError, Result};
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use tracing::info;

use super::types::ComputeService;

impl ComputeService {
    pub async fn get_strong_hashes(
        &self,
        target_file_path: &Path,
        chunks: &[crate::core::ChunkLocation],
    ) -> Result<Vec<crate::core::StrongHashResult>> {
        if !target_file_path.exists() {
            return Err(HarDataError::FileOperation(format!(
                "File not found: {:?}",
                target_file_path
            )));
        }

        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "Calculating strong hashes for {} chunks in {:?} (parallel)",
            chunks.len(),
            target_file_path
        );

        let path = target_file_path.to_path_buf();
        let chunks_vec: Vec<_> = chunks.iter().map(|c| (c.offset, c.length)).collect();

        let results = tokio::task::spawn_blocking(move || {
            let file = File::open(&path)?;
            let mmap = unsafe { Mmap::map(&file)? };

            #[cfg(unix)]
            {
                let _ = mmap.advise(memmap2::Advice::WillNeed);
            }

            let results: Vec<crate::core::StrongHashResult> = chunks_vec
                .par_iter()
                .map(|(offset, length)| {
                    let data = &mmap[*offset as usize..(*offset as usize + *length as usize)];
                    let strong_hash = *blake3::hash(data).as_bytes();
                    crate::core::StrongHashResult {
                        offset: *offset,
                        strong_hash,
                    }
                })
                .collect();

            Ok::<_, std::io::Error>(results)
        })
        .await
        .map_err(|e| HarDataError::Unknown(format!("Task join error: {}", e)))?
        .map_err(|e| HarDataError::FileOperation(format!("Failed to compute hashes: {}", e)))?;

        info!(
            "Calculated {} strong hashes for {:?} (parallel completed)",
            results.len(),
            target_file_path
        );

        Ok(results)
    }
}
