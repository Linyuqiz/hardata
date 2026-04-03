use crate::util::error::{HarDataError, Result};
use crate::util::file_ops::read_exact_at;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use tracing::info;

use super::types::ComputeService;

fn validate_chunk_range(
    file_len: usize,
    offset: u64,
    length: u64,
) -> std::io::Result<(usize, usize)> {
    let start = usize::try_from(offset).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Chunk offset {} exceeds platform limits", offset),
        )
    })?;
    let len = usize::try_from(length).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Chunk length {} exceeds platform limits", length),
        )
    })?;
    let end = start.checked_add(len).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Chunk range overflow: offset={} length={}", offset, length),
        )
    })?;

    if end > file_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Chunk range out of bounds: offset={} length={} file_size={}",
                offset, length, file_len
            ),
        ));
    }

    Ok((start, end))
}

impl ComputeService {
    pub async fn get_strong_hashes(
        &self,
        target_file_path: &Path,
        chunks: &[crate::core::ChunkLocation],
    ) -> Result<Vec<crate::core::StrongHashResult>> {
        Self::ensure_regular_file(target_file_path).await?;

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
            let file_len = usize::try_from(file.metadata()?.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "File size exceeds platform limits",
                )
            })?;
            let validated_chunks: Vec<(u64, usize)> = chunks_vec
                .iter()
                .map(|(offset, length)| {
                    validate_chunk_range(file_len, *offset, *length)
                        .map(|(start, end)| (*offset, end - start))
                })
                .collect::<std::io::Result<Vec<_>>>()?;

            let results: Vec<crate::core::StrongHashResult> = validated_chunks
                .par_iter()
                .map(
                    |(offset, length)| -> std::io::Result<crate::core::StrongHashResult> {
                        let mut data = vec![0u8; *length];
                        read_exact_at(&file, *offset, &mut data)?;
                        let strong_hash = *blake3::hash(&data).as_bytes();
                        Ok(crate::core::StrongHashResult {
                            offset: *offset,
                            strong_hash,
                        })
                    },
                )
                .collect::<std::io::Result<Vec<_>>>()?;

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

#[cfg(test)]
mod tests {
    use super::validate_chunk_range;
    use crate::agent::compute::ComputeService;
    use crate::core::ChunkLocation;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-agent-hash-{name}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn validate_chunk_range_rejects_overflow() {
        let err = validate_chunk_range(128, u64::MAX, 1).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn validate_chunk_range_rejects_out_of_bounds() {
        let err = validate_chunk_range(128, 120, 16).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn validate_chunk_range_accepts_valid_input() {
        assert_eq!(validate_chunk_range(128, 16, 32).unwrap(), (16, 48));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_strong_hashes_rejects_symlink_paths() {
        let root = temp_dir("strong-symlink");
        let outside = temp_dir("strong-symlink-outside");
        let target = outside.join("secret.txt");
        let link = root.join("secret-link.txt");
        std::fs::write(&target, b"secret").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();
        let err = compute
            .get_strong_hashes(
                &link,
                &[ChunkLocation {
                    offset: 0,
                    length: 4,
                }],
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Refusing to read symlink path"));

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside);
    }
}
