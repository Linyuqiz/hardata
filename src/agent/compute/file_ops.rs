use crate::util::error::{HarDataError, Result};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::info;

use super::types::{CacheEntry, ComputeService};

impl ComputeService {
    pub async fn get_file_hashes(
        &self,
        target_file_path: &Path,
        min_chunk_size: usize,
        avg_chunk_size: usize,
        max_chunk_size: usize,
    ) -> Result<(u64, Arc<Vec<crate::core::ChunkMetadata>>)> {
        use crate::util::cdc::{StreamingFastCDC, StreamingFastCDCConfig};

        if !target_file_path.exists() {
            return Err(HarDataError::FileOperation(format!(
                "File not found: {:?}",
                target_file_path
            )));
        }

        let metadata = tokio::fs::metadata(target_file_path)
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to get metadata: {}", e)))?;
        let file_size = metadata.len();
        let current_mtime = Self::get_file_mtime(target_file_path).await?;

        let relative_path = target_file_path
            .strip_prefix(&self.data_dir)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| target_file_path.to_string_lossy().to_string());

        let cache_key = target_file_path.to_path_buf();
        let cache_result = {
            if let Some(mut entry) = self.hash_cache.get_mut(&cache_key) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                let age_secs = now.saturating_sub(entry.created_at);
                let ttl_expired = age_secs > super::types::CACHE_TTL_SECS;
                let file_modified = entry.mtime != current_mtime || entry.file_size != file_size;

                if !ttl_expired && !file_modified {
                    entry.last_access = now;
                    Some((entry.file_size, Arc::clone(&entry.chunks)))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some((cached_file_size, cached_chunks)) = cache_result {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((cached_file_size, cached_chunks));
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        info!(
            "Cache MISS for {}: calculating FastCDC ({}/{}/{})",
            relative_path, min_chunk_size, avg_chunk_size, max_chunk_size
        );

        let config = StreamingFastCDCConfig {
            min_chunk_size,
            avg_chunk_size,
            max_chunk_size,
            window_size: 1024 * 1024 * 1024,
        };
        let cdc = StreamingFastCDC::new(config);
        let chunk_entries = cdc
            .chunk_file_weak_only(target_file_path)
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to chunk file: {}", e)))?;

        info!(
            "File {} chunked into {} blocks, total size: {} bytes",
            relative_path,
            chunk_entries.len(),
            file_size
        );

        let chunks: Vec<crate::core::ChunkMetadata> = chunk_entries
            .into_iter()
            .map(|entry| crate::core::ChunkMetadata {
                offset: entry.offset,
                length: entry.length as u64,
                weak_hash: entry.weak_hash,
                strong_hash: None,
            })
            .collect();

        let chunks_arc = Arc::new(chunks);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.hash_cache.insert(
            cache_key,
            CacheEntry {
                mtime: current_mtime,
                file_size,
                chunks: Arc::clone(&chunks_arc),
                created_at: now,
                last_access: now,
            },
        );

        self.maybe_cleanup_cache();

        Ok((file_size, chunks_arc))
    }

    pub async fn list_directory(
        &self,
        directory_path: &Path,
    ) -> Result<Vec<crate::core::FileInfo>> {
        use tokio::fs;

        if !directory_path.exists() {
            return Err(HarDataError::FileOperation(format!(
                "Path does not exist: {:?}",
                directory_path
            )));
        }

        if directory_path.is_file() {
            let metadata = fs::metadata(directory_path).await.map_err(|e| {
                HarDataError::FileOperation(format!("Failed to read file metadata: {}", e))
            })?;

            let file_name = directory_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            let modified = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            #[cfg(unix)]
            let mode = {
                use std::os::unix::fs::MetadataExt;
                metadata.mode()
            };
            #[cfg(not(unix))]
            let mode = 0u32;

            info!("Single file path detected: {:?}", directory_path);
            return Ok(vec![crate::core::FileInfo {
                path: file_name,
                size: metadata.len(),
                is_directory: false,
                modified,
                mode,
                is_symlink: false,
                symlink_target: None,
            }]);
        }

        let mut files = Vec::new();
        let mut entries = fs::read_dir(directory_path)
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to read directory: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to read entry: {}", e)))?
        {
            let path = entry.path();

            let sym_metadata = fs::symlink_metadata(&path).await.map_err(|e| {
                HarDataError::FileOperation(format!("Failed to read symlink metadata: {}", e))
            })?;

            let is_symlink = sym_metadata.file_type().is_symlink();
            let symlink_target = if is_symlink {
                fs::read_link(&path)
                    .await
                    .ok()
                    .map(|p| p.to_string_lossy().to_string())
            } else {
                None
            };

            let metadata = if is_symlink {
                sym_metadata
            } else {
                entry.metadata().await.map_err(|e| {
                    HarDataError::FileOperation(format!("Failed to read metadata: {}", e))
                })?
            };

            let relative_path = path
                .strip_prefix(directory_path)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();

            let modified = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            #[cfg(unix)]
            let mode = {
                use std::os::unix::fs::MetadataExt;
                metadata.mode()
            };
            #[cfg(not(unix))]
            let mode = 0u32;

            files.push(crate::core::FileInfo {
                path: relative_path,
                size: metadata.len(),
                is_directory: metadata.is_dir(),
                modified,
                mode,
                is_symlink,
                symlink_target,
            });
        }

        info!(
            "Listed {} files/directories in {:?}",
            files.len(),
            directory_path
        );

        Ok(files)
    }

    pub async fn read_block_by_offset(
        &self,
        file_path: &Path,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        use crate::util::handle_pool::acquire_file_handle;

        if !file_path.exists() {
            return Err(HarDataError::FileOperation(format!(
                "File does not exist: {:?}",
                file_path
            )));
        }

        let mut handle = acquire_file_handle(file_path)
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to acquire handle: {}", e)))?;

        let data = handle
            .read_at(offset, length as usize)
            .await
            .map_err(|e| HarDataError::FileOperation(format!("Failed to read: {}", e)))?;

        Ok(data)
    }
}
