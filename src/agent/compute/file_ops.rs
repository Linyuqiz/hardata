use crate::util::error::{HarDataError, Result};
use crate::util::time::{metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos};
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

        Self::ensure_regular_file(target_file_path).await?;

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
                let file_modified = entry.mtime != current_mtime
                    || entry.file_size != file_size
                    || entry.min_chunk_size != min_chunk_size
                    || entry.avg_chunk_size != avg_chunk_size
                    || entry.max_chunk_size != max_chunk_size;

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
                min_chunk_size,
                avg_chunk_size,
                max_chunk_size,
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

        let root_symlink_metadata = fs::symlink_metadata(directory_path).await.map_err(|e| {
            HarDataError::FileOperation(format!("Failed to read root path metadata: {}", e))
        })?;
        if root_symlink_metadata.file_type().is_symlink() {
            let file_name = directory_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            let modified = metadata_mtime_nanos(&root_symlink_metadata);
            let symlink_target = fs::read_link(directory_path)
                .await
                .ok()
                .map(|p| p.to_string_lossy().to_string());

            #[cfg(unix)]
            let mode = {
                use std::os::unix::fs::MetadataExt;
                root_symlink_metadata.mode()
            };
            #[cfg(not(unix))]
            let mode = 0u32;

            info!("Root symlink path detected: {:?}", directory_path);
            return Ok(vec![crate::core::FileInfo {
                path: file_name,
                size: root_symlink_metadata.len(),
                is_directory: false,
                modified,
                change_time: metadata_ctime_nanos(&root_symlink_metadata),
                inode: metadata_inode(&root_symlink_metadata),
                mode,
                is_symlink: true,
                symlink_target,
            }]);
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

            let modified = metadata_mtime_nanos(&metadata);

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
                change_time: metadata_ctime_nanos(&metadata),
                inode: metadata_inode(&metadata),
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

            let modified = metadata_mtime_nanos(&metadata);

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
                change_time: metadata_ctime_nanos(&metadata),
                inode: metadata_inode(&metadata),
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

        Self::ensure_regular_file(file_path).await?;

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

#[cfg(test)]
mod tests {
    use super::ComputeService;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-agent-file-ops-{name}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_directory_returns_root_symlink_as_single_entry() {
        let root = temp_dir("root-symlink");
        let target = root.join("target.txt");
        let link = root.join("link.txt");
        std::fs::write(&target, b"payload").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();
        let files = compute.list_directory(&link).await.unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "link.txt");
        assert!(files[0].is_symlink);
        assert!(!files[0].is_directory);
        assert!(files[0]
            .symlink_target
            .as_ref()
            .is_some_and(|target| target.ends_with("target.txt")));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_directory_returns_root_symlink_directory_as_single_entry() {
        let root = temp_dir("root-symlink-dir");
        let target = root.join("target-dir");
        let link = root.join("link-dir");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("file.txt"), b"payload").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();
        let files = compute.list_directory(&link).await.unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "link-dir");
        assert!(files[0].is_symlink);
        assert!(!files[0].is_directory);
        assert!(files[0]
            .symlink_target
            .as_ref()
            .is_some_and(|target| target.ends_with("target-dir")));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_directory_returns_broken_root_symlink_as_single_entry() {
        let root = temp_dir("root-broken-symlink");
        let link = root.join("broken-link.txt");
        std::os::unix::fs::symlink("missing.txt", &link).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();
        let files = compute.list_directory(&link).await.unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "broken-link.txt");
        assert!(files[0].is_symlink);
        assert!(!files[0].is_directory);
        assert_eq!(files[0].symlink_target.as_deref(), Some("missing.txt"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_file_hashes_rejects_symlink_paths() {
        let root = temp_dir("hash-symlink");
        let outside = temp_dir("hash-symlink-outside");
        let target = outside.join("secret.txt");
        let link = root.join("secret-link.txt");
        std::fs::write(&target, b"secret").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();
        let err = compute
            .get_file_hashes(&link, 1024, 2048, 4096)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Refusing to read symlink path"));

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_block_by_offset_rejects_symlink_paths() {
        let root = temp_dir("read-symlink");
        let outside = temp_dir("read-symlink-outside");
        let target = outside.join("secret.txt");
        let link = root.join("secret-link.txt");
        std::fs::write(&target, b"secret").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();
        let err = compute.read_block_by_offset(&link, 0, 4).await.unwrap_err();

        assert!(err.to_string().contains("Refusing to read symlink path"));

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[tokio::test]
    async fn get_file_hashes_cache_misses_when_chunk_profile_changes() {
        let root = temp_dir("hash-cache-profile");
        let file = root.join("payload.bin");
        let payload: Vec<u8> = (0..64 * 1024).map(|idx| (idx % 251) as u8).collect();
        std::fs::write(&file, payload).unwrap();

        let compute = ComputeService::new(root.to_str().unwrap()).await.unwrap();

        compute
            .get_file_hashes(&file, 1024, 2048, 4096)
            .await
            .unwrap();
        let (hits, misses, entries) = compute.cache_stats();
        assert_eq!((hits, misses, entries), (0, 1, 1));

        compute
            .get_file_hashes(&file, 1024, 2048, 4096)
            .await
            .unwrap();
        let (hits, misses, entries) = compute.cache_stats();
        assert_eq!((hits, misses, entries), (1, 1, 1));

        compute
            .get_file_hashes(&file, 2048, 4096, 8192)
            .await
            .unwrap();
        let (hits, misses, entries) = compute.cache_stats();
        assert_eq!((hits, misses, entries), (1, 2, 1));

        let _ = std::fs::remove_dir_all(root);
    }
}
