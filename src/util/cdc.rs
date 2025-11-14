use crate::util::error::{HarDataError, Result};
use fastcdc::ronomon::FastCDC;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use tracing::debug;
use xxhash_rust::xxh3::xxh3_64;

pub const DEFAULT_MIN_CHUNK_SIZE: usize = 256 * 1024;
pub const DEFAULT_AVG_CHUNK_SIZE: usize = 2 * 1024 * 1024;
pub const DEFAULT_MAX_CHUNK_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_WINDOW_SIZE: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct StreamingFastCDCConfig {
    pub min_chunk_size: usize,
    pub avg_chunk_size: usize,
    pub max_chunk_size: usize,
    pub window_size: usize,
}

impl Default for StreamingFastCDCConfig {
    fn default() -> Self {
        Self {
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            avg_chunk_size: DEFAULT_AVG_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            window_size: DEFAULT_WINDOW_SIZE,
        }
    }
}

pub struct StreamingFastCDC {
    config: StreamingFastCDCConfig,
}

#[derive(Debug, Clone)]
pub struct ChunkEntry {
    pub offset: u64,
    pub length: usize,
    pub weak_hash: u64,
    pub hash: Option<[u8; 32]>,
}

impl StreamingFastCDC {
    pub fn new(config: StreamingFastCDCConfig) -> Self {
        Self { config }
    }

    pub async fn chunk_file_weak_only(&self, path: &Path) -> Result<Vec<ChunkEntry>> {
        self.chunk_file_mmap(path, false).await
    }

    pub async fn chunk_file(&self, path: &Path) -> Result<Vec<ChunkEntry>> {
        self.chunk_file_mmap(path, true).await
    }

    async fn chunk_file_mmap(
        &self,
        path: &Path,
        compute_strong_hash: bool,
    ) -> Result<Vec<ChunkEntry>> {
        let path = path.to_path_buf();
        let config = self.config.clone();

        tokio::task::spawn_blocking(move || {
            Self::chunk_file_mmap_sync(&path, &config, compute_strong_hash)
        })
        .await
        .map_err(|e| HarDataError::Unknown(format!("Task join error: {}", e)))?
    }

    fn chunk_file_mmap_sync(
        path: &Path,
        config: &StreamingFastCDCConfig,
        compute_strong_hash: bool,
    ) -> Result<Vec<ChunkEntry>> {
        let file = File::open(path)
            .map_err(|e| HarDataError::FileOperation(format!("Failed to open file: {}", e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| HarDataError::FileOperation(format!("Failed to get metadata: {}", e)))?
            .len();

        if file_size == 0 {
            return Ok(Vec::new());
        }

        debug!(
            "CDC: size={} MB, chunk_size={}KB-{}MB",
            file_size / 1024 / 1024,
            config.min_chunk_size / 1024,
            config.max_chunk_size / 1024 / 1024
        );

        let mmap = unsafe {
            Mmap::map(&file)
                .map_err(|e| HarDataError::FileOperation(format!("Failed to mmap file: {}", e)))?
        };

        #[cfg(unix)]
        {
            mmap.advise(memmap2::Advice::Sequential)
                .unwrap_or_else(|e| debug!("madvise failed (non-critical): {}", e));
        }

        if file_size <= config.window_size as u64 {
            return Self::chunk_single_window(&mmap, 0, config, compute_strong_hash);
        }

        let mut all_chunks = Vec::new();
        let mut global_offset = 0u64;
        let window_size = config.window_size;

        while global_offset < file_size {
            let window_end = std::cmp::min(global_offset + window_size as u64, file_size);
            let is_last_window = window_end >= file_size;

            let window = &mmap[global_offset as usize..window_end as usize];

            let chunker = FastCDC::new(
                window,
                config.min_chunk_size,
                config.avg_chunk_size,
                config.max_chunk_size,
            );

            let window_chunks: Vec<_> = chunker.collect();

            let mut hashed_chunks: Vec<ChunkEntry> = window_chunks
                .par_iter()
                .map(|entry| {
                    let chunk_data = &window[entry.offset..entry.offset + entry.length];
                    let weak_hash = xxh3_64(chunk_data);
                    let hash = if compute_strong_hash {
                        Some(*blake3::hash(chunk_data).as_bytes())
                    } else {
                        None
                    };

                    ChunkEntry {
                        offset: global_offset + entry.offset as u64,
                        length: entry.length,
                        weak_hash,
                        hash,
                    }
                })
                .collect();

            if !is_last_window && !hashed_chunks.is_empty() {
                let last = hashed_chunks.pop().expect("hashed_chunks is not empty");

                global_offset = last.offset;
            } else {
                global_offset = window_end;
            }

            all_chunks.extend(hashed_chunks);

            debug!(
                "Processed window: {} chunks so far, next_offset={}",
                all_chunks.len(),
                global_offset
            );
        }

        debug!(
            "CDC done: {} chunks, avg_size={} KB",
            all_chunks.len(),
            if all_chunks.is_empty() {
                0
            } else {
                file_size / all_chunks.len() as u64 / 1024
            }
        );

        Ok(all_chunks)
    }

    fn chunk_single_window(
        data: &[u8],
        base_offset: u64,
        config: &StreamingFastCDCConfig,
        compute_strong_hash: bool,
    ) -> Result<Vec<ChunkEntry>> {
        let chunker = FastCDC::new(
            data,
            config.min_chunk_size,
            config.avg_chunk_size,
            config.max_chunk_size,
        );

        let window_chunks: Vec<_> = chunker.collect();

        let chunks: Vec<ChunkEntry> = window_chunks
            .par_iter()
            .map(|entry| {
                let chunk_data = &data[entry.offset..entry.offset + entry.length];
                let weak_hash = xxh3_64(chunk_data);
                let hash = if compute_strong_hash {
                    Some(*blake3::hash(chunk_data).as_bytes())
                } else {
                    None
                };

                ChunkEntry {
                    offset: base_offset + entry.offset as u64,
                    length: entry.length,
                    weak_hash,
                    hash,
                }
            })
            .collect();

        Ok(chunks)
    }
}
