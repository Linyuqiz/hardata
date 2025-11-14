use crate::util::error::{HarDataError, Result};
use std::path::{Path, PathBuf};
use tracing::warn;

pub const MAX_PAYLOAD_SIZE: usize = 256 * 1024 * 1024;
pub const MAX_BLOCK_SIZE: u64 = 100 * 1024 * 1024;
pub const MAX_CONCURRENT_CONNECTIONS: usize = 1000;
pub const REQUEST_TIMEOUT_SECS: u64 = 300;

pub fn compress_block_data(
    target_path: &Path,
    data: Vec<u8>,
) -> (Vec<u8>, Option<crate::core::CompressionInfo>) {
    use crate::util::compression::{smart_compress, CompressionLevel, FileTypeDetector};

    let original_size = data.len() as u64;

    let data_sample = if data.len() >= 4 {
        Some(&data[..4])
    } else {
        None
    };
    let file_type = FileTypeDetector::detect(target_path, data_sample);

    if !file_type.should_compress() {
        return (data, None);
    }

    match smart_compress(&data, target_path, CompressionLevel::Fastest) {
        Ok((compressed, algorithm)) => {
            let compressed_size = compressed.len() as u64;
            if algorithm == "none" {
                (data, None)
            } else {
                (
                    compressed,
                    Some(crate::core::CompressionInfo {
                        algorithm,
                        original_size,
                        compressed_size,
                    }),
                )
            }
        }
        Err(e) => {
            warn!("Compression failed: {}", e);
            (data, None)
        }
    }
}

pub fn resolve_request_path(data_dir: &Path, request_path: &str) -> Result<PathBuf> {
    let request_path_str = request_path.trim_start_matches('/');

    let data_dir_str = data_dir
        .to_str()
        .ok_or_else(|| HarDataError::FileOperation("Invalid UTF-8 in data_dir".to_string()))?;
    let data_dir_normalized = data_dir_str.trim_start_matches("./");

    let relative_path = if request_path_str.starts_with(data_dir_str)
        || request_path_str.starts_with(data_dir_normalized)
    {
        request_path_str
            .trim_start_matches(data_dir_str)
            .trim_start_matches(data_dir_normalized)
            .trim_start_matches('/')
    } else {
        request_path_str
    };

    let target_path = data_dir.join(relative_path);

    let canonical_data = data_dir.canonicalize().map_err(|e| {
        HarDataError::FileOperation(format!("Failed to canonicalize data dir: {}", e))
    })?;

    let canonical_target =
        if target_path.exists() {
            target_path.canonicalize().map_err(|e| {
                HarDataError::FileOperation(format!("Failed to canonicalize target path: {}", e))
            })?
        } else {
            let parent = target_path.parent().ok_or_else(|| {
                HarDataError::FileOperation("Invalid path: no parent".to_string())
            })?;

            if !parent.exists() {
                return Err(HarDataError::FileOperation(format!(
                    "Parent directory does not exist: {:?}",
                    parent
                )));
            }

            let canonical_parent = parent.canonicalize().map_err(|e| {
                HarDataError::FileOperation(format!("Failed to canonicalize parent: {}", e))
            })?;

            canonical_parent.join(target_path.file_name().ok_or_else(|| {
                HarDataError::FileOperation("Invalid path: no filename".to_string())
            })?)
        };

    if !canonical_target.starts_with(&canonical_data) {
        return Err(HarDataError::FileOperation(format!(
            "Path traversal detected: {:?} is outside data_dir {:?}",
            canonical_target, canonical_data
        )));
    }

    Ok(canonical_target)
}
