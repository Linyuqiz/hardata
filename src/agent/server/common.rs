use crate::util::error::{HarDataError, Result};
use std::path::{Path, PathBuf};
use tracing::warn;

pub const MAX_PAYLOAD_SIZE: usize = crate::core::constants::MAX_PROTOCOL_PAYLOAD_SIZE;
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
    if request_path.is_empty() {
        return Err(HarDataError::FileOperation(
            "Invalid path: empty request path".to_string(),
        ));
    }

    let raw_path = Path::new(request_path);
    let target_path = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        let relative_path = strip_relative_data_dir_prefix(data_dir, request_path)?;
        data_dir.join(relative_path)
    };

    let canonical_target = canonicalize_request_path(&target_path)?;
    let canonical_data = data_dir.canonicalize().map_err(|e| {
        HarDataError::FileOperation(format!("Failed to canonicalize data dir: {}", e))
    })?;
    if !canonical_target.starts_with(&canonical_data) {
        return Err(HarDataError::FileOperation(format!(
            "Path traversal detected: {:?} is outside data_dir {:?}",
            canonical_target, canonical_data
        )));
    }

    Ok(canonical_target)
}

fn strip_relative_data_dir_prefix<'a>(data_dir: &Path, request_path: &'a str) -> Result<&'a str> {
    let data_dir_str = data_dir
        .to_str()
        .ok_or_else(|| HarDataError::FileOperation("Invalid UTF-8 in data_dir".to_string()))?;
    let data_dir_normalized = data_dir_str.trim_start_matches("./");

    if let Some(relative_path) = strip_path_prefix(request_path, data_dir_str) {
        return Ok(relative_path);
    }
    if let Some(relative_path) = strip_path_prefix(request_path, data_dir_normalized) {
        return Ok(relative_path);
    }

    Ok(request_path)
}

fn strip_path_prefix<'a>(request_path: &'a str, prefix: &str) -> Option<&'a str> {
    if prefix.is_empty() {
        return None;
    }
    if request_path == prefix {
        return Some("");
    }

    request_path
        .strip_prefix(prefix)
        .and_then(|remaining| remaining.strip_prefix('/'))
}

fn canonicalize_request_path(target_path: &Path) -> Result<PathBuf> {
    if target_path.exists() && target_path.file_name().is_none() {
        return target_path.canonicalize().map_err(|e| {
            HarDataError::FileOperation(format!("Failed to canonicalize target path: {}", e))
        });
    }

    let parent = target_path
        .parent()
        .ok_or_else(|| HarDataError::FileOperation("Invalid path: no parent".to_string()))?;
    if !parent.exists() {
        return Err(HarDataError::FileOperation(format!(
            "Parent directory does not exist: {:?}",
            parent
        )));
    }

    let canonical_parent = parent.canonicalize().map_err(|e| {
        HarDataError::FileOperation(format!("Failed to canonicalize parent: {}", e))
    })?;
    let file_name = target_path
        .file_name()
        .ok_or_else(|| HarDataError::FileOperation("Invalid path: no filename".to_string()))?;
    Ok(canonical_parent.join(file_name))
}

#[cfg(test)]
mod tests {
    use super::resolve_request_path;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn resolve_request_path_supports_relative_paths_under_data_dir() {
        let root = create_temp_dir("relative");
        let file_path = root.join("nested/file.txt");
        fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        fs::write(&file_path, b"ok").unwrap();

        let resolved = resolve_request_path(&root, "nested/file.txt").unwrap();
        assert_eq!(resolved, file_path.canonicalize().unwrap());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_request_path_supports_absolute_paths() {
        let root = create_temp_dir("absolute-root");
        let file_path = root.join("nested/payload.bin");
        fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        fs::write(&file_path, b"ok").unwrap();

        let resolved = resolve_request_path(&root, file_path.to_str().unwrap()).unwrap();
        assert_eq!(resolved, file_path.canonicalize().unwrap());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_request_path_rejects_absolute_paths_outside_data_dir() {
        let root = create_temp_dir("absolute-root");
        let external = create_temp_dir("absolute-outside");
        let file_path = external.join("payload.bin");
        fs::write(&file_path, b"ok").unwrap();

        let err = resolve_request_path(&root, file_path.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("Path traversal detected"));

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(external);
    }

    #[test]
    fn resolve_request_path_preserves_symlink_leaf_inside_data_dir() {
        let root = create_temp_dir("symlink-leaf");
        let target = root.join("target.txt");
        let link = root.join("link.txt");
        fs::write(&target, b"ok").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let resolved = resolve_request_path(&root, "link.txt").unwrap();
        assert_eq!(
            resolved.file_name().and_then(|n| n.to_str()),
            Some("link.txt")
        );
        assert_eq!(resolved.parent().unwrap(), root.canonicalize().unwrap());
        assert!(fs::symlink_metadata(&resolved)
            .unwrap()
            .file_type()
            .is_symlink());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_request_path_rejects_relative_traversal_outside_data_dir() {
        let base = create_temp_dir("traversal");
        let root = base.join("data");
        let outside = base.join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("leak.txt"), b"secret").unwrap();

        let err = resolve_request_path(&root, "../outside/leak.txt").unwrap_err();
        assert!(err.to_string().contains("Path traversal detected"));

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn resolve_request_path_accepts_relative_request_with_data_dir_prefix() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let current_dir = std::env::current_dir().unwrap();
        let relative_root = PathBuf::from(format!(".tmp-hardata-prefix-{unique}"));
        let relative_data_dir = relative_root.join("data/agent");
        let dotted_data_dir = PathBuf::from(format!("./{}", relative_data_dir.display()));
        let absolute_root = current_dir.join(&relative_root);
        let absolute_data_dir = current_dir.join(&relative_data_dir);
        let file_path = absolute_data_dir.join("foo.txt");

        fs::create_dir_all(&absolute_data_dir).unwrap();
        fs::write(&file_path, b"ok").unwrap();

        let dotted_request = format!("{}/foo.txt", dotted_data_dir.display());
        let normalized_request = format!("{}/foo.txt", relative_data_dir.display());

        let dotted_resolved = resolve_request_path(&dotted_data_dir, &dotted_request).unwrap();
        let normalized_resolved =
            resolve_request_path(&dotted_data_dir, &normalized_request).unwrap();

        assert_eq!(dotted_resolved, file_path.canonicalize().unwrap());
        assert_eq!(normalized_resolved, file_path.canonicalize().unwrap());

        let _ = fs::remove_dir_all(absolute_root);
    }
}
