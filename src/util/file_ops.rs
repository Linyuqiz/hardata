use crate::util::error::{HarDataError, Result};
use crate::util::time::{metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos};
use crate::util::zero_copy::FileOperator;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::OnceLock;
use tokio::fs;
use tracing::debug;

static FILE_OPERATOR: OnceLock<FileOperator> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegularFileVersion {
    pub size: u64,
    pub modified: i64,
    pub change_time: Option<i64>,
    pub inode: Option<u64>,
}

fn get_file_operator() -> &'static FileOperator {
    FILE_OPERATOR.get_or_init(FileOperator::new)
}

pub fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let mut read_total = 0usize;

    while read_total < buf.len() {
        let bytes_read = read_once_at(file, offset + read_total as u64, &mut buf[read_total..])?;

        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Reached EOF while reading {} bytes at offset {}",
                    buf.len(),
                    offset
                ),
            ));
        }

        read_total += bytes_read;
    }

    Ok(())
}

pub fn read_file_range_from(file: &File, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0u8; length];
    read_exact_at(file, offset, &mut buffer)?;
    Ok(buffer)
}

#[cfg(unix)]
fn read_once_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_once_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn read_once_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};

    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.read(buf)
}

pub async fn read_file_range(path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    debug!(
        "Reading file: {} (offset: {}, length: {})",
        path, offset, length
    );

    let path = Path::new(path);

    let metadata = fs::metadata(path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            HarDataError::FileOperation(format!("File not found: {:?}", path))
        } else {
            HarDataError::from(e)
        }
    })?;

    let file_size = metadata.len();

    if offset >= file_size {
        return Err(HarDataError::FileOperation(format!(
            "Offset {} exceeds file size {}",
            offset, file_size
        )));
    }

    let actual_length = std::cmp::min(length, file_size - offset) as usize;

    let file_operator = get_file_operator();
    let buffer = file_operator
        .read_at(path, offset, actual_length)
        .await
        .map_err(|e| HarDataError::FileOperation(format!("Zero-copy read failed: {}", e)))?;

    Ok(buffer)
}

pub async fn write_file_range(path: &str, offset: u64, data: &[u8]) -> Result<()> {
    let path = Path::new(path);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let file_operator = get_file_operator();
    file_operator
        .write_at(path, offset, data)
        .await
        .map_err(|e| HarDataError::FileOperation(format!("Zero-copy write failed: {}", e)))?;

    Ok(())
}

pub async fn load_regular_file_version(path: &Path) -> Result<Option<RegularFileVersion>> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Ok(Some(RegularFileVersion {
                size: metadata.len(),
                modified: metadata_mtime_nanos(&metadata),
                change_time: metadata_ctime_nanos(&metadata),
                inode: metadata_inode(&metadata),
            }))
        }
        Ok(_) => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(HarDataError::FileOperation(format!(
            "Failed to inspect file version '{}': {}",
            path.display(),
            e
        ))),
    }
}

pub async fn sync_file_data(path: &str) -> Result<()> {
    let file = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .map_err(|e| {
            HarDataError::FileOperation(format!("Failed to open file for sync '{}': {}", path, e))
        })?;

    file.sync_all().await.map_err(|e| {
        HarDataError::FileOperation(format!("Failed to sync file '{}': {}", path, e))
    })?;

    Ok(())
}

#[cfg(unix)]
pub async fn sync_parent_directory(path: &str) -> Result<()> {
    let parent = Path::new(path).parent().ok_or_else(|| {
        HarDataError::FileOperation(format!("Failed to resolve parent directory for '{}'", path))
    })?;
    let parent = parent.to_path_buf();
    let display = parent.display().to_string();

    tokio::task::spawn_blocking(move || -> Result<()> {
        let dir = File::open(&parent).map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to open parent directory '{}' for sync: {}",
                display, e
            ))
        })?;
        dir.sync_all().map_err(|e| {
            HarDataError::FileOperation(format!(
                "Failed to sync parent directory '{}': {}",
                display, e
            ))
        })?;
        Ok(())
    })
    .await
    .map_err(|e| HarDataError::FileOperation(format!("Failed to join parent sync task: {}", e)))?
}

#[cfg(not(unix))]
pub async fn sync_parent_directory(path: &str) -> Result<()> {
    let _ = path;
    Ok(())
}
