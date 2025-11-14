use crate::util::error::{HarDataError, Result};
use crate::util::zero_copy::FileOperator;
use std::path::Path;
use std::sync::OnceLock;
use tokio::fs;
use tracing::debug;

static FILE_OPERATOR: OnceLock<FileOperator> = OnceLock::new();

fn get_file_operator() -> &'static FileOperator {
    FILE_OPERATOR.get_or_init(FileOperator::new)
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
