use crate::util::error::Result;
use std::path::Path;

use super::algorithms::{compress_with_algorithm, decompress_with_algorithm};
use super::types::{CompressionLevel, CompressionStrategy};

pub fn smart_compress(
    data: &[u8],
    file_path: &Path,
    level: CompressionLevel,
) -> Result<(Vec<u8>, String)> {
    let strategy = CompressionStrategy::default();
    let decision = strategy.should_compress(file_path, data.len() as u64, Some(data));

    if decision.should_compress {
        let algorithm = decision.file_type.recommended_algorithm();

        if algorithm == "none" {
            return Ok((data.to_vec(), "none".to_string()));
        }

        let compressed = compress_with_algorithm(data, algorithm, level)?;

        if compressed.len() < data.len() {
            Ok((compressed, algorithm.to_string()))
        } else {
            Ok((data.to_vec(), "none".to_string()))
        }
    } else {
        Ok((data.to_vec(), "none".to_string()))
    }
}

pub fn smart_decompress(data: &[u8], algorithm: &str) -> Result<Vec<u8>> {
    decompress_with_algorithm(data, algorithm)
}
