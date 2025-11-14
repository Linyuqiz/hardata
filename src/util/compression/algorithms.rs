use crate::util::error::{HarDataError, Result};

use super::types::CompressionLevel;

pub fn compress_with_zstd(data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
    let zstd_level = level.to_zstd_level();
    zstd::encode_all(data, zstd_level)
        .map_err(|e| HarDataError::Compression(format!("zstd compression failed: {}", e)))
}

pub fn decompress_with_zstd(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data)
        .map_err(|e| HarDataError::Compression(format!("zstd decompression failed: {}", e)))
}

pub fn compress_with_lz4(data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
    let lz4_level = level.to_lz4_level();
    let estimated_size = ((data.len() as f64 * 0.6) as usize).max(1024);
    let mut compressed = Vec::with_capacity(estimated_size);
    let mut encoder = lz4::EncoderBuilder::new()
        .level(lz4_level)
        .build(&mut compressed)
        .map_err(|e| HarDataError::Compression(format!("lz4 encoder creation failed: {}", e)))?;

    std::io::copy(&mut std::io::Cursor::new(data), &mut encoder)
        .map_err(|e| HarDataError::Compression(format!("lz4 compression failed: {}", e)))?;

    let (_output, result) = encoder.finish();
    result.map_err(|e| HarDataError::Compression(format!("lz4 finish failed: {}", e)))?;

    Ok(compressed)
}

pub fn decompress_with_lz4(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = lz4::Decoder::new(std::io::Cursor::new(data))
        .map_err(|e| HarDataError::Compression(format!("lz4 decoder creation failed: {}", e)))?;
    let estimated_size = (data.len() * 3).max(4096);
    let mut decompressed = Vec::with_capacity(estimated_size);
    std::io::copy(&mut decoder, &mut decompressed)
        .map_err(|e| HarDataError::Compression(format!("lz4 decompression failed: {}", e)))?;

    Ok(decompressed)
}

pub fn compress_with_brotli(data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
    let brotli_level = level.to_brotli_level();
    let estimated_size = ((data.len() as f64 * 0.3) as usize).max(1024);
    let mut compressed = Vec::with_capacity(estimated_size);
    let mut compressor =
        brotli::CompressorReader::new(std::io::Cursor::new(data), 4096, brotli_level, 22);

    std::io::copy(&mut compressor, &mut compressed)
        .map_err(|e| HarDataError::Compression(format!("brotli compression failed: {}", e)))?;

    Ok(compressed)
}

pub fn decompress_with_brotli(data: &[u8]) -> Result<Vec<u8>> {
    let estimated_size = (data.len() * 4).max(4096);
    let mut decompressed = Vec::with_capacity(estimated_size);
    let mut decompressor = brotli::Decompressor::new(std::io::Cursor::new(data), 4096);

    std::io::copy(&mut decompressor, &mut decompressed)
        .map_err(|e| HarDataError::Compression(format!("brotli decompression failed: {}", e)))?;

    Ok(decompressed)
}

pub fn compress_with_algorithm(
    data: &[u8],
    algorithm: &str,
    level: CompressionLevel,
) -> Result<Vec<u8>> {
    match algorithm {
        "zstd" => compress_with_zstd(data, level),
        "lz4" => compress_with_lz4(data, level),
        "brotli" => compress_with_brotli(data, level),
        "none" => Ok(data.to_vec()),
        _ => Err(HarDataError::Compression(format!(
            "Unknown compression algorithm: {}",
            algorithm
        ))),
    }
}

pub fn decompress_with_algorithm(data: &[u8], algorithm: &str) -> Result<Vec<u8>> {
    match algorithm {
        "zstd" => decompress_with_zstd(data),
        "lz4" => decompress_with_lz4(data),
        "brotli" => decompress_with_brotli(data),
        "none" => Ok(data.to_vec()),
        _ => Err(HarDataError::Compression(format!(
            "Unknown compression algorithm: {}",
            algorithm
        ))),
    }
}
