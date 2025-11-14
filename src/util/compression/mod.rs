mod algorithms;
mod detect;
mod smart;
mod types;

pub use algorithms::{
    compress_with_algorithm, compress_with_brotli, compress_with_lz4, compress_with_zstd,
    decompress_with_algorithm, decompress_with_brotli, decompress_with_lz4, decompress_with_zstd,
};
pub use detect::FileTypeDetector;
pub use smart::{smart_compress, smart_decompress};
pub use types::{CompressionDecision, CompressionLevel, CompressionStrategy, FileType};
