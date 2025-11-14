use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileType {
    Log,
    StructuredText,
    Code,
    PlainText,
    Database,
    Config,
    Image,
    Video,
    Audio,
    Archive,
    Executable,
    SimulationResult,
    ScientificData,
    Unknown,
}

impl FileType {
    pub fn should_compress(&self) -> bool {
        !matches!(
            self,
            FileType::Image | FileType::Video | FileType::Audio | FileType::Archive
        )
    }

    pub fn recommended_algorithm(&self) -> &'static str {
        match self {
            FileType::Log | FileType::Database => "lz4",
            FileType::StructuredText | FileType::Config => "brotli",
            FileType::SimulationResult | FileType::ScientificData => "zstd",
            FileType::Code | FileType::PlainText | FileType::Executable | FileType::Unknown => {
                "zstd"
            }
            FileType::Image | FileType::Video | FileType::Audio | FileType::Archive => "none",
        }
    }

    pub fn expected_compression_ratio(&self) -> f64 {
        match self {
            FileType::Log => 0.15,
            FileType::StructuredText => 0.20,
            FileType::Config => 0.25,
            FileType::Code => 0.30,
            FileType::PlainText => 0.35,
            FileType::Database => 0.50,
            FileType::Executable => 0.60,
            FileType::SimulationResult => 0.70,
            FileType::ScientificData => 0.65,
            FileType::Unknown => 0.70,
            _ => 1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressionDecision {
    pub should_compress: bool,
    pub file_type: FileType,
    pub estimated_ratio: f64,
    pub reason: String,
}

#[derive(Clone)]
pub struct CompressionStrategy {
    pub min_file_size: u64,
    pub max_file_size: u64,
    pub smart_detection: bool,
}

impl Default for CompressionStrategy {
    fn default() -> Self {
        Self {
            min_file_size: 1024,
            max_file_size: 100 * 1024 * 1024,
            smart_detection: true,
        }
    }
}

impl CompressionStrategy {
    pub fn should_compress(
        &self,
        path: &Path,
        file_size: u64,
        data_sample: Option<&[u8]>,
    ) -> CompressionDecision {
        use super::detect::FileTypeDetector;

        if file_size < self.min_file_size {
            return CompressionDecision {
                should_compress: false,
                file_type: FileType::Unknown,
                estimated_ratio: 1.0,
                reason: format!(
                    "File too small: {} bytes < {} bytes",
                    file_size, self.min_file_size
                ),
            };
        }

        if file_size > self.max_file_size {
            return CompressionDecision {
                should_compress: false,
                file_type: FileType::Unknown,
                estimated_ratio: 1.0,
                reason: format!(
                    "File too large: {} bytes > {} bytes",
                    file_size, self.max_file_size
                ),
            };
        }

        let file_type = if self.smart_detection {
            FileTypeDetector::detect(path, data_sample)
        } else {
            FileTypeDetector::detect_by_extension(path)
        };

        let should_compress = file_type.should_compress();
        let estimated_ratio = file_type.expected_compression_ratio();

        CompressionDecision {
            should_compress,
            file_type,
            estimated_ratio,
            reason: if should_compress {
                format!("File type {:?} is compressible", file_type)
            } else {
                format!("File type {:?} is already compressed", file_type)
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum CompressionLevel {
    Fastest,
    #[default]
    Balanced,
    Best,
    Custom(i32),
}

impl CompressionLevel {
    pub fn to_zstd_level(&self) -> i32 {
        match self {
            Self::Fastest => 1,
            Self::Balanced => 3,
            Self::Best => 10,
            Self::Custom(level) => (*level).clamp(1, 22),
        }
    }

    pub fn to_brotli_level(&self) -> u32 {
        match self {
            Self::Fastest => 4,
            Self::Balanced => 6,
            Self::Best => 11,
            Self::Custom(level) => (*level).clamp(0, 11) as u32,
        }
    }

    pub fn to_lz4_level(&self) -> u32 {
        match self {
            Self::Fastest => 1,
            Self::Balanced => 6,
            Self::Best => 12,
            Self::Custom(level) => (*level).clamp(1, 16) as u32,
        }
    }
}
