use std::path::Path;

use super::types::FileType;

pub struct FileTypeDetector;

impl FileTypeDetector {
    pub fn detect_by_extension(path: &Path) -> FileType {
        let extension = path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_lowercase())
            .unwrap_or_default();

        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_lowercase())
            .unwrap_or_default();

        if filename.starts_with("d3plot")
            || filename.starts_with("d3dump")
            || filename.starts_with("d3thdt")
            || filename.starts_with("d3eigv")
            || filename.starts_with("binout")
        {
            return FileType::SimulationResult;
        }

        match extension.as_str() {
            "log" => FileType::Log,
            "json" | "xml" | "html" | "htm" | "svg" => FileType::StructuredText,
            "yaml" | "yml" | "toml" | "conf" | "config" | "ini" => FileType::Config,
            "rs" | "go" | "java" | "c" | "cpp" | "h" | "hpp" | "py" | "js" | "ts" | "jsx"
            | "tsx" | "php" | "rb" | "sh" | "bash" | "ps1" | "sql" | "css" | "scss" | "less" => {
                FileType::Code
            }
            "txt" | "md" | "csv" | "tsv" | "rtf" => FileType::PlainText,
            "db" | "sqlite" | "sqlite3" | "mdb" => FileType::Database,
            "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" | "ico" | "tiff" | "tif" => {
                FileType::Image
            }
            "mp4" | "avi" | "mkv" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "mpeg" | "mpg" => {
                FileType::Video
            }
            "mp3" | "aac" | "ogg" | "wav" | "flac" | "m4a" | "wma" => FileType::Audio,
            "zip" | "gz" | "tar" | "bz2" | "xz" | "7z" | "rar" | "zst" | "lz4" => FileType::Archive,
            "exe" | "dll" | "so" | "dylib" | "bin" => FileType::Executable,
            "cas" | "dat" | "msh" => FileType::SimulationResult,
            "odb" | "fil" | "inp" => FileType::SimulationResult,
            "foam" => FileType::SimulationResult,
            "h5" | "hdf5" | "hdf" | "nc" | "nc4" => FileType::ScientificData,
            "vtk" | "vtu" | "vtp" | "pvd" => FileType::ScientificData,
            "plt" | "szplt" => FileType::SimulationResult,
            "bdf" | "op2" | "f06" => FileType::SimulationResult,
            _ => FileType::Unknown,
        }
    }

    pub fn detect_by_magic(data: &[u8]) -> FileType {
        if data.len() < 4 {
            return FileType::Unknown;
        }

        match &data[0..4] {
            [0x89, 0x50, 0x4E, 0x47] => FileType::Image,
            [0xFF, 0xD8, 0xFF, ..] => FileType::Image,
            [0x47, 0x49, 0x46, 0x38] => FileType::Image,
            [0x50, 0x4B, 0x03, 0x04] => FileType::Archive,
            [0x1F, 0x8B, ..] => FileType::Archive,
            [0x42, 0x5A, 0x68, ..] => FileType::Archive,
            _ => FileType::Unknown,
        }
    }

    pub fn detect(path: &Path, data_sample: Option<&[u8]>) -> FileType {
        let ext_type = Self::detect_by_extension(path);

        if ext_type != FileType::Unknown {
            return ext_type;
        }

        if let Some(data) = data_sample {
            return Self::detect_by_magic(data);
        }

        FileType::Unknown
    }
}
