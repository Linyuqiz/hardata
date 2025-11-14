use crate::util::error::Result;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub path: PathBuf,
    pub size: u64,
    pub modified: i64,
    pub is_dir: bool,
    pub mode: u32,
    pub is_symlink: bool,
    pub symlink_target: Option<String>,
}

pub struct FileScanner {
    root: PathBuf,
    exclude_regex: Vec<regex::Regex>,
    include_regex: Vec<regex::Regex>,
}

impl FileScanner {
    pub fn new(root: &str, exclude: Vec<String>, include: Vec<String>) -> Result<Self> {
        let root = PathBuf::from(root);

        let exclude_regex: Result<Vec<_>> = exclude
            .into_iter()
            .map(|pattern| {
                regex::Regex::new(&pattern).map_err(|e| {
                    crate::util::error::HarDataError::InvalidConfig(format!(
                        "Invalid exclude regex: {}",
                        e
                    ))
                })
            })
            .collect();

        let include_regex: Result<Vec<_>> = include
            .into_iter()
            .map(|pattern| {
                regex::Regex::new(&pattern).map_err(|e| {
                    crate::util::error::HarDataError::InvalidConfig(format!(
                        "Invalid include regex: {}",
                        e
                    ))
                })
            })
            .collect();

        Ok(Self {
            root,
            exclude_regex: exclude_regex?,
            include_regex: include_regex?,
        })
    }

    pub async fn scan(&self) -> Result<Vec<ScannedFile>> {
        debug!("Scanning: {:?}", self.root);

        let mut files = Vec::new();
        self.scan_recursive(&self.root, &mut files).await?;

        debug!("Scanned {} files", files.len());
        Ok(files)
    }

    fn scan_recursive<'a>(
        &'a self,
        path: &'a Path,
        files: &'a mut Vec<ScannedFile>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if !path.exists() {
                warn!("Path does not exist: {:?}", path);
                return Ok(());
            }

            let metadata = fs::metadata(path).await?;

            if metadata.is_dir() {
                let mut read_dir = fs::read_dir(path).await?;

                while let Some(entry) = read_dir.next_entry().await? {
                    let entry_path = entry.path();

                    if self.should_exclude(&entry_path) {
                        continue;
                    }

                    self.scan_recursive(&entry_path, files).await?;
                }
            } else {
                if !self.should_include(path) {
                    return Ok(());
                }

                let modified = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);

                #[cfg(unix)]
                let mode = {
                    use std::os::unix::fs::MetadataExt;
                    metadata.mode()
                };
                #[cfg(not(unix))]
                let mode = 0u32;

                files.push(ScannedFile {
                    path: path.to_path_buf(),
                    size: metadata.len(),
                    modified,
                    is_dir: false,
                    mode,
                    is_symlink: false,
                    symlink_target: None,
                });
            }

            Ok(())
        })
    }

    fn should_exclude(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();

        for regex in &self.exclude_regex {
            if regex.is_match(&path_str) {
                return true;
            }
        }

        false
    }

    fn should_include(&self, path: &Path) -> bool {
        if self.include_regex.is_empty() {
            return true;
        }

        let path_str = path.to_string_lossy();

        for regex in &self.include_regex {
            if regex.is_match(&path_str) {
                return true;
            }
        }

        false
    }
}
