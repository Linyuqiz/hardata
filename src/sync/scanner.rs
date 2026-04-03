use crate::util::error::Result;
use crate::util::time::{metadata_ctime_nanos, metadata_inode, metadata_mtime_nanos};
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub path: PathBuf,
    pub size: u64,
    pub modified: i64,
    pub change_time: Option<i64>,
    pub inode: Option<u64>,
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
            let metadata = match fs::symlink_metadata(path).await {
                Ok(metadata) => metadata,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    warn!("Path does not exist: {:?}", path);
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };

            if metadata.file_type().is_symlink() {
                if !self.should_include(path) {
                    return Ok(());
                }

                let modified = metadata_mtime_nanos(&metadata);
                let symlink_target = fs::read_link(path)
                    .await
                    .ok()
                    .map(|target| target.to_string_lossy().to_string());

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
                    change_time: metadata_ctime_nanos(&metadata),
                    inode: metadata_inode(&metadata),
                    is_dir: false,
                    mode,
                    is_symlink: true,
                    symlink_target,
                });
                return Ok(());
            }

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

                let modified = metadata_mtime_nanos(&metadata);

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
                    change_time: metadata_ctime_nanos(&metadata),
                    inode: metadata_inode(&metadata),
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

#[cfg(test)]
mod tests {
    use super::FileScanner;
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("hardata-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_scanner_keeps_symlink_entries_but_skips_descending_into_them() {
        let root = temp_dir("scanner-root");
        let local_file = root.join("local.txt");
        let external = temp_dir("scanner-external");
        let external_file = external.join("secret.txt");
        let linked_dir = root.join("linked");

        std::fs::write(&local_file, b"local").unwrap();
        std::fs::write(&external_file, b"external").unwrap();
        std::os::unix::fs::symlink(&external, &linked_dir).unwrap();

        let scanner = FileScanner::new(root.to_str().unwrap(), Vec::new(), Vec::new()).unwrap();
        let files = scanner.scan().await.unwrap();
        let file_set: HashSet<PathBuf> = files.into_iter().map(|f| f.path).collect();

        assert!(file_set.contains(&local_file));
        assert!(file_set.contains(&linked_dir));
        assert!(!file_set.contains(&linked_dir.join("secret.txt")));

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(external).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_scanner_includes_symlinked_files() {
        let root = temp_dir("scanner-symlink-file");
        let target = root.join("target.txt");
        let link = root.join("link.txt");
        std::fs::write(&target, b"target").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let scanner = FileScanner::new(root.to_str().unwrap(), Vec::new(), Vec::new()).unwrap();
        let files = scanner.scan().await.unwrap();
        let link_entry = files
            .into_iter()
            .find(|file| file.path == link)
            .expect("symlink file should be scanned");

        assert!(link_entry.is_symlink);
        assert_eq!(
            link_entry.symlink_target.as_deref(),
            Some(target.to_str().unwrap())
        );

        std::fs::remove_dir_all(root).unwrap();
    }
}
